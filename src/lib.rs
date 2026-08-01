#![allow(dead_code)]
//! YouTube download via yt-dlp. Uses yt-dlp.exe in the same folder as downloader.exe.

use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::debug;
use once_cell::sync::Lazy;
use serde::Deserialize;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use reqwest::Client;
use rusty_ytdl::{Video, VideoFormat, VideoInfo};

/// Raw format from yt-dlp -j output.
#[derive(Deserialize)]
struct YtdlpInfo {
    formats: Option<Vec<YtdlpFormat>>,
    subtitles: Option<HashMap<String, Vec<YtdlpSub>>>,
    automatic_captions: Option<HashMap<String, Vec<YtdlpSub>>>,
    /// The video's primary/original language (may be null).
    language: Option<String>,
}

#[derive(Deserialize)]
struct YtdlpFormat {
    format_id: Option<String>,
    ext: Option<String>,
    height: Option<u32>,
    tbr: Option<f64>,
    vcodec: Option<String>,
    acodec: Option<String>,
}

#[derive(Deserialize)]
struct YtdlpSub {
    ext: Option<String>,
    name: Option<String>,
}

/// One available subtitle/caption track for a video.
#[derive(Debug, Clone)]
pub struct SubtitleTrack {
    pub lang: String,
    pub name: String,
    /// "manual" (uploaded by author) or "auto" (auto-generated / auto-translated).
    pub kind: &'static str,
    pub exts: Vec<String>,
}

/// Stream info derived from yt-dlp.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamInfo {
    pub itag: Option<u64>,
    pub container: String,
    pub quality: String,
    pub bitrate: u64,
    #[serde(rename = "type")]
    pub stream_type: String,
    pub has_video: bool,
    pub has_audio: bool,
}

static DOWNLOAD_QUEUE: Lazy<Arc<Mutex<()>>> = Lazy::new(|| Arc::new(Mutex::new(())));

/// High-level wrapper that shells out to yt-dlp.exe (same folder as downloader.exe).
#[derive(Clone)]
pub struct YoutubeDownloader {
    download_dir: PathBuf,
    ytdlp_path: PathBuf,
}

impl YoutubeDownloader {
    /// Create a downloader. Requires yt-dlp.exe in the same folder as downloader.exe or in cwd.
    pub fn new() -> Result<Self, YtdlError> {
        let download_dir = PathBuf::from("downloaded");
        std::fs::create_dir_all(&download_dir)?;

        let ytdlp_path = resolve_ytdlp_exe_path()
            .ok_or_else(|| YtdlError::YtdlpNotFound(PathBuf::from("yt-dlp.exe")))?;
        log::info!("yt-dlp: using {:?}", ytdlp_path);

        Ok(Self {
            download_dir,
            ytdlp_path,
        })
    }

    pub async fn fetch_info(&self, input: &str) -> Result<VideoInfo, YtdlError> {
        let normalized = normalise_input(input);
        let video = Video::new(&normalized)?;
        let info = video.get_info().await?;
        Ok(info)
    }

    pub async fn download_format_to(
        &self,
        input: &str,
        itag: u64,
        overwrite: bool,
        progress: Option<UnboundedSender<u8>>,
    ) -> Result<(PathBuf, VideoFormat, VideoInfo), YtdlError> {
        let normalized = normalise_input(input);
        let video = Video::new(&normalized)?;
        let info = video.get_info().await?;

        let format = info
            .formats
            .iter()
            .find(|fmt| fmt.itag == itag)
            .cloned()
            .ok_or(YtdlError::FormatNotFound(itag))?;

        std::fs::create_dir_all(&self.download_dir)?;

        let video_id = info.video_details.video_id.clone();
        let extension = extension_from_format(&format);
        let base_name = sanitize_component(&format!("{video_id}_itag{itag}"));
        let output_path = self.download_dir.join(format!("{base_name}.{extension}"));
        let absolute_output = if output_path.is_absolute() {
            output_path.clone()
        } else {
            env::current_dir()?.join(&output_path)
        };

        if output_path.exists() && !overwrite {
            return Ok((output_path, format, info));
        }
        if output_path.exists() {
            let _ = std::fs::remove_file(&output_path);
        }

        let queue_guard = DOWNLOAD_QUEUE.lock().await;

        let mut command = Command::new(&self.ytdlp_path);
        command
            .args(["-f", &itag.to_string(), "-o", absolute_output.to_string_lossy().as_ref(), "--newline", "--no-playlist", &normalized])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(parent) = self.ytdlp_path.parent() {
            command.current_dir(parent);
        }

        debug!("Invoking yt-dlp for itag {itag}: {:?}", command.as_std());

        let mut child = command.spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| YtdlError::CliSpawn("missing stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| YtdlError::CliSpawn("missing stderr".into()))?;

        // yt-dlp prints progress (`[download] 45.2%`) to stdout, so parse stdout for
        // percentages and drain stderr in the background — this both avoids a full-pipe
        // deadlock and keeps stderr text for error reporting.
        let stderr_task = drain_to_string(stderr);

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let progress_sender = progress.as_ref();

        loop {
            line.clear();
            let bytes = reader.read_line(&mut line).await?;
            if bytes == 0 {
                break;
            }
            if let Some(percent) = parse_ytdlp_pct(line.trim()) {
                if let Some(sender) = progress_sender {
                    let _ = sender.send(percent as u8);
                }
            }
        }

        let status = child.wait().await?;
        let stderr_str = stderr_task.await.unwrap_or_default().trim().to_string();
        drop(queue_guard);

        if !status.success() {
            return Err(YtdlError::YtdlpCliFailed {
                code: status.code(),
                message: stderr_str,
            });
        }
        if !absolute_output.exists() {
            return Err(YtdlError::MissingOutput(absolute_output));
        }
        if let Some(sender) = progress_sender {
            let _ = sender.send(100);
        }
        Ok((output_path, format, info))
    }

    pub async fn fetch_formats_via_cli(&self, input: &str) -> Result<Vec<StreamInfo>, YtdlError> {
        let normalized = normalise_input(input);
        self.run_ytdlp_info_json(&self.ytdlp_path, &normalized).await
    }

    /// List available subtitle/caption tracks (manual + auto) for a video.
    pub async fn fetch_subtitles(&self, input: &str) -> Result<Vec<SubtitleTrack>, YtdlError> {
        let normalized = normalise_input(input);
        let info = self.run_ytdlp_raw_info(&normalized).await?;

        // Only keep Russian, English, and the video's original language — drop the
        // hundreds of auto-translated languages YouTube otherwise offers.
        let mut allowed: HashSet<String> = HashSet::from(["ru".to_string(), "en".to_string()]);
        if let Some(orig) = info.language.as_deref() {
            let base = base_lang(orig);
            if !base.is_empty() {
                allowed.insert(base);
            }
        }

        let mut tracks: Vec<SubtitleTrack> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        if let Some(subs) = info.subtitles {
            for (lang, list) in subs {
                if !allowed.contains(&base_lang(&lang)) {
                    continue;
                }
                let name = list
                    .iter()
                    .find_map(|s| s.name.clone())
                    .unwrap_or_else(|| lang.clone());
                seen.insert(lang.clone());
                tracks.push(SubtitleTrack { lang, name, kind: "manual", exts: collect_exts(&list) });
            }
        }
        if let Some(autos) = info.automatic_captions {
            for (lang, list) in autos {
                if !allowed.contains(&base_lang(&lang)) || seen.contains(&lang) {
                    continue; // filtered out, or a manual track already covers it
                }
                let name = list
                    .iter()
                    .find_map(|s| s.name.clone())
                    .unwrap_or_else(|| lang.clone());
                tracks.push(SubtitleTrack { lang, name, kind: "auto", exts: collect_exts(&list) });
            }
        }

        // Manual first, then auto; alphabetical by language code within each group.
        tracks.sort_by(|a, b| {
            let ka = u8::from(a.kind == "auto");
            let kb = u8::from(b.kind == "auto");
            ka.cmp(&kb).then_with(|| a.lang.cmp(&b.lang))
        });
        Ok(tracks)
    }

    /// Download one subtitle track via yt-dlp into downloaded/. `format` is an ext
    /// (srt/vtt/ttml/srv1..3/json3/ass/lrc) or "txt" for plain stripped text.
    pub async fn download_subtitle(
        &self,
        input: &str,
        lang: &str,
        format: &str,
        auto: bool,
    ) -> Result<PathBuf, YtdlError> {
        let normalized = normalise_input(input);
        std::fs::create_dir_all(&self.download_dir)?;

        let before = list_dir_files(&self.download_dir);

        let want_txt = format.eq_ignore_ascii_case("txt");
        let dl_format = if want_txt { "srt" } else { format };
        let convertible = matches!(dl_format.to_lowercase().as_str(), "srt" | "vtt" | "ass" | "lrc");

        // Absolute output template so yt-dlp writes into downloaded/ regardless of cwd.
        let dir_abs = if self.download_dir.is_absolute() {
            self.download_dir.clone()
        } else {
            env::current_dir()?.join(&self.download_dir)
        };
        let out_template = dir_abs.join("%(id)s").to_string_lossy().to_string();

        let mut command = Command::new(&self.ytdlp_path);
        command.arg("--skip-download").arg("--no-playlist");
        if auto {
            command.arg("--write-auto-subs");
        } else {
            command.arg("--write-subs");
        }
        command.arg("--sub-langs").arg(lang);
        if convertible {
            command.arg("--sub-format").arg("vtt/ttml/srv3/srv2/srv1/best");
            command.arg("--convert-subs").arg(dl_format);
        } else {
            command.arg("--sub-format").arg(dl_format);
        }
        command.arg("-o").arg(&out_template).arg(&normalized);
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(parent) = self.ytdlp_path.parent() {
            command.current_dir(parent);
        }

        let queue_guard = DOWNLOAD_QUEUE.lock().await;
        let output = command.output().await?;
        drop(queue_guard);

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(YtdlError::YtdlpCliFailed {
                code: output.status.code(),
                message: if stderr.is_empty() {
                    "yt-dlp failed (no subtitles for this language/format?)".to_string()
                } else {
                    stderr
                },
            });
        }

        // Find the newly written subtitle file (named "<id>.<lang>.<ext>").
        let after = list_dir_files(&self.download_dir);
        let lang_marker = format!(".{}.", lang);
        let candidates: Vec<PathBuf> = after
            .difference(&before)
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map_or(false, |n| n.contains(&lang_marker))
            })
            .cloned()
            .collect();

        let want_ext = dl_format.to_lowercase();
        let sub_path = candidates
            .iter()
            .find(|p| {
                p.extension()
                    .and_then(|e| e.to_str())
                    .map_or(false, |e| e.eq_ignore_ascii_case(&want_ext))
            })
            .cloned()
            .or_else(|| candidates.into_iter().next())
            .ok_or_else(|| {
                YtdlError::CliDiscovery(
                    "yt-dlp produced no subtitle file (none available for this language/format)".to_string(),
                )
            })?;

        if want_txt {
            let content = std::fs::read_to_string(&sub_path)?;
            let text = subtitle_to_text(&content);
            let txt_path = sub_path.with_extension("txt");
            std::fs::write(&txt_path, text)?;
            let _ = std::fs::remove_file(&sub_path);
            return Ok(txt_path);
        }
        Ok(sub_path)
    }

    /// Run `yt-dlp -j` and parse the raw metadata (formats + subtitles).
    async fn run_ytdlp_raw_info(&self, normalized: &str) -> Result<YtdlpInfo, YtdlError> {
        let mut command = Command::new(&self.ytdlp_path);
        command
            .args(["-j", "--no-playlist", "--no-download", normalized])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(parent) = self.ytdlp_path.parent() {
            command.current_dir(parent);
        }
        let output = command.output().await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout_s = stdout.trim().to_string();
            let msg = if stderr.is_empty() && !stdout_s.is_empty() {
                format!("status {:?} stdout: {}", output.status.code(), stdout_s)
            } else if stderr.is_empty() {
                format!("status {:?} (no stderr)", output.status.code())
            } else {
                stderr
            };
            return Err(YtdlError::YtdlpCliFailed { code: output.status.code(), message: msg });
        }
        serde_json::from_str::<YtdlpInfo>(stdout.trim())
            .map_err(|e| YtdlError::CliDiscovery(format!("parse yt-dlp -j: {e}")))
    }

    async fn run_ytdlp_info_json(&self, ytdlp_exe: &Path, normalized: &str) -> Result<Vec<StreamInfo>, YtdlError> {
        let mut command = Command::new(ytdlp_exe);
        command
            .args(["-j", "--no-playlist", "--no-download", normalized])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(parent) = ytdlp_exe.parent() {
            command.current_dir(parent);
        }
        let output = command.output().await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout_s = stdout.trim().to_string();
            let msg = if stderr.is_empty() && !stdout_s.is_empty() {
                format!("status {:?} stdout: {}", output.status.code(), stdout_s)
            } else if stderr.is_empty() {
                format!("status {:?} (no stderr)", output.status.code())
            } else {
                stderr
            };
            return Err(YtdlError::YtdlpCliFailed {
                code: output.status.code(),
                message: msg,
            });
        }
        let info: YtdlpInfo = serde_json::from_str(stdout.trim())
            .map_err(|e| YtdlError::CliDiscovery(format!("parse yt-dlp -j: {e}")))?;
        let list: Vec<StreamInfo> = info
            .formats
            .unwrap_or_default()
            .into_iter()
            .filter_map(|f| {
                let itag = f.format_id.as_ref().and_then(|id| id.parse::<u64>().ok());
                let container = f.ext.as_deref().unwrap_or("unknown").to_string();
                let quality = format_quality_ytdlp(&f);
                let bitrate = (f.tbr.unwrap_or(0.0) * 1000.0) as u64;
                let (stream_type, has_video, has_audio) = stream_type_ytdlp(&f);
                Some(StreamInfo {
                    itag,
                    container,
                    quality,
                    bitrate,
                    stream_type,
                    has_video,
                    has_audio,
                })
            })
            .collect();
        Ok(list)
    }

    pub async fn download_itag_to(
        &self,
        input: &str,
        itag: u64,
        container: &str,
        video_id: &str,
        progress: Option<UnboundedSender<u8>>,
    ) -> Result<PathBuf, YtdlError> {
        self.run_ytdlp_download_itag(&self.ytdlp_path, input, itag, container, video_id, progress.as_ref()).await
    }

    async fn run_ytdlp_download_itag(
        &self,
        ytdlp_exe: &Path,
        input: &str,
        itag: u64,
        container: &str,
        video_id: &str,
        progress: Option<&UnboundedSender<u8>>,
    ) -> Result<PathBuf, YtdlError> {
        let normalized = normalise_input(input);
        std::fs::create_dir_all(&self.download_dir)?;
        let base_name = sanitize_component(&format!("{video_id}_itag{itag}"));
        let output_path = self.download_dir.join(format!("{base_name}.{container}"));
        let absolute_output = if output_path.is_absolute() {
            output_path.clone()
        } else {
            env::current_dir()?.join(&output_path)
        };

        let queue_guard = DOWNLOAD_QUEUE.lock().await;

        let mut command = Command::new(ytdlp_exe);
        command
            .args([
                "-f",
                &itag.to_string(),
                "-o",
                absolute_output.to_string_lossy().as_ref(),
                "--newline",
                "--no-playlist",
                &normalized,
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(parent) = ytdlp_exe.parent() {
            command.current_dir(parent);
        }

        let mut child = command.spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| YtdlError::CliSpawn("missing stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| YtdlError::CliSpawn("missing stderr".into()))?;

        // Progress lines arrive on stdout; drain stderr in the background for errors.
        let stderr_task = drain_to_string(stderr);

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes = reader.read_line(&mut line).await?;
            if bytes == 0 {
                break;
            }
            if let Some(percent) = parse_ytdlp_pct(line.trim()) {
                if let Some(sender) = progress {
                    let _ = sender.send(percent as u8);
                }
            }
        }

        let status = child.wait().await?;
        let stderr_str = stderr_task.await.unwrap_or_default();
        drop(queue_guard);

        if !status.success() {
            return Err(YtdlError::YtdlpCliFailed {
                code: status.code(),
                message: stderr_str.trim().to_string(),
            });
        }
        if !absolute_output.exists() {
            return Err(YtdlError::MissingOutput(absolute_output));
        }
        if let Some(sender) = progress {
            let _ = sender.send(100);
        }
        Ok(output_path)
    }

    pub fn download_dir(&self) -> &Path {
        &self.download_dir
    }
}

/// Ensure yt-dlp.exe exists in the same folder as downloader.exe. Downloads from GitHub releases if missing.
pub async fn ensure_ytdlp_available() -> Result<(), YtdlError> {
    if resolve_ytdlp_exe_path().is_some() {
        return Ok(());
    }
    let app_dir = resolve_app_dir()
        .ok_or_else(|| YtdlError::YtdlpNotFound(PathBuf::from("yt-dlp.exe")))?;
    let yt_dlp_exe = app_dir.join("yt-dlp.exe");
    log::info!("yt-dlp.exe not found, downloading from GitHub releases...");
    std::fs::create_dir_all(&app_dir)?;
    let client = Client::builder()
        .user_agent("yt-dlp-downloader/1.0")
        .build()
        .map_err(|e| YtdlError::CliDiscovery(format!("reqwest: {}", e)))?;
    let url = "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe";
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| YtdlError::CliDiscovery(format!("download request: {}", e)))?;
    if !resp.status().is_success() {
        return Err(YtdlError::CliDiscovery(format!(
            "download failed: HTTP {}",
            resp.status()
        )));
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| YtdlError::CliDiscovery(format!("download read: {}", e)))?;
    let mut file = tokio::fs::File::create(&yt_dlp_exe)
        .await
        .map_err(|e| YtdlError::Io(std::io::Error::from(e)))?;
    file.write_all(&bytes)
        .await
        .map_err(|e| YtdlError::Io(std::io::Error::from(e)))?;
    log::info!("Downloaded yt-dlp.exe to {:?}", yt_dlp_exe);
    Ok(())
}

/// Resolve the directory where yt-dlp.exe should live (same folder as downloader.exe or cwd).
fn resolve_app_dir() -> Option<PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            return Some(parent.to_path_buf());
        }
    }
    std::env::current_dir().ok()
}

/// Resolve yt-dlp.exe in the same folder as downloader.exe or in cwd.
pub fn resolve_ytdlp_exe_path() -> Option<PathBuf> {
    if let Ok(cwd) = std::env::current_dir() {
        let candidate = cwd.join("yt-dlp.exe");
        if candidate.exists() {
            return Some(candidate);
        }
    }
    let mut current = std::env::current_exe().ok()?;
    for _ in 0..10 {
        if current.is_file() {
            if let Some(parent) = current.parent() {
                current = parent.to_path_buf();
            }
        }
        let candidate = current.join("yt-dlp.exe");
        if candidate.exists() {
            return Some(candidate);
        }
        current = current.parent()?.to_path_buf();
    }
    None
}

/// Resolve deno.exe (JS runtime yt-dlp needs for some videos): cwd, the app dir,
/// next to yt-dlp.exe, or anywhere on PATH.
pub fn resolve_deno_exe_path() -> Option<PathBuf> {
    if let Ok(cwd) = std::env::current_dir() {
        let c = cwd.join("deno.exe");
        if c.is_file() {
            return Some(c);
        }
    }
    if let Some(dir) = resolve_app_dir() {
        let c = dir.join("deno.exe");
        if c.is_file() {
            return Some(c);
        }
    }
    if let Some(yt) = resolve_ytdlp_exe_path() {
        if let Some(parent) = yt.parent() {
            let c = parent.join("deno.exe");
            if c.is_file() {
                return Some(c);
            }
        }
    }
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            let c = dir.join("deno.exe");
            if c.is_file() {
                return Some(c);
            }
        }
    }
    None
}

/// Download and unpack deno.exe next to the app binary, reporting 0..100 progress.
/// yt-dlp then auto-detects it (it runs with that folder as its working dir).
pub async fn install_deno(progress: Option<UnboundedSender<u8>>) -> Result<PathBuf, YtdlError> {
    if let Some(existing) = resolve_deno_exe_path() {
        if let Some(sender) = progress.as_ref() {
            let _ = sender.send(100);
        }
        return Ok(existing);
    }

    let app_dir = resolve_app_dir()
        .ok_or_else(|| YtdlError::CliDiscovery("cannot resolve app directory".into()))?;
    std::fs::create_dir_all(&app_dir)?;
    let target_exe = app_dir.join("deno.exe");
    let zip_path = app_dir.join("deno-download.zip");

    let url = "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-pc-windows-msvc.zip";
    let client = Client::builder()
        .user_agent("yt-dlp-downloader/1.0")
        .build()
        .map_err(|e| YtdlError::CliDiscovery(format!("reqwest: {}", e)))?;
    let mut resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| YtdlError::CliDiscovery(format!("deno download request: {}", e)))?;
    if !resp.status().is_success() {
        return Err(YtdlError::CliDiscovery(format!(
            "deno download failed: HTTP {}",
            resp.status()
        )));
    }

    let total = resp.content_length();
    let mut file = tokio::fs::File::create(&zip_path).await?;
    let mut downloaded: u64 = 0;
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| YtdlError::CliDiscovery(format!("deno download read: {}", e)))?
    {
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        if let (Some(t), Some(sender)) = (total, progress.as_ref()) {
            if t > 0 {
                // Reserve the last 1% for extraction so the bar isn't stuck at 100 mid-unzip.
                let pct = ((downloaded as f64 / t as f64) * 99.0) as u8;
                let _ = sender.send(pct.min(99));
            }
        }
    }
    file.flush().await?;
    drop(file);

    // Windows ships bsdtar as `tar`, which unpacks zip — no extra crate needed.
    let status = Command::new("tar")
        .arg("-xf")
        .arg(&zip_path)
        .arg("-C")
        .arg(&app_dir)
        .status()
        .await
        .map_err(|e| YtdlError::CliDiscovery(format!("tar spawn: {}", e)))?;
    let _ = std::fs::remove_file(&zip_path);
    if !status.success() {
        return Err(YtdlError::CliDiscovery("unzip (tar) failed".into()));
    }
    if !target_exe.is_file() {
        return Err(YtdlError::MissingOutput(target_exe));
    }
    log::info!("Installed deno to {:?}", target_exe);
    if let Some(sender) = progress.as_ref() {
        let _ = sender.send(100);
    }
    Ok(target_exe)
}

#[derive(Debug, Error)]
pub enum YtdlError {
    #[error("rusty_ytdl error: {0}")]
    RustyYtdl(#[from] rusty_ytdl::VideoError),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("yt-dlp not found at {0:?} (place yt-dlp.exe in same folder as downloader.exe)")]
    YtdlpNotFound(PathBuf),
    #[error("yt-dlp discovery/parse failed: {0}")]
    CliDiscovery(String),
    #[error("requested format with itag {0} not found")]
    FormatNotFound(u64),
    #[error("yt-dlp exited with status {code:?}: {message}")]
    YtdlpCliFailed {
        code: Option<i32>,
        message: String,
    },
    #[error("failed to spawn yt-dlp: {0}")]
    CliSpawn(String),
    #[error("expected output missing at {0:?}")]
    MissingOutput(PathBuf),
}

fn format_quality_ytdlp(f: &YtdlpFormat) -> String {
    if let Some(h) = f.height {
        if h > 0 {
            return format!("{}p", h);
        }
    }
    if let Some(tbr) = f.tbr {
        if tbr > 0.0 {
            return format!("{}kbps", tbr as u32);
        }
    }
    if f.vcodec.as_deref().map_or(true, |c| c == "none") {
        return "audio".to_string();
    }
    "unknown".to_string()
}

fn stream_type_ytdlp(f: &YtdlpFormat) -> (String, bool, bool) {
    let v = f.vcodec.as_deref().map_or(false, |c| c != "none" && !c.is_empty());
    let a = f.acodec.as_deref().map_or(false, |c| c != "none" && !c.is_empty());
    let t = if v && a {
        "muxed"
    } else if v {
        "video"
    } else if a {
        "audio"
    } else {
        "unknown"
    };
    (t.to_string(), v, a)
}

/// Spawn a task that reads an async stream to its end as a (lossy) String. Used to
/// drain yt-dlp's stderr without blocking the stdout progress loop or filling the pipe.
fn drain_to_string<R>(stream: R) -> tokio::task::JoinHandle<String>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut reader = BufReader::new(stream);
        let mut buf = String::new();
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => buf.push_str(&line),
            }
        }
        buf
    })
}

/// Base language code without region/variant suffix (e.g. "en-US" -> "en").
fn base_lang(code: &str) -> String {
    code.split(|c| c == '-' || c == '_')
        .next()
        .unwrap_or(code)
        .to_lowercase()
}

/// Unique subtitle extensions offered for a track, preserving first-seen order.
fn collect_exts(list: &[YtdlpSub]) -> Vec<String> {
    let mut exts: Vec<String> = Vec::new();
    for s in list {
        if let Some(e) = &s.ext {
            if !exts.contains(e) {
                exts.push(e.clone());
            }
        }
    }
    exts
}

/// Snapshot the set of regular files currently in `dir`.
fn list_dir_files(dir: &Path) -> HashSet<PathBuf> {
    let mut set = HashSet::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_file() {
                set.insert(p);
            }
        }
    }
    set
}

/// Strip SRT/VTT markup and timing to plain transcript text.
fn subtitle_to_text(content: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut last = String::new();
    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty()
            || line.contains("-->")
            || line.starts_with("WEBVTT")
            || line.starts_with("Kind:")
            || line.starts_with("Language:")
            || line.starts_with("NOTE")
            || line.chars().all(|c| c.is_ascii_digit())
        {
            continue;
        }
        let cleaned = strip_tags(line);
        let cleaned = cleaned.trim();
        if cleaned.is_empty() || cleaned == last {
            continue;
        }
        last = cleaned.to_string();
        out.push(cleaned.to_string());
    }
    out.join("\n")
}

/// Remove `<...>` tags and decode a few common HTML entities.
fn strip_tags(s: &str) -> String {
    let mut result = String::new();
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => result.push(c),
            _ => {}
        }
    }
    result
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&#39;", "'")
        .replace("&quot;", "\"")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

fn parse_ytdlp_pct(line: &str) -> Option<i32> {
    let idx = line.find('%')?;
    let num = line[..idx].trim().split_whitespace().last()?;
    num.parse::<f64>().ok().map(|f| f as i32)
}

fn normalise_input(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.contains("://") {
        trimmed.to_string()
    } else if trimmed.len() == 11 && trimmed.chars().all(is_video_id_char) {
        format!("https://www.youtube.com/watch?v={trimmed}")
    } else {
        trimmed.to_string()
    }
}

fn is_video_id_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'
}

fn sanitize_component(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => c,
        })
        .collect()
}

fn extension_from_format(format: &VideoFormat) -> String {
    match format.mime_type.container.as_str() {
        "mp4" if format.has_audio && !format.has_video => "m4a".to_string(),
        container => container.to_string(),
    }
}
