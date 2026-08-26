#![allow(dead_code)]
//! YouTube download via yt-dlp. Uses yt-dlp.exe in the same folder as downloader.exe.

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

/// Stream info derived from yt-dlp.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamInfo {
    /// Raw yt-dlp format id, e.g. "137", "251" or "248-sr" (may be non-numeric).
    pub itag: Option<String>,
    pub container: String,
    pub quality: String,
    pub height: Option<u32>,
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
        command.args(cookie_args());
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

    async fn run_ytdlp_info_json(&self, ytdlp_exe: &Path, normalized: &str) -> Result<Vec<StreamInfo>, YtdlError> {
        let mut command = Command::new(ytdlp_exe);
        command
            .args(["-j", "--no-playlist", "--no-download", normalized])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        command.args(cookie_args());
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
                let container = f.ext.as_deref().unwrap_or("unknown").to_string();
                // Drop storyboards / image tiles — they aren't downloadable media.
                if container == "mhtml" {
                    return None;
                }
                let itag = f.format_id.clone().filter(|s| !s.is_empty());
                let quality = format_quality_ytdlp(&f);
                let height = f.height;
                let bitrate = (f.tbr.unwrap_or(0.0) * 1000.0) as u64;
                let (stream_type, has_video, has_audio) = stream_type_ytdlp(&f);
                // Skip anything we can't classify (no usable video or audio stream).
                if stream_type == "unknown" {
                    return None;
                }
                Some(StreamInfo {
                    itag,
                    container,
                    quality,
                    height,
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
        itag: &str,
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
        itag: &str,
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
                itag,
                "-o",
                absolute_output.to_string_lossy().as_ref(),
                "--newline",
                "--no-playlist",
                &normalized,
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        command.args(cookie_args());
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

/// Best-effort: keep yt-dlp current so YouTube's frequent changes don't cause HTTP 403s
/// or "format not available" errors. Switches to (and stays on) the nightly channel,
/// which tracks YouTube fixes fastest. Failures (offline, exe busy) are logged, not fatal.
/// Set DOWNLOADER_NO_UPDATE=1 to skip.
pub async fn update_ytdlp() {
    if std::env::var("DOWNLOADER_NO_UPDATE").map(|v| !v.trim().is_empty()).unwrap_or(false) {
        log::info!("yt-dlp auto-update skipped (DOWNLOADER_NO_UPDATE set)");
        return;
    }
    let Some(exe) = resolve_ytdlp_exe_path() else {
        return;
    };
    log::info!("Checking yt-dlp for updates (nightly channel)…");
    match Command::new(&exe).args(["--update-to", "nightly"]).output().await {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            let last = text.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("").trim();
            log::info!("yt-dlp update: {}", last);
        }
        Err(e) => log::warn!("yt-dlp update check failed (continuing): {}", e),
    }
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
    let v = f.vcodec.as_deref().map_or(false, |c| c != "none" && !c.is_empty() && c != "images");
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

fn parse_ytdlp_pct(line: &str) -> Option<i32> {
    let idx = line.find('%')?;
    let num = line[..idx].trim().split_whitespace().last()?;
    num.parse::<f64>().ok().map(|f| f as i32)
}

/// A cookies.txt file to authenticate yt-dlp with, set at runtime (e.g. by the Chrome
/// extension via POST /cookies). Takes precedence over the env vars below.
static COOKIES_FILE: Lazy<std::sync::Mutex<Option<PathBuf>>> =
    Lazy::new(|| std::sync::Mutex::new(None));

/// Set (or clear with None) the cookies.txt file used for all yt-dlp calls.
pub fn set_cookies_file(path: Option<PathBuf>) {
    if let Ok(mut g) = COOKIES_FILE.lock() {
        *g = path;
    }
}

/// Extra yt-dlp args for YouTube cookie authentication — needed for age-restricted
/// or members-only videos, and a useful fallback when a plain download hits HTTP 403.
/// Priority: the runtime cookies file (set by the Chrome extension via POST /cookies),
/// then env vars:
///   DOWNLOADER_COOKIES_BROWSER — e.g. "chrome", "firefox", "edge", "brave"
///   DOWNLOADER_COOKIES_FILE    — path to an exported Netscape cookies.txt
/// Unset/empty means no cookies are passed.
fn cookie_args() -> Vec<String> {
    if let Ok(g) = COOKIES_FILE.lock() {
        if let Some(p) = g.as_ref() {
            return vec!["--cookies".to_string(), p.display().to_string()];
        }
    }
    if let Ok(browser) = std::env::var("DOWNLOADER_COOKIES_BROWSER") {
        let b = browser.trim();
        if !b.is_empty() {
            return vec!["--cookies-from-browser".to_string(), b.to_string()];
        }
    }
    if let Ok(file) = std::env::var("DOWNLOADER_COOKIES_FILE") {
        let f = file.trim();
        if !f.is_empty() {
            return vec!["--cookies".to_string(), f.to_string()];
        }
    }
    Vec::new()
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
