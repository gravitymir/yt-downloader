//! HTTP server for the standalone downloader: check video, list formats, download by itag.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use downloader::{ensure_ytdlp_available, set_cookies_file, update_ytdlp, StreamInfo, YoutubeDownloader};
use once_cell::sync::Lazy;
use rusty_ytdl::VideoInfo;
use serde_json::json;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::Mutex;
use url::form_urlencoded;

const TEMPLATE: &str = include_str!("index.html");
const MAX_REQUEST_SIZE: usize = 8192;

/// File explorer markup, rendered under the video card on the results page.
const EXPLORER_HTML: &str = r#"<div class="explorer" id="explorer">
    <div class="explorer-head">
        <h3 id="open-folder" title="Open this folder in your file explorer" role="button" tabindex="0">📂 downloaded/</h3>
        <div class="explorer-actions">
            <button type="button" id="refresh-files" class="btn-secondary">Refresh</button>
            <button type="button" id="mp3-btn" disabled>&rarr; MP3</button>
            <button type="button" id="merge-btn" disabled>Merge &rarr; MP4 (phones)</button>
        </div>
    </div>
    <div class="explorer-status" id="explorer-status"></div>
    <div class="file-list" id="file-list"></div>
</div>"#;
static JOB_COUNTER: AtomicU64 = AtomicU64::new(1);
static DOWNLOAD_JOBS: Lazy<Arc<Mutex<HashMap<String, DownloadJobState>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

#[derive(Clone)]
struct DownloadJobState {
    percent: u8,
    status: DownloadJobStatus,
    message: Option<String>,
    path: Option<String>,
    error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DownloadJobStatus {
    Running,
    Completed,
    Failed,
}

impl DownloadJobStatus {
    fn as_str(&self) -> &'static str {
        match self {
            DownloadJobStatus::Running => "running",
            DownloadJobStatus::Completed => "completed",
            DownloadJobStatus::Failed => "failed",
        }
    }
}

/// Bind to host:port and run the HTTP server. Uses 127.0.0.1:port (port from DOWNLOADER_PORT or 8080).
pub async fn run(port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ensure_ytdlp_available().await?;
    // Keep yt-dlp current so YouTube changes don't reintroduce 403s (best-effort, capped
    // so a slow/absent network never blocks startup for long).
    let _ = tokio::time::timeout(std::time::Duration::from_secs(90), update_ytdlp()).await;
    let listen_port = port;
    let addr = format!("127.0.0.1:{}", listen_port);
    let listener = TcpListener::bind(&addr).await?;
    let downloader = Arc::new(YoutubeDownloader::new()?);

    std::fs::create_dir_all("downloaded")?;

    println!("YouTube Downloader server: http://localhost:{}", listen_port);

    loop {
        let (stream, _) = listener.accept().await?;
        let downloader = downloader.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(downloader, stream).await {
                eprintln!("HTTP error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    downloader: Arc<YoutubeDownloader>,
    mut stream: TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = vec![0u8; MAX_REQUEST_SIZE];
    let n = stream.read(&mut buffer).await?;
    if n == 0 {
        return Ok(());
    }

    let first_line = String::from_utf8_lossy(&buffer[..n])
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();

    // CORS preflight — the Chrome extension probes before POSTing cookies.
    if first_line.starts_with("OPTIONS ") {
        let response = "HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: *\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await?;
        return Ok(());
    }

    // The Chrome extension uploads YouTube cookies here (body may exceed one read).
    if first_line.starts_with("POST /cookies") {
        return handle_post_cookies(&mut stream, &buffer[..n]).await;
    }

    let request = String::from_utf8_lossy(&buffer[..n]);
    let (status, body, content_type, _) = parse_and_handle_request(&downloader, &request).await;
    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        body.as_bytes().len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

/// Receive YouTube cookies (Netscape cookies.txt) from the extension, save them,
/// and point yt-dlp at the file for subsequent requests.
async fn handle_post_cookies(
    stream: &mut TcpStream,
    initial: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Split headers from the (possibly partial) body already read.
    let sep_pos = initial.windows(4).position(|w| w == b"\r\n\r\n");
    let (head, mut body): (&[u8], Vec<u8>) = match sep_pos {
        Some(p) => (&initial[..p], initial[p + 4..].to_vec()),
        None => (initial, Vec::new()),
    };
    let head_str = String::from_utf8_lossy(head);
    let content_length = head_str
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);

    // Read the rest of the body (cookies.txt can be larger than one TCP read).
    let mut buf = vec![0u8; 8192];
    while body.len() < content_length {
        let k = stream.read(&mut buf).await?;
        if k == 0 {
            break;
        }
        body.extend_from_slice(&buf[..k]);
    }
    if body.len() > content_length && content_length > 0 {
        body.truncate(content_length);
    }

    let text = String::from_utf8_lossy(&body).to_string();
    let count = text
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'))
        .count();

    let path = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("cookies.txt");
    if let Err(e) = tokio::fs::write(&path, text.as_bytes()).await {
        return write_plain(stream, "500 Internal Server Error", &format!("save failed: {}", e)).await;
    }
    set_cookies_file(Some(path.clone()));
    log::info!("Saved {} cookies to {:?}", count, path);

    let body_json =
        json!({ "status": "ok", "cookies": count, "saved": path.display().to_string() }).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body_json.as_bytes().len(),
        body_json
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

/// Write a tiny plain-text HTTP response (used for cookie-save errors).
async fn write_plain(
    stream: &mut TcpStream,
    status: &str,
    body: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.as_bytes().len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn parse_and_handle_request(
    downloader: &YoutubeDownloader,
    raw: &str,
) -> (String, String, String, Option<PathBuf>) {
    let line = raw.lines().next().unwrap_or_default();
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or("/");

    if method != "GET" {
        let body = render_page("", None, Some("Only GET allowed."), None, None, None);
        return ("405 Method Not Allowed".to_string(), body, "text/html; charset=utf-8".to_string(), None);
    }

    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (target, None),
    };

    if path == "/health" {
        return (
            "200 OK".to_string(),
            json!({ "status": "ok" }).to_string(),
            "application/json".to_string(),
            None,
        );
    }
    if path == "/files" {
        return handle_list_files().await;
    }
    if path == "/open-folder" {
        return handle_open_folder().await;
    }
    if path == "/merge" {
        return handle_merge(query).await;
    }
    if path == "/to-mp3" {
        return handle_to_mp3(query).await;
    }
    if path == "/download/status" {
        return handle_download_status(query).await;
    }
    if path.starts_with("/download") {
        return handle_download_request(downloader, query).await;
    }
    if path != "/" {
        let body = render_page("", None, Some("Not found."), None, None, None);
        return ("404 Not Found".to_string(), body, "text/html; charset=utf-8".to_string(), None);
    }

    let mut input_value = String::new();
    let mut maybe_video: Option<VideoInfo> = None;
    let mut maybe_error: Option<String> = None;

    if let Some(q) = query {
        for (key, value) in form_urlencoded::parse(q.as_bytes()) {
            if key == "url" {
                input_value = value.trim().to_string();
                break;
            }
        }
        if input_value.is_empty() {
            maybe_error = Some("Please enter a video URL or ID.".to_string());
        } else {
            match downloader.fetch_info(&input_value).await {
                Ok(info) => maybe_video = Some(info),
                Err(e) => maybe_error = Some(format!("Unable to fetch info: {}", e)),
            }
        }
    }

    // yt-dlp is the single source of truth for formats: the ids it lists are exactly
    // the ids it can download (including "-sr" upscaled variants), so what the user
    // clicks always matches what actually downloads.
    let (formats, formats_error) = if maybe_video.is_some() {
        match downloader.fetch_formats_via_cli(&input_value).await {
            Ok(list) => (Some(list), None),
            Err(e) => {
                log::warn!("Formats failed: {}", e);
                (None, Some(e.to_string()))
            }
        }
    } else {
        (None, None)
    };

    let body = render_page(
        &input_value,
        maybe_video.as_ref(),
        maybe_error.as_deref(),
        None,
        formats.as_deref(),
        formats_error.as_deref(),
    );
    ("200 OK".to_string(), body, "text/html; charset=utf-8".to_string(), None)
}

async fn handle_download_request(
    downloader: &YoutubeDownloader,
    query: Option<&str>,
) -> (String, String, String, Option<PathBuf>) {
    let Some(q) = query else {
        return (
            "400 Bad Request".to_string(),
            json!({"status":"error","message":"Missing query"}).to_string(),
            "application/json".to_string(),
            None,
        );
    };

    let mut video_param = None::<String>;
    let mut itag_param = None::<String>;
    let mut container_param = None::<String>;
    let mut video_id_param = None::<String>;

    for (key, value) in form_urlencoded::parse(q.as_bytes()) {
        match key.as_ref() {
            "video" => video_param = Some(value.into_owned()),
            "itag" => {
                let v = value.into_owned();
                if !v.is_empty() {
                    itag_param = Some(v);
                }
            }
            "container" => container_param = Some(value.into_owned()),
            "video_id" => video_id_param = Some(value.into_owned()),
            _ => {}
        }
    }

    let Some(video_url) = video_param else {
        return (
            "400 Bad Request".to_string(),
            json!({"status":"error","message":"Missing video"}).to_string(),
            "application/json".to_string(),
            None,
        );
    };
    let Some(itag) = itag_param else {
        return (
            "400 Bad Request".to_string(),
            json!({"status":"error","message":"Missing itag"}).to_string(),
            "application/json".to_string(),
            None,
        );
    };
    // yt-dlp is the single source now: every card carries the container + video id.
    let (Some(container), Some(video_id)) = (container_param, video_id_param) else {
        return (
            "400 Bad Request".to_string(),
            json!({"status":"error","message":"Missing container/video_id"}).to_string(),
            "application/json".to_string(),
            None,
        );
    };

    let job_id = format!("{:016x}", JOB_COUNTER.fetch_add(1, Ordering::Relaxed));
    {
        let mut jobs = DOWNLOAD_JOBS.lock().await;
        jobs.insert(
            job_id.clone(),
            DownloadJobState {
                percent: 0,
                status: DownloadJobStatus::Running,
                message: None,
                path: None,
                error: None,
            },
        );
    }

    let downloader = downloader.clone();
    let jobs_handle = DOWNLOAD_JOBS.clone();
    let video_url_clone = video_url.clone();
    let job_id_for_task = job_id.clone();
    let itag_task = itag.clone();

    tokio::spawn(async move {
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
        let progress_jobs = jobs_handle.clone();
        let progress_job_id = job_id_for_task.clone();

        let progress_task = tokio::spawn(async move {
            while let Some(percent) = progress_rx.recv().await {
                let mut jobs = progress_jobs.lock().await;
                if let Some(s) = jobs.get_mut(&progress_job_id) {
                    s.percent = percent;
                }
            }
        });

        let result = downloader
            .download_itag_to(&video_url_clone, &itag_task, &container, &video_id, Some(progress_tx))
            .await;

        let _ = progress_task.await;

        let mut jobs = jobs_handle.lock().await;
        if let Some(s) = jobs.get_mut(&job_id_for_task) {
            match result {
                Ok(path) => {
                    s.percent = 100;
                    s.status = DownloadJobStatus::Completed;
                    let rel = path
                        .strip_prefix(downloader.download_dir())
                        .unwrap_or(&path)
                        .display()
                        .to_string();
                    s.path = Some(rel.clone());
                    s.message = Some(format!("Saved itag {} to {}", itag_task, rel));
                }
                Err(e) => {
                    s.status = DownloadJobStatus::Failed;
                    s.error = Some(e.to_string());
                }
            }
        }
    });

    (
        "200 OK".to_string(),
        json!({"status":"started","jobId":job_id,"itag":itag}).to_string(),
        "application/json".to_string(),
        None,
    )
}

async fn handle_download_status(query: Option<&str>) -> (String, String, String, Option<PathBuf>) {
    let Some(q) = query else {
        return (
            "400 Bad Request".to_string(),
            json!({"status":"error","message":"Missing id"}).to_string(),
            "application/json".to_string(),
            None,
        );
    };
    let mut job_id = None::<String>;
    for (key, value) in form_urlencoded::parse(q.as_bytes()) {
        if key == "id" {
            job_id = Some(value.into_owned());
            break;
        }
    }
    let Some(job_id) = job_id else {
        return (
            "400 Bad Request".to_string(),
            json!({"status":"error","message":"Missing id"}).to_string(),
            "application/json".to_string(),
            None,
        );
    };

    let jobs = DOWNLOAD_JOBS.lock().await;
    let Some(s) = jobs.get(&job_id) else {
        return (
            "404 Not Found".to_string(),
            json!({"status":"error","message":"Job not found"}).to_string(),
            "application/json".to_string(),
            None,
        );
    };

    (
        "200 OK".to_string(),
        json!({
            "status": s.status.as_str(),
            "percent": s.percent,
            "message": s.message,
            "path": s.path,
            "error": s.error,
        })
        .to_string(),
        "application/json".to_string(),
        None,
    )
}

/// Folder where downloads and merged files live.
fn downloaded_dir() -> PathBuf {
    PathBuf::from("downloaded")
}

/// Classify a file by extension for display in the explorer.
fn classify_ext(ext: &str) -> &'static str {
    match ext.to_lowercase().as_str() {
        "mp3" | "m4a" | "aac" | "opus" | "ogg" | "oga" | "wav" | "weba" | "flac" => "audio",
        "mp4" | "webm" | "mkv" | "mov" | "avi" | "flv" | "m4v" | "ts" => "video",
        _ => "other",
    }
}

/// Reject names that could escape the downloaded/ folder.
fn is_safe_filename(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
        && !name.contains('\0')
}

/// Build the standard JSON-error response tuple used by /files and /merge.
fn json_error(message: &str) -> (String, String, String, Option<PathBuf>) {
    (
        "200 OK".to_string(),
        json!({ "status": "error", "message": message }).to_string(),
        "application/json".to_string(),
        None,
    )
}

/// List the files in downloaded/ as JSON for the web explorer. Probes the actual
/// streams so audio-only `.webm` (e.g. itag251 opus) reports as audio, not video,
/// and the UI can tell which files carry audio.
async fn handle_list_files() -> (String, String, String, Option<PathBuf>) {
    let dir = downloaded_dir();
    let ffprobe = resolve_ffprobe();
    let mut files: Vec<serde_json::Value> = Vec::new();
    if let Ok(mut rd) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let meta = entry.metadata().await.ok();
            let size = meta.as_ref().map(|m| m.len()).unwrap_or(0);
            // Unix-seconds mtime so the UI can highlight the most recent downloads.
            let modified = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let (has_video, has_audio) = probe_streams(&ffprobe, &path).await;
            let kind = if has_video {
                "video"
            } else if has_audio {
                "audio"
            } else {
                "other"
            };
            files.push(json!({
                "name": name,
                "size": size,
                "modified": modified,
                "type": kind,
                "hasAudio": has_audio,
                "hasVideo": has_video,
            }));
        }
    }
    files.sort_by(|a, b| {
        a["name"]
            .as_str()
            .unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    });
    (
        "200 OK".to_string(),
        json!({ "files": files }).to_string(),
        "application/json".to_string(),
        None,
    )
}

/// Open the downloaded/ folder in the OS file explorer.
async fn handle_open_folder() -> (String, String, String, Option<PathBuf>) {
    let dir = downloaded_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return json_error(&format!("Could not create folder: {}", e));
    }
    let abs = std::fs::canonicalize(&dir).unwrap_or(dir);
    // Windows canonicalize yields a \\?\ prefix that explorer.exe rejects.
    let path_str = abs.to_string_lossy().to_string();
    let cleaned = path_str
        .strip_prefix(r"\\?\")
        .unwrap_or(&path_str)
        .to_string();

    #[cfg(target_os = "windows")]
    let spawned = Command::new("explorer").arg(&cleaned).spawn();
    #[cfg(target_os = "macos")]
    let spawned = Command::new("open").arg(&cleaned).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let spawned = Command::new("xdg-open").arg(&cleaned).spawn();

    match spawned {
        Ok(_) => (
            "200 OK".to_string(),
            json!({ "status": "ok", "path": cleaned }).to_string(),
            "application/json".to_string(),
            None,
        ),
        Err(e) => json_error(&format!("Could not open folder: {}", e)),
    }
}

/// Locate a tool: prefer a local copy (cwd or next to the binary), else rely on PATH.
fn resolve_tool(names: &[&str], fallback: &str) -> PathBuf {
    if let Ok(cwd) = std::env::current_dir() {
        for n in names {
            let c = cwd.join(n);
            if c.is_file() {
                return c;
            }
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            for n in names {
                let c = parent.join(n);
                if c.is_file() {
                    return c;
                }
            }
        }
    }
    PathBuf::from(fallback)
}

fn resolve_ffmpeg() -> PathBuf {
    resolve_tool(&["ffmpeg.exe", "ffmpeg"], "ffmpeg")
}

fn resolve_ffprobe() -> PathBuf {
    resolve_tool(&["ffprobe.exe", "ffprobe"], "ffprobe")
}

/// Probe a file for which stream kinds it carries: (has_video, has_audio).
/// Falls back to an extension guess if ffprobe is unavailable.
async fn probe_streams(ffprobe: &Path, path: &Path) -> (bool, bool) {
    let mut command = Command::new(ffprobe);
    command
        .args(["-v", "error", "-show_entries", "stream=codec_type", "-of", "csv=p=0"])
        .arg(path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    match command.output().await {
        Ok(out) => {
            let s = String::from_utf8_lossy(&out.stdout);
            let has_video = s.lines().any(|l| l.trim() == "video");
            let has_audio = s.lines().any(|l| l.trim() == "audio");
            (has_video, has_audio)
        }
        Err(_) => {
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            match classify_ext(ext) {
                "video" => (true, false),
                "audio" => (false, true),
                _ => (false, false),
            }
        }
    }
}

/// Derive a base name for the merged output from the first selected file.
fn derive_merge_base(files: &[String]) -> String {
    let first = files.first().map(|s| s.as_str()).unwrap_or("merged");
    let stem = first.rsplit_once('.').map(|(s, _)| s).unwrap_or(first);
    let base = stem.split("_itag").next().unwrap_or(stem);
    if base.is_empty() {
        "merged".to_string()
    } else {
        base.to_string()
    }
}

/// Merge selected video + audio files with ffmpeg (stream copy) into downloaded/.
async fn handle_merge(query: Option<&str>) -> (String, String, String, Option<PathBuf>) {
    let Some(q) = query else {
        return json_error("Missing files");
    };

    let mut files: Vec<String> = Vec::new();
    for (key, value) in form_urlencoded::parse(q.as_bytes()) {
        if key == "files" {
            let v = value.into_owned();
            if !v.is_empty() {
                files.push(v);
            }
        }
    }

    if files.len() < 2 {
        return json_error("Select at least two files (one video and one audio) to merge.");
    }

    let dir = downloaded_dir();
    let mut input_paths: Vec<PathBuf> = Vec::new();
    for name in &files {
        if !is_safe_filename(name) {
            return json_error(&format!("Invalid file name: {}", name));
        }
        let path = dir.join(name);
        if !path.is_file() {
            return json_error(&format!("File not found: {}", name));
        }
        input_paths.push(path);
    }

    // Detect which selected file carries video and which carries audio, so the
    // user can tick them in any order (extensions like .webm are ambiguous).
    let ffprobe = resolve_ffprobe();
    let mut video_input: Option<PathBuf> = None;
    let mut audio_input: Option<PathBuf> = None;
    for path in &input_paths {
        let (has_video, has_audio) = probe_streams(&ffprobe, path).await;
        if has_video && video_input.is_none() {
            video_input = Some(path.clone());
            continue;
        }
        if has_audio && audio_input.is_none() {
            audio_input = Some(path.clone());
        }
    }
    // If audio is still missing (e.g. the only audio lives inside a muxed file
    // we already claimed for video), reuse any other file that has audio.
    if audio_input.is_none() {
        for path in &input_paths {
            if Some(path) == video_input.as_ref() {
                continue;
            }
            let (_, has_audio) = probe_streams(&ffprobe, path).await;
            if has_audio {
                audio_input = Some(path.clone());
                break;
            }
        }
    }

    let (Some(video_input), Some(audio_input)) = (video_input, audio_input) else {
        return json_error(
            "Select one file that has video and one that has audio (e.g. an itag137 .mp4 video and an itag251 .webm audio).",
        );
    };

    let out_name = format!("{}_telegram.mp4", derive_merge_base(&files));
    let out_path = dir.join(&out_name);

    // Probe the source duration up front so ffmpeg's time-based progress can be
    // turned into a percentage while it runs.
    let duration = probe_duration(&ffprobe, &video_input).await;

    // Run ffmpeg as a background job (same status map as downloads) so the UI can
    // poll /download/status and show a real progress bar instead of a frozen wait.
    let job_id = format!("{:016x}", JOB_COUNTER.fetch_add(1, Ordering::Relaxed));
    {
        let mut jobs = DOWNLOAD_JOBS.lock().await;
        jobs.insert(
            job_id.clone(),
            DownloadJobState {
                percent: 0,
                status: DownloadJobStatus::Running,
                message: None,
                path: None,
                error: None,
            },
        );
    }

    let jobs_handle = DOWNLOAD_JOBS.clone();
    let job_id_task = job_id.clone();
    let ffmpeg = resolve_ffmpeg();
    let out_name_task = out_name.clone();

    tokio::spawn(async move {
        // Re-encode to H.264 High / yuv420p + AAC with faststart — the broadly
        // compatible combo for Telegram, Android and iPhone playback. `-progress
        // pipe:1` streams machine-readable progress lines (out_time=…) to stdout.
        let mut command = Command::new(&ffmpeg);
        command
            .arg("-y")
            .arg("-i").arg(&video_input)
            .arg("-i").arg(&audio_input)
            .arg("-map").arg("0:v:0")
            .arg("-map").arg("1:a:0")
            .arg("-c:v").arg("libx264")
            .arg("-profile:v").arg("high")
            .arg("-pix_fmt").arg("yuv420p")
            .arg("-crf").arg("23")
            .arg("-preset").arg("fast")
            .arg("-c:a").arg("aac")
            .arg("-b:a").arg("128k")
            .arg("-movflags").arg("+faststart")
            .arg("-progress").arg("pipe:1")
            .arg("-nostats")
            .arg(&out_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = match command.spawn() {
            Ok(c) => c,
            Err(e) => {
                set_job_failed(&jobs_handle, &job_id_task, format!(
                    "Could not run ffmpeg ({}): {}. Install FFmpeg and add it to PATH, or place ffmpeg.exe next to downloader.exe.",
                    ffmpeg.display(), e,
                )).await;
                return;
            }
        };

        let Some(stdout) = child.stdout.take() else {
            set_job_failed(&jobs_handle, &job_id_task, "ffmpeg: missing stdout".to_string()).await;
            return;
        };
        let stderr = child.stderr.take();
        // Drain stderr in the background so we keep it for the error message.
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut s) = stderr {
                let _ = s.read_to_end(&mut buf).await;
            }
            buf
        });

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            if let Some(rest) = line.trim().strip_prefix("out_time=") {
                if let (Some(total), Some(secs)) = (duration, parse_ffmpeg_time(rest)) {
                    if total > 0.0 {
                        // Cap below 100 so the bar only completes once ffmpeg exits.
                        let pct = ((secs / total) * 100.0).clamp(0.0, 99.0) as u8;
                        let mut jobs = jobs_handle.lock().await;
                        if let Some(s) = jobs.get_mut(&job_id_task) {
                            s.percent = pct;
                        }
                    }
                }
            }
        }

        let status = child.wait().await;
        let stderr_bytes = stderr_task.await.unwrap_or_default();

        let mut jobs = jobs_handle.lock().await;
        if let Some(s) = jobs.get_mut(&job_id_task) {
            match status {
                Ok(st) if st.success() => {
                    s.percent = 100;
                    s.status = DownloadJobStatus::Completed;
                    s.path = Some(out_name_task.clone());
                    s.message = Some(format!("Merged into {}", out_name_task));
                }
                Ok(_) => {
                    s.status = DownloadJobStatus::Failed;
                    s.error = Some(format!("ffmpeg failed: {}", ffmpeg_stderr_tail(&stderr_bytes)));
                }
                Err(e) => {
                    s.status = DownloadJobStatus::Failed;
                    s.error = Some(format!("ffmpeg wait failed: {}", e));
                }
            }
        }
    });

    (
        "200 OK".to_string(),
        json!({ "status": "started", "jobId": job_id }).to_string(),
        "application/json".to_string(),
        None,
    )
}

/// Mark a job as failed with an error message.
async fn set_job_failed(
    jobs: &Arc<Mutex<HashMap<String, DownloadJobState>>>,
    id: &str,
    msg: String,
) {
    let mut g = jobs.lock().await;
    if let Some(s) = g.get_mut(id) {
        s.status = DownloadJobStatus::Failed;
        s.error = Some(msg);
    }
}

/// Parse an ffmpeg `out_time` value (`HH:MM:SS.micro`) into seconds.
fn parse_ffmpeg_time(s: &str) -> Option<f64> {
    let mut it = s.trim().split(':');
    let h: f64 = it.next()?.trim().parse().ok()?;
    let m: f64 = it.next()?.trim().parse().ok()?;
    let sec: f64 = it.next()?.trim().parse().ok()?;
    Some(h * 3600.0 + m * 60.0 + sec)
}

/// Total duration of a media file in seconds, via ffprobe (None if unavailable).
async fn probe_duration(ffprobe: &Path, path: &Path) -> Option<f64> {
    let out = Command::new(ffprobe)
        .args(["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0"])
        .arg(path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse::<f64>().ok()
}

/// Last few non-empty lines of ffmpeg stderr, for surfacing failures.
fn ffmpeg_stderr_tail(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    lines
        .iter()
        .rev()
        .take(6)
        .rev()
        .cloned()
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Convert each selected file (that has audio) to a 192 kbps MP3 in downloaded/.
async fn handle_to_mp3(query: Option<&str>) -> (String, String, String, Option<PathBuf>) {
    let Some(q) = query else {
        return json_error("Missing files");
    };

    let mut files: Vec<String> = Vec::new();
    for (key, value) in form_urlencoded::parse(q.as_bytes()) {
        if key == "files" {
            let v = value.into_owned();
            if !v.is_empty() {
                files.push(v);
            }
        }
    }
    if files.is_empty() {
        return json_error("Select at least one file to convert to MP3.");
    }

    let dir = downloaded_dir();
    let ffmpeg = resolve_ffmpeg();
    let ffprobe = resolve_ffprobe();
    let mut outputs: Vec<String> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    for name in &files {
        if !is_safe_filename(name) {
            errors.push(format!("{}: invalid name", name));
            continue;
        }
        if name.to_lowercase().ends_with(".mp3") {
            errors.push(format!("{}: already MP3", name));
            continue;
        }
        let path = dir.join(name);
        if !path.is_file() {
            errors.push(format!("{}: not found", name));
            continue;
        }
        let (_, has_audio) = probe_streams(&ffprobe, &path).await;
        if !has_audio {
            errors.push(format!("{}: no audio stream", name));
            continue;
        }

        let stem = name.rsplit_once('.').map(|(s, _)| s).unwrap_or(name);
        let out_name = format!("{}.mp3", stem);
        let out_path = dir.join(&out_name);

        let mut command = Command::new(&ffmpeg);
        command
            .arg("-y")
            .arg("-i")
            .arg(&path)
            .arg("-vn")
            .arg("-c:a")
            .arg("libmp3lame")
            .arg("-b:a")
            .arg("192k")
            .arg(&out_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        match command.output().await {
            Ok(out) if out.status.success() => outputs.push(out_name),
            Ok(out) => errors.push(format!("{}: {}", name, ffmpeg_stderr_tail(&out.stderr))),
            Err(e) => errors.push(format!("{}: could not run ffmpeg ({}): {}", name, ffmpeg.display(), e)),
        }
    }

    if outputs.is_empty() {
        return json_error(&format!("No files converted. {}", errors.join(" | ")));
    }

    let mut message = format!("Converted to MP3: {}", outputs.join(", "));
    if !errors.is_empty() {
        message.push_str(&format!(" — skipped: {}", errors.join(" | ")));
    }
    (
        "200 OK".to_string(),
        json!({ "status": "ok", "outputs": outputs, "message": message }).to_string(),
        "application/json".to_string(),
        None,
    )
}

fn render_page(
    input_value: &str,
    video_info: Option<&VideoInfo>,
    error: Option<&str>,
    _success: Option<&str>,
    formats: Option<&[StreamInfo]>,
    formats_error: Option<&str>,
) -> String {
    let escaped = html_escape(input_value);
    let mut result = String::new();
    if let Some(e) = error {
        result.push_str(&format!(r#"<div class="alert error">⚠️ {}</div>"#, html_escape(e)));
    }
    if let Some(info) = video_info {
        result.push_str(&render_video_info(info, formats, formats_error));
    } else {
        result.push_str(
            r#"<div class="placeholder">Paste a YouTube URL or 11-character video ID above and click “Check video”.</div>"#,
        );
    }
    // Always show the downloaded/ file explorer — on the start page and under the card.
    result.push_str(EXPLORER_HTML);
    TEMPLATE
        .replace("{{ESCAPED_INPUT}}", &escaped)
        .replace("{{RESULT_SECTION}}", &result)
}

fn render_video_info(
    info: &VideoInfo,
    formats: Option<&[StreamInfo]>,
    formats_error: Option<&str>,
) -> String {
    let d = &info.video_details;
    let video_id = d.video_id.as_ref();
    let video_url = html_escape(&d.video_url);
    let title = html_escape(&d.title);
    let duration = format_duration(&d.length_seconds);
    let views = format_number(&d.view_count);
    let publish_date = html_escape(&d.publish_date);
    let channel = d
        .author
        .as_ref()
        .map(|a| html_escape(&a.name))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| html_escape(&d.owner_channel_name));
    let raw_url = d.video_url.as_ref();
    let format_html = match formats {
        Some(s) => render_format_list_streams(s, raw_url, video_id),
        None => render_format_list_empty(formats_error),
    };
    let thumb = select_best_thumbnail(d).map(|t| html_escape(&t.url)).unwrap_or_default();
    let thumb_html = if thumb.is_empty() {
        String::new()
    } else {
        format!(r#"<div><a href="{video_url}" target="_blank" rel="noopener"><img src="{thumb}" alt="Thumbnail" loading="lazy" /></a></div>"#)
    };
    format!(
        r#"<div class="card"><div class="card-head">{thumb_html}<div class="meta"><h2><a href="{video_url}" target="_blank" rel="noopener">{title}</a></h2><div class="stats"><span>👤 {channel}</span><span>⏱ {duration}</span><span>👁 {views} views</span><span>📅 {publish_date}</span></div></div></div><div class="links">{format_html}</div></div>"#
    )
}

fn render_format_filter_radios() -> String {
    r#"<div class="format-filter" role="group" aria-label="Filter by stream type">
        <span class="format-filter-label">Filter:</span>
        <label><input type="radio" name="format-filter" value="all" checked> ALL</label>
        <label><input type="radio" name="format-filter" value="video"> V</label>
        <label><input type="radio" name="format-filter" value="audio"> A</label>
        <label><input type="radio" name="format-filter" value="muxed"> V & A</label>
        <button type="button" id="combo-best" class="combo-btn" title="Download the best ≤1080p video + best audio, then merge into a phone-ready MP4"><span class="combo-fill"></span><span class="combo-label">⬇ 1080p + 🎵 → MP4</span></button>
    </div>"#.to_string()
}

fn render_format_list_empty(formats_error: Option<&str>) -> String {
    let msg = formats_error
        .map(|e| {
            let t = e.trim();
            if t.is_empty() {
                "Ensure yt-dlp.exe is in the same folder as downloader.exe.".to_string()
            } else {
                format!("Formats could not be loaded: {}", html_escape(t))
            }
        })
        .unwrap_or_else(|| "No downloadable streams.".to_string());
    format!(r#"<div class="format-list"><h3>Available formats</h3><p class="muted">{}</p></div>"#, msg)
}

fn render_format_list_streams(streams: &[StreamInfo], video_url: &str, video_id: &str) -> String {
    let mut entries: Vec<&StreamInfo> = streams.iter().filter(|s| s.itag.is_some()).collect();
    if entries.is_empty() {
        return r#"<div class="format-list"><h3>Available formats</h3><p class="muted">No streams.</p></div>"#.to_string();
    }
    entries.sort_by(|a, b| a.itag.cmp(&b.itag).then_with(|| b.bitrate.cmp(&a.bitrate)));
    entries.dedup_by(|a, b| a.itag == b.itag);
    entries.sort_by(|a, b| b.bitrate.cmp(&a.bitrate));
    let cards: String = entries
        .iter()
        .map(|s| render_format_card_stream(s, video_url, video_id))
        .collect();
    let filter_radios = render_format_filter_radios();
    format!(r#"<div class="format-list"><h3>Available formats</h3>{filter_radios}<div class="format-grid">{cards}</div></div>"#)
}

/// Render one format as a single compact line: itag | quality | codecs | bitrate.
/// `height`/`bitrate_num` are emitted as data-attributes so the combo button can
/// pick the best video/audio streams client-side.
fn render_line_card(
    itag: &str,
    quality: &str,
    codecs: &str,
    bitrate: &str,
    endpoint: &str,
    data_type: &str,
    height: &str,
    bitrate_num: u64,
) -> String {
    format!(
        r##"<a class="format-card" href="#" role="button" data-endpoint="{endpoint}" data-itag="{itag}" data-stream-type="{data_type}" data-height="{height}" data-bitrate="{bitrate_num}"><span class="format-itag">{itag}</span><span class="format-sep">|</span><span class="format-quality">{quality}</span><span class="format-sep">|</span><span class="format-codecs">{codecs}</span><span class="format-sep">|</span><span class="format-meta">{bitrate}</span><span class="format-status"></span></a>"##
    )
}

fn render_format_card_stream(s: &StreamInfo, video_url: &str, video_id: &str) -> String {
    let itag = s.itag.clone().unwrap_or_default();
    let quality = html_escape(&s.quality);
    let codecs = html_escape(&s.container);
    let bitrate = html_escape(&format_bitrate(s.bitrate));
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("video", video_url)
        .append_pair("itag", &itag)
        .append_pair("container", &s.container)
        .append_pair("video_id", video_id)
        .finish();
    let endpoint = html_escape(&format!("/download?{query}"));
    let stream_type = s.stream_type.to_lowercase();
    let data_type = if stream_type == "muxed" { "muxed" } else if stream_type == "video" { "video" } else { "audio" };
    let height = s.height.map(|h| h.to_string()).unwrap_or_default();
    render_line_card(&itag, &quality, &codecs, &bitrate, &endpoint, data_type, &height, s.bitrate)
}

fn format_bitrate(bitrate: u64) -> String {
    if bitrate == 0 {
        return "Bitrate unknown".to_string();
    }
    let mbps = bitrate as f64 / 1_000_000.0;
    if mbps >= 1.0 {
        format!("{:.2} Mbps", mbps)
    } else {
        format!("{:.0} kbps", bitrate as f64 / 1_000.0)
    }
}

fn html_escape(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '&' => "&amp;".into(),
            '<' => "&lt;".into(),
            '>' => "&gt;".into(),
            '"' => "&quot;".into(),
            '\'' => "&#39;".into(),
            _ => c.to_string(),
        })
        .collect()
}

fn format_duration(secs: &str) -> String {
    match secs.parse::<u64>() {
        Ok(total) => {
            let h = total / 3600;
            let m = (total % 3600) / 60;
            let s = total % 60;
            if h > 0 {
                format!("{:02}:{:02}:{:02}", h, m, s)
            } else {
                format!("{:02}:{:02}", m, s)
            }
        }
        _ => secs.to_string(),
    }
}

fn format_number(s: &str) -> String {
    match s.parse::<u64>() {
        Ok(n) => {
            let mut d: Vec<char> = n.to_string().chars().collect();
            let mut out = String::new();
            let mut i = 0;
            while let Some(c) = d.pop() {
                if i > 0 && i % 3 == 0 {
                    out.insert(0, ' ');
                }
                out.insert(0, c);
                i += 1;
            }
            out
        }
        _ => s.to_string(),
    }
}

fn select_best_thumbnail(d: &rusty_ytdl::VideoDetails) -> Option<&rusty_ytdl::Thumbnail> {
    d.thumbnails
        .iter()
        .max_by_key(|t| t.width.saturating_mul(t.height))
}
