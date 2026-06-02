//! HTTP server for the standalone downloader: check video, list formats, download by itag.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use downloader::{ensure_ytdlp_available, StreamInfo, YoutubeDownloader};
use once_cell::sync::Lazy;
use rusty_ytdl::{VideoFormat, VideoInfo};
use serde_json::json;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use url::form_urlencoded;

const TEMPLATE: &str = include_str!("index.html");
const MAX_REQUEST_SIZE: usize = 8192;
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
    let request = String::from_utf8_lossy(&buffer[..n]);
    let (status, body, content_type, _) = parse_and_handle_request(&downloader, &request).await;
    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        body.as_bytes().len(),
        body
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

    let (formats, formats_error) = if let Some(ref info) = maybe_video {
        if info.formats.is_empty() {
            match downloader.fetch_formats_via_cli(&input_value).await {
                Ok(list) => (Some(list), None),
                Err(e) => {
                    log::warn!("Formats failed: {}", e);
                    (None, Some(e.to_string()))
                }
            }
        } else {
            (None, None)
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
    let mut itag_param = None::<u64>;
    let mut container_param = None::<String>;
    let mut video_id_param = None::<String>;

    for (key, value) in form_urlencoded::parse(q.as_bytes()) {
        match key.as_ref() {
            "video" => video_param = Some(value.into_owned()),
            "itag" => {
                if let Ok(n) = value.parse::<u64>() {
                    itag_param = Some(n);
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
    let container_param = container_param.clone();
    let video_id_param = video_id_param.clone();

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

        let result = if let (Some(ref container), Some(ref vid_id)) = (container_param.as_ref(), video_id_param.as_ref()) {
            downloader
                .download_itag_to(&video_url_clone, itag, container, vid_id, Some(progress_tx))
                .await
                .map(|path| (path, ()))
        } else {
            downloader
                .download_format_to(&video_url_clone, itag, false, Some(progress_tx))
                .await
                .map(|(path, _f, _i)| (path, ()))
        };

        let _ = progress_task.await;

        let mut jobs = jobs_handle.lock().await;
        if let Some(s) = jobs.get_mut(&job_id_for_task) {
            match result {
                Ok((path, _)) => {
                    s.percent = 100;
                    s.status = DownloadJobStatus::Completed;
                    let rel = path
                        .strip_prefix(downloader.download_dir())
                        .unwrap_or(&path)
                        .display()
                        .to_string();
                    s.path = Some(rel.clone());
                    s.message = Some(format!("Saved itag {} to {}", itag, rel));
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
        None => {
            if info.formats.is_empty() {
                render_format_list_empty(formats_error)
            } else {
                render_format_list(&info.formats, raw_url)
            }
        }
    };
    let thumb = select_best_thumbnail(d).map(|t| html_escape(&t.url)).unwrap_or_default();
    let thumb_html = if thumb.is_empty() {
        String::new()
    } else {
        format!(r#"<div><a href="{video_url}" target="_blank" rel="noopener"><img src="{thumb}" alt="Thumbnail" loading="lazy" /></a></div>"#)
    };
    format!(
        r#"<div class="card">{thumb_html}<div class="meta"><h2><a href="{video_url}" target="_blank" rel="noopener">{title}</a></h2><div class="stats"><span>👤 {channel}</span><span>⏱ {duration}</span><span>👁 {views} views</span><span>📅 {publish_date}</span></div></div><div class="links">{format_html}</div></div>"#
    )
}

fn render_format_filter_radios() -> String {
    r#"<div class="format-filter" role="group" aria-label="Filter by stream type">
        <span class="format-filter-label">Filter:</span>
        <label><input type="radio" name="format-filter" value="all" checked> ALL</label>
        <label><input type="radio" name="format-filter" value="video"> V</label>
        <label><input type="radio" name="format-filter" value="audio"> A</label>
        <label><input type="radio" name="format-filter" value="muxed"> V & A</label>
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
    entries.sort_by(|a, b| {
        let a_itag = a.itag.unwrap_or(0);
        let b_itag = b.itag.unwrap_or(0);
        a_itag.cmp(&b_itag).then_with(|| b.bitrate.cmp(&a.bitrate))
    });
    entries.dedup_by_key(|s| s.itag.unwrap_or(0));
    entries.sort_by(|a, b| b.bitrate.cmp(&a.bitrate));
    let cards: String = entries
        .iter()
        .map(|s| render_format_card_stream(s, video_url, video_id))
        .collect();
    let filter_radios = render_format_filter_radios();
    format!(r#"<div class="format-list"><h3>Available formats</h3>{filter_radios}<div class="format-grid">{cards}</div></div>"#)
}

fn render_format_card_stream(s: &StreamInfo, video_url: &str, video_id: &str) -> String {
    let itag = s.itag.unwrap_or(0);
    let badge = html_escape(&format!("itag {}", itag));
    let quality = html_escape(&s.quality);
    let codecs = html_escape(&s.stream_type);
    let meta = html_escape(&format!("{} · {}", s.container, format_bitrate(s.bitrate)));
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("video", video_url)
        .append_pair("itag", &itag.to_string())
        .append_pair("container", &s.container)
        .append_pair("video_id", video_id)
        .finish();
    let endpoint = html_escape(&format!("/download?{query}"));
    let stream_type = s.stream_type.to_lowercase();
    let data_type = if stream_type == "muxed" { "muxed" } else if stream_type == "video" { "video" } else { "audio" };
    format!(
        r##"<a class="format-card" href="#" role="button" data-endpoint="{endpoint}" data-itag="{itag}" data-stream-type="{data_type}"><span class="format-badge">{badge}</span><span class="format-quality">{quality}</span><span class="format-codecs">{codecs}</span><span class="format-meta">{meta}</span><span class="format-note">Server download</span><span class="format-status"></span></a>"##
    )
}

fn render_format_list(formats: &[VideoFormat], video_url: &str) -> String {
    if formats.is_empty() {
        return r#"<div class="format-list"><h3>Available formats</h3><p class="muted">No streams.</p></div>"#.to_string();
    }
    let mut entries: Vec<&VideoFormat> = formats.iter().collect();
    entries.sort_by(|a, b| {
        a.itag.cmp(&b.itag).then_with(|| b.bitrate.cmp(&a.bitrate))
    });
    entries.dedup_by_key(|f| f.itag);
    entries.sort_by(|a, b| b.bitrate.cmp(&a.bitrate));
    let cards: String = entries
        .iter()
        .map(|f| render_format_card(f, video_url))
        .collect();
    let filter_radios = render_format_filter_radios();
    format!(r#"<div class="format-list"><h3>Available formats</h3>{filter_radios}<div class="format-grid">{cards}</div></div>"#)
}

fn render_format_card(fmt: &VideoFormat, video_url: &str) -> String {
    let badge = html_escape(&format!("itag {}", fmt.itag));
    let quality = html_escape(&format_quality(fmt));
    let codecs = html_escape(&format_codecs(fmt));
    let meta = html_escape(&format_format_meta(fmt));
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("video", video_url)
        .append_pair("itag", &fmt.itag.to_string())
        .finish();
    let endpoint = html_escape(&format!("/download?{query}"));
    let stream_type = match (fmt.has_video, fmt.has_audio) {
        (true, true) => "muxed",
        (true, false) => "video",
        (false, true) => "audio",
        _ => "unknown",
    };
    format!(
        r##"<a class="format-card" href="#" role="button" data-endpoint="{endpoint}" data-itag="{}" data-stream-type="{stream_type}"><span class="format-badge">{badge}</span><span class="format-quality">{quality}</span><span class="format-codecs">{codecs}</span><span class="format-meta">{meta}</span><span class="format-note">Server download</span><span class="format-status"></span></a>"##,
        fmt.itag
    )
}

fn format_quality(fmt: &VideoFormat) -> String {
    if let Some(l) = &fmt.quality_label {
        if !l.is_empty() {
            return l.clone();
        }
    }
    if let Some(h) = fmt.height {
        if h > 0 {
            return format!("{}p", h);
        }
    }
    if let Some(aq) = &fmt.audio_quality {
        if !aq.is_empty() {
            return aq.clone();
        }
    }
    fmt.mime_type.container.to_uppercase()
}

fn format_format_meta(fmt: &VideoFormat) -> String {
    let t = match (fmt.has_video, fmt.has_audio) {
        (true, true) => "Video + Audio",
        (true, false) => "Video only",
        (false, true) => "Audio only",
        _ => "Data",
    };
    format!("{} · {}", t, format_bitrate(fmt.bitrate))
}

fn format_codecs(fmt: &VideoFormat) -> String {
    if fmt.mime_type.codecs.is_empty() {
        fmt.mime_type.container.to_uppercase()
    } else {
        fmt.mime_type.codecs.join(", ")
    }
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
