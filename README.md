# YouTube Downloader — standalone server

Self-contained project. You can move this `downloader` folder anywhere or into a separate repo.

## What’s inside

- **Server** — HTTP app to paste a YouTube URL, see formats, and download by itag (via yt-dlp).
- **ytdlp/** — Put `wrapper_ytdlp.exe` (built from this project) and `yt-dlp.exe` here. See `ytdlp/README.md`.

## Build

```powershell
cargo build
```

- Main server binary: `target\debug\downloader.exe` (or `release\downloader.exe` with `cargo build --release`).
- Wrapper for yt-dlp: `cargo build --bin wrapper_ytdlp` → `target\debug\wrapper_ytdlp.exe`. Copy it into `ytdlp\`.

## Run

1. Ensure `ytdlp\` contains `wrapper_ytdlp.exe` and `yt-dlp.exe` (see `ytdlp/README.md`).
2. From the `downloader` folder:
   ```powershell
   .\target\debug\downloader.exe
   ```
   Or: `cargo run`
3. Optional: set port with `DOWNLOADER_PORT` (default 8080):
   ```powershell
   $env:DOWNLOADER_PORT = "9090"; cargo run
   ```
4. Open in browser: **http://localhost:8080** (or the port you set).

## Layout

```
downloader/
  Cargo.toml
  src/
    lib.rs       # YouTube download logic (YoutubeDownloader, StreamInfo)
    main.rs      # Server entrypoint
    server.rs    # HTTP server and UI
    index.html   # Page template
    bin/
      wrapper_ytdlp.rs   # yt-dlp wrapper binary
  ytdlp/
    README.md    # Instructions for wrapper_ytdlp.exe + yt-dlp.exe
    # you add: wrapper_ytdlp.exe, yt-dlp.exe
```

No dependency on the parent `razum` project; this folder is an independent app.
