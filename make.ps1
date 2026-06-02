# YouTube Downloader — build automation (Windows / PowerShell)
#
# Native alternative to the Makefile for machines without `make` installed.
#
# Usage (from this folder):
#   .\make.ps1            # setup + build release (default)
#   .\make.ps1 setup      # create folders and download yt-dlp.exe if missing
#   .\make.ps1 build      # cargo build --release
#   .\make.ps1 run        # build + run the server
#   .\make.ps1 dev        # cargo run (debug)
#   .\make.ps1 clean      # remove build artifacts
#
# Requires the Rust toolchain (https://rustup.rs).

param(
    [ValidateSet('all', 'setup', 'build', 'run', 'dev', 'clean')]
    [string]$Task = 'all'
)

$ErrorActionPreference = 'Stop'

$YtdlpUrl = 'https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe'
$Bin      = 'target\release\downloader.exe'

function Invoke-Folders {
    foreach ($dir in @('downloaded', 'target\release')) {
        if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
    }
    Write-Host 'Folders ready.'
}

function Invoke-Ytdlp {
    if (Test-Path 'yt-dlp.exe') {
        Write-Host 'yt-dlp.exe already present, skipping download.'
    } else {
        Write-Host 'Downloading yt-dlp.exe ...'
        Invoke-WebRequest -Uri $YtdlpUrl -OutFile 'yt-dlp.exe'
    }
}

function Invoke-Setup { Invoke-Folders; Invoke-Ytdlp }
function Invoke-Build { cargo build --release; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } }

switch ($Task) {
    'all'   { Invoke-Setup; Invoke-Build }
    'setup' { Invoke-Setup }
    'build' { Invoke-Build }
    'run'   { Invoke-Setup; Invoke-Build; & ".\$Bin" }
    'dev'   { Invoke-Setup; cargo run }
    'clean' { cargo clean }
}
