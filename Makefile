# YouTube Downloader — build automation
#
# Usage (run from this folder):
#   make            # setup + build release (default)
#   make setup      # create folders and download yt-dlp.exe if missing
#   make build      # cargo build --release
#   make run        # build + run the server
#   make dev        # cargo run (debug)
#   make clean      # remove build artifacts
#
# Notes:
# - On Windows you need `make` (e.g. `choco install make`) plus the Rust
#   toolchain (https://rustup.rs). `setup` downloads yt-dlp.exe automatically.

# yt-dlp.exe lives next to the built binary so the server can find it.
YTDLP_URL := https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe
BIN_DIR   := target/release
BIN       := $(BIN_DIR)/downloader.exe
YTDLP     := yt-dlp.exe

.PHONY: all setup folders ytdlp build run dev clean

# Default: get everything ready and build.
all: setup build

# Create the folders the app uses and fetch yt-dlp.exe if it's missing.
setup: folders ytdlp

# Folders the server/downloader expects at runtime.
folders:
	@mkdir -p downloaded
	@mkdir -p $(BIN_DIR)

# Download yt-dlp.exe into the project root if not already present.
ytdlp:
	@if [ ! -f "$(YTDLP)" ]; then \
		echo "Downloading yt-dlp.exe ..."; \
		curl -L -o "$(YTDLP)" "$(YTDLP_URL)"; \
	else \
		echo "yt-dlp.exe already present, skipping download."; \
	fi

# Build the release binary.
build:
	cargo build --release

# Build and run the release server.
run: all
	./$(BIN)

# Quick debug run.
dev: setup
	cargo run

# Remove build artifacts.
clean:
	cargo clean
