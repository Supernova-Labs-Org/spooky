# Installation

## System Requirements

**Hardware:**
- CPU: 1 core minimum (2+ cores recommended for production)
- Memory: 256MB RAM minimum (1GB+ recommended)
- Disk: 100MB for binary and configuration files
- Network: UDP port access for QUIC traffic

**Software:**
- Rust 1.85 or later (2024 edition)
- Operating System: Linux, macOS, or Windows
- Build tools: CMake, pkg-config, C compiler toolchain

**Permissions:**
- Spooky must run as root (required for QUIC/UDP socket binding)

## Installation Methods

### Pre-built Binaries

Download the latest release from [GitHub Releases](https://github.com/nishujangra/spooky/releases).

**Linux (x86_64):**
```bash
wget https://github.com/nishujangra/spooky/releases/download/v0.1.0/spooky-linux-x86_64.tar.gz
tar -xzf spooky-linux-x86_64.tar.gz
sudo install -m 755 spooky /usr/local/bin/spooky
```

**macOS (x86_64/ARM64):**
```bash
wget https://github.com/nishujangra/spooky/releases/download/v0.1.0/spooky-macos-universal.tar.gz
tar -xzf spooky-macos-universal.tar.gz
sudo install -m 755 spooky /usr/local/bin/spooky
```

### Cargo Install

Install directly from crates.io:

```bash
cargo install spooky
```

This compiles from source and installs to `~/.cargo/bin/`. Ensure this directory is in your PATH.

### Build from Source

**Clone and build:**
```bash
git clone https://github.com/nishujangra/spooky.git
cd spooky
cargo build --release
```

The binary is generated at `target/release/spooky`.

**Run tests (optional):**
```bash
cargo test
cargo test -p spooky-edge --test lb_integration
```

**System-wide installation:**
```bash
sudo install -m 755 target/release/spooky /usr/local/bin/spooky
```

## Platform-Specific Setup

### Ubuntu/Debian

```bash
# Install build dependencies
sudo apt update
sudo apt install -y cmake build-essential pkg-config

# Install Rust if not present
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Build and install
git clone https://github.com/nishujangra/spooky.git
cd spooky
cargo build --release
sudo install -m 755 target/release/spooky /usr/local/bin/spooky
```

### CentOS/RHEL 8+

```bash
# Install build dependencies
sudo dnf groupinstall -y "Development Tools"
sudo dnf install -y cmake pkgconfig

# Install Rust if not present
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Build and install
git clone https://github.com/nishujangra/spooky.git
cd spooky
cargo build --release
sudo install -m 755 target/release/spooky /usr/local/bin/spooky
```

### macOS

```bash
# Install dependencies via Homebrew
brew install cmake pkg-config rust

# Build and install
git clone https://github.com/nishujangra/spooky.git
cd spooky
cargo build --release
sudo install -m 755 target/release/spooky /usr/local/bin/spooky
```

### Windows

**Prerequisites:**
1. Install Rust from [rustup.rs](https://rustup.rs/)
2. Install Visual Studio Build Tools with C++ support from [Microsoft](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

**Build:**
```powershell
git clone https://github.com/nishujangra/spooky.git
cd spooky
cargo build --release
```

Binary location: `target\release\spooky.exe`

## Docker Deployment

**Dockerfile:**
```dockerfile
FROM rust:1.70-slim as builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/spooky /usr/local/bin/spooky

EXPOSE 9889/udp
CMD ["spooky"]
```

**Build and run:**
```bash
docker build -t spooky:latest .

docker run -d \
  --name spooky \
  -p 9889:9889/udp \
  -v /path/to/config.yaml:/etc/spooky/config.yaml:ro \
  -v /path/to/certs:/etc/spooky/certs:ro \
  spooky:latest --config /etc/spooky/config.yaml
```

**Using Docker Compose:**
```yaml
version: '3.8'
services:
  spooky:
    build: .
    ports:
      - "9889:9889/udp"
    volumes:
      - ./config.yaml:/etc/spooky/config.yaml:ro
      - ./certs:/etc/spooky/certs:ro
    command: ["--config", "/etc/spooky/config.yaml"]
    restart: unless-stopped
```

## Installation Verification

```bash
# Verify binary is accessible
spooky --version

# Display help and available options
spooky --help

# Validate configuration syntax (startup validation happens before serving)
spooky --config /path/to/config.yaml
```

Expected output from `spooky --version`:
```
spooky 0.1.0
```

## Post-Installation Configuration

### 1. Create Configuration Directory

```bash
sudo mkdir -p /etc/spooky
sudo mkdir -p /etc/spooky/certs
sudo chown -R $(whoami) /etc/spooky
```

### 2. Generate TLS Certificates

**Self-signed certificates (development):**
```bash
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout /etc/spooky/certs/key.pem \
  -out /etc/spooky/certs/cert.pem \
  -days 365 \
  -subj "/CN=proxy.example.com"
```

For production certificates, see [TLS Configuration](../configuration/tls.md).

### 3. Create Base Configuration

Create `/etc/spooky/config.yaml`:

```yaml
version: 1

listen:
  protocol: http3
  port: 9889
  address: "0.0.0.0"
  tls:
    cert: "/etc/spooky/certs/cert.pem"
    key: "/etc/spooky/certs/key.pem"

upstream:
  default:
    load_balancing:
      type: "random"
    route:
      path_prefix: "/"
    backends:
      - id: "backend-1"
        address: "127.0.0.1:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

log:
  level: info
```

### 4. System Service Setup (Linux)

Create `/etc/systemd/system/spooky.service`:

```ini
[Unit]
Description=Spooky HTTP/3 to HTTP/2 Proxy
After=network.target

[Service]
Type=simple
User=spooky
Group=spooky
ExecStart=/usr/local/bin/spooky --config /etc/spooky/config.yaml
Restart=on-failure
RestartSec=5s

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/log/spooky

[Install]
WantedBy=multi-user.target
```

Create service user and enable:
```bash
sudo useradd -r -s /bin/false spooky
sudo systemctl daemon-reload
sudo systemctl enable spooky.service
sudo systemctl start spooky.service
```

### 5. Log Management

By default, Spooky logs to stderr (captured by journald under systemd). To write logs to a file instead, set `log.file.enabled: true` in your config:

```yaml
log:
  level: info
  file:
    enabled: true
    path: /var/log/spooky/spooky.log
```

Configure log rotation for file-based logging:

Create `/etc/logrotate.d/spooky`:

```
/var/log/spooky/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 0640 spooky spooky
    sharedscripts
    postrotate
        systemctl restart spooky.service >/dev/null 2>&1 || true
    endscript
}
```

## Troubleshooting Installation

**Build fails with linker errors:**
- Ensure build tools are installed: `cmake`, `pkg-config`, C compiler
- Update Rust toolchain: `rustup update`

**Permission denied when binding to port:**
- Use port > 1024, or grant capability: `sudo setcap CAP_NET_BIND_SERVICE=+eip /usr/local/bin/spooky`
- Run as privileged user (not recommended for production)

**Certificate errors on startup:**
- Verify certificate and key paths in configuration
- Check file permissions: certificates must be readable by the service user
- Validate certificate format: `openssl x509 -in cert.pem -text -noout`

**Binary not found after cargo install:**
- Add `~/.cargo/bin` to PATH: `export PATH="$HOME/.cargo/bin:$PATH"`
- Add to shell profile for persistence

## Next Steps

- [Configuration Reference](../configuration/reference.md) - Complete configuration options
- [TLS Setup Guide](../configuration/tls.md) - Production certificate management
- [Production Deployment](../deployment/production.md) - Production deployment best practices
- [Troubleshooting](../troubleshooting/common-issues.md) - Common issues and solutions