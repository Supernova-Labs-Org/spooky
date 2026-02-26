# Overview

Spooky is an HTTP/3 to HTTP/2 reverse proxy and load balancer. It terminates QUIC connections at the edge and forwards requests to HTTP/2 backends, enabling HTTP/3 client support without modifying existing infrastructure.

## What Spooky Does

Spooky bridges the gap between modern HTTP/3 clients and production HTTP/2 backends by:

- Terminating QUIC connections with TLS 1.3
- Converting HTTP/3 streams to HTTP/2 requests
- Distributing load across backend pools with active health checks
- Routing requests based on path prefix and hostname patterns

## Architecture

```
HTTP/3 Client → QUIC/TLS → Spooky Edge → HTTP/2 → Backend Servers
```

**Core Components:**

- **Edge**: QUIC termination and HTTP/3 session management
- **Bridge**: Protocol conversion between HTTP/3 and HTTP/2
- **Transport**: HTTP/2 connection pooling and lifecycle management
- **Load Balancer**: Backend selection algorithms and health tracking
- **Router**: Path and host-based request routing

## Key Features

**Protocol Support**
- HTTP/3 and QUIC (RFC 9114, RFC 9000)
- TLS 1.3 with certificate chain validation
- HTTP/2 backend connectivity

**Load Balancing**
- Random distribution
- Round-robin rotation (default)
- Consistent hashing with configurable virtual nodes
- Global load balancing strategy (same for all upstreams)

**Routing**
- Path prefix matching with longest-match selection
- Host-based routing
- Multiple upstream pools with independent configurations

**Health Management**
- Active HTTP health checks with configurable intervals
- Automatic backend removal on failure threshold
- Cooldown periods for recovery

## System Requirements

**Runtime Requirements:**
- Rust 1.85 or later (edition 2024)
- Linux, macOS, or Windows
- UDP port access for QUIC traffic
- 256MB RAM minimum (1GB recommended for production)

**Build Dependencies:**

```bash
# Ubuntu/Debian
sudo apt install cmake build-essential pkg-config

# macOS
brew install cmake pkg-config
```

## Quick Start

```bash
# Clone and build
git clone https://github.com/nishujangra/spooky.git
cd spooky
cargo build --release

# Generate certificates
make certs-selfsigned

# Start proxy
./target/release/spooky --config config/config.yaml
```

## Configuration Example

Spooky uses YAML configuration with validation at startup:

```yaml
version: 1

listen:
  protocol: http3
  port: 9889
  address: "0.0.0.0"
  tls:
    cert: "certs/cert.pem"
    key: "certs/key.pem"

upstream:
  api_backend:
    load_balancing:
      type: "round-robin"
    route:
      path_prefix: "/api"
    backends:
      - id: "api-1"
        address: "127.0.0.1:8001"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

  default_backend:
    load_balancing:
      type: "random"
    route:
      path_prefix: "/"
    backends:
      - id: "default-1"
        address: "127.0.0.1:8080"
        weight: 100

log:
  level: info
```

## Testing Connectivity

Verify the proxy is functioning with an HTTP/3 client:

```bash
curl --http3-only -k \
  --resolve proxy.example.com:9889:127.0.0.1 \
  https://proxy.example.com:9889/api/health
```

## Project Status

**Spooky is experimental.** Core features are implemented and functional, but the project is not production-ready. Expect rough edges, missing features, and breaking changes.

Currently working:

- QUIC termination and HTTP/3 support
- HTTP/2 backend forwarding with connection pooling
- Multiple load balancing algorithms
- Active health checking with automatic recovery
- Path and host-based routing with upstream pools

## Next Steps

- [Installation Guide](installation.md) - Complete installation instructions
- [Configuration Reference](../configuration/reference.md) - Full configuration documentation
- [TLS Setup](../configuration/tls.md) - Certificate generation and configuration
- [Load Balancing Guide](../user-guide/load-balancing.md) - Backend selection strategies
- [Production Deployment](../deployment/production.md) - Production deployment guidelines
