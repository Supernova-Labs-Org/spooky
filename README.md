# Spooky

**HTTP/3 to HTTP/2 reverse proxy and load balancer**

Spooky terminates HTTP/3/QUIC connections at the edge and forwards requests to HTTP/2 backends. Built in Rust for production environments requiring HTTP/3 client support without modifying existing infrastructure.

## Overview

Modern clients increasingly expect HTTP/3 support, but most production backends still use HTTP/2. Spooky bridges this gap by:

- Terminating QUIC connections with TLS 1.3
- Converting HTTP/3 streams to HTTP/2 requests
- Load balancing across backend pools with health checks
- Supporting path and host-based routing

## Quick Start

```bash
# Build release binary
cargo build --release

# Generate self-signed certificates
make certs-selfsigned

# Run with default configuration
./target/release/spooky --config config/config.yaml

# Test with HTTP/3 client
curl --http3-only -k \
  --resolve proxy.spooky.local:9889:127.0.0.1 \
  https://proxy.spooky.local:9889/api/health
```

## System Requirements

- **Rust**: 1.85 or later (edition 2024)
- **OS**: Linux
- **Permissions**: Must run as root (required for QUIC/UDP socket binding)
- **Network**: UDP port access for QUIC traffic
- **Memory**: 256MB minimum, 1GB recommended

### Build Dependencies

```bash
# Ubuntu/Debian
sudo apt install cmake build-essential pkg-config

# macOS
brew install cmake pkg-config
```

## Configuration

Spooky uses YAML configuration with validation at startup. See [configuration reference](docs/configuration/reference.md) for complete documentation.

### Minimal Example

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
        health_check:
          path: "/health"
          interval: 5000

log:
  level: info
```

### Key Configuration Features

**Upstream Pools**: Define multiple named upstream groups. Each pool configures its own routing rules and load balancing strategy independently.

**Routing**: Route requests based on path prefix and hostname. The most specific match (longest prefix) wins.

**Load Balancing**: Per-upstream pool strategies: random, round-robin, and consistent hashing.

**Health Checks**: Automatic backend health monitoring with configurable intervals, timeouts, and thresholds.

## Architecture

Spooky uses a modular architecture with clear separation of concerns:

```mermaid
flowchart LR
    client["HTTP/3<br/>Client"] -->|QUIC/TLS| quic

    subgraph edge["Spooky Edge"]
        direction TB
        quic["QUIC Listener<br/>(quiche)"] --> router["Router"] --> pool["HTTP/2 Pool"]
    end

    pool -->|HTTP/2| backend["Backend<br/>Servers"]

    quic -.- quic_note["Connection management<br/>Stream multiplexing<br/>Protocol bridging"]
    router -.- router_note["Path/host matching<br/>Upstream selection<br/>Load balancing"]
    pool -.- pool_note["Connection pooling<br/>Request forwarding<br/>Health checking"]

    classDef note fill:#F8FAFC,stroke:#94A3B8,stroke-width:1px,stroke-dasharray: 5 3,color:#334155;
    class quic_note,router_note,pool_note note;
```

### Components

- **Edge** (`crates/edge`): QUIC termination, HTTP/3 session management
- **Bridge** (`crates/bridge`): HTTP/3 to HTTP/2 protocol conversion
- **Transport** (`crates/transport`): HTTP/2 connection pooling
- **Load Balancer** (`crates/lb`): Backend selection algorithms and health tracking
- **Config** (`crates/config`): Configuration parsing and validation

## Features

**Core Functionality**
- HTTP/3 and QUIC (RFC 9114, RFC 9000)
- TLS 1.3 with certificate chain validation
- HTTP/2 backend connectivity
- Efficient request/response handling with full buffering

**Load Balancing**
- Random distribution
- Round-robin rotation
- Consistent hashing (with configurable replicas)
- Per-upstream strategy configuration

**Routing**
- Path prefix matching
- Host-based routing
- Longest-match selection for overlapping routes

**Health Management**
- Active health checks with HTTP probes
- Configurable failure thresholds and cooldown periods
- Automatic backend removal and recovery

**Observability**
- Structured logging with multiple levels (including Spooky-themed aliases)
- File-based log output via `log.file.enabled` and `log.file.path`
- Backend latency tracking
- Health transition logging

## Testing

```bash
# Run all tests
cargo test

# Run specific component tests
cargo test -p spooky-config
cargo test -p spooky-lb
cargo test -p spooky-edge

# Run integration tests
cargo test -p spooky-edge --test lb_integration
```


## Project Status

**Experimental.** Spooky is not production-ready. Core features are implemented and functional, but significant limitations remain (blocking backend I/O, full body buffering, no TLS peer verification, single-threaded QUIC processing).

See [roadmap](docs/roadmap.md) for known issues and planned improvements.

## Documentation

- [Architecture Overview](docs/architecture.md)
- [Configuration Reference](docs/configuration/reference.md)
- [TLS Setup Guide](docs/configuration/tls.md)
- [Load Balancing Guide](docs/user-guide/load-balancing.md)
- [Production Deployment](docs/deployment/production.md)
- [Troubleshooting](docs/troubleshooting/common-issues.md)

## Development

See [contributing guide](docs/development/contributing.md) for development setup and guidelines.

```bash
# Development build
cargo build

# Run with debug logging
RUST_LOG=debug cargo run -- --config config/config.yaml

# Format code
cargo fmt

# Lint
cargo clippy
```

## License

Elastic License 2.0 (ELv2) - see [LICENSE](LICENSE.md)
