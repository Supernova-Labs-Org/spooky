# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-04-16

Initial release of Spooky HTTP/3 edge proxy and load balancer.

### Core Features

**Protocol Support**
- HTTP/3 termination using quiche (RFC 9114)
- QUIC transport (RFC 9000)
- HTTP/2 backend connectivity (RFC 9113)
- TLS 1.3 with certificate chain loading (RFC 8446)

**Routing and Load Balancing**
- Upstream pool architecture with per-upstream configuration
- Route matching based on path prefix and host headers with longest-match selection
- Multiple load balancing algorithms:
  - Random distribution
  - Round-robin rotation
  - Consistent hashing with 64 replicas per backend
- Backend weight configuration for weighted load balancing

**Health Management**
- Active health checking with HTTP probes
- Configurable health check parameters:
  - Interval and timeout
  - Failure and success thresholds
  - Cooldown period for unhealthy backends
- Automatic backend removal and recovery
- Health state transitions logged for monitoring

**Connection Management**
- Connection ID-based routing for QUIC packets
- Prefix matching for Short packets with extended DCIDs
- Peer-based fallback for connection migration scenarios
- Version negotiation packet handling
- Proper 0-RTT handling to prevent crypto failures
- Graceful shutdown with connection draining (5s timeout)

**Configuration**
- YAML-based configuration with comprehensive validation
- Per-upstream load balancing strategy
- Embedded routing rules (path_prefix, host)
- Global and per-upstream load balancing fallbacks
- Configuration validated at startup with detailed error messages

**Observability**
- Structured logging with multiple levels:
  - Standard: trace, debug, info, warn, error
  - Spooky-themed: whisper, haunt, spooky, scream, poltergeist, silence
- Request/response metrics collection:
  - Total requests, successes, failures
  - Backend timeouts and errors
- Backend selection and health transition logging
- QUIC connection state debugging

**Operational**
- CLI with configuration file support
- Streaming request and response handling with bounded queues and hard body caps
- Deterministic cap-breach behavior via HTTP errors (`413`/`503`) under pressure
- Concurrent connection handling (10,000+ connections tested)
- Per-backend concurrency limiting (64 max in-flight requests)
- Backend timeout configuration (2s default)

### Architecture

**Crate Structure**
- `spooky`: Main binary and CLI
- `spooky-edge`: QUIC listener and HTTP/3 handling
- `spooky-bridge`: HTTP/3 to HTTP/2 protocol conversion
- `spooky-transport`: HTTP/2 connection pooling
- `spooky-lb`: Load balancing algorithms and health management
- `spooky-config`: Configuration parsing and validation
- `spooky-utils`: TLS and logging utilities

**Design**
- Synchronous UDP polling with 50ms timeout
- Async backend calls via Tokio runtime
- Health check tasks running asynchronously per backend
- Connection pooling with semaphore-based concurrency control

### Documentation

**User Documentation**
- Complete installation guide with system requirements
- Configuration reference with all options documented
- TLS setup guide with certificate generation procedures
- User guides for basic usage and load balancing
- Quick start tutorial for rapid deployment
- Production deployment guide with security hardening
- Comprehensive troubleshooting documentation

**Technical Documentation**
- Architecture overview and component interaction
- Detailed component breakdown by crate
- HTTP/3 and QUIC protocol documentation
- API documentation for metrics and logging
- Memory envelope and bounded-overflow behavior documentation
- Contributing guide for developers
- References to RFCs and external resources

### Known Limitations

These limitations are being addressed in future releases:

1. **Performance**
   - Single-threaded QUIC packet processing

2. **Operational**
   - No configuration hot reload
   - No distributed tracing integration
   - TLS peer verification disabled (development mode)

3. **Features**
   - No dynamic backend discovery

See [roadmap](docs/roadmap.md) for planned improvements.

### Configuration Example

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
      type: "consistent-hash"
    route:
      path_prefix: "/api"
    backends:
      - id: "api-1"
        address: "127.0.0.1:8001"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
          timeout_ms: 2000
          failure_threshold: 3
          success_threshold: 2
          cooldown_ms: 10000

  default_backend:
    load_balancing:
      type: "round-robin"
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

---

## Version Numbering

- **Major version** (X.0.0): Breaking changes to configuration or API
- **Minor version** (0.X.0): New features, non-breaking changes
- **Patch version** (0.0.X): Bug fixes, documentation updates

## Contributing

See [contributing guide](docs/development/contributing.md) for development guidelines.

## License

Elastic License 2.0 (ELv2) - see [LICENSE.md](LICENSE.md)
