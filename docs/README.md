# Spooky Documentation

Technical documentation for the Spooky HTTP/3 to HTTP/2 reverse proxy and load balancer.

## Quick Navigation

### Getting Started
- [Overview](getting-started/overview.md) - Project introduction and capabilities
- [Installation](getting-started/installation.md) - System requirements and installation procedures
- [Quick Start Tutorial](tutorials/quickstart.md) - Step-by-step guide to get running

### Configuration
- [Configuration Reference](configuration/reference.md) - Complete configuration documentation
- [TLS Setup](configuration/tls.md) - Certificate generation and management

### User Guides
- [Basic Usage](user-guide/basics.md) - Core concepts and usage patterns
- [Load Balancing](user-guide/load-balancing.md) - Load balancing algorithms and health checks

### Architecture
- [Architecture Overview](architecture.md) - System design and component interaction
- [Component Details](architecture/overview.md) - High-level architectural principles
- [Component Breakdown](architecture/components.md) - Detailed crate documentation

### Deployment
- [Production Deployment](deployment/production.md) - Production deployment guide
- [Troubleshooting](troubleshooting/common-issues.md) - Common issues and solutions

### Development
- [Contributing Guide](development/contributing.md) - Development setup and guidelines

### Protocol Reference
- [HTTP/3 Protocol](protocols/http3.md) - HTTP/3 overview and implementation
- [QUIC Protocol](protocols/quic.md) - QUIC fundamentals and usage

### API and Observability
- [API Overview](api/overview.md) - Metrics, logging, and future admin API

### Planning
- [Roadmap](roadmap.md) - Feature roadmap and priorities

## Documentation Structure

- `README.md`: Documentation index (this page)
- `architecture.md`: Main architecture document
- `roadmap.md`: Project roadmap
- `getting-started/`: Overview and installation guides
- `configuration/`: Configuration reference and TLS setup
- `user-guide/`: Basic usage and load balancing guide
- `architecture/`: High-level design and component details
- `deployment/`: Production deployment guidance
- `troubleshooting/`: Common issues and fixes
- `development/`: Development and contribution guidance
- `tutorials/`: Quickstart walkthroughs
- `protocols/`: HTTP/3 and QUIC protocol notes
- `api/`: API and observability overview
- `internal/`: Internal architecture notes

## Quick References

### Common Configuration Tasks

**Basic upstream pool**:
```yaml
upstream:
  backend:
    route:
      path_prefix: "/"
    backends:
      - id: "backend-1"
        address: "127.0.0.1:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
```

*Note: Load balancing strategy is configured globally, not per upstream.*

**Path-based routing**:
```yaml
upstream:
  api:
    route:
      path_prefix: "/api"
    # ... backends

  web:
    route:
      path_prefix: "/"
    # ... backends
```

**Load balancing algorithms**:
- `random` - Random selection
- `round-robin` - Sequential rotation
- `consistent-hash` - Hash-based affinity

### Common Commands

**Start Spooky**:
```bash
spooky --config /etc/spooky/config.yaml
```

**Test HTTP/3 connection**:
```bash
curl --http3-only -k \
  --resolve proxy.example.com:9889:127.0.0.1 \
  https://proxy.example.com:9889/health
```

**Check configuration**:
```bash
spooky --config config.yaml  # Starts serving after validation
```

**View logs**:
```bash
# All logs
RUST_LOG=info spooky --config config.yaml

# Debug QUIC only
RUST_LOG=spooky_edge=debug spooky --config config.yaml

# Trace everything
RUST_LOG=trace spooky --config config.yaml
```

## Documentation Guidelines

This documentation follows these principles:

1. **Technical Accuracy**: All examples are based on the actual codebase
2. **Honest Status**: Capabilities and limitations are documented as-is
3. **Direct Communication**: Clear, concise technical writing
4. **Complete Coverage**: All configuration options documented
5. **Practical Examples**: Working code and configuration samples

## Contributing to Documentation

To improve documentation:

1. Check accuracy against source code
2. Test all examples and commands
3. Use clear, technical language
4. Include practical examples
5. Update this index when adding new docs

See [Contributing Guide](development/contributing.md) for more details.

## External Resources

- [QUIC RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html)
- [HTTP/3 RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html)
- [quiche Documentation](https://docs.rs/quiche/)
- [Rust Documentation](https://doc.rust-lang.org/)

## Getting Help

- Review troubleshooting guide: [docs/troubleshooting/common-issues.md](troubleshooting/common-issues.md)
- Check GitHub issues for community discussions
- Read protocol documentation for HTTP/3 and QUIC specifics

## License

Elastic License 2.0 (ELv2) - see [LICENSE](https://github.com/nishujangra/spooky/blob/master/LICENSE.md) for details.
