## Contributing to Spooky

Spooky is an HTTP/3 edge proxy in Rust. It terminates QUIC at the
edge and forwards to HTTP/2 backends.

### Setup

Requirements: Rust 1.85+, cmake, pkg-config

```sh
git clone https://github.com/Supernova-Labs-Org/spooky.git
cd spooky
cargo build
    cargo test
```

Before touching anything:

```sh
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

### Code Layout

```text
    spooky/            entry point and initialization
    crates/config/     YAML parsing and validation
    crates/edge/       QUIC/HTTP3 listener, TLS handshake
    crates/bridge/     HTTP/3 to HTTP/2 header and stream conversion
    crates/transport/  HTTP/2 client, connection pool
    crates/lb/         load balancing algorithms (random, round-robin, consistent-hash)
    crates/utils/      logging, TLS helpers
    crates/errors/     shared error types
```

### Submitting Patches

Branch off master. One thing per branch. Keep commits atomic.

```sh
Commit format (conventional commits):

    feat: per-upstream load balancing
    fix: route matching with empty path prefix
    docs: update TLS configuration reference
    refactor: simplify connection state machine
```

Do not force-push after a review has started.

### Testing

Unit tests go in the same file as the code. Integration tests go under
crates/<name>/tests/.

All tests must pass:

    cargo test --workspace

### Coding Style

Clippy catches the obvious mistakes.

```sh
cargo clippy
```

Use Result for fallible operations. Name things clearly. No abbreviations
in public APIs.

## Questions

Open an issue on GitHub.
