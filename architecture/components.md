# Component Architecture

This document provides a detailed breakdown of Spooky's modular component architecture, including responsibilities, APIs, and implementation details for each crate.

## Component Overview

Spooky is organized as a Rust workspace with the following crates:

| Crate | Path | Responsibility |
|-------|------|----------------|
| spooky | `spooky/` | Main binary and application lifecycle |
| spooky-edge | `crates/edge/` | QUIC listener and HTTP/3 session management |
| spooky-bridge | `crates/bridge/` | HTTP/3 to HTTP/2 protocol conversion |
| spooky-transport | `crates/transport/` | HTTP/2 client and connection pooling |
| spooky-lb | `crates/lb/` | Load balancing algorithms and health tracking |
| spooky-config | `crates/config/` | Configuration parsing and validation |
| spooky-utils | `crates/utils/` | TLS utilities and logging setup |
| spooky-errors | `crates/errors/` | Shared error types (minimal) |

## Main Application (`spooky`)

### Responsibilities

- Command-line argument parsing
- Configuration file loading
- Logger initialization
- QUIC listener creation
- Signal handling for graceful shutdown
- Event loop coordination

### Key Types

```rust
struct Cli {
    config: Option<String>,
}
```

### Main Flow

```rust
#[tokio::main]
async fn main() {
    // 1. Parse CLI arguments
    let cli = Cli::parse();

    // 2. Load configuration
    let config = spooky_config::loader::read_config(&config_path)?;

    // 3. Initialize logger
    spooky_utils::logger::init_logger(&config.log.level);

    // 4. Validate configuration
    spooky_config::validator::validate(&config);

    // 5. Create QUIC listener
    let mut listener = spooky_edge::QUICListener::new(config)?;

    // 6. Setup shutdown handler
    let shutdown = Arc::new(AtomicBool::new(false));
    tokio::spawn(signal_handler(shutdown.clone()));

    // 7. Main event loop
    while !shutdown.load(Ordering::Relaxed) {
        listener.poll();
    }

    // 8. Graceful shutdown
    listener.start_draining();
    while !listener.drain_complete() {
        listener.poll();
    }
}
```

### Dependencies

- `clap`: CLI argument parsing
- `tokio`: Async runtime
- `log`: Logging facade

## Edge Listener (`spooky-edge`)

### Responsibilities

- UDP socket binding and management
- QUIC connection lifecycle (handshake, packet processing, closure)
- HTTP/3 session establishment via quiche
- Stream state management
- Request envelope construction
- Backend request forwarding
- Response streaming back to client
- Connection draining for graceful shutdown
- Metrics collection

### Key Types

```rust
pub struct QUICListener {
    pub socket: UdpSocket,
    pub config: Config,
    pub quic_config: quiche::Config,
    pub h3_config: Arc<quiche::h3::Config>,
    pub h2_pool: Arc<H2Pool>,
    pub upstream_pools: HashMap<String, Arc<Mutex<UpstreamPool>>>,
    pub load_balancer: LoadBalancing,
    pub metrics: Metrics,
    pub draining: bool,
    pub drain_start: Option<Instant>,
    pub recv_buf: [u8; 65535],
    pub send_buf: [u8; 65535],
    pub connections: HashMap<Vec<u8>, QuicConnection>,
}

pub struct QuicConnection {
    pub quic: quiche::Connection,
    pub h3: Option<quiche::h3::Connection>,
    pub h3_config: Arc<quiche::h3::Config>,
    pub streams: HashMap<u64, RequestEnvelope>,
    pub peer_address: SocketAddr,
    pub last_activity: Instant,
}

pub struct RequestEnvelope {
    pub method: String,
    pub path: String,
    pub authority: Option<String>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub start: Instant,
}

pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_success: AtomicU64,
    pub requests_failure: AtomicU64,
    pub backend_timeouts: AtomicU64,
    pub backend_errors: AtomicU64,
}
```

### Public API

```rust
impl QUICListener {
    /// Create new QUIC listener from configuration
    pub fn new(config: Config) -> Result<Self, ProxyError>;

    /// Process pending packets and events (main event loop)
    pub fn poll(&mut self);

    /// Begin graceful shutdown sequence
    pub fn start_draining(&mut self);

    /// Check if drain is complete
    pub fn drain_complete(&self) -> bool;
}

impl Metrics {
    pub fn inc_total(&self);
    pub fn inc_success(&self);
    pub fn inc_failure(&self);
    pub fn inc_timeout(&self);
    pub fn inc_backend_error(&self);
}
```

### Implementation Details

**Socket Management:**
- UDP socket bound to configured address:port
- Non-blocking mode for poll-based processing
- Fixed-size receive/send buffers (65535 bytes, max UDP payload)

**QUIC Configuration:**
- Built using quiche::Config
- TLS credentials loaded from configured cert/key paths
- HTTP/3 ALPN ("h3") configured
- Connection ID generation using HMAC-SHA256 (stateless retry tokens)

**Connection Tracking:**
- Connections keyed by Server Connection ID (SCID)
- Connection state includes QUIC and HTTP/3 layers
- Last activity timestamp for idle timeout detection

**Request Processing:**
1. Receive UDP datagram
2. Identify connection by DCID or create new connection
3. Feed packet to quiche
4. Poll HTTP/3 events
5. On headers: create RequestEnvelope
6. On body data: accumulate in envelope
7. On stream finished: process complete request
8. Route to upstream, select backend, forward request
9. Stream response back to client

**Upstream Routing:**
- Routes are matched by path prefix and optional host
- Longest matching path wins for overlapping routes
- No match returns error to client

**Health Integration:**
- Successful backend responses call `mark_success()`
- Failed/timed out responses call `mark_failure()`
- Health transitions are logged

**Graceful Shutdown:**
- `start_draining()` sets draining flag and records start time
- No new requests accepted during drain
- `drain_complete()` returns true when all connections closed or timeout reached
- Drain timeout: 5 seconds

### Error Handling

```rust
pub enum ProxyError {
    Bridge(BridgeError),
    Transport(String),
    Timeout,
    Tls(String),
}
```

Errors are logged and result in appropriate HTTP error responses to clients where possible.

### Dependencies

- `quiche`: QUIC and HTTP/3 implementation
- `tokio`: Async backend request execution
- `bytes`: Efficient byte buffer handling
- `http`: HTTP types for request construction
- `spooky-config`: Configuration types
- `spooky-lb`: Load balancing and health tracking
- `spooky-bridge`: Protocol conversion
- `spooky-transport`: HTTP/2 backend communication

## Protocol Bridge (`spooky-bridge`)

### Responsibilities

- Convert HTTP/3 requests to HTTP/2 format
- Normalize headers between protocol versions
- Handle pseudo-headers (:method, :path, :authority, :scheme)
- Filter hop-by-hop headers
- Construct proper HTTP/2 request objects

### Key Types

```rust
pub enum BridgeError {
    InvalidMethod,
    InvalidUri,
    InvalidHeader,
    Build(http::Error),
}
```

### Public API

```rust
pub fn build_h2_request(
    backend: &str,
    method: &str,
    path: &str,
    headers: &[(Vec<u8>, Vec<u8>)],
    body: &[u8],
) -> Result<Request<Full<Bytes>>, BridgeError>;
```

### Implementation Details

**Method Conversion:**
- HTTP/3 method string parsed into `http::Method`
- Validation ensures method is valid HTTP method

**URI Construction:**
- Backend address combined with request path
- Format: `http://{backend}{path}`
- Empty path defaults to "/"
- Parsed into `http::Uri`

**Header Processing:**
1. Skip pseudo-headers (starting with ':')
2. Filter hop-by-hop headers (Connection, Keep-Alive, Transfer-Encoding, Upgrade)
3. Skip Content-Length (recalculated from body)
4. Ensure Host header present (from authority or backend address)
5. Set Content-Length if body is non-empty

**Body Handling:**
- Body copied into `Full<Bytes>` for hyper compatibility
- Content-Length header set based on actual body size

**Edge Cases:**
- Missing Host: defaults to backend address
- Empty path: defaults to "/"
- Invalid headers: return BridgeError::InvalidHeader

### Error Handling

All errors are propagated to caller with specific error types. Invalid requests are not sent to backends.

### Dependencies

- `http`: HTTP types (Request, Method, Uri, HeaderName, HeaderValue)
- `http-body-util`: Body utilities (Full)
- `bytes`: Byte buffer types
- `quiche`: HTTP/3 header types (NameValue)

## Transport Layer (`spooky-transport`)

### Responsibilities

- Maintain HTTP/2 connections to backend servers
- Connection pooling and reuse
- Request multiplexing over HTTP/2 connections
- Flow control via semaphore-based concurrency limiting
- Request forwarding with timeout handling

### Key Types

```rust
pub struct H2Pool {
    backends: HashMap<String, BackendHandle>,
}

struct BackendHandle {
    client: H2Client,
    inflight: Arc<Semaphore>,
}

pub struct H2Client {
    client: Client<HttpConnector, Full<Bytes>>,
}

pub enum PoolError {
    UnknownBackend(String),
    Send(hyper_util::client::legacy::Error),
}
```

### Public API

```rust
impl H2Pool {
    /// Create new pool with specified backends and concurrency limit
    pub fn new<I>(backends: I, max_inflight: usize) -> Self
    where
        I: IntoIterator<Item = String>;

    /// Check if backend exists in pool
    pub fn has_backend(&self, backend: &str) -> bool;

    /// Send request to specified backend
    pub async fn send(
        &self,
        backend: &str,
        req: Request<Full<Bytes>>,
    ) -> Result<Response<Incoming>, PoolError>;
}

impl H2Client {
    /// Create new HTTP/2-only client
    pub fn new() -> Self;

    /// Send request over HTTP/2
    pub async fn send(
        &self,
        req: Request<Full<Bytes>>,
    ) -> Result<Response<Incoming>, hyper_util::client::legacy::Error>;
}
```

### Implementation Details

**Connection Pooling:**
- One `BackendHandle` per backend address
- Handle contains HTTP/2 client and concurrency limiter
- Connections created lazily on first request
- Connection reuse managed automatically by hyper

**Concurrency Control:**
- Semaphore limits concurrent requests per backend
- Default: 64 concurrent requests per backend
- Backpressure applied when limit reached (async wait)
- Permit released when request completes

**HTTP/2 Client:**
- Built using hyper legacy client API
- HTTP/2-only mode enforced
- HttpConnector with HTTP enforcement disabled (allows non-HTTPS URIs)
- TokioExecutor for spawning connection tasks

**Error Handling:**
- Unknown backend: `PoolError::UnknownBackend`
- Connection errors: `PoolError::Send` wrapping hyper error
- Timeout handled by caller (edge layer)

### Dependencies

- `hyper`: HTTP client implementation
- `hyper-util`: Connection pooling and legacy client
- `http-body-util`: Body utilities
- `tokio`: Async runtime
- `bytes`: Byte buffers

## Load Balancer (`spooky-lb`)

### Responsibilities

- Backend selection algorithms (Random, Round Robin, Consistent Hash)
- Health state tracking per backend
- Failure threshold detection
- Recovery threshold tracking
- Cooldown period management
- Upstream pool management

### Key Types

```rust
pub struct BackendState {
    address: String,
    weight: u32,
    health_check: HealthCheck,
    consecutive_failures: u32,
    health_state: HealthState,
}

enum HealthState {
    Healthy,
    Unhealthy { until: Instant, successes: u32 },
}

pub enum HealthTransition {
    BecameHealthy,
    BecameUnhealthy,
}

pub struct BackendPool {
    backends: Vec<BackendState>,
}

pub struct UpstreamPool {
    pub pool: BackendPool,
    pub strategy: String,
}

pub enum LoadBalancing {
    RoundRobin(RoundRobin),
    ConsistentHash(ConsistentHash),
    Random(Random),
}

pub struct RoundRobin {
    next: usize,
}

pub struct ConsistentHash {
    replicas: u32,
}

pub struct Random;
```

### Public API

```rust
impl BackendState {
    pub fn new(backend: &Backend) -> Self;
    pub fn is_healthy(&self) -> bool;
    pub fn address(&self) -> &str;
    pub fn health_check(&self) -> &HealthCheck;
    pub fn weight(&self) -> u32;
    pub fn record_success(&mut self) -> Option<HealthTransition>;
    pub fn record_failure(&mut self) -> Option<HealthTransition>;
}

impl BackendPool {
    pub fn new_from_states(backends: Vec<BackendState>) -> Self;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn address(&self, index: usize) -> Option<&str>;
    pub fn mark_success(&mut self, index: usize) -> Option<HealthTransition>;
    pub fn mark_failure(&mut self, index: usize) -> Option<HealthTransition>;
    pub fn health_check(&self, index: usize) -> Option<HealthCheck>;
    pub fn healthy_indices(&self) -> Vec<usize>;
    pub fn all_indices(&self) -> Vec<usize>;
    pub fn backend(&self, index: usize) -> Option<&BackendState>;
}

impl UpstreamPool {
    pub fn from_upstream(upstream: &Upstream) -> Result<Self, String>;
}

impl LoadBalancing {
    pub fn from_config(value: &str) -> Result<Self, String>;
    pub fn pick(&mut self, key: &str, pool: &UpstreamPool) -> Option<usize>;
}

impl RoundRobin {
    pub fn new() -> Self;
    pub fn pick(&mut self, pool: &BackendPool) -> Option<usize>;
}

impl ConsistentHash {
    pub fn new(replicas: u32) -> Self;
    pub fn pick(&self, key: &str, pool: &BackendPool) -> Option<usize>;
}

impl Random {
    pub fn new() -> Self;
    pub fn pick(&mut self, pool: &BackendPool) -> Option<usize>;
}
```

### Implementation Details

**Health State Machine:**

```
Healthy
  │
  │ (failures >= failure_threshold)
  ▼
Unhealthy { until: cooldown_end, successes: 0 }
  │
  │ (now >= cooldown_end && successes >= success_threshold)
  ▼
Healthy
```

**Health Tracking:**
- `record_success()`: Resets consecutive failures if healthy; increments success counter if unhealthy
- `record_failure()`: Increments consecutive failures; transitions to unhealthy when threshold reached
- Transitions return `Option<HealthTransition>` for logging

**Backend Selection:**
- Only healthy backends are candidates
- If no healthy backends, returns None
- Each algorithm filters to healthy backends before selection

**Round Robin:**
- Maintains next index counter
- Wraps around when reaching end of healthy backends
- Ensures even distribution across healthy backends

**Consistent Hash:**
- Builds hash ring with virtual nodes (replicas)
- Replica count = base_replicas * backend_weight
- Hash function: FNV-1a (fast, good distribution)
- Key is hashed, closest node on ring is selected
- Same key always routes to same backend (session affinity)

**Random:**
- Simple random selection from healthy backends
- Uses thread-local RNG for performance
- Uniform distribution

**Weight Support:**
- Weights affect consistent hash replica count
- Higher weight = more virtual nodes = more traffic
- Round robin and random ignore weights currently

### Algorithm Selection

Supported configuration strings:
- "round-robin", "round_robin", "rr" → RoundRobin
- "consistent-hash", "consistent_hash", "ch" → ConsistentHash
- "random" → Random

Default replicas for consistent hash: 64

### Dependencies

- `rand`: Random number generation
- `spooky-config`: Configuration types (Backend, HealthCheck, Upstream)
- Standard library: BTreeMap for hash ring, Duration/Instant for timing

## Configuration System (`spooky-config`)

### Responsibilities

- YAML configuration parsing
- Configuration structure definitions
- Default value provision
- Configuration validation
- Error reporting for invalid configurations

### Key Types

```rust
pub struct Config {
    pub version: u32,
    pub listen: Listen,
    pub upstream: HashMap<String, Upstream>,
    pub load_balancing: Option<LoadBalancing>,
    pub log: Log,
}

pub struct Listen {
    pub protocol: String,
    pub port: u32,
    pub address: String,
    pub tls: Tls,
}

pub struct Tls {
    pub cert: String,
    pub key: String,
}

pub struct Upstream {
    pub load_balancing: LoadBalancing,
    pub route: RouteMatch,
    pub backends: Vec<Backend>,
}

pub struct Backend {
    pub id: String,
    pub address: String,
    pub weight: u32,
    pub health_check: HealthCheck,
}

pub struct RouteMatch {
    pub host: Option<String>,
    pub path_prefix: Option<String>,
    pub method: Option<String>,
}

pub struct HealthCheck {
    pub path: String,
    pub interval: u64,
    pub timeout_ms: u64,
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub cooldown_ms: u64,
}

pub struct LoadBalancing {
    pub lb_type: String,
    pub key: Option<String>,
}

pub struct Log {
    pub level: String,
}
```

### Public API

```rust
// loader.rs
pub fn read_config(path: &str) -> Result<Config, String>;

// validator.rs
pub fn validate(config: &Config) -> bool;
```

### Implementation Details

**Configuration Loading:**
1. Read file from path
2. Parse YAML via serde_yaml
3. Apply default values via serde defaults
4. Return Config or error message

**Default Values:**
- version: 1
- protocol: "http3"
- port: 9889
- address: "0.0.0.0"
- log level: "info"
- backend weight: 100
- health check path: "/health"
- health check interval: 5000ms
- health timeout: 1000ms
- failure threshold: 3
- success threshold: 2
- cooldown: 5000ms

**Validation Checks:**
1. TLS certificate file exists and is readable
2. TLS key file exists and is readable
3. At least one upstream configured
4. Each upstream has at least one backend
5. Backend addresses are non-empty
6. Log level is valid

**Error Handling:**
- File not found: clear error message with path
- Parse errors: YAML line/column information
- Validation errors: specific validation failure message

### Dependencies

- `serde`: Serialization framework
- `serde_yaml`: YAML parsing
- `log`: Logging

## Utilities (`spooky-utils`)

### Responsibilities

- TLS certificate and key loading
- Logging initialization
- Common helper functions

### Modules

**tls.rs:**
```rust
pub fn load_certs(path: &str) -> Result<Vec<Certificate>, String>;
pub fn load_private_key(path: &str) -> Result<PrivateKey, String>;
```

**logger.rs:**
```rust
pub fn init_logger(level: &str);
```

### Implementation Details

**TLS Loading:**
- Reads PEM files from filesystem
- Parses DER-encoded certificates
- Validates format
- Returns rustls-compatible types

**Logger Initialization:**
- Configures env_logger with specified level
- Maps custom log levels (if configured)
- Enables timestamp and module path

### Dependencies

- `rustls-pki-types`: TLS types
- `env_logger`: Logging implementation
- `log`: Logging facade

## Component Interaction Flow

### Request Path

```
Client HTTP/3 Request
         ▼
[spooky-edge::QUICListener]
  ├─ Receive QUIC packets
  ├─ Decode HTTP/3 headers
  └─ Create RequestEnvelope
         ▼
[spooky-edge::quic_listener::find_upstream_for_request]
  ├─ Match path_prefix and host
  └─ Select upstream pool
         ▼
[spooky-lb::LoadBalancing::pick]
  ├─ Filter to healthy backends
  ├─ Apply algorithm
  └─ Return backend index
         ▼
[spooky-bridge::build_h2_request]
  ├─ Convert HTTP/3 → HTTP/2
  ├─ Normalize headers
  └─ Construct Request<Full<Bytes>>
         ▼
[spooky-transport::H2Pool::send]
  ├─ Acquire semaphore permit
  ├─ Get backend client
  └─ Forward request
         ▼
Backend HTTP/2 Server
         ▼
[spooky-edge::QUICListener]
  ├─ Receive response
  ├─ Update health state
  ├─ Update metrics
  └─ Stream response to client
```

### Configuration Path

```
[spooky::main]
  └─ Parse CLI args
         ▼
[spooky-config::loader::read_config]
  ├─ Read YAML file
  └─ Parse with serde
         ▼
[spooky-config::validator::validate]
  ├─ Check TLS files
  ├─ Validate structure
  └─ Return bool
         ▼
[spooky-edge::QUICListener::new]
  ├─ Load TLS via spooky-utils
  ├─ Create H2Pool with backends
  ├─ Create UpstreamPools
  └─ Initialize load balancers
         ▼
Runtime
```

## Testing Strategy

### Unit Tests

Each crate includes unit tests for core functionality:

**spooky-lb:**
- Round robin cycling behavior
- Consistent hash stability
- Health state transitions
- Backend recovery
- Empty pool handling

**spooky-bridge:**
- Header conversion
- Pseudo-header handling
- URI construction
- Error cases

**spooky-config:**
- YAML parsing
- Default value application
- Validation logic

**spooky-transport:**
- Pool initialization
- Backend existence checks

### Integration Tests

**spooky-edge:**
- Full request/response flow
- Health check integration
- Upstream routing
- Multiple load balancing strategies

### Test Execution

```bash
# All tests
cargo test

# Specific crate
cargo test -p spooky-lb

# Integration tests only
cargo test -p spooky-edge --test lb_integration
```

## Performance Optimization

### Hot Path Optimizations

**Zero-Copy Where Possible:**
- UDP receive buffer reused
- QUIC packet processing avoids allocations
- Header slices avoid string copies

**Lock-Free Metrics:**
- AtomicU64 for counters
- No mutex on request path

**Connection Pooling:**
- HTTP/2 connection reuse
- Amortize handshake cost

**Async I/O:**
- Backend requests with full body buffering (streaming planned)
- Efficient task scheduling via Tokio

### Memory Management

**Fixed Buffers:**
- 64KB receive/send buffers per listener
- No per-packet allocation

**Bounded Collections:**
- Connection map grows with active connections
- Stream map per connection, cleared on completion

**Reference Counting:**
- Arc for shared config and pools
- Amortize clone cost

## Deployment Considerations

### Binary Distribution

Single statically-linked binary containing all components. No runtime dependencies except system TLS libraries.

### Resource Requirements

- File descriptors: 2 per backend + connection count
- Memory: ~1-2KB per QUIC connection + connection pools
- CPU: Scales with core count via Tokio runtime
- Network: UDP port for client traffic, TCP for backends

### Operational Checklist

1. TLS certificates and keys readable by process user
2. UDP port accessible for QUIC traffic
3. Backend addresses reachable from proxy
4. File descriptor limits sufficient (ulimit -n)
5. Configuration validated before deployment
6. Logging configured appropriately for environment

### Monitoring Recommendations

- Track requests_total, requests_success, requests_failure
- Monitor backend_timeouts and backend_errors
- Alert on health state transitions
- Log slow requests (duration tracking)
- Monitor connection count
- Track memory usage growth
