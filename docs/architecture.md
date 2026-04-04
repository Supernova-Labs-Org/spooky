# Architecture

## Overview

Spooky is a reverse proxy that terminates HTTP/3/QUIC connections and forwards requests to HTTP/2 backends. The architecture prioritizes correctness, observability, and operational simplicity.

## Design Principles

1. **Protocol Isolation**: QUIC termination is separate from HTTP/2 backend communication
2. **Fail Fast**: Configuration errors are caught at startup, not during runtime
3. **Health-Aware Routing**: Backend selection considers health state
4. **Observability First**: All state transitions and errors are logged

## Component Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Spooky Process                       │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │               Main Event Loop                       │ │
│  │  (Synchronous UDP polling with timeout)            │ │
│  └───────────┬────────────────────────────────────────┘ │
│              │                                           │
│  ┌───────────▼────────────────────────────────────────┐ │
│  │           QUIC Listener (crates/edge)              │ │
│  │  - UDP socket management                           │ │
│  │  - quiche connection handling                      │ │
│  │  - Hierarchical Connection ID routing (O(1) fast  │ │
│  │    path + O(k) radix trie for prefix matching)   │ │
│  │  - HTTP/3 stream multiplexing                      │ │
│  └───────────┬────────────────────────────────────────┘ │
│              │                                           │
│  ┌───────────▼────────────────────────────────────────┐ │
│  │         Router (find_upstream_for_request)         │ │
│  │  - Path prefix matching                            │ │
│  │  - Host header matching                            │ │
│  │  - Longest match selection                         │ │
│  └───────────┬────────────────────────────────────────┘ │
│              │                                           │
│  ┌───────────▼────────────────────────────────────────┐ │
│  │    Load Balancer (crates/lb)                       │ │
│  │  - Backend selection algorithms                    │ │
│  │  - Health state filtering                          │ │
│  │  - Per-upstream strategy                           │ │
│  └───────────┬────────────────────────────────────────┘ │
│              │                                           │
│  ┌───────────▼────────────────────────────────────────┐ │
│  │    Protocol Bridge (crates/bridge)                 │ │
│  │  - HTTP/3 to HTTP/2 header conversion             │ │
│  │  - Body buffering                                  │ │
│  └───────────┬────────────────────────────────────────┘ │
│              │                                           │
│  ┌───────────▼────────────────────────────────────────┐ │
│  │    HTTP/2 Pool (crates/transport)                  │ │
│  │  - Backend connection pooling                      │ │
│  │  - Request forwarding                              │ │
│  │  - Concurrency limiting                            │ │
│  └───────────┬────────────────────────────────────────┘ │
│              │                                           │
│  ┌───────────▼────────────────────────────────────────┐ │
│  │    Health Checker (async tasks)                    │ │
│  │  - Periodic HTTP probes                            │ │
│  │  - Backend state tracking                          │ │
│  │  - Health transition logging                       │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Request Flow

### 1. Connection Establishment

```
Client                  Spooky                    Backend
  │                       │                          │
  ├─ QUIC Initial ───────>│                          │
  │                       │                          │
  │<───── ServerHello ────┤                          │
  │                       │                          │
  ├─ Handshake ──────────>│                          │
  │                       │                          │
  │<───── Handshake ──────┤                          │
  │                       │                          │
  │    [Connection ID routing established]          │
```

**Key Points**:
- Server generates 16-byte SCID for each connection
- Connection stored by SCID for subsequent packet routing
- Prefix matching handles clients that extend DCID
- Peer-based fallback for connection migration

### 2. HTTP/3 Request Processing

```
  │                       │                          │
  ├─ HEADERS frame ──────>│                          │
  ├─ DATA frame ─────────>│                          │
  │                       │                          │
  │                       ├─ Route matching          │
  │                       ├─ Upstream selection      │
  │                       ├─ Backend selection       │
  │                       │                          │
  │                       ├─ HTTP/2 request ────────>│
  │                       │                          │
  │                       │<──── HTTP/2 response ────┤
  │                       │                          │
  │<── HEADERS frame ─────┤                          │
  │<── DATA frame ────────┤                          │
```

**Processing Steps**:

1. **Stream Reception**: HTTP/3 frames decoded via quiche
2. **Request Envelope**: Headers, path, authority, and body buffered
3. **Route Matching**: Find upstream with longest matching path prefix
4. **Load Balancing**: Select healthy backend from upstream pool
5. **Protocol Bridge**: Convert HTTP/3 request to HTTP/2
6. **Backend Call**: Forward via connection pool with timeout
7. **Response Streaming**: Convert HTTP/2 response to HTTP/3

### 3. Route Matching Algorithm

```rust
fn find_upstream_for_request(
    upstreams: HashMap<String, Upstream>,
    path: &str,
    host: Option<&str>
) -> Option<String> {
    let mut best_match = None;
    let mut best_length = 0;

    for (name, upstream) in upstreams {
        // Check host match
        if let Some(required_host) = upstream.route.host {
            if host != Some(required_host) {
                continue;
            }
        }

        // Check path prefix match
        if let Some(prefix) = upstream.route.path_prefix {
            if path.starts_with(prefix) && prefix.len() > best_length {
                best_match = Some(name);
                best_length = prefix.len();
            }
        }
    }

    best_match
}
```

**Example**:
- Request: `/api/users/123`
- Routes: `/` (length 1), `/api` (length 4)
- Selected: `/api` (longest match)

## Connection Management

### Connection ID Routing

Spooky uses a multi-level, hierarchical connection ID-based routing scheme to multiplex multiple QUIC connections efficiently:

#### Routing Hierarchy (in lookup order)

1. **Exact DCID Match** → `connections: HashMap<Arc<[u8]>, QuicConnection>`
   - Key: Server-generated 16-byte SCID
   - Lookup: O(1), handles typical packets
   - Coverage: ~99% of packets in steady state

2. **SCID Alias Lookup** → `cid_routes: HashMap<Vec<u8>, Vec<u8>>`
   - Maps non-primary SCIDs to primary SCID during rotation
   - Lookup: O(1), handles SCID rotation scenarios

3. **Peer Address Fallback** → `peer_routes: HashMap<SocketAddr, Arc<[u8]>>`
   - Maps peer address to primary SCID for connection migration
   - Lookup: O(1), handles peer IP changes

4. **Radix Prefix Match** → `cid_radix: CidRadix` (byte-radix trie)
   - Handles clients that extend DCID with extra bytes
   - Lookup: O(k) where k = DCID length (8-20 bytes, constant)
   - Uses longest-prefix matching (prefers longer prefixes)
   - Memory: O(Σ SCID_length), shares common byte prefixes

5. **New Connection Creation**
   - Only for Initial packets
   - Generates new 16-byte SCID
   - Stores in all four indices

#### Performance Characteristics

| Lookup Step | Complexity | Typical Time |
|-------------|-----------|--------------|
| Exact DCID | O(1) | <1 μs |
| SCID alias | O(1) | <1 μs |
| Peer fallback | O(1) | <1 μs |
| Radix prefix | O(k) | ~5 μs (k≈16 bytes) |

With 10,000 concurrent connections, radix lookup time remains constant (~5 μs) instead of scaling linearly (~500+ μs for naive scan).

#### SCID Lifecycle

- **Generation**: 16 random bytes, one per connection
- **Rotation**: Every 60 seconds or after 8 packets (SCID_ROTATION_INTERVAL, SCID_ROTATION_PACKET_THRESHOLD)
- **Tracking**: Active SCIDs maintained in `connection.routing_scids` HashSet
- **Retirement**: Older SCIDs removed from all indices (cid_radix, cid_routes)
- **Updates**: Handled incrementally on sync_connection_routes() call, not per-packet

### Connection Lifecycle

```
[Initial Packet] → [Handshake] → [Established] → [Active] → [Draining] → [Closed]
       │                                │            │           │
       ▼                                ▼            ▼           ▼
  Accept & SCID                   HTTP/3 Streams  Shutdown  Cleanup
  Generation                                       Signal
```

## Load Balancing

### Backend Selection

Each upstream pool maintains its own backend list with health state:

```rust
struct BackendState {
    address: String,
    weight: u32,
    health_state: HealthState,
    consecutive_failures: u32,
}

enum HealthState {
    Healthy,
    Unhealthy {
        until: Instant,      // Cooldown expiry
        successes: u32,      // Success count during recovery
    },
}
```

### Algorithms

**Random**:
```
candidates = healthy_backends()
index = random(0, candidates.len())
return candidates[index]
```

**Round Robin**:
```
candidates = healthy_backends()
index = (next_counter % candidates.len())
next_counter += 1
return candidates[index]
```

**Consistent Hash**:
```
ring = build_ring(backends, replicas=64)
key_hash = hash(request_key)
position = ring.find_next(key_hash)
return backends[position]
```

### Health Checking

Each backend has an independent health checker that:

1. Issues periodic HTTP GET to configured path
2. Evaluates response status (2xx = healthy)
3. Updates backend state on success/failure
4. Applies threshold-based state transitions

**State Transitions**:
```
Healthy ─[failure_threshold fails]─> Unhealthy
Unhealthy ─[cooldown expires + success_threshold succeeds]─> Healthy
```

## Data Structures

### QUICListener

Main QUIC connection handler. Manages all active connections and routes packets to them.

```rust
pub struct QUICListener {
    socket: UdpSocket,
    quic_config: quiche::Config,
    h3_config: Arc<quiche::h3::Config>,

    // Connection ID routing indices
    connections: HashMap<Arc<[u8]>, QuicConnection>,  // Primary: SCID → Connection
    cid_routes: HashMap<Vec<u8>, Vec<u8>>,             // Alias: non-primary SCID → primary SCID
    peer_routes: HashMap<SocketAddr, Arc<[u8]>>,       // Fallback: Peer address → primary SCID
    cid_radix: CidRadix,                               // Prefix: byte-radix trie for DCID matching

    upstream_pools: HashMap<String, Arc<Mutex<UpstreamPool>>>,
    h2_pool: Arc<H2Pool>,
    metrics: Metrics,
    // ...
}
```

**Connection ID Indices Explanation**:
- **connections**: Primary index, O(1) exact DCID lookup (fast path)
- **cid_routes**: Handles SCID rotation where old SCIDs map to current primary
- **peer_routes**: Allows connection migration when client IP changes
- **cid_radix**: O(k) longest-prefix matching when client extends DCID bytes

### QuicConnection

```rust
pub struct QuicConnection {
    quic: quiche::Connection,
    h3: Option<quiche::h3::Connection>,
    streams: HashMap<u64, RequestEnvelope>,
    peer_address: SocketAddr,
    last_activity: Instant,
}
```

### UpstreamPool

```rust
pub struct UpstreamPool {
    pool: BackendPool,      // Backend list with health state
    strategy: String,       // Load balancing algorithm name
}
```

## Concurrency Model

### Poll Thread (Synchronous, Non-Blocking)

The main poll thread never blocks on I/O:

- UDP socket polling with 50ms timeout
- QUIC packet processing via quiche
- HTTP/3 stream event dispatch (`h3.poll`)
- Route matching and backend selection
- Non-blocking stream state advancement via `advance_streams_non_blocking`

All per-stream work follows a state machine driven by `try_recv` / `try_send`:

```
ReceivingRequest
    │  (Event::Finished — body drained to channel, body_tx dropped)
    ▼
AwaitingUpstream
    │  (upstream_result_rx.try_recv() returns Ok)
    ▼
SendingResponse       ← H3 response headers sent; body-pump task spawned
    │  (response_chunk_rx.try_recv() drains Data/End/Error chunks)
    ▼
Completed / Failed    → stream removed from map
```

`advance_streams_non_blocking` is called:
1. After every packet-driven `handle_h3` pass.
2. On every `handle_timeouts` tick — so streams progress even when no new
   client packets arrive.

### Async Tasks (Tokio Runtime)

- Health check probes (one task per backend)
- H2 request forwarding (one task per in-flight stream)
- Response body pump (one task per in-flight stream, enforces `backend_timeout()`)
- Shutdown signal handling

### Why No Blocking Calls

The poll thread owns `quiche::Connection` and `quiche::h3::Connection`, both of
which are `!Send`. Blocking the poll thread on async I/O would stall all QUIC
connections sharing the thread. Instead:

- **Request body** is streamed to the H2 task via `mpsc::channel` using
  `try_send`; overflow chunks are buffered in `body_buf` and retried.
- **Upstream result** is delivered via `oneshot::channel`; the poll thread
  polls it with `try_recv` each maintenance pass.
- **Response body** is pumped by an async task into a bounded
  `mpsc::channel<ResponseChunk>`; the poll thread drains it with `try_recv`
  and writes to H3 with `h3.send_body`. QUIC flow-control backpressure
  (`StreamBlocked`) parks the current chunk in `pending_chunk` for retry.

## Configuration System

### Validation Pipeline

```
YAML file → Parse → Validate → Build runtime structures
              │         │              │
              ▼         ▼              ▼
         serde::de   Validator    QUICListener::new
                                  UpstreamPool::from_upstream
                                  LoadBalancing::from_config
```

### Validation Checks

- TLS certificate and key files exist and are readable
- Listen port in valid range (1-65535)
- All backend addresses are parseable
- Load balancing types are supported
- Health check intervals are non-zero
- Route patterns are valid

## Error Handling

### Request-Level Errors

| Error Source | HTTP Status | Action |
|--------------|-------------|--------|
| Invalid request | 400 | Return error to client |
| No healthy backends | 503 | Return error to client |
| Backend timeout | 503 | Mark backend failure, return error |
| Backend connection error | 502 | Mark backend failure, return error |
| Backend 5xx response | Pass through | Mark backend failure |

### Connection-Level Errors

| Error Type | Action |
|------------|--------|
| QUIC crypto failure | Log and close connection |
| QUIC protocol violation | Log and close connection |
| HTTP/3 stream error | Reset stream, keep connection |
| Idle timeout | Close connection |

### System-Level Errors

| Error Type | Action |
|------------|--------|
| Config validation failure | Exit on startup |
| TLS load failure | Exit on startup |
| Socket bind failure | Exit on startup |
| Health check task panic | Log error, continue |

## Performance Characteristics

### Memory Usage

- Base process: ~50MB
- Per connection: ~1-2KB
- Per stream: ~500B
- Buffer sizes: 64KB (configurable)

### CPU Usage

- Packet processing: Minimal (quiche handles crypto)
- Route matching: O(N) where N = upstream count
- Load balancing: O(1) for random/round-robin, O(log M) for consistent hash where M = backend count
- Health checking: Periodic, minimal impact

### Bottlenecks

Current architectural bottlenecks:

1. **Consistent hash ring**: Rebuilds on every request
2. **Single-threaded poll loop**: All QUIC processing on one thread

See [roadmap](roadmap.md) for planned improvements.

## Security

### TLS Configuration

- TLS 1.3 only (via quiche)
- ALPN: h3 (HTTP/3)
- Peer verification disabled (development mode)
- Certificate chain loaded from PEM files

### Attack Mitigation

Current protections:

- Connection ID randomization
- Idle timeout enforcement
- Buffer size limits
- Health check prevents amplification to backends

Missing protections (planned):

- Rate limiting per client IP
- Request size limits
- DDoS protection
- TLS peer verification

## Observability

### Logging

Structured logging at multiple levels:

- **Error**: Critical failures, backend errors
- **Warn**: Backend health transitions, timeouts
- **Info**: Request processing, backend selection
- **Debug**: QUIC packet handling, connection state
- **Trace**: Detailed protocol messages

### Metrics

Current metrics (AtomicU64):

- `requests_total`: Total requests received
- `requests_success`: Successfully forwarded requests
- `requests_failure`: Failed requests
- `backend_timeouts`: Backend timeout count
- `backend_errors`: Backend error count

No metrics exporter currently implemented.

### Debugging

Connection state logging:

```rust
debug!("Packet DCID (len={}): {:02x?}, type: {:?}, active connections: {}",
    dcid_bytes.len(), &dcid_bytes, header.ty, self.connections.len());
```

Backend selection logging:

```rust
info!("Selected backend {} via {}", backend_addr, lb_name(load_balancer));
```

Health transition logging:

```rust
info!("Backend {} became unhealthy", addr);
```

## Future Directions

See [roadmap](roadmap.md) for planned architectural improvements:

- Async data plane
- Streaming request/response bodies
- Multi-threaded QUIC handling
- Metrics export
- Configuration hot reload
