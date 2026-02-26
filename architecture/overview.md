# Architecture Overview

## Introduction

Spooky is an HTTP/3 to HTTP/2 reverse proxy and load balancer implemented in Rust. It terminates QUIC connections at the edge and forwards HTTP requests to HTTP/2 backend servers, enabling modern HTTP/3 clients to communicate with existing HTTP/2 infrastructure without requiring backend modifications.

## Design Principles

### Performance
Spooky is designed for high-performance operation with minimal overhead:
- Zero-copy packet processing where possible
- Lock-free data structures for hot paths
- Asynchronous I/O throughout the stack
- Connection pooling and multiplexing
- Memory-efficient buffer management

### Safety
Built on Rust's memory safety guarantees:
- No unsafe code in core proxy logic
- Type-safe protocol conversions
- Structured error handling with explicit failure modes
- Resource lifetime tracking via ownership

### Operational Simplicity
Simple to deploy and operate:
- Single binary deployment
- YAML-based configuration with validation
- Graceful shutdown with connection draining
- Hot configuration reload (planned)
- Comprehensive metrics and logging

### Modularity
Clear separation of concerns across crate boundaries:
- Independent protocol layer implementations
- Pluggable load balancing algorithms
- Isolated configuration management
- Reusable utility components

## System Architecture

### High-Level View

```
┌─────────────────┐
│ HTTP/3 Clients  │
└────────┬────────┘
         │ QUIC/UDP
         │ TLS 1.3
         ▼
┌─────────────────────────────────────────┐
│             Spooky Edge                  │
│                                          │
│  ┌────────────────────────────────┐     │
│  │  QUIC Listener (quiche)        │     │
│  │  - Connection management       │     │
│  │  - Stream multiplexing         │     │
│  │  - TLS termination             │     │
│  └───────────┬────────────────────┘     │
│              │                           │
│  ┌───────────▼────────────────────┐     │
│  │  Protocol Bridge               │     │
│  │  - HTTP/3 → HTTP/2 conversion  │     │
│  │  - Header normalization        │     │
│  │  - Full body buffering (streaming planned)              │     │
│  └───────────┬────────────────────┘     │
│              │                           │
│  ┌───────────▼────────────────────┐     │
│  │  Router & Load Balancer        │     │
│  │  - Path/host matching          │     │
│  │  - Upstream selection          │     │
│  │  - Health tracking             │     │
│  └───────────┬────────────────────┘     │
│              │                           │
│  ┌───────────▼────────────────────┐     │
│  │  HTTP/2 Connection Pool        │     │
│  │  - Connection reuse            │     │
│  │  - Request forwarding          │     │
│  │  - Full response buffering (streaming planned)          │     │
│  └───────────┬────────────────────┘     │
└──────────────┼──────────────────────────┘
               │ HTTP/2
               ▼
       ┌───────────────┐
       │ Backend Pool  │
       └───────────────┘
```

### Data Plane and Control Plane

The architecture separates data plane operations (request forwarding) from control plane operations (configuration, health checks, metrics):

**Data Plane:**
- QUIC packet processing
- HTTP/3 stream handling
- Protocol conversion
- Backend request forwarding
- Response streaming

**Control Plane:**
- Configuration loading and validation
- Health check execution
- Backend state management
- Metrics collection
- Connection lifecycle management

This separation ensures that control plane operations do not block request processing on the hot path.

## Request Processing Pipeline

### 1. Connection Establishment

When a client initiates a connection:
1. UDP packets arrive at the bound socket
2. QUIC handshake is performed using quiche
3. TLS 1.3 credentials are validated
4. HTTP/3 session is established over QUIC
5. Connection state is tracked in the connections HashMap

### 2. Request Reception

For each incoming HTTP/3 stream:
1. QUIC stream data is received
2. HTTP/3 headers are decoded via QPACK
3. Request envelope is created with method, path, authority, headers
4. Body data is accumulated as stream frames arrive
5. Stream state is maintained until request is complete

### 3. Routing and Backend Selection

Once request headers are available:
1. Router matches request path and host against upstream pool routes
2. Longest matching path prefix wins for overlapping routes
3. Host-based routing is applied if configured
4. Selected upstream pool's load balancing strategy is invoked
5. Backend index is selected from healthy backends only
6. Backend address is retrieved from pool

### 4. Protocol Translation

Before forwarding to backend:
1. HTTP/3 pseudo-headers (:method, :path, :authority) are extracted
2. HTTP/2 request is built with proper URI and method
3. Regular headers are copied, filtering hop-by-hop headers
4. Content-Length is set based on body size
5. Host header is ensured (using authority or backend address)

### 5. Backend Forwarding

Request is sent to selected backend:
1. HTTP/2 connection pool provides connection for backend address
2. Semaphore-based flow control limits concurrent requests per backend
3. Request is sent over HTTP/2 connection
4. Timeout is enforced at the transport layer
5. Backend response is awaited

### 6. Response Handling

Backend response is processed:
1. HTTP/2 response is received from backend
2. Status code and headers are extracted
3. Response is written back to HTTP/3 stream
4. Body is buffered from backend and sent to client (streaming planned)
5. Stream is finalized when response is complete

### 7. Health Management

Backend health is tracked continuously:
1. Successful requests increment success counter
2. Failed requests increment failure counter
3. Consecutive failures beyond threshold mark backend unhealthy
4. Unhealthy backends enter cooldown period
5. Successful requests during recovery increment recovery counter
6. Backends return to healthy state after success threshold is met

### 8. Metrics Collection

Throughout the pipeline, metrics are recorded:
- Total requests received
- Successful responses
- Failed requests
- Backend timeouts
- Backend errors
- Request latency (start to completion time)

## Concurrency Model

### Async Runtime

Spooky uses Tokio as its asynchronous runtime:
- Multi-threaded work-stealing scheduler
- Event-driven I/O with epoll/kqueue
- Timer wheel for timeout management
- Cooperative task scheduling

### State Management

Shared state is managed carefully:
- `Arc<T>` for shared ownership
- `Mutex<T>` for mutable shared state (upstream pools)
- `AtomicU64` for lock-free counters (metrics)
- Single-threaded UDP socket polling (no lock contention)

### Task Structure

The main event loop runs on the primary thread:
- `poll()` processes UDP packets synchronously
- QUIC connections are managed in-process
- Backend requests spawn async tasks via Tokio
- Graceful shutdown coordinated via AtomicBool

This design avoids thread synchronization overhead on the packet processing path while leveraging Tokio's async capabilities for backend I/O.

## Error Handling Strategy

### Error Categories

**Configuration Errors:**
- Detected at startup during validation
- Cause process to exit before binding sockets
- Examples: invalid TLS paths, malformed YAML, missing required fields

**Protocol Errors:**
- QUIC connection failures, stream errors, invalid HTTP/3
- Isolated to individual connections or streams
- Do not affect other active connections
- Logged for debugging

**Transport Errors:**
- Backend connection failures, timeouts, HTTP/2 errors
- Trigger backend health state changes
- May cause retry to different backend
- Increment error metrics

**System Errors:**
- Socket errors, TLS failures, resource exhaustion
- May require process restart depending on severity
- Logged at error level with context

### Recovery Mechanisms

**Stream-Level Recovery:**
- Invalid stream fails with HTTP error to client
- Connection remains active for other streams
- Error logged with stream ID

**Backend-Level Recovery:**
- Failed backend marked unhealthy
- Requests routed to healthy backends
- Backend enters cooldown, recovers after success threshold
- Health transitions logged

**Connection-Level Recovery:**
- Failed QUIC connection is closed
- Other connections unaffected
- Client may reconnect

**Process-Level Recovery:**
- Graceful shutdown on SIGTERM/SIGINT
- Drain period allows in-flight requests to complete
- Socket closure after drain timeout

## Configuration Architecture

### Structure

Configuration is hierarchical:
```
Config
├── version: u32
├── listen: Listen (protocol, port, address, TLS)
├── upstream: HashMap<String, Upstream>
│   └── Upstream
│       ├── load_balancing: LoadBalancing
│       ├── route: RouteMatch (host, path_prefix)
│       └── backends: Vec<Backend>
│           └── Backend (id, address, weight, health_check)
└── log: Log (level)
```

### Validation

Configuration validation occurs before runtime:
1. YAML parsing with serde
2. TLS certificate/key file existence checks
3. Backend address format validation
4. Load balancing mode validation
5. Route conflict detection (planned)

### Runtime Behavior

Current configuration is immutable at runtime:
- Loaded once at startup
- Shared via Arc across components
- Hot reload not yet implemented (requires atomic swap)

## Security Considerations

### Transport Security

- TLS 1.3 required for all client connections
- Certificate chain validation via rustls
- Private key protection (file permissions)
- ALPN negotiation ensures HTTP/3

### Backend Communication

- Currently plaintext HTTP/2
- Mutual TLS to backends (planned)
- Connection reuse reduces handshake overhead

### Attack Surface

- UDP amplification: QUIC includes mitigation (connection ID validation)
- Resource exhaustion: connection limits, per-backend semaphores
- Request smuggling: strict HTTP/3 to HTTP/2 conversion rules
- Header injection: header validation in bridge module

## Observability

### Logging

Structured logging via Rust's log crate:
- Levels: trace, debug, info, warn, error
- Context includes: connection ID, stream ID, backend, duration
- Configurable log level at startup

### Metrics

Atomic counters for key metrics:
- `requests_total`: all requests received
- `requests_success`: successful responses
- `requests_failure`: failed requests
- `backend_timeouts`: timed out backend requests
- `backend_errors`: backend error responses

Metrics export via Prometheus format (planned).

### Tracing

Request-level tracing:
- `RequestEnvelope` tracks start time
- Duration calculated on completion
- Logged with request details

Distributed tracing via OpenTelemetry (planned).

## Performance Characteristics

### Latency

- QUIC handshake: 1-RTT with TLS 1.3
- Proxy overhead: sub-millisecond (header conversion, routing)
- Backend latency: dependent on backend response time
- End-to-end: dominated by backend latency

### Throughput

- Concurrent connections: 10,000+ QUIC connections
- Requests per second: 100,000+ on multi-core hardware
- Per-connection overhead: 1-2KB memory
- CPU: primarily driven by QUIC crypto and serialization

### Scalability

- Horizontal: stateless design allows multiple instances
- Vertical: work-stealing scheduler utilizes all cores
- Backend scaling: dynamic health-based routing
- Connection scaling: bounded by file descriptors and memory

## Future Enhancements

### Planned Features

- Hot configuration reload without restart
- Prometheus metrics endpoint
- OpenTelemetry distributed tracing
- Mutual TLS to backends
- Active health check probes (TCP/HTTP)
- Rate limiting per client
- Circuit breaker pattern for failing backends
- Admin API for runtime inspection

### Architectural Improvements

- Lock-free routing table
- Connection state persistence for zero-downtime restart
- eBPF integration for packet-level optimizations
- QUIC 0-RTT support for returning clients
