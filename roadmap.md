# Roadmap

## Current Status

**Experimental. Core features functional and tested.** Spooky can terminate HTTP/3 connections, forward to HTTP/2 backends, and perform load balancing with health checks. It is not production-ready — significant known limitations exist (see Technical Debt below).

### Completed

- HTTP/3 termination via quiche
- HTTP/2 backend connectivity
- Path and host-based routing
- Multiple load balancing algorithms (random, round-robin, consistent hash)
- Active health checking with automatic backend management
- Per-upstream configuration and routing
- Connection ID management and QUIC packet routing
- TLS 1.3 with certificate chain loading
- Structured logging with multiple levels
- Configuration validation at startup
- Graceful shutdown with connection draining

## Phase 1: Operational Hardening

**Goal**: Expand operational and scalability capabilities for production deployments.

### Performance

- **Async data plane**: Move backend forwarding off the main poll thread
- **Streaming bodies**: Implement incremental request/response streaming instead of full buffering
- **Multi-threading**: Support multi-threaded QUIC packet processing
- **Connection pooling optimizations**: Reduce allocation overhead in HTTP/2 pool

### Observability

- **Metrics export**: Prometheus endpoint for scraping metrics
- **Distributed tracing**: OpenTelemetry integration
- **Request logging**: Per-request structured logs with correlation IDs
- **Connection metrics**: Track QUIC RTT, packet loss, stream count

### Operational

- **Configuration hot reload**: Reload config on SIGHUP without dropping connections
- **Health check improvements**: Separate client pool for probes to avoid contention
- **TLS certificate reload**: Automatic reload on certificate rotation
- **Admin API**: HTTP endpoint for runtime statistics and control

### Reliability

- **Circuit breaker**: Per-backend circuit breakers to prevent cascading failures
- **Retry logic**: Configurable request retry with exponential backoff
- **Request timeouts**: Per-route timeout configuration
- **Rate limiting**: Per-IP and per-route rate limits

## Phase 2: Advanced Features

**Goal**: Add advanced traffic management and operational capabilities.

### Traffic Management

- **Weighted routing**: Route percentage of traffic to different upstreams
- **Header-based routing**: Route by arbitrary request headers
- **Request rewriting**: URL rewriting and header manipulation
- **Compression**: Automatic response compression

### Load Balancing

- **Least connections**: Track active connections per backend
- **Response time**: Route based on backend latency
- **Weighted least connection**: Combine weights with connection count
- **Cached consistent hash**: Cache hash ring to avoid rebuilds

### Security

- **TLS peer verification**: Enable certificate verification for production
- **mTLS support**: Client certificate authentication
- **Request validation**: Size limits, header validation
- **IP allowlist/blocklist**: Simple access control

### Deployment

- **Dynamic backend discovery**: Service discovery integration (DNS SRV, Consul, etcd)
- **Backend metadata**: Tags and labels for flexible routing
- **A/B testing support**: Route subset of traffic to experimental backends
- **Canary deployments**: Gradually shift traffic to new backend versions

## Phase 3: Enterprise Features

**Goal**: Support large-scale deployments with advanced requirements.

### Multi-Tenancy

- **Namespace isolation**: Separate routing tables per tenant
- **Resource limits**: Per-tenant connection and request limits
- **Tenant routing**: Route by tenant ID or subdomain

### Advanced Observability

- **APM integration**: Datadog, New Relic, etc.
- **Custom metrics**: User-defined metric collection
- **Traffic replay**: Record and replay production traffic
- **Query logs**: SQL-like queries over request logs

### Extensions

- **WebAssembly plugins**: Custom routing logic via WASM
- **Lua scripting**: Dynamic request/response transformation
- **gRPC support**: Native gRPC proxying
- **WebSocket support**: WebSocket over HTTP/3

### High Availability

- **Connection migration**: Support QUIC connection migration
- **State replication**: Share connection state across instances
- **Zero-downtime updates**: Binary updates without connection loss
- **Multi-region support**: Geographic routing and failover

## Phase 4: Protocol Extensions

**Goal**: Support emerging protocols and optimizations.

### HTTP/3 Features

- **0-RTT support**: Enable 0-RTT with proper anti-replay measures
- **QUIC multipath**: Support multiple network paths
- **Datagram support**: QUIC DATAGRAM frames for low-latency data
- **Priority trees**: HTTP/3 priority and scheduling

### Additional Protocols

- **HTTP/1.1 support**: Serve HTTP/1.1 clients
- **TCP proxy mode**: Layer 4 TCP proxying
- **UDP proxy**: Forward UDP traffic
- **MQTT support**: IoT protocol support

### Optimizations

- **Zero-copy**: Eliminate unnecessary data copies
- **Kernel bypass**: AF_XDP or DPDK integration
- **Hardware offload**: TLS offload to NICs
- **eBPF**: Use eBPF for packet filtering and routing

## Implementation Priorities

### High Priority (Next 3 months)

1. Async data plane - unblock main thread
2. Metrics export - essential for production
3. Configuration hot reload - reduce operational friction
4. Streaming bodies - reduce memory usage
5. TLS peer verification - production security

### Medium Priority (3-6 months)

1. Circuit breakers - improve reliability
2. Distributed tracing - debugging complex issues
3. Rate limiting - protect backends
4. Health check improvements - reduce contention
5. Admin API - operational visibility

### Low Priority (6+ months)

1. Dynamic backend discovery - integration complexity
2. Advanced load balancing - diminishing returns
3. WebAssembly plugins - adds complexity
4. Protocol extensions - limited immediate value
5. Multi-tenancy - niche use case

## Technical Debt

### Current Known Issues

1. **Blocking backend calls**: Main thread blocks during HTTP/2 requests
2. **Full body buffering**: High memory usage for large requests/responses
3. **Consistent hash rebuilds**: Ring rebuilt on every request
4. **No metrics export**: Metrics collected but not exposed
5. **Health check contention**: Shares connection pool with production traffic
6. **Single-threaded**: QUIC processing limited to one thread
7. **No TLS verification**: Development-only security posture

### Refactoring Needs

1. **Error handling**: Unify error types across crates
2. **Configuration**: Type-safe config builders
3. **Testing**: Expand integration test coverage
4. **Documentation**: API documentation and examples
5. **Logging**: Reduce debug log verbosity in hot path

## Non-Goals

Features explicitly not planned:

- **Full service mesh**: Focus remains on edge proxying
- **Content caching**: Use CDN or dedicated cache
- **WAF capabilities**: Use dedicated security tools
- **Database proxying**: Use specialized database proxies
- **Custom protocols**: Stick to HTTP family

## Contributing

Contributions are welcome. See [contributing guide](development/contributing.md) for development setup and guidelines.

Priority areas for contributions:

1. Metrics export (Prometheus)
2. Streaming request/response bodies
3. Configuration hot reload
4. Integration tests
5. Documentation and examples
