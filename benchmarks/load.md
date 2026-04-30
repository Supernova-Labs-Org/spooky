# Spooky HTTP/3 Load Test Results

Load testing with the internal `vex` HTTP/3 load tool against Spooky proxy.

## Test Environment

| | |
|---|---|
| **Machine** | Intel i5-11320H @ 3.20 GHz, 4 physical cores / 8 logical, 15 GiB RAM |
| **OS** | Linux 6.14, NVMe storage |
| **Target** | `127.0.0.1:9889` (loopback) |
| **Protocol** | HTTP/3 over QUIC, TLS (self-signed) |
| **Backends** | 2 × upstream servers on `127.0.0.1:7001` and `127.0.0.1:7002` |
| **Config** | `config/config.yaml` — worker_threads=4, per_backend_inflight_limit=256, global_inflight_limit=4096, adaptive admission enabled |

---

## Scenarios

### Burst — high concurrency, clean traffic

3,000 requests, 160 concurrent connections.

| Metric | Value |
|---|---|
| Throughput | **20,311 req/s** |
| Success rate | 3000/3000 — **100%** |
| p50 latency | 21.1 ms |
| p95 latency | 117.4 ms |
| p99 latency | 125.5 ms |

### Burst — moderate concurrency

3,000 requests, 80 concurrent connections.

| Metric | Value |
|---|---|
| Throughput | **16,331 req/s** |
| Success rate | 3000/3000 — **100%** |
| p50 latency | 22.7 ms |
| p95 latency | 53.1 ms |
| p99 latency | 66.9 ms |

### Slow upstream — backend latency simulation

1,000 requests, 80 concurrent connections, upstream introduces delay.

| Metric | Value |
|---|---|
| Throughput | **9,239 req/s** |
| Success rate | 1000/1000 — **100%** |
| p50 latency | 25.5 ms |
| p95 latency | 65.1 ms |
| p99 latency | 71.0 ms |

### QUIC packet loss — lossy network simulation

1,500 requests, 120 concurrent connections, simulated packet loss.

| Metric | Value |
|---|---|
| Throughput | **12,433 req/s** |
| Success rate | 1500/1500 — **100%** |
| p50 latency | 43.9 ms |
| p95 latency | 81.6 ms |
| p99 latency | 95.4 ms |

---

## Summary

Spooky sustains **20k+ req/s** at full concurrency with zero errors across burst, slow-upstream, and packet-loss scenarios. p99 latency stays under 130 ms even at 160 concurrent connections.

The async data plane, per-backend inflight limits, and adaptive admission control together ensure that slow or lossy backends do not stall the proxy — fast paths remain fast while degraded upstreams are isolated.
