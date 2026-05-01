# Spooky HTTP/3 Load Test Results

## Test Environment

| | |
|---|---|
| **Machine** | Intel i5-11320H @ 3.20 GHz, 4 physical cores / 8 logical, 15 GiB RAM |
| **OS** | Linux 6.14, NVMe storage |
| **Target** | `127.0.0.1:9889` (loopback) |
| **Protocol** | HTTP/3 over QUIC → HTTP/2 upstream, TLS (self-signed) |
| **Backends** | 2 × upstream servers on `127.0.0.1:7001` and `127.0.0.1:7002` |
| **Config** | `config/config.yaml` — worker_threads=4, per_backend_inflight_limit=256, global_inflight_limit=4096, adaptive admission enabled |
| **Run ID** | `20260501T170035Z` |

---

## Scenarios

### Burst — peak concurrency

3,000 requests, 120 concurrent connections.

| Metric | Value |
|---|---|
| Throughput | **21,235 req/s** |
| Success rate | 3000/3000 — **100%** |
| p50 latency | 19.1 ms |
| p95 latency | 87.8 ms |
| p99 latency | 102.4 ms |

### Burst — high concurrency

3,000 requests, 80 concurrent connections.

| Metric | Value |
|---|---|
| Throughput | **14,691 req/s** |
| Success rate | 3000/3000 — **100%** |
| p50 latency | 25.1 ms |
| p95 latency | 57.7 ms |
| p99 latency | 64.6 ms |

### Slow upstream — backend latency simulation

1,000 requests, 80 concurrent connections, upstream introduces delay.

| Metric | Value |
|---|---|
| Throughput | **9,549 req/s** |
| Success rate | 1000/1000 — **100%** |
| p50 latency | 26.9 ms |
| p95 latency | 58.1 ms |
| p99 latency | 62.0 ms |

### QUIC packet loss — lossy network simulation

1,500 requests, 120 concurrent connections, simulated packet loss.

| Metric | Value |
|---|---|
| Throughput | **12,500 req/s** |
| Success rate | 1500/1500 — **100%** |
| p50 latency | 44.2 ms |
| p95 latency | 79.7 ms |
| p99 latency | 91.2 ms |

---

## Summary

Spooky sustains **21k+ req/s** at full concurrency with zero errors across burst, slow-upstream, and packet-loss scenarios. p99 latency stays under 103 ms even at 120 concurrent connections.

The async data plane, per-backend inflight limits, and adaptive admission control ensure that slow or lossy backends do not stall the proxy — fast paths remain fast while degraded upstreams are isolated.
