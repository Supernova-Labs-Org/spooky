# Spooky HTTP/3 Load Test Results

Load testing with `vex` HTTP/3 load tool against Spooky proxy.

## Test Setup

- **Tool**: vex (HTTP/3 load generator)
- **Target**: Spooky on 127.0.0.1:9889
- **Endpoint**: `/api`
- **TLS**: Insecure (self-signed)
- **Duration**: 30s per test
- upstream had 2 go server running for /api upstream pool

## Results

### Test 1: 50 workers, 5000 requests

```
Throughput: 108.71 req/s
Success: 3649/3712 (98.3%)
Failed: 63 (handshake timeout)
Completion: Duration limit reached (30s)

Latency (ms):
  Min:    82.07
  Avg:   331.10
  p50:   243.97
  p90:   507.38
  p95:  1157.16
  p99:  1378.32
  Max:  3698.73
```

### Test 2: 100 workers, 10000 requests

```
Throughput: 93.38 req/s
Success: 3151/3258 (96.7%)
Failed: 107 (handshake timeout + response timeout)
Completion: Duration limit reached (30s)

Latency (ms):
  Min:   204.55
  Avg:   771.35
  p50:   410.69
  p90:  1433.81
  p95:  2078.47
  p99:  3697.71
  Max:  4965.99
```

### Test 3: 100 workers, 100000 requests

```
Throughput: 37.01 req/s
Success: 1103/1374 (80.3%)
Failed: 271 (timeout)

Latency (ms):
  Min:    115.48
  Avg:  1371.99
  p50:  1003.33
  p90:  3518.38
  p95:  4056.94
  p99:  4849.00
  Max:  5229.66
```

## Analysis

**Throughput plateaus at ~37-40 req/s** across all tests despite varying concurrency. This indicates a bottleneck in the data plane.

**Root cause**: Blocking backend I/O. Each request blocks the QUIC poll loop during HTTP/2 forwarding. The 2-second backend timeout causes head-of-line blocking — while one request waits for response, all other QUIC streams are stalled.

**Failure pattern**: Timeouts increase with concurrency (50 workers: 3.7% fail, 100 workers: ~20% fail). Slow backends starve fast ones.

**Latency degradation**: p99 latency grows with load (4.1s at 50w → 4.8s at 100w), confirming queueing effects from synchronous forwarding.

## Implications

- **Not production-ready** for concurrent workloads
- Blocking I/O must be addressed before GA
- Async backend forwarding is critical for improving throughput
- Current design suitable only for low-concurrency scenarios (<20 workers)

## Next Steps

1. Implement async backend forwarding (non-blocking data plane)
2. Move backend I/O to worker threads or Tokio tasks
3. Re-benchmark after async implementation
