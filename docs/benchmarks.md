# Benchmarks

Spooky includes a dedicated benchmark harness for CPU and memory regression tracking.

## Scope

The suite measures:

- Route lookup (indexed and linear reference)
- Load balancer selection (round-robin, random, consistent-hash)
- Connection lookup primitives (exact, alias, CID prefix scan, peer scan/map fallback)

Each benchmark runs at scales:

- 100
- 1,000
- 10,000

## Metrics

For every benchmark case, the harness reports:

- `latency_ns_per_op` (CPU)
- `alloc_calls` (allocation count)
- `alloc_bytes` (total allocated bytes)
- `rss_delta_kb` (resident memory delta)

Outputs are written to JSON and optional Markdown summary.

## Run Locally

Generate a fresh benchmark report:

```bash
cargo run -p spooky-bench --release -- \
  --output bench/latest.json \
  --markdown-out bench/latest.md
```

Run regression checks against baseline:

```bash
cargo run -p spooky-bench --release -- \
  --output bench/latest.json \
  --baseline bench/baseline.json \
  --check-baseline \
  --cpu-threshold 0.40 \
  --mem-threshold 0.20 \
  --markdown-out bench/latest.md
```

## Baseline and Thresholds

Baseline file:

- `bench/baseline.json`

Current thresholds:

- CPU regression threshold: `40%`
- Memory regression threshold: `20%`

Regression checks fail if benchmark metrics exceed thresholds versus baseline.

## Linux Burst-Tolerance Tuning

For high-burst UDP traffic tests, tune host kernel networking before benchmarking:

```bash
sudo ./scripts/sysctl-linux-network-tuning.sh
```

Recommended runtime config pairing in `performance`:

- `udp_recv_buffer_bytes: 8388608`
- `udp_send_buffer_bytes: 8388608`

## Memory Guardrail Policy

All performance-related changes must include memory deltas in reports.

- Regressions with significant memory inflation should be rejected.
- If memory growth is intentional, it must be justified in PR notes with benchmark evidence.

## Artifacts

Benchmark runs can emit:

- JSON report
- Markdown summary

Use these artifacts to track trend lines and justify threshold updates when needed.
