# Benchmarks

Spooky ships with a manifest-driven benchmark system that covers both micro and macro performance and enforces regression gates in CI.

## Scope

### Microbenchmarks (kept and required)

- Route lookup
  - `route_lookup_indexed_hit`
  - `route_lookup_linear_hit`
  - `route_lookup_indexed_miss`
- Load balancer selection
  - `lb_round_robin_pick`
  - `lb_random_pick`
  - `lb_consistent_hash_pick`
- Connection lookup primitives
  - `connection_exact_lookup`
  - `connection_alias_lookup`
  - `connection_prefix_scan_miss_lookup`
  - `connection_peer_scan_miss`
  - `connection_peer_map_hit`
  - `connection_peer_map_miss`

### Macrobenchmarks

- `macro_traffic_mix`
  - Simulates realistic mixed traffic patterns across routing, LB decisions, and connection lookup paths.
- `macro_long_lived_stream`
  - Simulates long-lived stream handling with multi-chunk work per request to capture tail behavior under sustained work.

## Manifest

Benchmark settings live in:

- `bench/manifest.yaml`

It controls:

- run profiles (`full`, `ci`)
- micro and macro scales/iterations
- regression gate thresholds for:
  - CPU (`ns/op`)
  - memory (`rss_delta_kb`)
  - allocations (`alloc_calls`, `alloc_bytes`)
  - tail latency (`p99`)

## Baselines Per Release

Release baselines are tracked in:

- `bench/baselines/releases.json`

Each release maps to:

- `micro` baseline JSON
- `macro` baseline JSON

Current release baseline pointers are used by regression gates when `--check-baseline` is enabled.

## Run Locally

### Microbench

```bash
./scripts/bench-micro.sh
```

Outputs:

- `bench/micro/latest.json`
- `bench/micro/latest.md`
- compatibility copies:
  - `bench/latest.json`
  - `bench/latest.md`

### Macrobench

```bash
./scripts/bench-macro.sh
```

Outputs:

- `bench/macro/latest.json`
- `bench/macro/latest.md`

### Regression gates (CI profile)

```bash
./scripts/bench-gate.sh
```

This runs both micro and macro suites with baseline checks and fails on severe regressions by default.

## Promote New Release Baseline

After validating benchmark results for a release:

```bash
./scripts/bench-promote-baseline.sh vX.Y.Z
```

This:

- copies latest micro/macro reports into `bench/baselines/vX.Y.Z/`
- updates `bench/baselines/releases.json`
- optionally moves `current_release` pointer (default: true)

## Direct CLI (advanced)

Micro run:

```bash
cargo run -p spooky-bench --release -- \
  --suite micro \
  --profile full \
  --manifest bench/manifest.yaml \
  --output bench/micro/latest.json \
  --markdown-out bench/micro/latest.md
```

Macro run with baseline gate:

```bash
cargo run -p spooky-bench --release -- \
  --suite macro \
  --profile ci \
  --manifest bench/manifest.yaml \
  --baseline-index bench/baselines/releases.json \
  --check-baseline \
  --fail-on severe \
  --output bench/macro/latest.json \
  --markdown-out bench/macro/latest.md
```

## CI Policy

Regression gates evaluate:

- CPU slowdown (`latency_ns_per_op`)
- memory growth (`rss_delta_kb`)
- allocation inflation (`alloc_calls`, `alloc_bytes`)
- tail latency regression (`latency_p99_ns`) for sampled-latency cases (macro suite and any sampled micro cases)

CI fails on severe regressions. Warn-level regressions remain visible in markdown artifacts for review.
