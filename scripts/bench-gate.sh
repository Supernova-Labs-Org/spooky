#!/usr/bin/env bash
set -euo pipefail

PROFILE="${PROFILE:-ci}"
MANIFEST="${MANIFEST:-bench/manifest.yaml}"
BASELINE_INDEX="${BASELINE_INDEX:-bench/baselines/releases.json}"
BASELINE_RELEASE="${BASELINE_RELEASE:-}"
FAIL_ON="${FAIL_ON:-severe}"
BENCH_GATE_RETRIES="${BENCH_GATE_RETRIES:-1}"

mkdir -p bench/micro bench/macro

ARGS=(
  --check-baseline
  --manifest "$MANIFEST"
  --baseline-index "$BASELINE_INDEX"
  --profile "$PROFILE"
  --fail-on "$FAIL_ON"
)

if [[ -n "$BASELINE_RELEASE" ]]; then
  ARGS+=(--baseline-release "$BASELINE_RELEASE")
fi

run_suite_with_retries() {
  local suite="$1"
  local out_json="$2"
  local out_md="$3"
  local attempt=0
  local max_attempts=$((BENCH_GATE_RETRIES + 1))

  while [[ "$attempt" -lt "$max_attempts" ]]; do
    if cargo run -q -p spooky-bench --release -- \
      --suite "$suite" \
      --output "$out_json" \
      --markdown-out "$out_md" \
      "${ARGS[@]}"; then
      return 0
    fi

    attempt=$((attempt + 1))
    if [[ "$attempt" -lt "$max_attempts" ]]; then
      echo "Benchmark gate failed for suite '$suite' (attempt $attempt/$max_attempts); retrying..."
    fi
  done

  echo "Benchmark gate failed for suite '$suite' after $max_attempts attempts."
  return 1
}

run_suite_with_retries micro bench/micro/latest.json bench/micro/latest.md

cp bench/micro/latest.json bench/latest.json
cp bench/micro/latest.md bench/latest.md

run_suite_with_retries macro bench/macro/latest.json bench/macro/latest.md

echo "Benchmark regression gates passed (profile=$PROFILE, fail_on=$FAIL_ON, retries=$BENCH_GATE_RETRIES)"
