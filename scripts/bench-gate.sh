#!/usr/bin/env bash
set -euo pipefail

PROFILE="${PROFILE:-ci}"
MANIFEST="${MANIFEST:-bench/manifest.yaml}"
BASELINE_INDEX="${BASELINE_INDEX:-bench/baselines/releases.json}"
BASELINE_RELEASE="${BASELINE_RELEASE:-}"
FAIL_ON="${FAIL_ON:-severe}"

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

cargo run -q -p spooky-bench --release -- \
  --suite micro \
  --output bench/micro/latest.json \
  --markdown-out bench/micro/latest.md \
  "${ARGS[@]}"

cp bench/micro/latest.json bench/latest.json
cp bench/micro/latest.md bench/latest.md

cargo run -q -p spooky-bench --release -- \
  --suite macro \
  --output bench/macro/latest.json \
  --markdown-out bench/macro/latest.md \
  "${ARGS[@]}"

echo "Benchmark regression gates passed (profile=$PROFILE, fail_on=$FAIL_ON)"
