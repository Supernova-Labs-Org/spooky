#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <release-tag>"
  exit 1
fi

RELEASE="$1"
BASELINE_INDEX="${BASELINE_INDEX:-bench/baselines/releases.json}"
MICRO_REPORT="${MICRO_REPORT:-bench/micro/latest.json}"
MACRO_REPORT="${MACRO_REPORT:-bench/macro/latest.json}"
SET_CURRENT="${SET_CURRENT:-true}"

cargo run -q -p spooky-bench -- \
  --promote-release "$RELEASE" \
  --baseline-index "$BASELINE_INDEX" \
  --promote-micro-report "$MICRO_REPORT" \
  --promote-macro-report "$MACRO_REPORT" \
  --set-current-release "$SET_CURRENT"
