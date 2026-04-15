#!/usr/bin/env bash
set -euo pipefail

PROFILE="${PROFILE:-full}"
MANIFEST="${MANIFEST:-bench/manifest.yaml}"
OUT_JSON="${OUT_JSON:-bench/macro/latest.json}"
OUT_MD="${OUT_MD:-bench/macro/latest.md}"

mkdir -p "$(dirname "$OUT_JSON")"

cargo run -q -p spooky-bench --release -- \
  --suite macro \
  --profile "$PROFILE" \
  --manifest "$MANIFEST" \
  --output "$OUT_JSON" \
  --markdown-out "$OUT_MD"

echo "Macro benchmark report: $OUT_JSON"
