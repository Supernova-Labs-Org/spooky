#!/usr/bin/env bash
set -euo pipefail

PROFILE="${PROFILE:-full}"
MANIFEST="${MANIFEST:-bench/manifest.yaml}"
OUT_JSON="${OUT_JSON:-bench/micro/latest.json}"
OUT_MD="${OUT_MD:-bench/micro/latest.md}"

mkdir -p "$(dirname "$OUT_JSON")"

cargo run -q -p spooky-bench --release -- \
  --suite micro \
  --profile "$PROFILE" \
  --manifest "$MANIFEST" \
  --output "$OUT_JSON" \
  --markdown-out "$OUT_MD"

# Keep backward-compatible artifact names.
cp "$OUT_JSON" bench/latest.json
cp "$OUT_MD" bench/latest.md

echo "Micro benchmark report: $OUT_JSON"
