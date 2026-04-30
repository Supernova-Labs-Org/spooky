#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOAD_SCRIPT="${ROOT_DIR}/scripts/load-scenarios.sh"

TARGET="${TARGET:-127.0.0.1:9889}"
HOST="${HOST:-localhost}"
H3_CLIENT_BIN="${H3_CLIENT_BIN:-${ROOT_DIR}/target/release/h3_client}"
OUT_BASE="${OUT_BASE:-${ROOT_DIR}/bench/load/matrix}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_DIR="${OUT_BASE}/${RUN_ID}"

# Shared per-scenario request counts (can override per run).
BURST_REQUESTS="${BURST_REQUESTS:-3000}"
SLOW_REQUESTS="${SLOW_REQUESTS:-1000}"
LOSS_REQUESTS="${LOSS_REQUESTS:-1500}"
BURST_PATH="${BURST_PATH:-/api}"
SLOW_PATH="${SLOW_PATH:-/slow}"
LOSS_PATH="${LOSS_PATH:-/api}"
NETEM_IFACE="${NETEM_IFACE:-}"
LOSS_PERCENT="${LOSS_PERCENT:-2}"

usage() {
  cat <<USAGE
Usage: scripts/load-matrix.sh

Runs four predefined load profiles and stores each profile's artifacts plus
a combined summary under:
  bench/load/matrix/<timestamp>/

Environment overrides:
  TARGET=127.0.0.1:9889
  HOST=localhost
  H3_CLIENT_BIN=./target/release/h3_client
  OUT_BASE=bench/load/matrix
  RUN_ID=custom-id

Shared request/path overrides:
  BURST_REQUESTS=3000 SLOW_REQUESTS=1000 LOSS_REQUESTS=1500
  BURST_PATH=/api SLOW_PATH=/slow LOSS_PATH=/api

Optional Linux loss injection passthrough:
  NETEM_IFACE=eth0 LOSS_PERCENT=2
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -x "${LOAD_SCRIPT}" ]]; then
  echo "error: load script not found: ${LOAD_SCRIPT}" >&2
  exit 1
fi

if [[ ! -x "${H3_CLIENT_BIN}" ]]; then
  echo "error: h3 client binary not found: ${H3_CLIENT_BIN}" >&2
  echo "build with: cargo build --release -p spooky --bin h3_client" >&2
  exit 1
fi

mkdir -p "${RUN_DIR}"

run_profile() {
  local profile="$1"
  local streams="$2"
  local burst_conc="$3"
  local slow_conc="$4"
  local loss_conc="$5"

  local out_dir="${RUN_DIR}/${profile}"
  mkdir -p "${out_dir}"

  echo "==> Running profile '${profile}' (streams=${streams}, burst=${burst_conc}, slow=${slow_conc}, loss=${loss_conc})"

  TARGET="${TARGET}" \
  HOST="${HOST}" \
  H3_CLIENT_BIN="${H3_CLIENT_BIN}" \
  OUT_DIR="${out_dir}" \
  STREAMS_PER_WORKER="${streams}" \
  BURST_PATH="${BURST_PATH}" \
  BURST_REQUESTS="${BURST_REQUESTS}" \
  BURST_CONCURRENCY="${burst_conc}" \
  SLOW_PATH="${SLOW_PATH}" \
  SLOW_REQUESTS="${SLOW_REQUESTS}" \
  SLOW_CONCURRENCY="${slow_conc}" \
  LOSS_PATH="${LOSS_PATH}" \
  LOSS_REQUESTS="${LOSS_REQUESTS}" \
  LOSS_CONCURRENCY="${loss_conc}" \
  NETEM_IFACE="${NETEM_IFACE}" \
  LOSS_PERCENT="${LOSS_PERCENT}" \
  "${LOAD_SCRIPT}"
}

run_profile "profile_1_s4_c40_20_30" 4 40 20 30
run_profile "profile_2_s8_c80_40_60" 8 80 40 60
run_profile "profile_3_s16_c120_60_90" 16 120 60 90
run_profile "profile_4_s24_c160_80_120" 24 160 80 120

summary_tsv="${RUN_DIR}/summary.tsv"
summary_md="${RUN_DIR}/summary.md"
summary_json="${RUN_DIR}/summary.json"
latest_note="${OUT_BASE}/latest_run_path.txt"

echo -n >"${summary_tsv}"

for profile_dir in "${RUN_DIR}"/profile_*; do
  profile="$(basename "${profile_dir}")"
  if [[ ! -f "${profile_dir}/latest.tsv" ]]; then
    continue
  fi

  awk -F'\t' -v p="${profile}" '{printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", p,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12}' "${profile_dir}/latest.tsv" >>"${summary_tsv}"
done

{
  echo "# Spooky Load Matrix Summary"
  echo
  echo "- Run ID: \`${RUN_ID}\`"
  echo "- Run Dir: \`${RUN_DIR}\`"
  echo "- Target: \`${TARGET}\`"
  echo "- Host: \`${HOST}\`"
  echo "- Generated: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  echo
  echo "| profile | scenario | path | requests | concurrency | success | errors | throughput req/s | min ms | avg ms | p50 ms | p95 ms | p99 ms |"
  echo "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  awk -F'\t' '{
    min_ms=$9/1000000.0; avg_ms=$10/1000000.0; p50_ms=$11/1000000.0; p95_ms=$12/1000000.0; p99_ms=$13/1000000.0;
    printf("| %s | %s | %s | %s | %s | %s | %s | %s | %.3f | %.3f | %.3f | %.3f | %.3f |\n",
      $1,$2,$3,$4,$5,$6,$7,$8,min_ms,avg_ms,p50_ms,p95_ms,p99_ms)
  }' "${summary_tsv}"
} >"${summary_md}"

{
  echo '{'
  echo '  "run_id": "'"${RUN_ID}"'",'
  echo '  "run_dir": "'"${RUN_DIR}"'",'
  echo '  "target": "'"${TARGET}"'",'
  echo '  "host": "'"${HOST}"'",'
  echo '  "generated_unix_secs": '"$(date +%s)"','
  echo '  "results": ['
  awk -F'\t' 'BEGIN{first=1} {
    if (!first) printf(",\n");
    first=0;
    printf("    {\"profile\":\"%s\",\"scenario\":\"%s\",\"path\":\"%s\",\"requests\":%s,\"concurrency\":%s,\"success\":%s,\"errors\":%s,\"throughput_req_s\":%s,\"latency_ns\":{\"min\":%s,\"avg\":%s,\"p50\":%s,\"p95\":%s,\"p99\":%s}}",
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
  } END{printf("\n")} ' "${summary_tsv}"
  echo '  ]'
  echo '}'
} >"${summary_json}"

echo "${RUN_DIR}" >"${latest_note}"

echo "Load matrix summary: ${summary_md}"
echo "Load matrix data:    ${summary_json}"
echo "Latest run pointer:  ${latest_note}"
