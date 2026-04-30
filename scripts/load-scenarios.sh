#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-127.0.0.1:9889}"
HOST="${HOST:-localhost}"
H3_CLIENT_BIN="${H3_CLIENT_BIN:-./target/release/h3_client}"
OUT_DIR="${OUT_DIR:-bench/load}"
STREAMS_PER_WORKER="${STREAMS_PER_WORKER:-8}"
WARMUP_REQUESTS="${WARMUP_REQUESTS:-200}"
WARMUP_CONCURRENCY="${WARMUP_CONCURRENCY:-20}"
WARMUP_PATH="${WARMUP_PATH:-/api}"
READINESS_TIMEOUT_SEC="${READINESS_TIMEOUT_SEC:-20}"
READINESS_POLL_INTERVAL_SEC="${READINESS_POLL_INTERVAL_SEC:-0.5}"
BACKEND_READY_URL="${BACKEND_READY_URL:-http://127.0.0.1:8080/health}"
CONTROL_READY_URL="${CONTROL_READY_URL:-http://127.0.0.1:9902/healthz}"
METRICS_READY_URL="${METRICS_READY_URL:-http://127.0.0.1:9901/metrics}"

BURST_PATH="${BURST_PATH:-/api}"
BURST_REQUESTS="${BURST_REQUESTS:-3000}"
BURST_CONCURRENCY="${BURST_CONCURRENCY:-200}"

SLOW_PATH="${SLOW_PATH:-/slow}"
SLOW_REQUESTS="${SLOW_REQUESTS:-1000}"
SLOW_CONCURRENCY="${SLOW_CONCURRENCY:-80}"

LOSS_PATH="${LOSS_PATH:-/api}"
LOSS_REQUESTS="${LOSS_REQUESTS:-1500}"
LOSS_CONCURRENCY="${LOSS_CONCURRENCY:-120}"
LOSS_PERCENT="${LOSS_PERCENT:-2}"
NETEM_IFACE="${NETEM_IFACE:-}"
SCENARIO_RETRY_ATTEMPTS="${SCENARIO_RETRY_ATTEMPTS:-1}"
SCENARIO_RETRY_COOLDOWN_SEC="${SCENARIO_RETRY_COOLDOWN_SEC:-2}"
SCENARIO_RETRY_ON_ERROR="${SCENARIO_RETRY_ON_ERROR:-1}"

usage() {
  cat <<USAGE
Usage: scripts/load-scenarios.sh

Environment overrides:
  TARGET=127.0.0.1:9889
  HOST=localhost
  H3_CLIENT_BIN=./target/release/h3_client
  OUT_DIR=bench/load
  STREAMS_PER_WORKER=8
  WARMUP_REQUESTS=200 WARMUP_CONCURRENCY=20 WARMUP_PATH=/api
  READINESS_TIMEOUT_SEC=20 READINESS_POLL_INTERVAL_SEC=0.5
  BACKEND_READY_URL=http://127.0.0.1:8080/health
  CONTROL_READY_URL=http://127.0.0.1:9902/healthz
  METRICS_READY_URL=http://127.0.0.1:9901/metrics

  BURST_PATH=/api BURST_REQUESTS=3000 BURST_CONCURRENCY=200
  SLOW_PATH=/slow SLOW_REQUESTS=1000 SLOW_CONCURRENCY=80
  LOSS_PATH=/api LOSS_REQUESTS=1500 LOSS_CONCURRENCY=120
  SCENARIO_RETRY_ATTEMPTS=1 SCENARIO_RETRY_COOLDOWN_SEC=2
  SCENARIO_RETRY_ON_ERROR=1

Optional Linux netem injection for packet-loss scenario:
  NETEM_IFACE=eth0 LOSS_PERCENT=2 scripts/load-scenarios.sh
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -x "${H3_CLIENT_BIN}" ]]; then
  echo "error: h3 client binary not found at ${H3_CLIENT_BIN}" >&2
  echo "build it with: cargo build --release -p spooky --bin h3_client" >&2
  exit 1
fi

if [[ "${STREAMS_PER_WORKER}" -lt 1 ]]; then
  echo "error: STREAMS_PER_WORKER must be >= 1 (got ${STREAMS_PER_WORKER})" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"

results_tsv="${OUT_DIR}/latest.tsv"
results_json="${OUT_DIR}/latest.json"
results_md="${OUT_DIR}/latest.md"

echo -n >"${results_tsv}"

now_ns() {
  perl -MTime::HiRes=time -e 'printf("%.0f\n", time()*1000000000)'
}

percentile_from_sorted_file() {
  local p="$1"
  local file="$2"
  local count
  count=$(wc -l <"${file}")
  if [[ "${count}" -eq 0 ]]; then
    echo 0
    return
  fi
  local idx=$(( (p * count + 99) / 100 ))
  if [[ "${idx}" -lt 1 ]]; then
    idx=1
  fi
  sed -n "${idx}p" "${file}"
}

wait_ready_url() {
  local name="$1"
  local url="$2"
  local mode="${3:-http1}"

  if [[ -z "${url}" || "${url}" == "-" ]]; then
    return 0
  fi

  if ! command -v curl >/dev/null 2>&1; then
    echo "error: curl is required for readiness checks (${name})" >&2
    exit 1
  fi

  local deadline
  deadline=$(( $(date +%s) + READINESS_TIMEOUT_SEC ))
  while true; do
    if [[ "${mode}" == "h2c" ]]; then
      if curl --help all 2>/dev/null | grep -q -- "--http2-prior-knowledge"; then
        if curl -fsS --http2-prior-knowledge --max-time 2 "${url}" >/dev/null 2>&1; then
          return 0
        fi
      else
        # Fallback: plain TCP reachability when curl lacks explicit h2c support.
        if perl -MIO::Socket::INET -e '
          my $u = $ARGV[0];
          if ($u =~ m{^[a-z]+://([^/:]+)(?::(\d+))?/?}i) {
            my ($h,$p)=($1,$2||80);
            my $s=IO::Socket::INET->new(PeerHost=>$h,PeerPort=>$p,Proto=>"tcp",Timeout=>2);
            exit($s ? 0 : 1);
          }
          exit 1;
        ' "${url}" >/dev/null 2>&1; then
          return 0
        fi
      fi
    elif curl -fsS --max-time 2 "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if [[ "$(date +%s)" -ge "${deadline}" ]]; then
      echo "error: ${name} readiness check failed: ${url}" >&2
      exit 1
    fi
    sleep "${READINESS_POLL_INTERVAL_SEC}"
  done
}

warmup_client_and_server() {
  if [[ "${WARMUP_REQUESTS}" -le 0 ]]; then
    return 0
  fi

  local warmup_streams="${WARMUP_CONCURRENCY}"
  if [[ "${warmup_streams}" -gt "${WARMUP_REQUESTS}" ]]; then
    warmup_streams="${WARMUP_REQUESTS}"
  fi
  if [[ "${warmup_streams}" -lt 1 ]]; then
    warmup_streams=1
  fi

  "${H3_CLIENT_BIN}" \
    --connect "${TARGET}" \
    --host "${HOST}" \
    --path "${WARMUP_PATH}" \
    --insecure \
    --requests "${WARMUP_REQUESTS}" \
    --parallel-streams "${warmup_streams}" \
    --report-latency \
    >/dev/null 2>/dev/null || true
}

run_scenario() {
  local name="$1"
  local path="$2"
  local requests="$3"
  local concurrency="$4"
  local max_retries
  max_retries="${SCENARIO_RETRY_ATTEMPTS}"
  if [[ "${max_retries}" -lt 0 ]]; then
    max_retries=0
  fi

  local attempt=0
  while true; do
    local raw
    local lat_ok
    raw="$(mktemp)"
    lat_ok="$(mktemp)"

    local start_ns
    local end_ns
    start_ns="$(now_ns)"

    local workers
    workers="${concurrency}"
    if [[ "${workers}" -gt "${requests}" ]]; then
      workers="${requests}"
    fi
    if [[ "${workers}" -lt 1 ]]; then
      workers=1
    fi

    local batch_base batch_remainder worker_reqs
    batch_base=$((requests / workers))
    batch_remainder=$((requests % workers))

    local tmpdir
    tmpdir="$(mktemp -d)"
    local -a pids=()
    local i
    for ((i=0; i<workers; i++)); do
      worker_reqs="${batch_base}"
      if [[ "${i}" -lt "${batch_remainder}" ]]; then
        worker_reqs=$((worker_reqs + 1))
      fi
      if [[ "${worker_reqs}" -le 0 ]]; then
        continue
      fi

      local worker_streams
      worker_streams="${STREAMS_PER_WORKER}"
      if [[ "${worker_streams}" -gt "${worker_reqs}" ]]; then
        worker_streams="${worker_reqs}"
      fi

      "${H3_CLIENT_BIN}" \
        --connect "${TARGET}" \
        --host "${HOST}" \
        --path "${path}" \
        --insecure \
        --requests "${worker_reqs}" \
        --parallel-streams "${worker_streams}" \
        --report-latency \
        >"${tmpdir}/worker.${i}.out" \
        2>"${tmpdir}/worker.${i}.err" &
      pids+=("$!")
    done

    local pid
    for pid in "${pids[@]}"; do
      wait "${pid}" || true
    done

    # Aggregate only well-formed "<ok> <latency_ns>" output lines.
    awk '/^[01] [0-9]+$/ {print $1, $2}' "${tmpdir}"/worker.*.out >"${raw}" || true

    end_ns="$(now_ns)"
    rm -rf "${tmpdir}"

    local success
    local errors
    success=$(awk '$1 == 1 {c++} END {print c+0}' "${raw}")
    errors=$((requests - success))
    if [[ "${errors}" -lt 0 ]]; then
      errors=0
    fi

    awk '$1 == 1 {print $2}' "${raw}" | sort -n >"${lat_ok}"

    local min_ns avg_ns p50_ns p95_ns p99_ns max_ns
    min_ns=$(awk 'NR==1{print; exit}' "${lat_ok}")
    avg_ns=$(awk '{s+=$1} END {if (NR==0) print 0; else printf "%.0f", s/NR}' "${lat_ok}")
    p50_ns=$(percentile_from_sorted_file 50 "${lat_ok}")
    p95_ns=$(percentile_from_sorted_file 95 "${lat_ok}")
    p99_ns=$(percentile_from_sorted_file 99 "${lat_ok}")
    max_ns=$(awk 'END{print ($1+0)}' "${lat_ok}")

    min_ns=${min_ns:-0}
    avg_ns=${avg_ns:-0}
    p50_ns=${p50_ns:-0}
    p95_ns=${p95_ns:-0}
    p99_ns=${p99_ns:-0}
    max_ns=${max_ns:-0}

    local dur_ns
    local throughput
    dur_ns=$((end_ns - start_ns))
    throughput=$(awk -v ok="${success}" -v ns="${dur_ns}" 'BEGIN { if (ns <= 0) print "0.00"; else printf "%.2f", (ok*1000000000.0)/ns }')

    local parsed
    parsed=$(wc -l <"${raw}")
    local retry_required=0
    if [[ "${success}" -eq 0 || "${parsed}" -eq 0 || $((success + errors)) -lt "${requests}" ]]; then
      retry_required=1
    elif [[ "${SCENARIO_RETRY_ON_ERROR}" -eq 1 && "${errors}" -gt 0 ]]; then
      retry_required=1
    fi

    if [[ "${retry_required}" -eq 1 && "${attempt}" -lt "${max_retries}" ]]; then
      echo "warn: scenario '${name}' retrying (success=${success}, errors=${errors}, parsed=${parsed})..." >&2
      attempt=$((attempt + 1))
      rm -f "${raw}" "${lat_ok}"
      sleep "${SCENARIO_RETRY_COOLDOWN_SEC}"
      continue
    fi

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "${name}" "${path}" "${requests}" "${concurrency}" "${success}" "${errors}" \
      "${throughput}" "${min_ns}" "${avg_ns}" "${p50_ns}" "${p95_ns}" "${p99_ns}" >>"${results_tsv}"

    rm -f "${raw}" "${lat_ok}"
    break
  done
}

apply_netem_loss_if_configured() {
  if [[ -z "${NETEM_IFACE}" ]]; then
    return 1
  fi
  if ! command -v tc >/dev/null 2>&1; then
    echo "warn: tc not available; running quic_loss scenario without netem injection" >&2
    return 1
  fi

  sudo tc qdisc replace dev "${NETEM_IFACE}" root netem loss "${LOSS_PERCENT}%"
  return 0
}

clear_netem_loss_if_configured() {
  if [[ -z "${NETEM_IFACE}" ]]; then
    return 0
  fi
  if ! command -v tc >/dev/null 2>&1; then
    return 0
  fi
  sudo tc qdisc del dev "${NETEM_IFACE}" root >/dev/null 2>&1 || true
}

trap clear_netem_loss_if_configured EXIT

wait_ready_url "backend" "${BACKEND_READY_URL}" "h2c"
wait_ready_url "control" "${CONTROL_READY_URL}"
wait_ready_url "metrics" "${METRICS_READY_URL}"
warmup_client_and_server

run_scenario "burst" "${BURST_PATH}" "${BURST_REQUESTS}" "${BURST_CONCURRENCY}"
run_scenario "slow_upstream" "${SLOW_PATH}" "${SLOW_REQUESTS}" "${SLOW_CONCURRENCY}"
if apply_netem_loss_if_configured; then
  run_scenario "quic_loss" "${LOSS_PATH}" "${LOSS_REQUESTS}" "${LOSS_CONCURRENCY}"
  clear_netem_loss_if_configured
else
  run_scenario "quic_loss" "${LOSS_PATH}" "${LOSS_REQUESTS}" "${LOSS_CONCURRENCY}"
fi

# JSON export
{
  echo '{'
  echo '  "target": "'"${TARGET}"'",'
  echo '  "host": "'"${HOST}"'",'
  echo '  "generated_unix_secs": '"$(date +%s)"','
  echo '  "scenarios": ['
  awk -F'\t' 'BEGIN{first=1} {
    if (!first) printf(",\n");
    first=0;
    printf("    {\"name\":\"%s\",\"path\":\"%s\",\"requests\":%s,\"concurrency\":%s,\"success\":%s,\"errors\":%s,\"throughput_req_s\":%s,\"latency_ns\":{\"min\":%s,\"avg\":%s,\"p50\":%s,\"p95\":%s,\"p99\":%s}}",
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
  } END{printf("\n")} ' "${results_tsv}"
  echo '  ]'
  echo '}'
} >"${results_json}"

# Markdown export
{
  echo "# Spooky Load Scenarios"
  echo
  echo "- Target: \`${TARGET}\`"
  echo "- Host: \`${HOST}\`"
  echo "- Generated: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  echo
  echo "| scenario | path | requests | concurrency | success | errors | throughput req/s | min ms | avg ms | p50 ms | p95 ms | p99 ms |"
  echo "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  awk -F'\t' '{
    min_ms=$8/1000000.0; avg_ms=$9/1000000.0; p50_ms=$10/1000000.0; p95_ms=$11/1000000.0; p99_ms=$12/1000000.0;
    printf("| %s | %s | %s | %s | %s | %s | %s | %.3f | %.3f | %.3f | %.3f | %.3f |\n",
      $1,$2,$3,$4,$5,$6,$7,min_ms,avg_ms,p50_ms,p95_ms,p99_ms)
  }' "${results_tsv}"
} >"${results_md}"

echo "Load scenario report: ${results_md}"
echo "Load scenario data:   ${results_json}"
