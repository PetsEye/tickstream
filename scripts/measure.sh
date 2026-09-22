#!/usr/bin/env bash
# Run the tickstream benchmark suite against a running stack and print a report.
#
# Isolates the pipeline by pausing the live producer for the throughput run, then
# restores it. Safe to re-run; does not modify configuration.
#
#   COUNT=50000 LATENCY_COUNT=3000 SSE_SECONDS=75 ./scripts/measure.sh
set -euo pipefail

export PYTHONPATH=src
PY="${PY:-python3}"
BENCH="$PY scripts/benchmark.py"

COUNT="${COUNT:-50000}"
LATENCY_COUNT="${LATENCY_COUNT:-3000}"
SSE_SECONDS="${SSE_SECONDS:-75}"

records() { $BENCH delta | $PY -c 'import sys,json;print(json.load(sys.stdin)["total_records"])'; }

# Wait until the row count stops growing for several consecutive polls.
wait_for_idle() {
  local previous=-1 current stable=0 tries="${1:-40}"
  for _ in $(seq 1 "${tries}"); do
    current=$(records)
    if [ "${current}" = "${previous}" ]; then
      stable=$((stable + 1))
      [ "${stable}" -ge 3 ] && return 0
    else
      stable=0
    fi
    previous="${current}"
    sleep 3
  done
}

# Wait until at least `target` rows exist (the whole backlog drained).
wait_for_target() {
  local target="$1" previous=-1 current stable=0 tries="${2:-120}"
  for _ in $(seq 1 "${tries}"); do
    current=$(records)
    [ "${current}" -ge "${target}" ] && return 0
    if [ "${current}" = "${previous}" ]; then
      stable=$((stable + 1))
      [ "${stable}" -ge 4 ] && return 0
    else
      stable=0
    fi
    previous="${current}"
    sleep 3
  done
}

echo "== tickstream benchmark =="
echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "throughput_count=${COUNT} latency_count=${LATENCY_COUNT} sse_seconds=${SSE_SECONDS}"

echo
echo "-- SSE delivery latency (database write -> client, live traffic) --"
$BENCH sse --seconds "${SSE_SECONDS}"

echo
echo "-- container footprint --"
$BENCH resources

echo
echo "-- pausing the live producer to isolate the pipeline --"
docker compose stop producer >/dev/null 2>&1 || true
trap 'docker compose start producer >/dev/null 2>&1 || true' EXIT

echo "-- waiting for the pipeline to go idle --"
wait_for_idle 40

echo
echo "-- producer -> Kafka throughput --"
before_total=$(records)
started=$(date +%s)
$BENCH produce --count "${COUNT}" --mode throughput

echo
echo "-- waiting for Spark to process the whole backlog --"
wait_for_target "$((before_total + COUNT))" 120
finished=$(date +%s)
after_total=$(records)

echo
echo "-- end-to-end processed throughput (Spark -> Delta) --"
$PY - "${before_total}" "${after_total}" "$((finished - started))" "${COUNT}" <<'PYEOF'
import json
import sys

before, after, elapsed, produced = (int(v) for v in sys.argv[1:5])
processed = after - before
print(json.dumps({
    "produced_records": produced,
    "processed_records": processed,
    "elapsed_seconds": elapsed,
    "end_to_end_records_per_sec": round(processed / elapsed, 1) if elapsed else None,
    "max_offsets_per_trigger": 5000,
    "trigger_interval": "5s",
}, indent=2))
PYEOF

echo
echo "-- producer publish latency --"
$BENCH produce --count "${LATENCY_COUNT}" --mode latency

echo
echo "== done =="
