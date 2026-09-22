#!/usr/bin/env bash
# End-to-end smoke test: verify the pipeline is flowing into TimescaleDB.
set -euo pipefail

CONTAINER="${TICKSTREAM_PG_CONTAINER:-tickstream-timescaledb}"
PG_USER="${POSTGRES_USER:-tickstream}"
PG_DB="${POSTGRES_DB:-tickstream}"

query() {
  docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-tickstream}" "${CONTAINER}" \
    psql -U "${PG_USER}" -d "${PG_DB}" -tAc "$1"
}

echo "waiting for candles to land in TimescaleDB (up to 120s)..."
for _ in $(seq 1 60); do
  count=$(query "SELECT count(*) FROM candles;" 2>/dev/null || echo 0)
  if [ "${count}" -gt 0 ]; then
    echo "OK: ${count} candles written"
    query "SELECT symbol, window_start, close, vwap, trade_count FROM candles ORDER BY window_start DESC LIMIT 5;"
    anomalies=$(query "SELECT count(*) FROM anomalies;" 2>/dev/null || echo 0)
    echo "anomalies detected: ${anomalies}"
    exit 0
  fi
  sleep 2
done

echo "FAIL: no candles were written within the timeout" >&2
exit 1
