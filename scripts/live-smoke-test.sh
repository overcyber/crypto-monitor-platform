#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

fail=0
TMP="/tmp/crypto-v4-smoke.$$"
trap 'rm -f "$TMP"' EXIT

pass() { echo "PASS  $*"; }
fail_msg() { echo "FAIL  $*"; fail=1; }

check_http() {
  local name="$1" url="$2"
  if curl -fsS --max-time 10 "$url" >"$TMP" 2>/dev/null; then pass "$name  $url"; else fail_msg "$name  $url"; fi
}

check_404() {
  local name="$1" url="$2" code
  code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 "$url" || true)
  if [ "$code" = "404" ]; then pass "$name isolation"; else fail_msg "$name expected 404 got $code"; fi
}

check_http "monitor-api" "http://127.0.0.1:${MONITOR_API_PORT:-8081}/health"
check_http "analytics-api" "http://127.0.0.1:${ANALYTICS_API_PORT:-8082}/health"
check_http "replay-api" "http://127.0.0.1:${REPLAY_API_PORT:-8084}/health"
check_http "flink-ui" "http://127.0.0.1:${FLINK_UI_PORT:-8083}/overview"
check_http "clickhouse" "http://127.0.0.1:${CLICKHOUSE_HTTP_PORT:-8123}/ping"
check_http "lakekeeper" "http://127.0.0.1:${LAKEKEEPER_PORT:-8181}/health"
check_http "grafana" "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health"
check_http "web-ui" "http://127.0.0.1:${WEB_UI_PORT:-8090}/health"
check_404 "monitor-has-no-analytics" "http://127.0.0.1:${MONITOR_API_PORT:-8081}/v1/analyze/BTCUSDT"
check_404 "analytics-has-no-monitor" "http://127.0.0.1:${ANALYTICS_API_PORT:-8082}/v1/quote/BTCUSDT"

if ./scripts/compose-safe.sh exec -T garage /garage status >/dev/null 2>&1; then pass "garage object store"; else fail_msg "garage object store"; fi

if ./scripts/compose-safe.sh exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list | grep -qx 'market.raw'; then
  pass "kafka topic market.raw"
else
  fail_msg "kafka topic market.raw"
fi

if curl -fsS "http://127.0.0.1:${FLINK_UI_PORT:-8083}/jobs/overview" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert any(j.get("name")=="market-raw-to-iceberg-and-clean-v4" and j.get("state") in {"RUNNING","RESTARTING"} for j in d.get("jobs",[]))'; then
  pass "Flink streaming job active"
else
  fail_msg "Flink streaming job active"
fi

if curl -fsS -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
  --data-binary "SELECT count() FROM system.tables WHERE database='${CLICKHOUSE_DB}' AND name='events_hot' FORMAT TSV" \
  "http://127.0.0.1:${CLICKHOUSE_HTTP_PORT:-8123}/" | grep -qx '1'; then
  pass "clickhouse events_hot schema"
else
  fail_msg "clickhouse events_hot schema"
fi

# Validate the complete live pipeline. This is intentionally a wait: health alone is not evidence of data flow.
WAIT_SECONDS="${SMOKE_WAIT_SECONDS:-180}"
DEADLINE=$((SECONDS + WAIT_SECONDS))
HOT_COUNT=0
while [ "$SECONDS" -lt "$DEADLINE" ]; do
  HOT_COUNT=$(curl -fsS -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    --data-binary "SELECT count() FROM ${CLICKHOUSE_DB}.events_hot" \
    "http://127.0.0.1:${CLICKHOUSE_HTTP_PORT:-8123}/" 2>/dev/null | tr -d '[:space:]' || echo 0)
  case "$HOT_COUNT" in ''|*[!0-9]*) HOT_COUNT=0;; esac
  [ "$HOT_COUNT" -gt 0 ] && break
  sleep 5
done
if [ "$HOT_COUNT" -gt 0 ]; then pass "WebSocket -> Kafka -> Flink -> ClickHouse events=$HOT_COUNT"; else fail_msg "no hot events after ${WAIT_SECONDS}s"; fi

# Read at least one raw Iceberg row through Lakekeeper/S3 using the same Python client as reconciliation/replay.
if ./scripts/compose-safe.sh exec -T analytics-api python - <<'PY' >/dev/null 2>&1
from src.common.iceberg import raw_table
reader = raw_table().scan(selected_fields=("event_id",)).to_arrow_batch_reader()
first = next(iter(reader), None)
if first is None or first.num_rows < 1:
    raise SystemExit(1)
PY
then
  pass "Flink -> Iceberg/Lakekeeper/Garage raw truth readable"
else
  fail_msg "Iceberg raw truth has no readable rows"
fi

# Monitoring API must be able to serve the default Binance symbol after the hot path is alive.
SMOKE_SYMBOL="${SMOKE_SYMBOL:-BTCUSDT}"
if curl -fsS --max-time 10 "http://127.0.0.1:${MONITOR_API_PORT:-8081}/v1/quote/${SMOKE_SYMBOL}" >/dev/null; then
  pass "monitor quote ${SMOKE_SYMBOL}"
else
  fail_msg "monitor quote ${SMOKE_SYMBOL}"
fi

exit "$fail"
