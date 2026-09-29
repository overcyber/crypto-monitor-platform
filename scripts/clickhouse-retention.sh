#!/usr/bin/env bash
set -euo pipefail
HOT_DAYS="${CLICKHOUSE_HOT_RETENTION_DAYS:-90}"
CANDLE_DAYS="${CLICKHOUSE_CANDLE_RETENTION_DAYS:-365}"
ALERT_DAYS="${CLICKHOUSE_ALERT_RETENTION_DAYS:-180}"
RECON_DAYS="${CLICKHOUSE_RECON_RETENTION_DAYS:-365}"
HOST=clickhouse
AUTH=(--host "$HOST" --user "${CLICKHOUSE_USER:-market_app}" --password "${CLICKHOUSE_PASSWORD}")
for table in events_hot microstructure_features; do
  clickhouse-client "${AUTH[@]}" --query \
    "ALTER TABLE ${CLICKHOUSE_DB:-market}.${table} MODIFY TTL event_time + INTERVAL ${HOT_DAYS} DAY DELETE"
done
clickhouse-client "${AUTH[@]}" --query "ALTER TABLE ${CLICKHOUSE_DB:-market}.candles_1m MODIFY TTL open_time + INTERVAL ${CANDLE_DAYS} DAY DELETE"
clickhouse-client "${AUTH[@]}" --query "ALTER TABLE ${CLICKHOUSE_DB:-market}.alerts MODIFY TTL event_time + INTERVAL ${ALERT_DAYS} DAY DELETE"
clickhouse-client "${AUTH[@]}" --query "ALTER TABLE ${CLICKHOUSE_DB:-market}.reconciliation_results MODIFY TTL created_at + INTERVAL ${RECON_DAYS} DAY DELETE"
echo "ClickHouse retention: hot=${HOT_DAYS}d candles=${CANDLE_DAYS}d alerts=${ALERT_DAYS}d reconciliation=${RECON_DAYS}d"
