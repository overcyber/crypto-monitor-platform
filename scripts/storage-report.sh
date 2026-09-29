#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env
set -a; source .env; set +a
printf '%-24s %12s  %s\n' COMPONENT SIZE PATH
for entry in \
  "kafka|${KAFKA_DATA_DIR:-./data/kafka}" \
  "clickhouse|${CLICKHOUSE_DATA_DIR:-./data/clickhouse}" \
  "clickhouse-logs|${CLICKHOUSE_LOG_DIR:-./data/clickhouse-logs}" \
  "iceberg-garage-data|${GARAGE_DATA_DIR:-./data/garage/data}" \
  "garage-metadata|${GARAGE_META_DIR:-./data/garage/meta}" \
  "lakekeeper-postgres|${LAKEKEEPER_PG_DATA_DIR:-./data/lakekeeper-postgres}" \
  "flink-checkpoints|${FLINK_CHECKPOINT_DIR:-./data/flink/checkpoints}" \
  "flink-savepoints|${FLINK_SAVEPOINT_DIR:-./data/flink/savepoints}" \
  "grafana|${GRAFANA_DATA_DIR:-./data/grafana}" \
  "reconcile-stage|${RECONCILE_DATA_DIR:-./data/reconcile}" \
  "replay-stage|${REPLAY_DATA_DIR:-./data/replay}"; do
  name=${entry%%|*}; path=${entry#*|}
  size=$(du -sh "$path" 2>/dev/null | awk '{print $1}' || echo 0)
  printf '%-24s %12s  %s\n' "$name" "$size" "$path"
done
printf '\nFilesystem capacity:\n'
df -h "${GARAGE_DATA_DIR:-./data/garage/data}" "${CLICKHOUSE_DATA_DIR:-./data/clickhouse}" "${KAFKA_DATA_DIR:-./data/kafka}" | awk '!seen[$1]++'
