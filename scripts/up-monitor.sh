#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

./scripts/bootstrap-host.sh
./scripts/preflight.sh

SERVICES=(
  kafka
  kafka-init
  garage
  lakekeeper-db
  lakekeeper-migrate
  lakekeeper
  lakekeeper-bootstrap
  lakekeeper-warehouse
  clickhouse
  clickhouse-schema-sync
  clickhouse-retention-init
  flink-jobmanager
  flink-taskmanager
  flink-job-supervisor
  ingestor-binance
  ingestor-coinbase
  ingestor-kraken
  monitor-api
  telegram-bot
)

if [[ "${1:-}" == "--with-ui" ]]; then
  SERVICES+=(web-ui)
fi

echo "Iniciando modo leve: Monitor de Preços + Alertas Telegram..."
./scripts/compose-safe.sh up -d --no-build --pull never "${SERVICES[@]}"

echo "Serviços ativos:"
./scripts/compose-safe.sh ps "${SERVICES[@]}"
