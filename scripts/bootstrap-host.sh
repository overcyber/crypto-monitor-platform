#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"
[ -f .env ] || cp .env.example .env
set -a
# shellcheck disable=SC1091
source .env
set +a
paths=(
  "${KAFKA_DATA_DIR:-./data/kafka}"
  "${CLICKHOUSE_DATA_DIR:-./data/clickhouse}"
  "${CLICKHOUSE_LOG_DIR:-./data/clickhouse-logs}"
  "${GARAGE_META_DIR:-./data/garage/meta}"
  "${GARAGE_DATA_DIR:-./data/garage/data}"
  "$(dirname "${GARAGE_CONFIG_FILE:-./data/garage/config/garage.toml}")"
  "${LAKEKEEPER_PG_DATA_DIR:-./data/lakekeeper-postgres}"
  "${GRAFANA_DATA_DIR:-./data/grafana}"
  "${RECONCILE_DATA_DIR:-./data/reconcile}"
  "${REPLAY_DATA_DIR:-./data/replay}"
  "${TELEGRAM_DATA_DIR:-./data/telegram}"
  "${FLINK_CHECKPOINT_DIR:-./data/flink/checkpoints}"
  "${FLINK_SAVEPOINT_DIR:-./data/flink/savepoints}"
)
for path in "${paths[@]}"; do
  mkdir -p "$path"
  # Development/single-host compatibility across container UIDs.
  # See docs/SECURITY.md for hardened ownership recommendations.
  chmod a+rwx "$path"
done

GARAGE_CONFIG_FILE="${GARAGE_CONFIG_FILE:-./data/garage/config/garage.toml}"
python3 - "$GARAGE_CONFIG_FILE" <<'PY'
import os, pathlib, sys
out=pathlib.Path(sys.argv[1])
tpl=pathlib.Path('infra/garage/garage.toml.template').read_text()
tpl=tpl.replace('__GARAGE_RPC_SECRET__', os.environ['GARAGE_RPC_SECRET'])
tpl=tpl.replace('__S3_REGION__', os.environ.get('S3_REGION','us-east-1'))
out.write_text(tpl)
out.chmod(0o600)
PY

printf 'Host storage prepared:\n'
printf '  %s\n' "${paths[@]}"
echo "Garage config rendered: ${GARAGE_CONFIG_FILE}"
