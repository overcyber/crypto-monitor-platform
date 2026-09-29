#!/usr/bin/env bash
set -euo pipefail
AUTH=(--host clickhouse --user "${CLICKHOUSE_USER:-market_app}" --password "${CLICKHOUSE_PASSWORD}" --database "${CLICKHOUSE_DB:-market}")
shopt -s nullglob
for f in /opt/app/clickhouse-init/*.sql; do
  echo "Applying $f"
  clickhouse-client "${AUTH[@]}" --multiquery < "$f"
done
for f in /opt/app/clickhouse-init/*.sql.template; do
  echo "Applying rendered $f"
  envsubst < "$f" | clickhouse-client "${AUTH[@]}" --multiquery
done
