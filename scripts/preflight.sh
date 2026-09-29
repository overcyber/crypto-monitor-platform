#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env
set -a; source .env; set +a

err=0
warn=0
need() { command -v "$1" >/dev/null 2>&1 || { echo "FAIL missing command: $1"; err=1; }; }
need docker
need curl
if command -v docker >/dev/null 2>&1; then
  docker compose version >/dev/null 2>&1 || { echo "FAIL Docker Compose v2 is required"; err=1; }
fi

paths=(
  "${KAFKA_DATA_DIR:-./data/kafka}"
  "${CLICKHOUSE_DATA_DIR:-./data/clickhouse}"
  "${CLICKHOUSE_LOG_DIR:-./data/clickhouse-logs}"
  "${GARAGE_META_DIR:-./data/garage/meta}"
  "${GARAGE_DATA_DIR:-./data/garage/data}"
  "${LAKEKEEPER_PG_DATA_DIR:-./data/lakekeeper-postgres}"
  "${FLINK_CHECKPOINT_DIR:-./data/flink/checkpoints}"
  "${FLINK_SAVEPOINT_DIR:-./data/flink/savepoints}"
  "${GRAFANA_DATA_DIR:-./data/grafana}"
  "${RECONCILE_DATA_DIR:-./data/reconcile}"
  "${REPLAY_DATA_DIR:-./data/replay}"
)
for p in "${paths[@]}"; do
  mkdir -p "$p"
  if [ ! -w "$p" ]; then echo "FAIL not writable: $p"; err=1; fi
  df -Pk "$p" | awk -v p="$p" 'NR==2 {printf "INFO %-48s free=%0.1f GiB\n", p, $4/1024/1024}'
done


# Lakekeeper release tags published on quay.io use a leading "v" (for example v0.13.6).
lk_ver="${LAKEKEEPER_VERSION:-v0.13.6}"
if [[ "$lk_ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "FAIL LAKEKEEPER_VERSION=$lk_ver is missing the required leading v; use v$lk_ver"
  err=1
fi

if ! [[ "${GARAGE_RPC_SECRET:-}" =~ ^[0-9a-fA-F]{64}$ ]]; then
  echo "FAIL GARAGE_RPC_SECRET must be exactly 64 hex characters (32 bytes)"
  err=1
fi
if ! [[ "${S3_ACCESS_KEY:-}" =~ ^GK[0-9A-Fa-f]{24}$ ]]; then
  echo "FAIL S3_ACCESS_KEY must match Garage key form GK + 24 hex characters"
  err=1
fi

if [ "$(ulimit -n)" -lt 65535 ] 2>/dev/null; then
  echo "WARN open-files limit is $(ulimit -n); 65535+ is preferable for sustained streaming"
  warn=1
fi

if [ "${CLICKHOUSE_PASSWORD:-}" = "change-me-clickhouse" ] || \
   [ "${CLICKHOUSE_GRAFANA_PASSWORD:-}" = "change-me-grafana-clickhouse" ] || \
   [ "${S3_SECRET_KEY:-}" = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" ] || \
   [ "${GARAGE_RPC_SECRET:-}" = "0000000000000000000000000000000000000000000000000000000000000000" ] || \
   [ "${GRAFANA_ADMIN_PASSWORD:-}" = "change-me-grafana" ] || \
   [ "${LAKEKEEPER_PG_PASSWORD:-}" = "change-me-lakekeeper-pg" ]; then
  echo "WARN one or more example passwords are still in use"
  warn=1
  if [ "${BIND_ADDRESS:-127.0.0.1}" != "127.0.0.1" ] && [ "${BIND_ADDRESS:-127.0.0.1}" != "localhost" ]; then
    echo "FAIL default passwords must not be exposed on BIND_ADDRESS=${BIND_ADDRESS}"
    err=1
  fi
fi

if command -v free >/dev/null 2>&1; then
  free -h | awk 'NR==2 {print "INFO host RAM total=" $2 " available=" $7}'
fi

if [ "$err" -eq 0 ]; then
  if [ "$warn" -eq 0 ]; then echo "PASS preflight"; else echo "PASS preflight with warnings"; fi
fi
exit "$err"
