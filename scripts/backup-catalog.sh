#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo ".env missing" >&2; exit 1; }
set -a; source .env; set +a
OUT_DIR="${BACKUP_DIR:-./data/backups}"
mkdir -p "$OUT_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="$OUT_DIR/lakekeeper-${STAMP}.sql.gz"
./scripts/compose-safe.sh exec -T lakekeeper-db pg_dump \
  -U "$LAKEKEEPER_PG_USER" -d "$LAKEKEEPER_PG_DB" --no-owner --no-acl | gzip -9 > "$OUT"
sha256sum "$OUT" > "$OUT.sha256"
echo "$OUT"
