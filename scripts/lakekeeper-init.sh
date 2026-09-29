#!/bin/sh
set -eu
BODY=$(cat <<JSON
{
  "warehouse-name": "${ICEBERG_WAREHOUSE:-market-data}",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "delete-profile": {"type": "hard"},
  "storage-profile": {
    "type": "s3",
    "bucket": "${ICEBERG_BUCKET:-market-iceberg}",
    "endpoint": "${S3_ENDPOINT:-http://garage:3900}",
    "region": "${S3_REGION:-us-east-1}",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false,
    "remote-signing-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "${S3_ACCESS_KEY}",
    "aws-secret-access-key": "${S3_SECRET_KEY}"
  }
}
JSON
)
code=$(curl -sS -o /tmp/warehouse.out -w '%{http_code}' \
  -X POST http://lakekeeper:8181/management/v1/warehouse \
  -H 'Content-Type: application/json' --data "$BODY" || true)
case "$code" in
  200|201|204|409)
    echo "Lakekeeper warehouse ready (HTTP $code)" ;;
  400)
    if grep -q 'CreateWarehouseStorageProfileOverlap' /tmp/warehouse.out; then
      echo "Lakekeeper warehouse already exists and matches storage profile (HTTP 400)"
    else
      echo "Lakekeeper warehouse creation failed: HTTP $code" >&2
      cat /tmp/warehouse.out >&2 || true
      exit 1
    fi ;;
  *)
    echo "Lakekeeper warehouse creation failed: HTTP $code" >&2
    cat /tmp/warehouse.out >&2 || true
    exit 1 ;;
esac
