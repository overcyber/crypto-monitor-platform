#!/usr/bin/env bash
set -euo pipefail
JOB_NAME="market-raw-to-iceberg-and-clean-v4"
TEMPLATE=/opt/app/flink/jobs/01_streaming.sql.template
RENDERED=/tmp/01_streaming.sql
while true; do
  if curl -fsS http://flink-jobmanager:8081/jobs/overview 2>/dev/null | \
      jq -e --arg n "$JOB_NAME" '.jobs[]? | select(.name == $n and (.state == "RUNNING" or .state == "RESTARTING" or .state == "CREATED"))' >/dev/null; then
    sleep 30
    continue
  fi
  echo "Flink active job not found; rendering/submitting $JOB_NAME"
  envsubst < "$TEMPLATE" > "$RENDERED"
  /opt/flink/bin/sql-client.sh -f "$RENDERED"
  sleep 30
done
