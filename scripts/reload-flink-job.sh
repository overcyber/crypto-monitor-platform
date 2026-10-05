#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; source .env; set +a; fi
JOB_NAME="market-raw-to-iceberg-and-clean-v4"
JOB_ID=$(curl --noproxy '*' -fsS "http://127.0.0.1:${FLINK_UI_PORT:-8083}/jobs/overview" 2>/dev/null | \
  python3 -c 'import json,sys; d=json.load(sys.stdin); n="market-raw-to-iceberg-and-clean-v4"; print(next((j["jid"] for j in d.get("jobs",[]) if j.get("name")==n and j.get("state") in {"RUNNING","RESTARTING","CREATED"}),""))' || true)
if [ -n "$JOB_ID" ]; then
  echo "Cancelling Flink job $JOB_ID ($JOB_NAME)"
  curl --noproxy '*' -fsS -X PATCH "http://127.0.0.1:${FLINK_UI_PORT:-8083}/jobs/${JOB_ID}?mode=cancel" >/dev/null || true
else
  echo "No active Flink job found"
fi
# Supervisor notices the missing job and resubmits the bind-mounted SQL template.
echo "The supervisor will resubmit the current infra/flink/jobs/01_streaming.sql.template."
