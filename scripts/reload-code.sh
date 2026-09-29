#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Source code is bind-mounted. Restart code-bearing services only; no image rebuild.
./scripts/compose-safe.sh restart \
  ingestor-binance ingestor-coinbase book-worker alert-worker candle-worker replay-book-worker \
  monitor-api analytics-api reconciler replay-api
