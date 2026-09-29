#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
limit="${COMPOSE_PARALLEL_LIMIT:-1}"
exec docker compose --parallel "$limit" "$@"
