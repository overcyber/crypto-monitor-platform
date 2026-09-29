#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/preflight.sh
./scripts/compose-safe.sh pull --ignore-buildable
