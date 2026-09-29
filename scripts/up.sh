#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/bootstrap-host.sh
./scripts/preflight.sh
# Pull registry images serially. Docker Compose versions affected by
# docker/compose#12747 can panic with "concurrent map writes" during parallel pull.
./scripts/compose-safe.sh pull --ignore-buildable
# Images built by `make build` are local; do not trigger another implicit pull/build here.
./scripts/compose-safe.sh up -d --no-build --pull never
