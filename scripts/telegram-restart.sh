#!/usr/bin/env bash
set -euo pipefail
./scripts/compose-safe.sh up -d --force-recreate --no-deps telegram-bot
./scripts/compose-safe.sh ps telegram-bot
