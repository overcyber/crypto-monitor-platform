#!/usr/bin/env sh
set -eu
MODULE="$1"
PORT="$2"
set -- uvicorn "$MODULE" --host 0.0.0.0 --port "$PORT"
case "${API_RELOAD:-false}" in
  1|true|TRUE|yes|YES) set -- "$@" --reload --reload-dir /opt/app/src ;;
esac
exec "$@"
