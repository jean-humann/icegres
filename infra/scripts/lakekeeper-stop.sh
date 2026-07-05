#!/usr/bin/env bash
# Idempotent Lakekeeper stop script.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
PID_FILE="$INFRA_DIR/.data/lakekeeper.pid"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  PID="$(cat "$PID_FILE")"
  kill "$PID"
  for i in $(seq 1 15); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 1
  done
  kill -0 "$PID" 2>/dev/null && kill -9 "$PID" || true
  echo "lakekeeper stopped (pid $PID)"
else
  echo "lakekeeper not running"
fi
rm -f "$PID_FILE"
