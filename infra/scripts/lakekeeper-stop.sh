#!/usr/bin/env bash
# Idempotent Lakekeeper stop script.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
BIN="$INFRA_DIR/src/lakekeeper/target/release/lakekeeper"
PID_FILE="$INFRA_DIR/.data/lakekeeper.pid"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  # Guard against PID reuse: the pidfile survives reboots/crashes, so only
  # trust it if the PID actually belongs to a lakekeeper process.
  if kill -0 "$PID" 2>/dev/null && [[ "$(ps -o comm= -p "$PID" 2>/dev/null)" == lakekeeper ]]; then
    kill "$PID"
    for i in $(seq 1 15); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 1
    done
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID" || true
    echo "lakekeeper stopped (pid $PID)"
    rm -f "$PID_FILE"
    exit 0
  fi
  echo "lakekeeper not running under recorded pid (stale pid file removed)"
  rm -f "$PID_FILE"
fi
# Fallback: stop a lakekeeper serve orphaned from its pidfile.
pkill -f "$BIN serve" 2>/dev/null && echo "lakekeeper stopped (via pkill)" || echo "lakekeeper not running"
