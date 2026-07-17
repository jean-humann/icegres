#!/usr/bin/env bash
# Idempotent Trino stop script.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
PID_FILE="$INFRA_DIR/.data/trino.pid"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  # Guard against PID reuse: the pidfile survives reboots/crashes, so only
  # trust it if the PID actually belongs to a trino-server process.
  if kill -0 "$PID" 2>/dev/null && [[ "$(ps -o comm= -p "$PID" 2>/dev/null)" == trino-server ]]; then
    kill "$PID"
    for i in $(seq 1 30); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 1
    done
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID" || true
    echo "trino stopped (pid $PID)"
    rm -f "$PID_FILE"
    exit 0
  fi
  echo "trino not running under recorded pid (stale pid file removed)"
  rm -f "$PID_FILE"
fi
# Fallback: stop a trino-server orphaned from its pidfile (comm is renamed to
# trino-server by the launcher, so this cannot match unrelated java processes).
if pkill -x trino-server 2>/dev/null; then
  for i in $(seq 1 30); do
    pgrep -x trino-server >/dev/null 2>&1 || break
    sleep 1
  done
  pkill -9 -x trino-server 2>/dev/null || true
  echo "trino stopped (via pkill)"
else
  echo "trino not running"
fi
