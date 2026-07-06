#!/usr/bin/env bash
# Stop the Arrow Flight SQL endpoint (icegres flight-serve) started by
# flightsql-start.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
PID_FILE="$REPO_DIR/infra/.data/flightsql.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "icegres flight-serve not running (no pidfile)"
  exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null \
    && [[ "$(ps -o comm= -p "$PID" 2>/dev/null)" == icegres ]] \
    && grep -qz -- flight-serve "/proc/$PID/cmdline" 2>/dev/null; then
  kill "$PID"
  for _ in $(seq 1 50); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
  fi
  echo "icegres flight-serve stopped (pid $PID)"
else
  echo "icegres flight-serve not running (stale pidfile)"
fi
rm -f "$PID_FILE"
