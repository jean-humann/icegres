#!/usr/bin/env bash
# Stop RustFS (idempotent).
set -euo pipefail

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$INFRA_DIR/.data/rustfs.pid"

if [ -f "$PID_FILE" ]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 1
    done
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID" 2>/dev/null || true
    echo "rustfs stopped (pid $PID)"
  else
    echo "rustfs not running (stale pid file removed)"
  fi
  rm -f "$PID_FILE"
else
  # Fallback: kill any rustfs server bound to our data dir.
  pkill -f "rustfs server .*/infra/.data/rustfs" 2>/dev/null && echo "rustfs stopped (via pkill)" || echo "rustfs not running"
fi
