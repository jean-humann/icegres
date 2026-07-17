#!/usr/bin/env bash
# Stop RustFS (idempotent).
set -euo pipefail

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$INFRA_DIR/.data/rustfs.pid"

if [ -f "$PID_FILE" ]; then
  PID="$(cat "$PID_FILE")"
  # Guard against PID reuse: the pidfile survives reboots/crashes, so only
  # trust it if the PID actually belongs to a rustfs process.
  if kill -0 "$PID" 2>/dev/null && [ "$(ps -o comm= -p "$PID" 2>/dev/null)" = "rustfs" ]; then
    kill "$PID" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 1
    done
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID" 2>/dev/null || true
    echo "rustfs stopped (pid $PID)"
    rm -f "$PID_FILE"
    exit 0
  fi
  echo "rustfs not running under recorded pid (stale pid file removed)"
  rm -f "$PID_FILE"
fi
# Fallback: kill any rustfs server bound to our data dir.
pkill -f "rustfs server .*/infra/.data/rustfs" 2>/dev/null && echo "rustfs stopped (via pkill)" || echo "rustfs not running"
