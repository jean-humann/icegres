#!/usr/bin/env bash
# Start RustFS single-node S3 server on 127.0.0.1:9000 (idempotent).
set -euo pipefail

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$INFRA_DIR/.data/rustfs"
LOG_FILE="$INFRA_DIR/.data/rustfs.log"
PID_FILE="$INFRA_DIR/.data/rustfs.pid"
RUSTFS_BIN="${RUSTFS_BIN:-$INFRA_DIR/.data/bin/rustfs}"

ADDRESS="127.0.0.1:9000"
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfssecret"

mkdir -p "$DATA_DIR" "$(dirname "$LOG_FILE")"

# Idempotency: if already running and answering, do nothing.
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  if curl -sf -o /dev/null "http://$ADDRESS/" || curl -s -o /dev/null -w '%{http_code}' "http://$ADDRESS/" | grep -qE '^(200|403|404)$'; then
    echo "rustfs already running (pid $(cat "$PID_FILE")) on $ADDRESS"
    exit 0
  fi
fi
rm -f "$PID_FILE"

if [ ! -x "$RUSTFS_BIN" ]; then
  echo "ERROR: rustfs binary not found at $RUSTFS_BIN" >&2
  exit 1
fi

RUSTFS_ACCESS_KEY="$ACCESS_KEY" \
RUSTFS_SECRET_KEY="$SECRET_KEY" \
RUSTFS_ADDRESS="$ADDRESS" \
RUSTFS_VOLUMES="$DATA_DIR" \
RUSTFS_CONSOLE_ENABLE=false \
RUST_LOG="${RUST_LOG:-warn}" \
nohup "$RUSTFS_BIN" server "$DATA_DIR" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

# Wait for the S3 endpoint to answer.
for _ in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://$ADDRESS/" || true)
  if [ "$code" != "000" ] && [ -n "$code" ]; then
    echo "rustfs up on $ADDRESS (pid $(cat "$PID_FILE"), http $code)"
    exit 0
  fi
  if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "ERROR: rustfs exited during startup; see $LOG_FILE" >&2
    tail -20 "$LOG_FILE" >&2 || true
    rm -f "$PID_FILE"
    exit 1
  fi
  sleep 1
done
echo "ERROR: rustfs did not become ready in 60s; see $LOG_FILE" >&2
exit 1
