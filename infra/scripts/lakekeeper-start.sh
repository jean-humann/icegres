#!/usr/bin/env bash
# Idempotent Lakekeeper start script.
# Runs DB migrations, then starts the Lakekeeper server on 127.0.0.1:8181.
# Requires Postgres on 127.0.0.1:5433 (see pg-start.sh).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$INFRA_DIR/.data"
BIN="$INFRA_DIR/src/lakekeeper/target/release/lakekeeper"
PID_FILE="$DATA_DIR/lakekeeper.pid"
LOG_FILE="$DATA_DIR/lakekeeper.log"

mkdir -p "$DATA_DIR"

if [[ ! -x "$BIN" ]]; then
  echo "ERROR: lakekeeper binary not found at $BIN" >&2
  echo "Build it: cd $INFRA_DIR/src/lakekeeper && RUSTUP_TOOLCHAIN=stable SQLX_OFFLINE=true cargo build --release --bin lakekeeper -p lakekeeper-bin" >&2
  exit 1
fi

# Already running? Trust the pidfile only if the PID is a live lakekeeper
# process AND the server answers on :8181 (a pidfile surviving a reboot/crash
# may name a PID recycled by an unrelated process).
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null \
    && [[ "$(ps -o comm= -p "$(cat "$PID_FILE")" 2>/dev/null)" == lakekeeper ]] \
    && curl -sf -o /dev/null http://127.0.0.1:8181/health; then
  echo "lakekeeper already running (pid $(cat "$PID_FILE"))"
  exit 0
fi
rm -f "$PID_FILE"

export LAKEKEEPER__PG_DATABASE_URL_READ="postgresql://lakekeeper:lakekeeper@127.0.0.1:5433/lakekeeper"
export LAKEKEEPER__PG_DATABASE_URL_WRITE="postgresql://lakekeeper:lakekeeper@127.0.0.1:5433/lakekeeper"
export LAKEKEEPER__PG_ENCRYPTION_KEY="lakekeeper-dev-encryption-key"
export LAKEKEEPER__LISTEN_PORT=8181
export LAKEKEEPER__BIND_IP=127.0.0.1
# Metrics server defaults to port 9000 which collides with RustFS; move it.
export LAKEKEEPER__METRICS__PORT=9090
export RUST_LOG="${RUST_LOG:-info}"

# Migrations are idempotent; run them every start.
"$BIN" migrate >>"$LOG_FILE" 2>&1

nohup "$BIN" serve >>"$LOG_FILE" 2>&1 &
CHILD=$!
echo "$CHILD" > "$PID_FILE"

# Wait for health. Check the spawned server is still alive first, so an
# instant crash fails fast and a foreign listener on :8181 can't make the
# loop report success for a dead child.
for i in $(seq 1 30); do
  if ! kill -0 "$CHILD" 2>/dev/null; then
    echo "ERROR: lakekeeper exited during startup; see $LOG_FILE" >&2
    tail -20 "$LOG_FILE" >&2 || true
    rm -f "$PID_FILE"
    exit 1
  fi
  if curl -sf -o /dev/null http://127.0.0.1:8181/health; then
    echo "lakekeeper running (pid $CHILD) on http://127.0.0.1:8181"
    exit 0
  fi
  sleep 1
done
echo "ERROR: lakekeeper failed to become healthy; see $LOG_FILE" >&2
exit 1
