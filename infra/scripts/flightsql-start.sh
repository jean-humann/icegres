#!/usr/bin/env bash
# Idempotent start script for the Arrow Flight SQL endpoint
# (bench/flightsql-server) over the same Iceberg tables icegres serves.
# Serves grpc://127.0.0.1:50051. Requires the base stack (up.sh) to be
# running: Lakekeeper :8181, RustFS :9000.
#
# Build first if needed:
#   cd bench/flightsql-server && \
#   CARGO_TARGET_DIR=../../icegres/target cargo build --release
#
# Prints STARTUP_MS (spawn -> port accepting, which the server only does
# after the Iceberg catalog is wired) and RSS_MB once ready.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
DATA_DIR="$REPO_DIR/infra/.data"
BIN="$REPO_DIR/icegres/target/release/flightsql-server"
PID_FILE="$DATA_DIR/flightsql.pid"
LOG_FILE="$DATA_DIR/flightsql.log"
PORT="${FLIGHTSQL_PORT:-50051}"

mkdir -p "$DATA_DIR"

if [[ ! -x "$BIN" ]]; then
  echo "ERROR: flightsql-server binary not found at $BIN" >&2
  echo "Build it: cd $REPO_DIR/bench/flightsql-server && CARGO_TARGET_DIR=$REPO_DIR/icegres/target cargo build --release" >&2
  exit 1
fi

port_open() {
  python3 -c "import socket,sys; s=socket.socket(); s.settimeout(0.3);
sys.exit(0 if s.connect_ex(('127.0.0.1', $PORT))==0 else 1)"
}

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null \
    && [[ "$(ps -o comm= -p "$(cat "$PID_FILE")" 2>/dev/null)" == flightsql-serve* ]] \
    && port_open; then
  echo "flightsql-server already running (pid $(cat "$PID_FILE"))"
  exit 0
fi
rm -f "$PID_FILE"

export FLIGHTSQL_HOST="${FLIGHTSQL_HOST:-0.0.0.0}"
export FLIGHTSQL_PORT="$PORT"
export RUST_LOG="${RUST_LOG:-info}"

START_NS=$(date +%s%N)
nohup "$BIN" >>"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" >"$PID_FILE"

for _ in $(seq 1 300); do
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "ERROR: flightsql-server exited during startup; tail of $LOG_FILE:" >&2
    tail -20 "$LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
  fi
  if port_open; then
    STARTUP_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
    RSS_KB=$(awk '/^VmRSS/{print $2}' "/proc/$PID/status")
    echo "flightsql-server ready (pid $PID) on grpc://127.0.0.1:$PORT"
    echo "STARTUP_MS=$STARTUP_MS"
    echo "RSS_MB=$(( RSS_KB / 1024 ))"
    exit 0
  fi
  sleep 0.1
done

echo "ERROR: flightsql-server did not open port $PORT in 30s" >&2
exit 1
