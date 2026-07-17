#!/usr/bin/env bash
# Idempotent Trino start script.
# Starts a single-node Trino coordinator on 127.0.0.1:8082 serving the shared
# Lakekeeper/RustFS Iceberg warehouse as catalog "iceberg" (tables appear as
# iceberg.demo.trips etc.). Requires the base stack (up.sh) to be running.
#
# Trino is NOT vendored in the repo: set TRINO_HOME to an extracted
# trino-server distribution that already contains an etc/ directory
# (config.properties on :8082, jvm.config -Xmx3g, catalog/iceberg.properties
# pointing at http://127.0.0.1:8181/catalog + s3 http://127.0.0.1:9000).
# NOTE: this Trino generation must match the installed JDK — Java 21 supports
# trino-server 446 (447+ require Java 22+).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$INFRA_DIR/.data"
TRINO_HOME="${TRINO_HOME:-/tmp/claude-0/-home-user-jean-humann/917b2dd2-1f49-560f-8a42-71e5677bbc01/scratchpad/engines/trino}"
TRINO_PORT="${TRINO_PORT:-8082}"
PID_FILE="$DATA_DIR/trino.pid"
LOG_FILE="${TRINO_LOG:-$DATA_DIR/trino.log}"

mkdir -p "$DATA_DIR"

if [[ ! -x "$TRINO_HOME/bin/launcher" ]]; then
  echo "ERROR: trino launcher not found at $TRINO_HOME/bin/launcher" >&2
  echo "Download trino-server-446.tar.gz from repo1.maven.org (io/trino/trino-server/446)," >&2
  echo "extract it, add etc/ (node/config/jvm + catalog/iceberg.properties), then" >&2
  echo "re-run with TRINO_HOME=<extracted dir>." >&2
  exit 1
fi
if [[ ! -f "$TRINO_HOME/etc/config.properties" ]]; then
  echo "ERROR: $TRINO_HOME/etc/config.properties missing — Trino is unconfigured" >&2
  exit 1
fi

healthy() {
  # /v1/info reports "starting":false once the server is fully up.
  curl -sf "http://127.0.0.1:$TRINO_PORT/v1/info" 2>/dev/null | grep -q '"starting":false'
}

# Already running? Trust the pidfile only if the PID is a live trino-server
# process AND the server answers on :$TRINO_PORT (a pidfile surviving a
# reboot/crash may name a PID recycled by an unrelated process).
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null \
    && [[ "$(ps -o comm= -p "$(cat "$PID_FILE")" 2>/dev/null)" == trino-server ]] \
    && healthy; then
  echo "trino already running (pid $(cat "$PID_FILE"))"
  exit 0
fi
rm -f "$PID_FILE"

mkdir -p "$TRINO_HOME/data"
# `launcher run` exec-chains bash -> launcher.py -> java, so $! stays the
# server PID; the JVM renames its comm to "trino-server".
nohup "$TRINO_HOME/bin/launcher" run \
    --etc-dir "$TRINO_HOME/etc" --data-dir "$TRINO_HOME/data" \
    >>"$LOG_FILE" 2>&1 &
CHILD=$!
echo "$CHILD" > "$PID_FILE"

# Wait for readiness. Check the spawned server is still alive first, so an
# instant crash fails fast and a foreign listener on the port can't make the
# loop report success for a dead child. JVM startup takes ~30s.
for i in $(seq 1 120); do
  if ! kill -0 "$CHILD" 2>/dev/null; then
    echo "ERROR: trino exited during startup; see $LOG_FILE" >&2
    tail -20 "$LOG_FILE" >&2 || true
    rm -f "$PID_FILE"
    exit 1
  fi
  if healthy; then
    echo "trino running (pid $CHILD) on http://127.0.0.1:$TRINO_PORT (catalog iceberg, e.g. iceberg.demo.trips)"
    exit 0
  fi
  sleep 1
done
echo "ERROR: trino failed to become ready in 120s; see $LOG_FILE" >&2
exit 1
