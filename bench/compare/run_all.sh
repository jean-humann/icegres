#!/usr/bin/env bash
# Orchestrate the multi-engine comparison: for each engine in turn —
#   1. quiesce check (no other engine process still alive)
#   2. start the engine via its infra/scripts/*-start.sh (icegres: direct
#      `icegres serve`), timing startup_ms
#   3. run compare.py for that engine (passing the engine pid so VmHWM is
#      recorded as rss_peak_mb at the end of the run)
#   4. stop the engine and verify it is gone
# then merge the per-engine jsons into bench/results/compare-<ts>.json + .md.
#
# Engines run ONE AT A TIME (4 cores / 15 GB — JVMs get 2-3 GB heaps).
# Requires the base stack: bash infra/scripts/up.sh (Postgres :5433,
# RustFS :9000, Lakekeeper :8181) and demo.trips_big
# (python3 bench/compare/make_trips_big.py).
#
# Usage: bench/compare/run_all.sh [engine ...]   (default: all four)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
INFRA="$REPO_DIR/infra/scripts"
ICEGRES_BIN="$REPO_DIR/icegres/target/release/icegres"
DATA_DIR="$REPO_DIR/infra/.data"
OUT_DIR="$(mktemp -d /tmp/compare-run.XXXXXX)"
ENGINES=("$@")
[ ${#ENGINES[@]} -gt 0 ] || ENGINES=(icegres trino spark flightsql)

mkdir -p "$DATA_DIR"
echo "per-engine output dir: $OUT_DIR"

now_ms() { echo $(( $(date +%s%N) / 1000000 )); }

# List pids of real engine processes matching a cmdline pattern. pgrep -f
# alone also matches unrelated shells whose command line merely *mentions* an
# engine (e.g. a watcher script), so filter by the process comm name too.
engine_procs() {  # $1 = cmdline regex
  local pid comm
  for pid in $(pgrep -f "$1" 2>/dev/null); do
    comm=$(cat "/proc/$pid/comm" 2>/dev/null) || continue
    case "$comm" in
      java|trino-server|icegres) echo "$pid $comm" ;;
    esac
  done
}

# NOTE the flightsql engine is `icegres flight-serve` (same binary as the
# icegres engine, different wire protocol); "icegres serve" does NOT match
# its cmdline ("icegres flight-serve") so the two stay distinguishable.
ANY_ENGINE='trino-server|HiveThriftServer2|icegres flight-serve|icegres serve'
SCRATCH_ENGINES="${SCRATCH_ENGINES:-/tmp/claude-0/-home-user-jean-humann/917b2dd2-1f49-560f-8a42-71e5677bbc01/scratchpad/engines}"

# Poll until the engine answers a real client query (SELECT 1 over its own
# wire protocol) so startup_ms = cold start -> first successful query.
wait_first_query() {  # $1 = engine name; up to ~120 s
  local eng=$1
  for _ in $(seq 1 240); do
    python3 -c "
import sys; sys.path.insert(0, '$SCRIPT_DIR')
from compare import ENGINES
e = ENGINES['$eng']; c = e.connect(); e.run(c, 'SELECT 1'); c.close()
" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}

rss_now_mb() {  # $1 = pid -> current VmRSS in MB
  awk '/^VmRSS/{printf "%.1f", $2/1024}' "/proc/$1/status" 2>/dev/null
}

footprint() {
  case "$1" in
    icegres)   du -h "$ICEGRES_BIN" | cut -f1 ;;
    flightsql) du -h "$ICEGRES_BIN" | cut -f1 ;;  # same binary, flight-serve subcommand
    trino)     du -sh "$SCRATCH_ENGINES/trino" 2>/dev/null | cut -f1 ;;
    spark)     du -sh "$SCRATCH_ENGINES/spark" 2>/dev/null | cut -f1 ;;
  esac
}

quiesce() {
  # No query engine may be running before we start the next one.
  for _ in $(seq 1 10); do
    [ -z "$(engine_procs "$ANY_ENGINE")" ] && return 0
    sleep 2
  done
  echo "ERROR: another engine is still running:" >&2
  engine_procs "$ANY_ENGINE" >&2
  return 1
}

engine_pid() {
  case "$1" in
    icegres)   cat "$DATA_DIR/compare-icegres.pid" 2>/dev/null ;;
    trino)     cat "$DATA_DIR/trino.pid" 2>/dev/null ;;
    flightsql) cat "$DATA_DIR/flightsql.pid" 2>/dev/null ;;
    spark)     pgrep -f 'HiveThriftServer2' | head -1 ;;
  esac
}

start_engine() {
  case "$1" in
    icegres)
      nohup "$ICEGRES_BIN" serve --port 5439 \
        >>"$DATA_DIR/compare-icegres.log" 2>&1 &
      echo $! > "$DATA_DIR/compare-icegres.pid"
      # ready when pgwire answers
      for _ in $(seq 1 100); do
        python3 - <<'EOF' && return 0
import socket, sys
s = socket.socket(); s.settimeout(0.3)
sys.exit(0 if s.connect_ex(("127.0.0.1", 5439)) == 0 else 1)
EOF
        sleep 0.2
      done
      return 1 ;;
    trino)     bash "$INFRA/trino-start.sh" ;;
    spark)     bash "$INFRA/spark-start.sh" ;;
    flightsql) bash "$INFRA/flightsql-start.sh" ;;
  esac
}

stop_engine() {
  case "$1" in
    icegres)
      local pid; pid=$(cat "$DATA_DIR/compare-icegres.pid" 2>/dev/null || true)
      [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null
      rm -f "$DATA_DIR/compare-icegres.pid" ;;
    trino)     bash "$INFRA/trino-stop.sh" ;;
    spark)     bash "$INFRA/spark-stop.sh" ;;
    flightsql) bash "$INFRA/flightsql-stop.sh" ;;
  esac
  # wait for full exit (JVMs can take a few seconds)
  local pat
  case "$1" in
    icegres)   pat='icegres serve' ;;
    trino)     pat='trino-server' ;;
    spark)     pat='HiveThriftServer2' ;;
    flightsql) pat='icegres flight-serve' ;;
  esac
  for _ in $(seq 1 30); do
    [ -z "$(engine_procs "$pat")" ] && return 0
    sleep 1
  done
  echo "WARNING: $1 did not exit within 30s" >&2
  return 1
}

FAILED=()
for eng in "${ENGINES[@]}"; do
  echo
  echo "=================== $eng ==================="
  if ! quiesce; then FAILED+=("$eng(quiesce)"); continue; fi

  T0=$(now_ms)
  if ! start_engine "$eng"; then
    echo "ERROR: failed to start $eng" >&2
    FAILED+=("$eng(start)"); stop_engine "$eng" || true; continue
  fi
  if ! wait_first_query "$eng"; then
    echo "ERROR: $eng never answered a query" >&2
    FAILED+=("$eng(first-query)"); stop_engine "$eng" || true; continue
  fi
  STARTUP_MS=$(( $(now_ms) - T0 ))
  PID="$(engine_pid "$eng" || true)"
  RSS_IDLE="$( [ -n "${PID:-}" ] && rss_now_mb "$PID" )"
  FOOT="$(footprint "$eng")"
  echo "$eng up: startup_ms=$STARTUP_MS pid=${PID:-?} rss_idle_mb=${RSS_IDLE:-?} footprint=${FOOT:-?}"

  if ! python3 "$SCRIPT_DIR/compare.py" run --engine "$eng" \
        --out "$OUT_DIR" ${PID:+--pid "$PID"} --startup-ms "$STARTUP_MS" \
        ${RSS_IDLE:+--rss-idle-mb "$RSS_IDLE"} ${FOOT:+--footprint "$FOOT"}; then
    echo "ERROR: compare.py failed for $eng" >&2
    FAILED+=("$eng(bench)")
  fi

  stop_engine "$eng" || FAILED+=("$eng(stop)")
done

echo
python3 "$SCRIPT_DIR/compare.py" merge --out-dir "$OUT_DIR"

if [ ${#FAILED[@]} -gt 0 ]; then
  echo "FAILED: ${FAILED[*]}" >&2
  exit 1
fi
echo "all engines completed"
