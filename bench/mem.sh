#!/usr/bin/env bash
# icegres MEMORY-UNDER-LOAD bench.
#
# Answers one question empirically: for each operation, does the SERVER's peak
# resident memory grow with the data volume (unbounded / materialized) or stay
# flat (bounded / streamed)? It runs every operation under a FRESH server so the
# process's VmHWM (peak RSS since start) is that operation's peak alone, then
# reads VmHWM from /proc/<pid>/status.
#
# Per volume N it uses ONE scratch table: ingest N rows (the write path under
# test AND the read fixture), then scan it back over Flight and pgwire. So the
# read lanes are measured at exactly the volume that was written.
#
# Operations x volumes measured:
#   ingest       (Flight ADBC bulk / CommandStatementIngest)   -> write path
#   read-flight  (Flight DoGet full scan)                      -> Flight read
#   read-pg      (pgwire COPY (SELECT *) TO STDOUT)            -> pgwire read
#
# Usage:
#   bench/mem.sh                       # default curve: 100000 500000 2000000
#   bench/mem.sh 100000 1000000 4000000
#   bench/mem.sh smoke                 # quick 50k validation
#
# FOREGROUND / long-running. Disk-frugal: one scratch table at a time, dropped
# before the next volume.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
BIN="$REPO_DIR/icegres/target/release/icegres"
PROBE="$SCRIPT_DIR/clients/mem_probe.py"
RESULTS_DIR="$SCRIPT_DIR/results"
PG_PORT=5459
FLIGHT_PORT=50051
TABLE="mem_scratch"
LOGDIR="$REPO_DIR/infra/.data"
SERVELOG="$LOGDIR/mem-serve.log"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_JSON="$RESULTS_DIR/mem-$TS.json"

export MEM_PG_PORT="$PG_PORT"
export MEM_FLIGHT_URI="grpc://127.0.0.1:$FLIGHT_PORT"

mkdir -p "$RESULTS_DIR" "$LOGDIR"
[[ -x "$BIN" ]] || { echo "FATAL: release binary missing: $BIN (run: cargo build --release --bins)"; exit 1; }

case "${1:-}" in
  smoke) VOLUMES=(50000); shift || true ;;
  "")    VOLUMES=(100000 500000 2000000) ;;
  *)     VOLUMES=("$@") ;;
esac

log() { echo "[mem $(date -u +%H:%M:%S)] $*"; }

# ---------------------------------------------------------------- stack
ensure_stack() {
  if curl -sf -m 3 "http://127.0.0.1:8181/catalog/v1/config?warehouse=lakehouse" >/dev/null 2>&1; then
    log "lakehouse stack already up"; return 0
  fi
  log "starting lakehouse stack (infra/scripts/up.sh)"
  bash "$REPO_DIR/infra/scripts/up.sh" >"$LOGDIR/mem-up.log" 2>&1 \
    || { tail -n 25 "$LOGDIR/mem-up.log" >&2; echo "FATAL: up.sh failed"; exit 1; }
}

# ---------------------------------------------------------------- server
SERVER_PID=""
port_open() { python3 - "$1" <<'PY'
import socket,sys
s=socket.socket(); s.settimeout(0.3)
sys.exit(0 if s.connect_ex(("127.0.0.1",int(sys.argv[1])))==0 else 1)
PY
}
stop_server() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null
  for _ in $(seq 1 50); do kill -0 "$SERVER_PID" 2>/dev/null || break; sleep 0.1; done
  SERVER_PID=""
}
sweep_port() { for p in $(pgrep -f "icegres (serve|flight-serve) --host 127.0.0.1 --port $1" 2>/dev/null); do kill "$p" 2>/dev/null; done; sleep 0.3; }
start_server() {  # $1 = pg|flight
  local kind=$1 sub port
  if [[ "$kind" == flight ]]; then sub="flight-serve"; port=$FLIGHT_PORT; else sub="serve"; port=$PG_PORT; fi
  sweep_port "$port"
  nohup "$BIN" "$sub" --host 127.0.0.1 --port "$port" >>"$SERVELOG" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 150); do
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "FATAL: $sub exited on startup (see $SERVELOG)"; tail -n 15 "$SERVELOG" >&2; exit 1; }
    port_open "$port" && return 0
    sleep 0.2
  done
  echo "FATAL: $sub did not open :$port"; exit 1
}
status_kb() { awk -v f="$1:" '$1==f{print $2}' "/proc/$SERVER_PID/status" 2>/dev/null; }

# Run one operation under a fresh server; echo "peak_mb base_mb" for that op.
# $1 = pg|flight ; $2.. = probe argv
measure() {
  local kind=$1; shift
  start_server "$kind"
  local base_kb; base_kb=$(status_kb VmRSS)
  python3 "$PROBE" "$@" >>"$OUT_JSON.raw" 2>>"$LOGDIR/mem-probe.log" \
    || { echo "FATAL: probe failed: $* (see $LOGDIR/mem-probe.log)"; tail -n 8 "$LOGDIR/mem-probe.log" >&2; stop_server; exit 1; }
  local hwm_kb; hwm_kb=$(status_kb VmHWM)
  stop_server
  awk -v h="$hwm_kb" -v b="$base_kb" 'BEGIN{printf "%.1f %.1f", h/1024.0, b/1024.0}'
}

# ---------------------------------------------------------------- run
ensure_stack
: >"$OUT_JSON.raw"
declare -a ROWS
log "volumes: ${VOLUMES[*]}   (fresh server per op; VmHWM = per-op peak)"
printf '%s\n' "# icegres memory-under-load — $TS" > "$RESULTS_DIR/mem-$TS.md"
printf '%s\n' "" >> "$RESULTS_DIR/mem-$TS.md"
printf '%s\n' "| rows | ingest peak MB | ingest Δ | read-flight peak MB | rf Δ | read-pg peak MB | rpg Δ |" >> "$RESULTS_DIR/mem-$TS.md"
printf '%s\n' "|---:|---:|---:|---:|---:|---:|---:|" >> "$RESULTS_DIR/mem-$TS.md"

for N in "${VOLUMES[@]}"; do
  log "=== volume $N rows ==="
  # fresh scratch table
  measure pg drop-table --table "$TABLE" >/dev/null 2>&1 || true
  measure pg create-table --table "$TABLE" >/dev/null

  log "  ingest $N (Flight)…"
  read -r ing_peak ing_base <<<"$(measure flight ingest --rows "$N" --table "$TABLE")"
  log "  read-flight (scan $N)…"
  read -r rf_peak rf_base <<<"$(measure flight read-flight --table "$TABLE")"
  log "  read-pg (COPY $N)…"
  read -r rpg_peak rpg_base <<<"$(measure pg read-pg --table "$TABLE")"

  measure pg drop-table --table "$TABLE" >/dev/null 2>&1 || true

  ing_d=$(awk -v p="$ing_peak" -v b="$ing_base" 'BEGIN{printf "%.1f", p-b}')
  rf_d=$(awk -v p="$rf_peak" -v b="$rf_base" 'BEGIN{printf "%.1f", p-b}')
  rpg_d=$(awk -v p="$rpg_peak" -v b="$rpg_base" 'BEGIN{printf "%.1f", p-b}')
  printf '| %s | %s | +%s | %s | +%s | %s | +%s |\n' \
    "$N" "$ing_peak" "$ing_d" "$rf_peak" "$rf_d" "$rpg_peak" "$rpg_d" >> "$RESULTS_DIR/mem-$TS.md"
  log "  -> ingest ${ing_peak}MB (Δ+${ing_d})  read-flight ${rf_peak}MB (Δ+${rf_d})  read-pg ${rpg_peak}MB (Δ+${rpg_d})"
  ROWS+=("$N ing=$ing_peak/+$ing_d rf=$rf_peak/+$rf_d rpg=$rpg_peak/+$rpg_d")
done

echo
log "RESULTS  (peak = VmHWM of the server process; Δ = peak − post-startup baseline)"
cat "$RESULTS_DIR/mem-$TS.md"
echo
log "markdown: $RESULTS_DIR/mem-$TS.md   raw json: $OUT_JSON.raw"
