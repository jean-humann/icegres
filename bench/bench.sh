#!/usr/bin/env bash
# Benchmark runner for icegres (bench/SPEC.md §2).
#
#   bash bench/bench.sh
#
# Ensures the lakehouse stack is healthy, seeds the demo data, starts a FRESH
# release-build icegres server on :5439, runs the Rust bench harness
# (bench/harness — tokio-postgres only, builds in seconds), writes
# bench/results/bench-<ts>.json and appends a human-readable table to
# bench/SCORECARD.md.
#
# Notes:
#   - Only one benchmark should run at a time on this box (CPU-noise).
#   - The harness appends rows to demo.trips with trip_id >= 2_000_000
#     (append-only Iceberg; e2e's exact-value assertions filter trip_id
#     1..280, so this is safe by design).
#   - Ports used: 5439 (server under test), 5442 (cold-start runs).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
ICEGRES_DIR="$REPO_DIR/icegres"
RESULTS_DIR="$SCRIPT_DIR/results"
RUN_DIR="$SCRIPT_DIR/.run"
SCORECARD="$SCRIPT_DIR/SCORECARD.md"

PG_HOST=127.0.0.1
PG_PORT=5439
COLD_PORT=5442
CATALOG_URI="http://127.0.0.1:8181/catalog"
WAREHOUSE=lakehouse
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfssecret
export AWS_DEFAULT_REGION=us-east-1
export PGCONNECT_TIMEOUT=5

BIN="$ICEGRES_DIR/target/release/icegres"
HARNESS_BIN="$SCRIPT_DIR/harness/target/release/icegres-bench"
PID_FILE="$RUN_DIR/bench-serve.pid"
SERVE_LOG="$RUN_DIR/bench-serve.log"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_JSON="$RESULTS_DIR/bench-$TS.json"
mkdir -p "$RESULTS_DIR" "$RUN_DIR"

log()   { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fatal() { printf '\033[1;31mFATAL\033[0m %s\n' "$*" >&2; exit 1; }

port_answers() { # port
  psql -h "$PG_HOST" -p "$1" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1
}

stop_pidfile() { # identity-checked, like icegres/tests/e2e.sh
  if [[ -f "$PID_FILE" ]]; then
    local pid; pid=$(cat "$PID_FILE")
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegres ]]; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
}
trap stop_pidfile EXIT

# ---------------------------------------------------------------------------
# 1. Stack healthy
# ---------------------------------------------------------------------------
log "checking lakehouse stack"
if ! { pg_isready -h 127.0.0.1 -p 5433 -q \
       && curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" >/dev/null; }; then
  log "stack unhealthy — running infra/scripts/up.sh"
  bash "$REPO_DIR/infra/scripts/up.sh" >"$RUN_DIR/up.log" 2>&1 \
    || { tail -n 20 "$RUN_DIR/up.log" >&2; fatal "infra/scripts/up.sh failed"; }
fi

# ---------------------------------------------------------------------------
# 2. Release binaries (server under test + harness)
# ---------------------------------------------------------------------------
log "building icegres (release) — no-op when fresh"
(cd "$ICEGRES_DIR" && cargo build --release --quiet) || fatal "cargo build --release failed"
[[ -x "$BIN" ]] || fatal "release binary not found at $BIN"

log "building bench harness (release)"
(cd "$SCRIPT_DIR/harness" && cargo build --release --quiet) || fatal "harness build failed"
[[ -x "$HARNESS_BIN" ]] || fatal "harness binary not found at $HARNESS_BIN"

# ---------------------------------------------------------------------------
# 3. Seed (idempotent) + fresh release server on :5439
# ---------------------------------------------------------------------------
log "seeding demo data"
"$BIN" seed >"$RUN_DIR/seed.log" 2>&1 \
  || { tail -n 20 "$RUN_DIR/seed.log" >&2; fatal "icegres seed failed"; }

stop_pidfile
if port_answers "$PG_PORT"; then
  fatal "something not started by this script is listening on :$PG_PORT — stop it first (benchmark requires a fresh release server)"
fi
if port_answers "$COLD_PORT"; then
  fatal "cold-start port :$COLD_PORT is occupied — free it first"
fi

log "starting fresh release server on :$PG_PORT"
: >"$SERVE_LOG"
"$BIN" serve --host "$PG_HOST" --port "$PG_PORT" >>"$SERVE_LOG" 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" >"$PID_FILE"
for _ in $(seq 1 60); do
  port_answers "$PG_PORT" && break
  kill -0 "$SERVER_PID" 2>/dev/null || { tail -n 20 "$SERVE_LOG" >&2; fatal "server exited during startup"; }
  sleep 0.5
done
port_answers "$PG_PORT" || fatal "server not ready on :$PG_PORT within 30s"

# ---------------------------------------------------------------------------
# 4. Run the harness
# ---------------------------------------------------------------------------
log "running bench harness (this takes a few minutes: 11 metrics, warmups, 10s qps window, 5 cold starts)"
"$HARNESS_BIN" --host "$PG_HOST" --port "$PG_PORT" \
  --server-bin "$BIN" --server-pid "$SERVER_PID" --cold-port "$COLD_PORT" \
  >"$OUT_JSON" || { rm -f "$OUT_JSON"; fatal "bench harness failed"; }
log "wrote $OUT_JSON"

stop_pidfile

# ---------------------------------------------------------------------------
# 5. Append human table to SCORECARD.md
# ---------------------------------------------------------------------------
if [[ ! -f "$SCORECARD" ]]; then
  {
    echo "# icegres Scorecard"
    echo
    echo "Generated by \`bench/parity.sh\` (parity matrix) and \`bench/bench.sh\`"
    echo "(benchmark runs). See \`bench/SPEC.md\` for the contract."
    echo
    echo "## Benchmark runs"
    echo
    echo "<!-- bench:runs — appended by bench/bench.sh -->"
  } >"$SCORECARD"
elif ! grep -q '^## Benchmark runs' "$SCORECARD"; then
  { echo; echo "## Benchmark runs"; echo; echo "<!-- bench:runs — appended by bench/bench.sh -->"; } >>"$SCORECARD"
fi

{
  echo
  echo "### Bench $TS"
  echo
  echo "Release binary \`${BIN#"$REPO_DIR"/}\` · raw: \`bench/results/bench-$TS.json\` ·"
  jq -r '"warmups discarded: \(.warmup_discarded), iterations: \(.iterations), cold-start runs: \(.cold_start_runs)"' "$OUT_JSON"
  echo
  echo "| metric | p50 | p95 | n / detail |"
  echo "|--------|-----|-----|------------|"
  jq -r '
    def row(name; m):
      if m == null then empty
      elif (m | has("p50")) then "| \(name) | \(m.p50) | \(m.p95) | n=\(m.n) |"
      elif name == "qps_8conn" then "| \(name) | \(m.value) | — | \(m.connections) conns, \(m.window_s)s window |"
      else "| \(name) | \(m.value) | — | |"
      end;
    .metrics as $m |
    ( ["connect_ms","point_lookup_ms","filtered_scan_ms","aggregate_ms","join_ms",
       "insert_single_ms","insert_batch100_ms","freshness_ms","qps_8conn",
       "cold_start_ms","binary_size_mb","rss_idle_mb"][] ) as $k |
    row($k; $m[$k])
  ' "$OUT_JSON"
} >>"$SCORECARD"

log "appended benchmark table to $SCORECARD"
log "done"
