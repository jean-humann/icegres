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
#   - Write metrics target demo.bench_scratch, a bench-owned table this
#     script creates fresh (REST catalog) before each run and drops after.
#     demo.trips is READ-ONLY for the benchmark: append-only Iceberg means
#     every insert adds a Parquet file + snapshot, and a growing demo.trips
#     made read metrics drift 40-80% between consecutive runs.
#   - Layout maintenance: e2e/parity DO append to demo.trips between bench
#     runs, so before measuring, demo.trips is rewritten to its canonical
#     single-file seed layout (drop + reseed) when it has >2 data files.
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

# The server under test is started by this script and must be permissive and
# plaintext — the benchmark measures the default wire path, and a stray
# ICEGRES_AUTH_FILE/ICEGRES_TLS_* in the caller's environment would silently
# change what is being measured. (Clients would still pass credentials when
# configured: psql/tokio-postgres read PGPASSWORD from the environment.)
unset ICEGRES_AUTH_FILE ICEGRES_TLS_CERT ICEGRES_TLS_KEY
# Same reasoning for buffered-write mode: the DEFAULT server under test must
# be fully synchronous; only the dedicated section 4b run turns the flag on.
unset ICEGRES_WRITE_BUFFER_MS ICEGRES_WRITE_BUFFER_MAX_ROWS

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

# --- bench-owned scratch table (write-metric target) -----------------------
SCRATCH_TABLE=bench_scratch
CATALOG_PREFIX=""

catalog_prefix() {
  if [[ -z "$CATALOG_PREFIX" ]]; then
    CATALOG_PREFIX=$(curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" \
      | jq -r '.defaults.prefix')
    [[ -n "$CATALOG_PREFIX" && "$CATALOG_PREFIX" != null ]] \
      || fatal "could not resolve catalog prefix for warehouse $WAREHOUSE"
  fi
  printf '%s' "$CATALOG_PREFIX"
}

drop_scratch() { # drop the bench-owned scratch table (created by this script)
  local prefix; prefix=$(catalog_prefix) || return 0
  curl -sf -X DELETE \
    "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/$SCRATCH_TABLE?purgeRequested=true" \
    >/dev/null 2>&1 || true
}

# --- demo.trips layout maintenance (drift control) --------------------------
# e2e/parity runs append rows to demo.trips; append-only Iceberg turns every
# such insert into an extra small Parquet file, and full-scan metrics degrade
# ~1 ms per extra file — baselines taken at different drift levels do not
# compare. Before measuring, demo.trips is rewritten to its canonical
# single-file seed layout whenever it has more than 2 data files.
#
# The live data-file count is the sum of `added-data-files` minus the sum
# of `deleted-data-files` over the table's snapshots: INSERTs fast_append,
# and UPDATE/DELETE (icegres DML) produce copy-on-write overwrite snapshots
# that record both fields exactly. (The per-snapshot `total-*` summary
# fields written by iceberg-rust 0.9.1 fast_append are per-commit, NOT
# cumulative — do not trust them.)
trips_data_files() {
  local prefix; prefix=$(catalog_prefix)
  curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/trips" \
    | jq -r '([.metadata.snapshots[]?.summary."added-data-files" // "0" | tonumber] | add // 0)
             - ([.metadata.snapshots[]?.summary."deleted-data-files" // "0" | tonumber] | add // 0)'
}

drop_trips() {
  local prefix; prefix=$(catalog_prefix)
  curl -sf -X DELETE \
    "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/trips?purgeRequested=true" \
    >/dev/null
}

# Rewrite demo.trips to the canonical single-file layout: drop (purged) and
# re-seed. iceberg-rust 0.9.1 has no replace-files/rewrite transaction action
# (only fast_append), so an in-place `icegres compact` cannot be implemented
# safely against the pinned matrix — drop + single-commit reseed is the
# documented canonicalization path (see icegres/src/seed.rs module docs).
# Rows appended by e2e/parity (trip_id >= 900000) are disposable test
# artifacts and are intentionally discarded.
canonicalize_trips() {
  local files; files=$(trips_data_files)
  [[ "$files" =~ ^[0-9]+$ ]] || fatal "could not determine demo.trips data-file count (got: $files)"
  if (( files > 2 )); then
    log "demo.trips has $files data files (layout drift from e2e/parity appends) — rewriting to canonical single-file layout"
    drop_trips || fatal "failed to drop demo.trips via REST catalog"
    "$BIN" seed >"$RUN_DIR/reseed.log" 2>&1 \
      || { tail -n 20 "$RUN_DIR/reseed.log" >&2; fatal "icegres seed (canonicalize) failed"; }
    files=$(trips_data_files)
    (( files <= 2 )) || fatal "demo.trips still has $files data files after canonicalization"
  fi
  TRIPS_DATA_FILES=$files
  log "demo.trips layout: $files data file(s) — canonical"
}

create_scratch() { # same schema as demo.trips (see icegres/src/seed.rs)
  local prefix; prefix=$(catalog_prefix)
  curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces/demo/tables" \
    -H 'Content-Type: application/json' -d @- <<'JSON' >/dev/null
{
  "name": "bench_scratch",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "trip_id",     "required": false, "type": "long"},
      {"id": 2, "name": "city",        "required": false, "type": "string"},
      {"id": 3, "name": "distance_km", "required": false, "type": "double"},
      {"id": 4, "name": "fare",        "required": false, "type": "double"},
      {"id": 5, "name": "ts",          "required": false, "type": "timestamp"}
    ]
  }
}
JSON
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
stop_icegresd_pidfile() { # identity-checked (comm=icegresd); SIGTERM
  # makes icegresd terminate its computes before exiting.
  local pidfile="$RUN_DIR/bench-icegresd.pid" pid
  if [[ -f "$pidfile" ]]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegresd ]]; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 40); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  fi
}

cleanup() {
  stop_pidfile
  stop_icegresd_pidfile
  drop_scratch
}
trap cleanup EXIT

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

log "layout maintenance: canonicalizing demo.trips (drift control)"
TRIPS_DATA_FILES=""
canonicalize_trips

log "resetting bench scratch table demo.$SCRATCH_TABLE (write-metric target)"
drop_scratch   # remove leftover from a previous (possibly aborted) run
create_scratch || fatal "could not create demo.$SCRATCH_TABLE via REST catalog"

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

# Record the measured table layout so runs are auditable for comparability.
jq --argjson f "${TRIPS_DATA_FILES:-null}" '. + {trips_data_files: $f}' \
  "$OUT_JSON" >"$OUT_JSON.tmp" && mv "$OUT_JSON.tmp" "$OUT_JSON"
log "wrote $OUT_JSON"

stop_pidfile

# ---------------------------------------------------------------------------
# 4b. Buffered-mode metrics (ADDITIONAL, ungated): a separate server run with
#     --write-buffer-ms measures insert_single_buffered_ms and
#     freshness_buffered_ms. The gated default-mode metrics above came from a
#     default-mode server and are unaffected. Writes go to the same
#     bench-owned scratch table (distinct id range >= 3,000,000).
# ---------------------------------------------------------------------------
WRITE_BUFFER_MS=100
BUF_LOG="$RUN_DIR/bench-serve-buffered.log"
BUF_JSON="$RUN_DIR/bench-buffered.json"
log "starting buffered-mode server on :$PG_PORT (--write-buffer-ms $WRITE_BUFFER_MS)"
: >"$BUF_LOG"
"$BIN" serve --host "$PG_HOST" --port "$PG_PORT" --write-buffer-ms "$WRITE_BUFFER_MS" \
  >>"$BUF_LOG" 2>&1 &
BUF_PID=$!
echo "$BUF_PID" >"$PID_FILE"
for _ in $(seq 1 60); do
  port_answers "$PG_PORT" && break
  kill -0 "$BUF_PID" 2>/dev/null || { tail -n 20 "$BUF_LOG" >&2; fatal "buffered server exited during startup"; }
  sleep 0.5
done
port_answers "$PG_PORT" || fatal "buffered server not ready on :$PG_PORT within 30s"

log "running buffered-mode harness subset (insert_single_buffered_ms, freshness_buffered_ms)"
"$HARNESS_BIN" --host "$PG_HOST" --port "$PG_PORT" --buffered >"$BUF_JSON" \
  || { tail -n 5 "$BUF_LOG" >&2; fatal "buffered bench harness failed"; }

# Let the last acked rows flush (cadence + margin), then stop the server and
# merge the buffered metrics into the run document (annotated with the mode).
sleep 1
stop_pidfile
jq -s --argjson ms "$WRITE_BUFFER_MS" \
  '.[0] * {metrics: (.[0].metrics + .[1].metrics), write_buffer_ms_buffered_run: $ms}' \
  "$OUT_JSON" "$BUF_JSON" >"$OUT_JSON.tmp" && mv "$OUT_JSON.tmp" "$OUT_JSON"
log "merged buffered-mode metrics into $OUT_JSON"

# ---------------------------------------------------------------------------
# 4c. Wake-after-idle latency through icegresd (ADDITIONAL, ungated):
#     cold_start_via_proxy_ms = first-connection-after-idle latency through
#     the control plane (compute cold start + proxy wake + splice setup,
#     measured with a timed psql so client overhead ~a few ms is included).
#     One icegresd on :$PROXY_PORT supervises a compute on :$PROXY_COMPUTE
#     with --idle-shutdown-secs 1; each iteration waits for the idle exit,
#     then times the auto-wake query. The gated direct-serve metrics above
#     are unaffected (their server never runs behind the proxy).
# ---------------------------------------------------------------------------
PROXY_PORT=5447
PROXY_COMPUTE=5446
DBIN="$ICEGRES_DIR/target/release/icegresd"
PROXY_STATUS="$RUN_DIR/bench-icegresd-status.json"
PROXY_RUNS=5
if [[ -x "$DBIN" ]]; then
  log "measuring cold_start_via_proxy_ms ($PROXY_RUNS wake-after-idle cycles via icegresd on :$PROXY_PORT)"
  if port_answers "$PROXY_PORT" || port_answers "$PROXY_COMPUTE"; then
    fatal "port :$PROXY_PORT or :$PROXY_COMPUTE is occupied — free it first"
  fi
  stop_icegresd_pidfile
  rm -f "$PROXY_STATUS"
  : >"$RUN_DIR/bench-icegresd.log"
  # --pool-size 0: this section measures the BARE wake-after-idle path; a
  # warm pool would hold sessions on the compute and block the idle exit
  # each iteration waits for (the pooled path is section 4d).
  "$DBIN" serve --host "$PG_HOST" --port "$PROXY_PORT" --main-port "$PROXY_COMPUTE" \
    --icegres-bin "$BIN" --idle-shutdown-secs 1 --pool-size 0 --status-file "$PROXY_STATUS" \
    >>"$RUN_DIR/bench-icegresd.log" 2>&1 &
  echo $! >"$RUN_DIR/bench-icegresd.pid"
  proxy_up=0
  for _ in $(seq 1 40); do
    if (exec 3<>"/dev/tcp/$PG_HOST/$PROXY_PORT") 2>/dev/null; then exec 3>&- 3<&-; proxy_up=1; break; fi
    sleep 0.25
  done
  [[ "$proxy_up" == 1 ]] || fatal "icegresd not listening on :$PROXY_PORT"
  wake_runs=()
  for i in $(seq 1 "$PROXY_RUNS"); do
    # Ensure the compute is idle-exited (state 'stopped' or never started).
    settled=0
    for _ in $(seq 1 80); do
      st=$(jq -r '.computes[] | select(.key=="main") | .state' "$PROXY_STATUS" 2>/dev/null)
      if [[ -z "$st" || "$st" == "stopped" ]]; then settled=1; break; fi
      sleep 0.25
    done
    [[ "$settled" == 1 ]] || fatal "compute did not idle-exit before wake run $i (state=$st)"
    t0=$(($(date +%s%N) / 1000000))
    r=$(psql -h "$PG_HOST" -p "$PROXY_PORT" -U postgres -d icegres -tA -c 'select 1' 2>&1)
    t1=$(($(date +%s%N) / 1000000))
    [[ "$r" == 1 ]] || fatal "wake-after-idle query $i failed: $r"
    wake_runs+=($((t1 - t0)))
  done
  stop_icegresd_pidfile
  proxy_json=$(printf '%s\n' "${wake_runs[@]}" | jq -s \
    '{p50: (sort | .[(length*0.5|floor)]), p95: (sort | .[-1]), n: length, runs: .}')
  jq --argjson m "$proxy_json" '.metrics.cold_start_via_proxy_ms = $m' \
    "$OUT_JSON" >"$OUT_JSON.tmp" && mv "$OUT_JSON.tmp" "$OUT_JSON"
  log "cold_start_via_proxy_ms: $(printf '%s ' "${wake_runs[@]}")(ms; ungated extra metric)"
else
  log "icegresd release binary not found at $DBIN — skipping cold_start_via_proxy_ms"
fi

# ---------------------------------------------------------------------------
# 4d. Session-pooled proxy metrics (ADDITIONAL, ungated): a fresh icegresd
#     with the DEFAULT warm session pool (--pool-size 8). One priming
#     connection wakes the compute and triggers pool warming, then the
#     harness --proxy subset measures:
#       connect_via_proxy_ms  — client connect -> ReadyForQuery via a WARM
#                               pooled handout (the API-workload connect;
#                               compare cold_start_via_proxy_ms above)
#       qps_via_proxy_8conn   — the qps_8conn workload through the proxy
#                               (splice overhead evidence vs direct qps_8conn)
#     The gated direct-serve metrics are unaffected.
# ---------------------------------------------------------------------------
if [[ -x "$DBIN" ]]; then
  log "measuring pooled-proxy metrics (icegresd on :$PROXY_PORT, warm pool, compute :$PROXY_COMPUTE)"
  if port_answers "$PROXY_PORT" || port_answers "$PROXY_COMPUTE"; then
    fatal "port :$PROXY_PORT or :$PROXY_COMPUTE is occupied — free it first"
  fi
  stop_icegresd_pidfile
  rm -f "$PROXY_STATUS"
  "$DBIN" serve --host "$PG_HOST" --port "$PROXY_PORT" --main-port "$PROXY_COMPUTE" \
    --icegres-bin "$BIN" --idle-shutdown-secs 300 --pool-size 8 --pool-idle-secs 300 \
    --status-file "$PROXY_STATUS" \
    >>"$RUN_DIR/bench-icegresd.log" 2>&1 &
  echo $! >"$RUN_DIR/bench-icegresd.pid"
  proxy_up=0
  for _ in $(seq 1 40); do
    if (exec 3<>"/dev/tcp/$PG_HOST/$PROXY_PORT") 2>/dev/null; then exec 3>&- 3<&-; proxy_up=1; break; fi
    sleep 0.25
  done
  [[ "$proxy_up" == 1 ]] || fatal "icegresd (pooled) not listening on :$PROXY_PORT"
  # Prime: first connection wakes the compute and kicks off pool warming.
  port_answers "$PROXY_PORT" || fatal "priming connection through the pooled proxy failed"
  pool_warm=0
  for _ in $(seq 1 40); do
    w=$(jq -r '.computes[] | select(.key=="main") | .pool.warm' "$PROXY_STATUS" 2>/dev/null)
    if [[ "$w" == 8 ]]; then pool_warm=1; break; fi
    sleep 0.25
  done
  [[ "$pool_warm" == 1 ]] || fatal "pool did not warm to 8 conns (warm=$w)"

  PROXY_JSON="$RUN_DIR/bench-proxy.json"
  "$HARNESS_BIN" --host "$PG_HOST" --port "$PROXY_PORT" --proxy >"$PROXY_JSON" \
    || { tail -n 20 "$RUN_DIR/bench-icegresd.log" >&2; fatal "proxy bench harness failed"; }
  pooled_sessions=$(jq -r '.computes[] | select(.key=="main") | .pool.pooled_sessions' "$PROXY_STATUS")
  stop_icegresd_pidfile
  jq -s --argjson ps "${pooled_sessions:-null}" \
    '.[0] * {metrics: (.[0].metrics + .[1].metrics), proxy_pooled_sessions: $ps}' \
    "$OUT_JSON" "$PROXY_JSON" >"$OUT_JSON.tmp" && mv "$OUT_JSON.tmp" "$OUT_JSON"
  log "pooled-proxy metrics merged (pooled sessions served: ${pooled_sessions:-?})"
else
  log "icegresd release binary not found at $DBIN — skipping pooled-proxy metrics"
fi

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
  jq -r '"warmups discarded: \(.warmup_discarded), iterations: \(.iterations), cold-start runs: \(.cold_start_runs), demo.trips data files: \(.trips_data_files // "?")"' "$OUT_JSON"
  echo
  echo "| metric | p50 | p95 | n / detail |"
  echo "|--------|-----|-----|------------|"
  jq -r '
    def row(name; m):
      if m == null then empty
      elif (m | has("p50")) then "| \(name) | \(m.p50) | \(m.p95) | n=\(m.n) |"
      elif (name == "qps_8conn" or name == "qps_via_proxy_8conn") then "| \(name) | \(m.value) | — | median of \(m.windows // [] | map(tostring) | join(", ")) (\(m.connections) conns, \(m.window_s)s windows) |"
      elif name == "rss_peak_mb" then "| \(name) | \(m.value) | — | qps-window peak \(m.qps_window_peak_mb // "—") MB, \(m.samples // "?") samples @ \(m.interval_ms // "?")ms |"
      else "| \(name) | \(m.value) | — | |"
      end;
    .metrics as $m |
    ( ["connect_ms","point_lookup_ms","filtered_scan_ms","aggregate_ms","join_ms",
       "insert_single_ms","insert_batch100_ms","freshness_ms","qps_8conn",
       "cold_start_ms","binary_size_mb","rss_idle_mb","rss_peak_mb","rss_after_load_mb",
       "insert_single_buffered_ms","freshness_buffered_ms","cold_start_via_proxy_ms",
       "connect_via_proxy_ms","qps_via_proxy_8conn"][] ) as $k |
    row($k; $m[$k])
  ' "$OUT_JSON"
} >>"$SCORECARD"

log "appended benchmark table to $SCORECARD"
log "done"
