#!/usr/bin/env bash
# Memory-bound micro-check for the pinned time-travel provider cache
# (icegres/src/cache.rs, MAX_PINNED_PER_TABLE): proves that querying many
# DISTINCT historical snapshots does not grow server memory without bound.
#
#   bash icegres/tests/pinned_bound.sh
#
# What it does:
#   1. Creates its own scratch table demo.cache_bound_scratch via the REST
#      catalog (NEVER touches demo.trips — see the layout-drift note in
#      bench/bench.sh) and appends NUM_SNAPSHOTS rows one at a time, giving
#      NUM_SNAPSHOTS distinct Iceberg snapshots. Dropped (purged) on exit.
#   2. Starts a fresh icegres server on port 5444 with icegres=debug logging
#      and queries every one of those snapshots via time travel
#      (select ... from demo."cache_bound_scratch@<id>").
#   3. Asserts the bound three ways:
#        a. the server's own cache-size instrumentation never reports more
#           than MAX_PINNED (16) pinned providers for the table;
#        b. exactly NUM_SNAPSHOTS - 16 LRU evictions were logged (every
#           insert past the cap evicts one entry);
#        c. process RSS grows by less than RSS_LIMIT_MB between "cache full
#           at cap" (after the first 16 snapshot queries) and end of sweep.
#      (a)+(b) prove the map itself is bounded; (c) guards against any other
#      per-snapshot accumulation. Note the per-provider footprint on this
#      tiny table is small, so (c) alone would not catch a map leak at this
#      scale — that is what (a)/(b) and the cache.rs unit tests are for.
#
# Non-destructive, self-contained, idempotent. Uses port 5444 (left free).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ICEGRES_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$ICEGRES_DIR")"
RUN_DIR="$ICEGRES_DIR/.e2e"
BIN="${BIN:-$ICEGRES_DIR/target/release/icegres}"

PG_HOST=127.0.0.1
PG_PORT=5444
PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d icegres -v ON_ERROR_STOP=1)
export PGCONNECT_TIMEOUT=5

CATALOG_URI="http://127.0.0.1:8181/catalog"
WAREHOUSE=lakehouse
TABLE=cache_bound_scratch

NUM_SNAPSHOTS=${NUM_SNAPSHOTS:-56} # > 50 distinct historical snapshots
MAX_PINNED=16                      # must match cache.rs MAX_PINNED_PER_TABLE
RSS_LIMIT_MB=${RSS_LIMIT_MB:-50}   # max RSS growth cap-full -> end of sweep

SERVE_LOG="$RUN_DIR/pinned-bound-serve.log"
PID_FILE="$RUN_DIR/pinned-bound.pid"
mkdir -p "$RUN_DIR"

PASS_COUNT=0
log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
pass() { PASS_COUNT=$((PASS_COUNT + 1)); printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

q() { "${PSQL[@]}" -tA -c "$1"; }

catalog_prefix() {
  curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" | jq -r '.defaults.prefix'
}

drop_scratch() {
  local prefix="$1"
  curl -sf -X DELETE \
    "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/$TABLE?purgeRequested=true" \
    >/dev/null 2>&1 || true
}

create_scratch() { # minimal schema; every insert = one Iceberg snapshot
  local prefix="$1"
  curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces/demo/tables" \
    -H 'Content-Type: application/json' -d @- <<'JSON' >/dev/null
{
  "name": "cache_bound_scratch",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "id", "required": false, "type": "long"},
      {"id": 2, "name": "v",  "required": false, "type": "double"}
    ]
  }
}
JSON
}

stop_server() { # identity-checked, like e2e.sh
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

PREFIX=""
cleanup() {
  stop_server
  [[ -n "$PREFIX" ]] && drop_scratch "$PREFIX"
}
trap cleanup EXIT

rss_mb() { # of the server process
  local pid; pid=$(cat "$PID_FILE")
  awk '/^VmRSS:/ {printf "%.1f", $2 / 1024}' "/proc/$pid/status"
}

strip_ansi() { sed 's/\x1b\[[0-9;]*m//g'; }

# ---------------------------------------------------------------------------
# 0. Stack + binary
# ---------------------------------------------------------------------------
log "checking lakehouse stack"
if ! curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" >/dev/null; then
  bash "$REPO_DIR/infra/scripts/up.sh" >"$RUN_DIR/pinned-bound-up.log" 2>&1 \
    || fail "infra/scripts/up.sh failed (log: $RUN_DIR/pinned-bound-up.log)"
fi
[[ -x "$BIN" ]] || fail "binary not found at $BIN (build with: cargo build --release)"
PREFIX=$(catalog_prefix)
[[ -n "$PREFIX" && "$PREFIX" != null ]] || fail "could not resolve catalog prefix"
pass "stack healthy (prefix $PREFIX), binary $BIN"

# ---------------------------------------------------------------------------
# 1. Fresh scratch table with NUM_SNAPSHOTS distinct snapshots
# ---------------------------------------------------------------------------
log "creating demo.$TABLE and committing $NUM_SNAPSHOTS snapshots"
drop_scratch "$PREFIX"
create_scratch "$PREFIX" || fail "could not create demo.$TABLE via REST catalog"

stop_server
if q "select 1" >/dev/null 2>&1; then
  fail "something else is listening on $PG_HOST:$PG_PORT — stop it first"
fi
: >"$SERVE_LOG"
RUST_LOG="info,icegres=debug" "$BIN" serve --host "$PG_HOST" --port "$PG_PORT" \
  >>"$SERVE_LOG" 2>&1 &
echo $! >"$PID_FILE"
for _ in $(seq 1 60); do
  q "select 1" >/dev/null 2>&1 && break
  kill -0 "$(cat "$PID_FILE")" 2>/dev/null \
    || { tail -n 20 "$SERVE_LOG" >&2; fail "server exited during startup"; }
  sleep 0.5
done
q "select 1" >/dev/null 2>&1 || fail "server not ready on :$PG_PORT within 30s"

for i in $(seq 1 "$NUM_SNAPSHOTS"); do
  q "insert into demo.$TABLE (id, v) values ($i, $i.5)" >/dev/null \
    || fail "insert $i failed"
done
mapfile -t SNAPSHOTS < <(q "select snapshot_id from demo.\"$TABLE\$snapshots\" order by committed_at")
[[ ${#SNAPSHOTS[@]} -ge $NUM_SNAPSHOTS ]] \
  || fail "expected >= $NUM_SNAPSHOTS snapshots, got ${#SNAPSHOTS[@]}"
pass "demo.$TABLE has ${#SNAPSHOTS[@]} distinct snapshots"

# Restart so the sweep starts from a cold, empty pinned cache and RSS is not
# skewed by the insert workload above.
stop_server
: >"$SERVE_LOG"
RUST_LOG="info,icegres=debug" "$BIN" serve --host "$PG_HOST" --port "$PG_PORT" \
  >>"$SERVE_LOG" 2>&1 &
echo $! >"$PID_FILE"
for _ in $(seq 1 60); do
  q "select 1" >/dev/null 2>&1 && break
  sleep 0.5
done
q "select 1" >/dev/null 2>&1 || fail "server did not restart on :$PG_PORT"

# ---------------------------------------------------------------------------
# 2. Sweep every distinct snapshot via time travel
# ---------------------------------------------------------------------------
log "sweeping ${#SNAPSHOTS[@]} distinct historical snapshots (time travel)"
n=0
RSS_AT_CAP=""
for sid in "${SNAPSHOTS[@]}"; do
  n=$((n + 1))
  rows=$(q "select count(*) from demo.\"$TABLE@$sid\"") \
    || fail "time-travel query failed for snapshot $sid"
  [[ "$rows" == "$n" ]] || fail "snapshot #$n ($sid): expected $n rows, got $rows"
  if [[ $n -eq $MAX_PINNED ]]; then
    RSS_AT_CAP=$(rss_mb)
  fi
done
RSS_END=$(rss_mb)
pass "all ${#SNAPSHOTS[@]} snapshot queries returned correct historical row counts"

# ---------------------------------------------------------------------------
# 3. Assertions: bounded cache + bounded RSS
# ---------------------------------------------------------------------------
log "asserting bounds (cap $MAX_PINNED, rss growth < ${RSS_LIMIT_MB} MB)"

max_pinned=$(strip_ansi <"$SERVE_LOG" \
  | grep 'pinned snapshot cache size' \
  | grep -o 'pinned=[0-9]*' | cut -d= -f2 | sort -n | tail -1)
[[ -n "$max_pinned" ]] || fail "no cache-size instrumentation in server log ($SERVE_LOG)"
[[ "$max_pinned" -le "$MAX_PINNED" ]] \
  || fail "pinned cache exceeded cap: max observed $max_pinned > $MAX_PINNED"
pass "pinned cache never exceeded cap (max observed: $max_pinned <= $MAX_PINNED)"

evictions=$(strip_ansi <"$SERVE_LOG" | grep -c 'evicted LRU pinned snapshot provider' || true)
expected_evictions=$(( ${#SNAPSHOTS[@]} - MAX_PINNED ))
[[ "$evictions" -eq "$expected_evictions" ]] \
  || fail "expected $expected_evictions LRU evictions, saw $evictions"
pass "LRU evictions: $evictions (= ${#SNAPSHOTS[@]} distinct snapshots - cap $MAX_PINNED)"

growth=$(awk -v a="$RSS_AT_CAP" -v b="$RSS_END" 'BEGIN {printf "%.1f", b - a}')
ok=$(awk -v g="$growth" -v lim="$RSS_LIMIT_MB" 'BEGIN {print (g < lim) ? 1 : 0}')
echo "     rss at cap (after $MAX_PINNED snapshots): ${RSS_AT_CAP} MB"
echo "     rss at end (after ${#SNAPSHOTS[@]} snapshots): ${RSS_END} MB (growth: ${growth} MB)"
[[ "$ok" == 1 ]] \
  || fail "RSS grew ${growth} MB across the sweep (limit ${RSS_LIMIT_MB} MB)"
pass "RSS bounded: +${growth} MB from cache-full to end of sweep (< ${RSS_LIMIT_MB} MB)"

log "all assertions passed ($PASS_COUNT)"
