#!/usr/bin/env bash
# End-to-end test for icegres against the local lakehouse stack
# (Postgres + RustFS + Lakekeeper). Self-contained and idempotent:
#
#   bash icegres/tests/e2e.sh
#
# Design notes:
#   - The harness is NON-DESTRUCTIVE: it never drops catalog tables. The
#     seeded dataset is deterministic (LCG seed 42) and occupies
#     trip_id 1..280 in demo.trips, so all "exact value" assertions filter
#     on that id range. The write-path test appends one row per run with a
#     fresh unique trip_id >= 900000 (append-only Iceberg; DELETE is not
#     supported by iceberg-datafusion 0.9.1), which the range filter keeps
#     out of the deterministic assertions.
#   - Every psql invocation is a NEW connection (psql -c opens/closes one),
#     so read-your-writes checks always cross connection boundaries.

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths / config
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ICEGRES_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$ICEGRES_DIR")"
E2E_DIR="$ICEGRES_DIR/.e2e"
BIN="$ICEGRES_DIR/target/debug/icegres"

PG_HOST=127.0.0.1
PG_PORT=5439
PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d icegres -v ON_ERROR_STOP=1)
export PGCONNECT_TIMEOUT=5

CATALOG_URI="http://127.0.0.1:8181/catalog"
WAREHOUSE=lakehouse
S3_ENDPOINT="http://127.0.0.1:9000"
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfssecret
export AWS_DEFAULT_REGION=us-east-1

SERVE_PID_FILE="$E2E_DIR/serve.pid"
SERVE_LOG="$E2E_DIR/serve.log"

mkdir -p "$E2E_DIR"

PASS_COUNT=0

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
pass() { PASS_COUNT=$((PASS_COUNT + 1)); printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

# assert_eq <name> <expected> <actual>
assert_eq() {
  local name=$1 expected=$2 actual=$3
  if [[ "$actual" == "$expected" ]]; then
    pass "$name (== $expected)"
  else
    fail "$name: expected [$expected], got [$actual]"
  fi
}

# q <sql> -> unaligned tuples-only result over a fresh psql connection
q() { "${PSQL[@]}" -tA -c "$1"; }

# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------
stop_server() {
  if [[ -f "$SERVE_PID_FILE" ]]; then
    local pid
    pid=$(cat "$SERVE_PID_FILE")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$SERVE_PID_FILE"
  fi
}

start_server() {
  "$BIN" serve --host 127.0.0.1 --port "$PG_PORT" >>"$SERVE_LOG" 2>&1 &
  echo $! >"$SERVE_PID_FILE"
  for _ in $(seq 1 60); do
    if q "select 1" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$(cat "$SERVE_PID_FILE")" 2>/dev/null; then
      tail -n 30 "$SERVE_LOG" >&2
      fail "icegres serve exited during startup (log tail above: $SERVE_LOG)"
    fi
    sleep 0.5
  done
  tail -n 30 "$SERVE_LOG" >&2
  fail "icegres serve did not become ready on port $PG_PORT within 30s"
}

cleanup() { stop_server; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 0. Stack up
# ---------------------------------------------------------------------------
log "starting lakehouse stack (infra/scripts/up.sh)"
bash "$REPO_DIR/infra/scripts/up.sh" >"$E2E_DIR/up.log" 2>&1 \
  || { tail -n 30 "$E2E_DIR/up.log" >&2; fail "infra/scripts/up.sh failed (log: $E2E_DIR/up.log)"; }
pass "lakehouse stack healthy"

# ---------------------------------------------------------------------------
# 1. Build (cargo skips work when the binary is fresh)
# ---------------------------------------------------------------------------
log "building icegres"
(cd "$ICEGRES_DIR" && cargo build --quiet) \
  || fail "cargo build failed"
[[ -x "$BIN" ]] || fail "binary not found at $BIN"
pass "cargo build"

# ---------------------------------------------------------------------------
# 2. Port must be ours to use
# ---------------------------------------------------------------------------
stop_server # kill any server left over from a previous (crashed) run
if q "select 1" >/dev/null 2>&1; then
  fail "something is already listening on $PG_HOST:$PG_PORT — stop it first (not started by this harness)"
fi

# ---------------------------------------------------------------------------
# 3. Seed (idempotent)
# ---------------------------------------------------------------------------
log "seeding demo data"
"$BIN" seed >"$E2E_DIR/seed.log" 2>&1 \
  || { tail -n 30 "$E2E_DIR/seed.log" >&2; fail "icegres seed failed (log: $E2E_DIR/seed.log)"; }
pass "icegres seed"

# ---------------------------------------------------------------------------
# 4. Serve
# ---------------------------------------------------------------------------
log "starting icegres serve on port $PG_PORT"
: >"$SERVE_LOG"
start_server
pass "server ready on port $PG_PORT"

# ---------------------------------------------------------------------------
# (a) Row counts match the seeded dataset
# ---------------------------------------------------------------------------
log "(a) seeded row counts"
assert_eq "demo.cities count" 20 "$(q 'select count(*) from demo.cities')"
assert_eq "demo.trips seeded rows (trip_id 1..280)" 280 \
  "$(q 'select count(*) from demo.trips where trip_id between 1 and 280')"

# ---------------------------------------------------------------------------
# (b) WHERE filter with exact expected count (deterministic: LCG seed 42)
# ---------------------------------------------------------------------------
log "(b) WHERE filter"
assert_eq "seeded trips with distance_km > 20" 104 \
  "$(q 'select count(*) from demo.trips where trip_id between 1 and 280 and distance_km > 20')"

# ---------------------------------------------------------------------------
# (c) Aggregate GROUP BY with exact expected first row
# ---------------------------------------------------------------------------
log "(c) aggregate GROUP BY"
assert_eq "top city by trips (city|trips|avg_fare)" "Berlin|21|25.42" \
  "$(q "select city, count(*) as trips, round(avg(fare), 2) as avg_fare
        from demo.trips where trip_id between 1 and 280
        group by city order by trips desc, city asc limit 1")"

# ---------------------------------------------------------------------------
# (d) JOIN trips x cities with exact expected value
# ---------------------------------------------------------------------------
log "(d) JOIN"
assert_eq "top country by trips via join (country|trips)" "United Kingdom|33" \
  "$(q "select c.country, count(*) as trips
        from demo.trips t join demo.cities c on t.city = c.city
        where t.trip_id between 1 and 280
        group by c.country order by trips desc, c.country asc limit 1")"

# ---------------------------------------------------------------------------
# (e) Write path over the wire: INSERT via psql, verify from new connections
# ---------------------------------------------------------------------------
log "(e) INSERT over pgwire"
total_before=$(q 'select count(*) from demo.trips')
max_id=$(q 'select coalesce(max(trip_id), 0) from demo.trips')
new_id=$((max_id >= 900000 ? max_id + 1 : 900000))

insert_tag=$("${PSQL[@]}" -c "insert into demo.trips (trip_id, city, distance_km, fare, ts)
  values ($new_id, 'E2E City', 1.23, 4.56, TIMESTAMP '2026-07-05 12:34:56')" | tail -n 1)
assert_eq "INSERT command tag" "INSERT 0 1" "$insert_tag"

# Both checks below run on NEW psql connections.
total_after=$(q 'select count(*) from demo.trips')
assert_eq "demo.trips count after INSERT" "$((total_before + 1))" "$total_after"
assert_eq "inserted row readable from a new connection" \
  "$new_id|E2E City|1.23|4.56|2026-07-05 12:34:56.000000" \
  "$(q "select trip_id, city, distance_km, fare, ts from demo.trips where trip_id = $new_id")"

# ---------------------------------------------------------------------------
# (f) Data really lives in the lakehouse (catalog registration + S3 Parquet)
# ---------------------------------------------------------------------------
log "(f) lakehouse storage and catalog registration"
prefix=$(curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" | jq -r '.overrides.prefix // .defaults.prefix') \
  || fail "could not fetch catalog config from $CATALOG_URI"
[[ -n "$prefix" && "$prefix" != "null" ]] || fail "no prefix in catalog config response"

tables_json=$(curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables") \
  || fail "could not list tables in namespace demo via the REST catalog"
for t in trips cities; do
  echo "$tables_json" | jq -e --arg t "$t" '.identifiers[] | select(.name == $t)' >/dev/null \
    || fail "table demo.$t not registered in the REST catalog: $tables_json"
  pass "demo.$t registered in Lakekeeper catalog"

  location=$(curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/$t" | jq -r '.metadata.location')
  [[ "$location" == s3://lakehouse/* ]] || fail "unexpected table location for demo.$t: $location"
  key_prefix=${location#s3://lakehouse/}
  parquet_count=$(aws --endpoint-url "$S3_ENDPOINT" s3 ls --recursive "s3://lakehouse/$key_prefix/data/" \
    | grep -c '\.parquet$' || true)
  [[ "$parquet_count" -gt 0 ]] \
    || fail "no Parquet data files on RustFS under $location/data/ for demo.$t"
  pass "demo.$t has $parquet_count Parquet data file(s) on RustFS under $location/data/"
done

# ---------------------------------------------------------------------------
# (g) Restart durability: data lives in Iceberg, not the server
# ---------------------------------------------------------------------------
log "(g) restart durability"
stop_server
if q "select 1" >/dev/null 2>&1; then
  fail "server still answering after kill"
fi
start_server
assert_eq "demo.trips count after server restart" "$total_after" \
  "$(q 'select count(*) from demo.trips')"
assert_eq "seeded rows intact after restart" 280 \
  "$(q 'select count(*) from demo.trips where trip_id between 1 and 280')"
assert_eq "wire-inserted row survived restart" "$new_id" \
  "$(q "select trip_id from demo.trips where trip_id = $new_id")"

# ---------------------------------------------------------------------------
log "all assertions passed ($PASS_COUNT)"
