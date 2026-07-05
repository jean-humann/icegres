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
#     on that id range. The write-path tests append/update/delete only rows
#     with fresh unique trip_id >= 900000 (sections (e) and (i)), which the
#     range filter keeps out of the deterministic assertions.
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
SECURE_PORT=5443 # auth+TLS server for section (h)
PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d icegres -v ON_ERROR_STOP=1)
export PGCONNECT_TIMEOUT=5

# Harness-owned servers are permissive/plaintext by design (except the
# dedicated auth+TLS server in section (h), configured explicitly): a stray
# ICEGRES_AUTH_FILE/ICEGRES_TLS_* in the caller's environment must not flip
# them. Clients still pass credentials when configured: psql reads PGPASSWORD
# from the (inherited) environment on every invocation below.
unset ICEGRES_AUTH_FILE ICEGRES_TLS_CERT ICEGRES_TLS_KEY

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
    # Only signal the PID if it is actually an icegres process: a pidfile left
    # behind by a crashed run may name a PID recycled by an unrelated process.
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegres ]]; then
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

SECURE_PID_FILE="$E2E_DIR/serve-secure.pid"
SECURE_LOG="$E2E_DIR/serve-secure.log"

stop_secure_server() {
  if [[ -f "$SECURE_PID_FILE" ]]; then
    local pid
    pid=$(cat "$SECURE_PID_FILE")
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegres ]]; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$SECURE_PID_FILE"
  fi
}

cleanup() { stop_server; stop_secure_server; }
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
# (h) Auth (--auth-file, SCRAM-SHA-256) + TLS (--tls-cert/--tls-key)
# ---------------------------------------------------------------------------
log "(h) auth + TLS on :$SECURE_PORT"
stop_secure_server
if psql -h "$PG_HOST" -p "$SECURE_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  fail "something is already listening on :$SECURE_PORT — stop it first"
fi

bash "$REPO_DIR/infra/scripts/gen-dev-cert.sh" >/dev/null \
  || fail "gen-dev-cert.sh failed"
TLS_CRT="$REPO_DIR/infra/.data/tls/dev.crt"
TLS_KEY="$REPO_DIR/infra/.data/tls/dev.key"
AUTH_FILE="$E2E_DIR/auth.conf"
printf '# e2e credentials\ne2e_user:e2e-secret-pw\n' >"$AUTH_FILE"
chmod 600 "$AUTH_FILE"

: >"$SECURE_LOG"
"$BIN" serve --host "$PG_HOST" --port "$SECURE_PORT" \
  --auth-file "$AUTH_FILE" --tls-cert "$TLS_CRT" --tls-key "$TLS_KEY" \
  >>"$SECURE_LOG" 2>&1 &
echo $! >"$SECURE_PID_FILE"
secure_ready=0
for _ in $(seq 1 60); do
  if PGPASSWORD=e2e-secret-pw psql "host=$PG_HOST port=$SECURE_PORT user=e2e_user dbname=icegres sslmode=require" \
       -tA -c 'select 1' >/dev/null 2>&1; then
    secure_ready=1; break
  fi
  if ! kill -0 "$(cat "$SECURE_PID_FILE")" 2>/dev/null; then
    tail -n 30 "$SECURE_LOG" >&2
    fail "auth+TLS server exited during startup (log: $SECURE_LOG)"
  fi
  sleep 0.5
done
[[ "$secure_ready" == 1 ]] || { tail -n 30 "$SECURE_LOG" >&2; fail "auth+TLS server not ready on :$SECURE_PORT"; }
pass "auth+TLS server ready on :$SECURE_PORT"

assert_eq "right password over sslmode=require" 1 \
  "$(PGPASSWORD=e2e-secret-pw psql "host=$PG_HOST port=$SECURE_PORT user=e2e_user dbname=icegres sslmode=require" -tA -c 'select 1' 2>&1)"

assert_eq "right password + sslmode=verify-full (pinned dev cert)" 1 \
  "$(PGPASSWORD=e2e-secret-pw psql "host=localhost port=$SECURE_PORT user=e2e_user dbname=icegres sslmode=verify-full sslrootcert=$TLS_CRT" -tA -c 'select 1' 2>&1)"

if PGPASSWORD=totally-wrong psql "host=$PG_HOST port=$SECURE_PORT user=e2e_user dbname=icegres" \
     -tA -c 'select 1' >/dev/null 2>&1; then
  fail "wrong password was ACCEPTED on the auth-enabled server"
fi
pass "wrong password rejected"

if PGPASSWORD=e2e-secret-pw psql "host=$PG_HOST port=$SECURE_PORT user=no_such_user dbname=icegres" \
     -tA -c 'select 1' >/dev/null 2>&1; then
  fail "unknown user was ACCEPTED on the auth-enabled server"
fi
pass "unknown user rejected"

tls_line=$(echo | openssl s_client -starttls postgres -connect "$PG_HOST:$SECURE_PORT" 2>/dev/null \
  | grep -Eo 'TLSv1\.[23], Cipher is [A-Z0-9_]+' | head -n 1)
[[ -n "$tls_line" ]] || fail "openssl s_client -starttls postgres saw no TLS handshake on :$SECURE_PORT"
pass "TLS handshake proven by openssl s_client ($tls_line)"

# The data path works authenticated + encrypted end to end.
assert_eq "authenticated+encrypted query result" 20 \
  "$(PGPASSWORD=e2e-secret-pw psql "host=$PG_HOST port=$SECURE_PORT user=e2e_user dbname=icegres sslmode=require" -tA -c 'select count(*) from demo.cities' 2>&1)"

stop_secure_server

# ---------------------------------------------------------------------------
# (i) UPDATE/DELETE: copy-on-write DML over the wire (SPEC B2/B3)
# ---------------------------------------------------------------------------
log "(i) UPDATE/DELETE copy-on-write DML"
U_ID=$((new_id + 1))
D_ID=$((new_id + 2))
"${PSQL[@]}" -q -c "insert into demo.trips (trip_id, city, distance_km, fare, ts) values
  ($U_ID, 'DML Update', 1.0, 10.0, TIMESTAMP '2026-07-05 00:00:00'),
  ($D_ID, 'DML Delete', 2.0, 20.0, TIMESTAMP '2026-07-05 00:00:00')" \
  || fail "seeding DML test rows failed"

pre_dml_snap=$(q 'select snapshot_id from demo."trips$snapshots" order by committed_at desc limit 1')
[[ "$pre_dml_snap" =~ ^[0-9]+$ ]] || fail "could not read the pre-DML snapshot id"

update_tag=$("${PSQL[@]}" -c "update demo.trips set fare = 123.45 where trip_id = $U_ID" | tail -n 1)
assert_eq "UPDATE command tag" "UPDATE 1" "$update_tag"
assert_eq "updated row readable from a new connection" "$U_ID|DML Update|123.45" \
  "$(q "select trip_id, city, fare from demo.trips where trip_id = $U_ID")"

delete_tag=$("${PSQL[@]}" -c "delete from demo.trips where trip_id = $D_ID" | tail -n 1)
assert_eq "DELETE command tag" "DELETE 1" "$delete_tag"
assert_eq "deleted row gone from a new connection" 0 \
  "$(q "select count(*) from demo.trips where trip_id = $D_ID")"
assert_eq "sibling row survived the DELETE" "$U_ID" \
  "$(q "select trip_id from demo.trips where trip_id = $U_ID")"
assert_eq "seeded rows untouched by DML" 280 \
  "$(q 'select count(*) from demo.trips where trip_id between 1 and 280')"

# Time travel is intact after DML: the pre-DML snapshot still serves the
# deleted row and the pre-update fare (copy-on-write never mutates history).
assert_eq "pre-DML snapshot still serves the deleted row" 1 \
  "$(q "select count(*) from demo.\"trips@$pre_dml_snap\" where trip_id = $D_ID")"
assert_eq "pre-DML snapshot still serves the pre-update fare" "10.0" \
  "$(q "select fare from demo.\"trips@$pre_dml_snap\" where trip_id = $U_ID")"

# Optimistic-concurrency retry, proven against the real catalog:
# ICEGRES_DML_INJECT_CONFLICT sabotages attempt 1's assert-ref-snapshot-id,
# Lakekeeper answers 409, and the engine recomputes+retries successfully.
ICEGRES_DML_INJECT_CONFLICT=1 "$BIN" sql -e "delete from demo.trips where trip_id = $U_ID" \
  >"$E2E_DIR/dml-conflict.log" 2>&1 \
  || { tail -n 20 "$E2E_DIR/dml-conflict.log" >&2; fail "conflict-injected DELETE failed"; }
grep -q "commit conflict (409)" "$E2E_DIR/dml-conflict.log" \
  || fail "conflict injection did not produce a 409 (log: $E2E_DIR/dml-conflict.log)"
grep -q "DELETE 1" "$E2E_DIR/dml-conflict.log" \
  || fail "conflict-injected DELETE did not commit on retry (log: $E2E_DIR/dml-conflict.log)"
pass "DML conflict retry: 409 from Lakekeeper on attempt 1, committed on attempt 2"
assert_eq "conflict-retried DELETE took effect" 0 \
  "$(q "select count(*) from demo.trips where trip_id = $U_ID")"

# ---------------------------------------------------------------------------
log "all assertions passed ($PASS_COUNT)"
