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
PK_PORT=5448     # --enforce-pk server for section (k)
STRICT_PORT=5450 # ICEGRES_TXN_STRICT server for section (u)
VBUF_PORT=5451   # buffered durability server for section (v)
PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d icegres -v ON_ERROR_STOP=1)
export PGCONNECT_TIMEOUT=5

# Harness-owned servers are permissive/plaintext by design (except the
# dedicated auth+TLS server in section (h), configured explicitly): a stray
# ICEGRES_AUTH_FILE/ICEGRES_TLS_* in the caller's environment must not flip
# them. Clients still pass credentials when configured: psql reads PGPASSWORD
# from the (inherited) environment on every invocation below.
unset ICEGRES_AUTH_FILE ICEGRES_TLS_CERT ICEGRES_TLS_KEY
# Same for buffered-write mode: only section (l)'s dedicated server enables it.
unset ICEGRES_WRITE_BUFFER_MS ICEGRES_WRITE_BUFFER_MAX_ROWS
# And for strict transaction mode: only section (u)'s dedicated server enables it.
unset ICEGRES_TXN_STRICT

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
PK_PID_FILE="$E2E_DIR/serve-pk.pid"
PK_LOG="$E2E_DIR/serve-pk.log"

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

stop_pidfile_generic() { # pidfile — identity-checked kill
  local pidfile=$1 pid
  if [[ -f "$pidfile" ]]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegres ]]; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  fi
}

stop_icegresd() { # identity-checked kill of the control plane (comm=icegresd)
  local pidfile="$E2E_DIR/icegresd.pid" pid
  if [[ -f "$pidfile" ]]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegresd ]]; then
      kill "$pid" 2>/dev/null || true # SIGTERM: icegresd terminates its computes
      for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  fi
}

cleanup() {
  stop_server
  stop_secure_server
  stop_pidfile_generic "$PK_PID_FILE"
  stop_pidfile_generic "$E2E_DIR/serve-buffered.pid"
  stop_pidfile_generic "$E2E_DIR/serve-branch.pid"
  stop_pidfile_generic "$E2E_DIR/flight.pid"
  stop_pidfile_generic "$E2E_DIR/flight-secure.pid"
  stop_pidfile_generic "$E2E_DIR/flight-authz.pid"
  stop_pidfile_generic "$E2E_DIR/serve-strict.pid"
  stop_pidfile_generic "$E2E_DIR/serve-vbuf.pid"
  stop_pidfile_generic "$E2E_DIR/serve-obs.pid"
  stop_pidfile_generic "$E2E_DIR/serve-thr.pid"
  stop_pidfile_generic "$E2E_DIR/flight-tls.pid"
  stop_icegresd
}
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
# (j) Explicit transactions (SPEC B4): ROLLBACK undoes, COMMIT is one atomic
#     snapshot across statements, errors abort, concurrent writers conflict.
# ---------------------------------------------------------------------------
log "(j) explicit transactions BEGIN/COMMIT/ROLLBACK"
TX_A=$((new_id + 3))
TX_B=$((new_id + 4))
TX_C=$((new_id + 5))

# The trips snapshot count is read via the REST catalog (the $snapshots
# metadata table has a pre-existing upstream projection bug on count()).
trips_snap_count() {
  curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/trips" \
    | jq '[.metadata.snapshots[]?] | length'
}

# j1: ROLLBACK undoes the INSERT; the row was visible INSIDE the txn (RYOW).
txn_out=$("${PSQL[@]}" 2>&1 <<EOF
BEGIN;
insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($TX_A, 'E2E Txn', 1.0, 1.0, TIMESTAMP '2026-07-05 00:00:00');
select count(*) from demo.trips where trip_id = $TX_A;
ROLLBACK;
EOF
)
echo "$txn_out" | grep -q "INSERT 0 1" || fail "txn INSERT tag missing: $txn_out"
echo "$txn_out" | grep -qE '^\s*1$' || fail "read-your-own-writes inside txn failed: $txn_out"
echo "$txn_out" | grep -q "ROLLBACK" || fail "ROLLBACK tag missing: $txn_out"
pass "txn INSERT visible inside the transaction (read-your-own-writes)"
assert_eq "ROLLBACK undid the INSERT (new connection)" 0 \
  "$(q "select count(*) from demo.trips where trip_id = $TX_A")"

# j2: multi-statement txn (2 INSERTs + UPDATE + DELETE) commits as ONE
#     Iceberg snapshot; final state correct from new connections.
snaps_before=$(trips_snap_count)
"${PSQL[@]}" -q 2>"$E2E_DIR/txn-commit.err" <<EOF || { cat "$E2E_DIR/txn-commit.err" >&2; fail "multi-statement txn failed"; }
BEGIN;
insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($TX_A, 'E2E Txn', 1.0, 10.0, TIMESTAMP '2026-07-05 00:00:00');
insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($TX_B, 'E2E Txn', 2.0, 20.0, TIMESTAMP '2026-07-05 00:00:00');
update demo.trips set fare = 99.0 where trip_id = $TX_A;
delete from demo.trips where trip_id = $TX_B;
COMMIT;
EOF
snaps_after=$(trips_snap_count)
assert_eq "COMMIT produced exactly ONE new snapshot for 4 statements" \
  "$((snaps_before + 1))" "$snaps_after"
assert_eq "post-commit state (INSERT+UPDATE composed)" "$TX_A|99.0" \
  "$(q "select trip_id, fare from demo.trips where trip_id = $TX_A")"
assert_eq "post-commit state (INSERT+DELETE composed away)" 0 \
  "$(q "select count(*) from demo.trips where trip_id = $TX_B")"

# j3: a failed statement aborts the transaction: subsequent statements are
#     rejected (25P02) and COMMIT answers ROLLBACK; nothing landed. This
#     probe must keep the session running past the error, so it uses psql
#     WITHOUT ON_ERROR_STOP.
txn_out=$(psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d icegres 2>&1 <<EOF
BEGIN;
select 1/0;
insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($TX_C, 'E2E Abort', 1.0, 1.0, TIMESTAMP '2026-07-05 00:00:00');
COMMIT;
EOF
) || true
echo "$txn_out" | grep -q "current transaction is aborted" \
  || fail "aborted txn did not block the follow-up statement: $txn_out"
echo "$txn_out" | grep -q "ROLLBACK" \
  || fail "COMMIT after a failed statement did not roll back: $txn_out"
pass "failed statement aborts the txn; COMMIT rolls back"
assert_eq "nothing from the aborted txn landed" 0 \
  "$(q "select count(*) from demo.trips where trip_id = $TX_C")"

# j4: snapshot isolation is first-committer-wins: a writer that commits
#     between BEGIN and COMMIT makes the txn's COMMIT fail with 40001.
( echo "BEGIN;"
  echo "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($TX_A + 100, 'E2E Conflict', 1.0, 1.0, TIMESTAMP '2026-07-05 00:00:00');"
  sleep 3
  echo "COMMIT;" ) | "${PSQL[@]}" >"$E2E_DIR/txn-conflict.out" 2>&1 &
TXN_PID=$!
sleep 1.5
"${PSQL[@]}" -q -c "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($TX_C, 'E2E Winner', 1.0, 1.0, TIMESTAMP '2026-07-05 00:00:00')" \
  || fail "concurrent autocommit INSERT failed"
wait "$TXN_PID" || true
grep -q "could not serialize access due to concurrent update" "$E2E_DIR/txn-conflict.out" \
  || { cat "$E2E_DIR/txn-conflict.out" >&2; fail "txn COMMIT did not fail with a serialization error"; }
pass "concurrent writer -> COMMIT fails with serialization_failure (first-committer-wins)"
assert_eq "loser txn's row absent, winner's row present" "0|1" \
  "$(q "select count(*) from demo.trips where trip_id = $TX_A + 100")|$(q "select count(*) from demo.trips where trip_id = $TX_C")"

# ---------------------------------------------------------------------------
# (j2) Atomic multi-table transactions (roadmap Phase 3): a COMMIT touching
#      N tables is ONE all-or-nothing catalog request against Lakekeeper's
#      POST /v1/{prefix}/transactions/commit. Both tables commit together
#      (exactly one new snapshot each), and a staged conflict is a clean
#      40001 with NEITHER table changed — the 40003 partial-apply outcome is
#      unreachable on this path. Whole-lakehouse branches ride the same
#      endpoint: create-all/drop-all set/remove the ref on EVERY table —
#      tables in NESTED namespaces included, each request pinning main to
#      the head captured at load (consistent-or-nothing cut) — in one
#      atomic transaction.
# ---------------------------------------------------------------------------
log "(j2) atomic multi-table transactions + whole-lakehouse branches"

# snap_count <table>: snapshot count of demo.<table> via the REST catalog.
snap_count() {
  curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/$1" \
    | jq '[.metadata.snapshots[]?] | length'
}

q 'drop table if exists demo.e2e_mt_a' >/dev/null 2>&1 || true
q 'drop table if exists demo.e2e_mt_b' >/dev/null 2>&1 || true
q 'create table demo.e2e_mt_a (id bigint, v double)' >/dev/null
q 'create table demo.e2e_mt_b (id bigint, v double)' >/dev/null

# j2-1: a two-table COMMIT lands atomically: both rows visible from new
# connections, exactly ONE new snapshot per table, and the server used the
# multi-table transaction endpoint (not N ordered per-table commits).
mt_atomic_before=$(grep -c 'transaction committed atomically via transactions/commit' "$SERVE_LOG" || true)
snaps_a_before=$(snap_count e2e_mt_a)
snaps_b_before=$(snap_count e2e_mt_b)
"${PSQL[@]}" -q 2>"$E2E_DIR/mt-commit.err" <<EOF || { cat "$E2E_DIR/mt-commit.err" >&2; fail "multi-table txn COMMIT failed"; }
BEGIN;
insert into demo.e2e_mt_a values (1, 1.0);
insert into demo.e2e_mt_b values (2, 2.0);
COMMIT;
EOF
assert_eq "both tables visible from new connections after ONE COMMIT" "1|1" \
  "$(q 'select count(*) from demo.e2e_mt_a')|$(q 'select count(*) from demo.e2e_mt_b')"
assert_eq "exactly one new snapshot per table" \
  "$((snaps_a_before + 1))|$((snaps_b_before + 1))" \
  "$(snap_count e2e_mt_a)|$(snap_count e2e_mt_b)"
mt_atomic_after=$(grep -c 'transaction committed atomically via transactions/commit' "$SERVE_LOG" || true)
assert_eq "COMMIT went through the atomic transactions/commit endpoint" \
  "$((mt_atomic_before + 1))" "$mt_atomic_after"

# j2-2: staged conflict — a second writer commits to ONE touched table while
# the transaction is open. COMMIT fails with 40001 (serialization_failure,
# retryable) and NEITHER table changed: no partial apply, no 40003.
snaps_a_before=$(snap_count e2e_mt_a)
snaps_b_before=$(snap_count e2e_mt_b)
( echo "BEGIN;"
  echo "insert into demo.e2e_mt_a values (10, 10.0);"
  echo "insert into demo.e2e_mt_b values (11, 11.0);"
  sleep 3
  echo "COMMIT;" ) | "${PSQL[@]}" -v VERBOSITY=verbose >"$E2E_DIR/mt-conflict.out" 2>&1 &
MT_PID=$!
sleep 1.5
"${PSQL[@]}" -q -c 'insert into demo.e2e_mt_b values (777, 7.0)' \
  || fail "concurrent autocommit INSERT failed"
wait "$MT_PID" || true
grep -q 'could not serialize access due to concurrent update' "$E2E_DIR/mt-conflict.out" \
  || { cat "$E2E_DIR/mt-conflict.out" >&2; fail "multi-table conflict did not report a serialization failure"; }
grep -q '40001' "$E2E_DIR/mt-conflict.out" \
  || { cat "$E2E_DIR/mt-conflict.out" >&2; fail "multi-table conflict sqlstate is not 40001"; }
grep -q 'no changes were applied' "$E2E_DIR/mt-conflict.out" \
  || { cat "$E2E_DIR/mt-conflict.out" >&2; fail "conflict error does not state that nothing was applied"; }
pass "staged conflict -> 40001 serialization_failure (all-or-nothing, retryable)"
assert_eq "NEITHER table has the loser txn's rows; the winner's row landed" "0|0|1" \
  "$(q 'select count(*) from demo.e2e_mt_a where id = 10')|$(q 'select count(*) from demo.e2e_mt_b where id = 11')|$(q 'select count(*) from demo.e2e_mt_b where id = 777')"
assert_eq "conflicted COMMIT wrote no snapshot (only the winner's on table b)" \
  "$snaps_a_before|$((snaps_b_before + 1))" \
  "$(snap_count e2e_mt_a)|$(snap_count e2e_mt_b)"

# j2-3: whole-lakehouse branches: create-all sets the ref on EVERY table in
# ONE atomic transaction (per-table assert-ref-snapshot-id=null guard, plus
# a main=<captured head> anchor per table so the cut is consistent-or-
# nothing); drop-all removes it everywhere the same way. Tables in NESTED
# namespaces are part of "every table": list_all_tables walks the namespace
# tree (the REST list_namespaces answers one level per call), so a table in
# demo_nested.child must get the ref too — before that fix it was silently
# excluded from the cut.
ALL_BR=e2e_all

# Nested-namespace fixture: demo_nested.child.nested_t with ONE honest EMPTY
# snapshot (an Iceberg branch ref must point at a snapshot; a real
# zero-manifest manifest list is uploaded to the table's metadata dir and
# committed via the REST API — no SQL surface reaches nested namespaces).
NESTED_NS_URL="demo_nested%1Fchild" # %1F = REST spec namespace level separator
NESTED_SNAP=424242424242
nested_table_url() { echo "$CATALOG_URI/v1/$prefix/namespaces/$NESTED_NS_URL/tables/nested_t"; }
# crashed-run cleanup, then (re)create parent + child namespaces and table
curl -sf -X DELETE "$(nested_table_url)?purgeRequested=true" >/dev/null 2>&1 || true
curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces" -H 'Content-Type: application/json' \
  -d '{"namespace":["demo_nested"]}' >/dev/null 2>&1 || true # may already exist
curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces" -H 'Content-Type: application/json' \
  -d '{"namespace":["demo_nested","child"]}' >/dev/null 2>&1 || true
curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces/$NESTED_NS_URL/tables" \
  -H 'Content-Type: application/json' -d '{
  "name": "nested_t",
  "schema": {"type":"struct","schema-id":0,"fields":[
    {"id":1,"name":"id","required":false,"type":"long"}]}
}' >/dev/null || fail "could not create demo_nested.child.nested_t via the REST catalog"
nested_loc=$(curl -sf "$(nested_table_url)" | jq -r '.metadata.location')
[[ "$nested_loc" == s3://lakehouse/* ]] || fail "unexpected nested table location: $nested_loc"
# A valid EMPTY manifest list is an Avro OCF with a header (magic + writer
# schema + null codec + sync marker) and zero data blocks.
python3 - "$E2E_DIR/nested-empty-manifest-list.avro" <<'PYEOF' \
  || fail "could not generate the empty manifest list"
import json, os, sys
schema = {"type": "record", "name": "manifest_file", "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "field-id": 517},
    {"name": "sequence_number", "type": "long", "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514},
    {"name": "partitions", "field-id": 507, "type": ["null", {
        "type": "array", "element-id": 508, "items": {
            "type": "record", "name": "r508", "fields": [
                {"name": "contains_null", "type": "boolean", "field-id": 509},
                {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 518},
                {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 510},
                {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 511}]},
    }]},
]}
def vlong(n):  # Avro zigzag varint
    n = (n << 1) ^ (n >> 63)
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)
meta = {"avro.schema": json.dumps(schema).encode(),
        "avro.codec": b"null", "format-version": b"2"}
buf = b"Obj\x01" + vlong(len(meta))
for k, v in meta.items():
    buf += vlong(len(k.encode())) + k.encode() + vlong(len(v)) + v
buf += vlong(0)        # end of the header metadata map
buf += os.urandom(16)  # sync marker; zero data blocks follow
open(sys.argv[1], "wb").write(buf)
PYEOF
aws --endpoint-url "$S3_ENDPOINT" s3 cp "$E2E_DIR/nested-empty-manifest-list.avro" \
  "$nested_loc/metadata/snap-$NESTED_SNAP-0-e2e-empty.avro" >/dev/null \
  || fail "could not upload the empty manifest list for demo_nested.child.nested_t"
curl -sf -X POST "$(nested_table_url)" -H 'Content-Type: application/json' -d "{
  \"requirements\": [{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":null}],
  \"updates\": [
    {\"action\":\"add-snapshot\",\"snapshot\":{
      \"snapshot-id\": $NESTED_SNAP,
      \"sequence-number\": 1,
      \"timestamp-ms\": $(date +%s%3N),
      \"manifest-list\": \"$nested_loc/metadata/snap-$NESTED_SNAP-0-e2e-empty.avro\",
      \"summary\": {\"operation\":\"append\"},
      \"schema-id\": 0
    }},
    {\"action\":\"set-snapshot-ref\",\"ref-name\":\"main\",
     \"type\":\"branch\",\"snapshot-id\": $NESTED_SNAP}
  ]
}" >/dev/null || fail "could not commit the empty snapshot to demo_nested.child.nested_t"
pass "nested-namespace fixture: demo_nested.child.nested_t with one (empty) snapshot"
# The branch ref of the nested table, read via the REST catalog (the branch
# CLI addresses <namespace>.<table> only; nested tables are asserted here).
nested_ref() {
  curl -sf "$(nested_table_url)" \
    | jq -r ".metadata.refs.\"$ALL_BR\".\"snapshot-id\" // \"absent\""
}

"$BIN" branch drop-all "$ALL_BR" >/dev/null 2>&1 || true # crashed-run cleanup
"$BIN" branch create-all "$ALL_BR" >"$E2E_DIR/branch-create-all.log" 2>&1 \
  || { cat "$E2E_DIR/branch-create-all.log" >&2; fail "branch create-all failed"; }
grep -q "ONE atomic transaction" "$E2E_DIR/branch-create-all.log" \
  || fail "create-all output unexpected: $(cat "$E2E_DIR/branch-create-all.log")"
for t in trips cities e2e_mt_a e2e_mt_b; do
  "$BIN" branch list "demo.$t" 2>/dev/null | grep -q "^$ALL_BR	" \
    || fail "branch $ALL_BR missing on demo.$t after create-all"
done
pass "branch create-all: ref present on every table (trips, cities, e2e_mt_a, e2e_mt_b)"
grep -q "created branch $ALL_BR on demo_nested.child.nested_t at snapshot $NESTED_SNAP" \
  "$E2E_DIR/branch-create-all.log" \
  || fail "create-all output does not mention the nested table: $(cat "$E2E_DIR/branch-create-all.log")"
assert_eq "create-all reached the NESTED namespace (ref on demo_nested.child.nested_t)" \
  "$NESTED_SNAP" "$(nested_ref)"
if "$BIN" branch create-all "$ALL_BR" >/dev/null 2>&1; then
  fail "duplicate create-all was ACCEPTED (per-table assert-ref-snapshot-id=null must reject it)"
fi
pass "duplicate create-all rejected (all-or-nothing, nothing applied)"
"$BIN" branch drop-all "$ALL_BR" >"$E2E_DIR/branch-drop-all.log" 2>&1 \
  || { cat "$E2E_DIR/branch-drop-all.log" >&2; fail "branch drop-all failed"; }
for t in trips cities e2e_mt_a e2e_mt_b; do
  if "$BIN" branch list "demo.$t" 2>/dev/null | grep -q "^$ALL_BR	"; then
    fail "branch $ALL_BR still on demo.$t after drop-all"
  fi
done
pass "branch drop-all: ref removed from every table"
assert_eq "drop-all removed the ref from the NESTED table too" \
  "absent" "$(nested_ref)"
if "$BIN" branch drop-all "$ALL_BR" >/dev/null 2>&1; then
  fail "drop-all of a nonexistent branch was ACCEPTED (must error when no table has it)"
fi
pass "drop-all errors when no table has the branch"
q 'drop table demo.e2e_mt_a' >/dev/null 2>&1 || true
q 'drop table demo.e2e_mt_b' >/dev/null 2>&1 || true
# Nested fixture cleanup: table (with purge — the empty manifest list is a
# real file under its location), then child + parent namespaces.
curl -sf -X DELETE "$(nested_table_url)?purgeRequested=true" >/dev/null 2>&1 || true
curl -sf -X DELETE "$CATALOG_URI/v1/$prefix/namespaces/$NESTED_NS_URL" >/dev/null 2>&1 || true
curl -sf -X DELETE "$CATALOG_URI/v1/$prefix/namespaces/demo_nested" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# (k) Opt-in PK enforcement (SPEC B5): --enforce-pk + icegres.primary-key
# ---------------------------------------------------------------------------
log "(k) PK enforcement (--enforce-pk) on :$PK_PORT"
stop_pk_server() { stop_pidfile_generic "$PK_PID_FILE"; }
drop_pk_table() {
  curl -sf -X DELETE \
    "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/e2e_pk?purgeRequested=true" \
    >/dev/null 2>&1 || true
}
stop_pk_server
drop_pk_table
curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces/demo/tables" \
  -H 'Content-Type: application/json' -d '{
  "name": "e2e_pk",
  "schema": {"type":"struct","schema-id":0,"fields":[
    {"id":1,"name":"id","required":false,"type":"long"},
    {"id":2,"name":"val","required":false,"type":"string"}]},
  "properties": {"icegres.primary-key": "id"}
}' >/dev/null || fail "could not create demo.e2e_pk via REST catalog"

: >"$PK_LOG"
"$BIN" serve --host "$PG_HOST" --port "$PK_PORT" --enforce-pk >>"$PK_LOG" 2>&1 &
echo $! >"$PK_PID_FILE"
pk_ready=0
for _ in $(seq 1 60); do
  if psql -h "$PG_HOST" -p "$PK_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
    pk_ready=1; break
  fi
  sleep 0.5
done
[[ "$pk_ready" == 1 ]] || { tail -n 20 "$PK_LOG" >&2; fail "--enforce-pk server not ready on :$PK_PORT"; }
PKQ=(psql -h "$PG_HOST" -p "$PK_PORT" -U postgres -d icegres)

assert_eq "first insert accepted" "INSERT 0 1" \
  "$("${PKQ[@]}" -c "insert into demo.e2e_pk (id, val) values (1, 'a')" 2>&1 | tail -n 1)"
dup_out=$("${PKQ[@]}" -c "insert into demo.e2e_pk (id, val) values (1, 'dup')" 2>&1) || true
echo "$dup_out" | grep -q 'duplicate key value violates unique constraint "e2e_pk_pkey"' \
  || fail "duplicate insert not rejected: $dup_out"
pass "duplicate key rejected (23505 unique violation)"
null_out=$("${PKQ[@]}" -c "insert into demo.e2e_pk (id, val) values (NULL, 'n')" 2>&1) || true
echo "$null_out" | grep -q "violates not-null constraint" \
  || fail "NULL key not rejected: $null_out"
pass "NULL key rejected (23502 not-null violation)"
# Enforcement also applies to rows buffered in a transaction (RYOW check).
txn_pk_out=$("${PKQ[@]}" 2>&1 <<'EOF'
BEGIN;
insert into demo.e2e_pk (id, val) values (2, 'b');
insert into demo.e2e_pk (id, val) values (2, 'dup-in-txn');
COMMIT;
EOF
) || true
echo "$txn_pk_out" | grep -q "duplicate key value" \
  || fail "in-txn duplicate not rejected: $txn_pk_out"
pass "duplicate against the txn's own buffered rows rejected"
assert_eq "table holds exactly the accepted rows" "1|a" \
  "$(psql -h "$PG_HOST" -p "$PK_PORT" -U postgres -d icegres -tA -c 'select id, val from demo.e2e_pk order by id')"

stop_pk_server
drop_pk_table

# ---------------------------------------------------------------------------
# (l) Buffered write mode (--write-buffer-ms, Moonlink-style union reads):
#     insert burst acked from the buffer, instantly readable on NEW
#     connections BEFORE any Iceberg commit (union read proven by the
#     unchanged snapshot count), group-committed as ONE snapshot at the
#     flush cadence, and durable across an UNCLEAN kill once flushed.
# ---------------------------------------------------------------------------
BUF_PORT=5449
BUF_PID_FILE="$E2E_DIR/serve-buffered.pid"
BUF_LOG="$E2E_DIR/serve-buffered.log"
BUF_MS=1500 # long cadence so the "readable before commit" check is deterministic
log "(l) buffered write mode (--write-buffer-ms $BUF_MS) on :$BUF_PORT"
stop_pidfile_generic "$BUF_PID_FILE"
if psql -h "$PG_HOST" -p "$BUF_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  fail "something is already listening on :$BUF_PORT — stop it first"
fi
: >"$BUF_LOG"
"$BIN" serve --host "$PG_HOST" --port "$BUF_PORT" --write-buffer-ms "$BUF_MS" \
  >>"$BUF_LOG" 2>&1 &
echo $! >"$BUF_PID_FILE"
buf_ready=0
for _ in $(seq 1 60); do
  if psql -h "$PG_HOST" -p "$BUF_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
    buf_ready=1; break
  fi
  sleep 0.5
done
[[ "$buf_ready" == 1 ]] || { tail -n 20 "$BUF_LOG" >&2; fail "buffered server not ready on :$BUF_PORT"; }
BQ=(psql -h "$PG_HOST" -p "$BUF_PORT" -U postgres -d icegres -tA)

# The enabled mode must announce its durability trade loudly.
grep -q "write buffering is ENABLED" "$BUF_LOG" \
  || fail "buffered server did not log the durability WARN (log: $BUF_LOG)"
pass "startup WARN present (acked-write loss window documented)"

# Burst: 25 rows in 5 INSERT statements over one connection, all acked from
# the buffer, then read back from NEW connections. The instant-visibility
# assertion (count == 25) is timing-independent — the union view is correct
# whether or not a flush has happened. The stronger "served from the buffer,
# NOT the lake" proof (snapshot count unchanged across the burst) races the
# background flush tick by construction, so it retries with fresh ids until
# an attempt completes inside one flush window (3 attempts, each ~0.5 s in a
# ${BUF_MS} ms window — a systematic failure means the union read is broken).
buf_base=$(( $("${BQ[@]}" -c 'select coalesce(max(trip_id), 0) from demo.trips') + 1 ))
(( buf_base >= 900000 )) || buf_base=950000
snaps_start=$(trips_snap_count)
burst_stmts=""
attempts=0
union_proven=0
for attempt in 1 2 3; do
  attempts=$attempt
  base=$((buf_base + (attempt - 1) * 25))
  snaps_pre=$(trips_snap_count)
  burst_stmts=""
  for k in 0 1 2 3 4; do
    vals=""
    for j in 0 1 2 3 4; do
      id=$((base + k * 5 + j))
      vals+="${vals:+, }($id, 'E2E Buffered', 1.0, 2.0, TIMESTAMP '2026-07-06 00:00:00')"
    done
    burst_stmts+="insert into demo.trips (trip_id, city, distance_km, fare, ts) values $vals;"$'\n'
  done
  burst_out=$(psql -h "$PG_HOST" -p "$BUF_PORT" -U postgres -d icegres -v ON_ERROR_STOP=1 2>&1 <<<"$burst_stmts") \
    || fail "buffered INSERT burst failed: $burst_out"
  [[ "$(grep -c '^INSERT 0 5$' <<<"$burst_out")" == 5 ]] || fail "burst tags wrong: $burst_out"
  # Union read: all 25 rows visible IMMEDIATELY on a NEW connection.
  burst_count=$("${BQ[@]}" -c "select count(*) from demo.trips where trip_id between $base and $((base + 24))")
  [[ "$burst_count" == 25 ]] || fail "burst not instantly readable on a new connection (union read broken): got $burst_count/25"
  # Aggregates also see the buffered rows (whole-scan union, no special case).
  agg=$("${BQ[@]}" -c "select city, count(*) from demo.trips where trip_id between $base and $((base + 24)) group by city")
  [[ "$agg" == "E2E Buffered|25" ]] || fail "aggregate over the union view wrong: $agg"
  snaps_post=$(trips_snap_count)
  if [[ "$snaps_post" == "$snaps_pre" ]]; then
    union_proven=1
    break
  fi
  log "  flush tick landed inside attempt $attempt's burst window (snapshots $snaps_pre -> $snaps_post); retrying with fresh ids"
done
[[ "$union_proven" == 1 ]] || fail "no burst attempt completed with an unchanged snapshot count — rows are not being served from the buffer"
pass "burst of 25 rows readable instantly on new connections with ZERO new Iceberg snapshots (union read, acked from the buffer)"
total_rows=$((attempts * 25))

# Wait one flush cadence: the buffered statements group-commit. 5 INSERT
# statements per attempt would be 5 snapshots each in synchronous mode; the
# buffer coalesces each attempt's burst into one flush (2 max if a tick
# split an earlier retried attempt).
sleep $(( BUF_MS / 1000 + 2 ))
snaps_settled=$(trips_snap_count)
new_snaps=$((snaps_settled - snaps_start))
if (( new_snaps >= 1 && new_snaps <= attempts + 1 )); then
  pass "group commit: $((attempts * 5)) INSERT statements ($total_rows rows) produced $new_snaps snapshot(s) (sync mode would produce $((attempts * 5)))"
else
  fail "unexpected snapshot count after flush: $new_snaps new snapshots for $attempts burst attempt(s)"
fi
assert_eq "all burst rows committed after the flush cadence" "$total_rows" \
  "$("${BQ[@]}" -c "select count(*) from demo.trips where trip_id between $buf_base and $((buf_base + total_rows - 1))")"

# UNCLEAN kill (SIGKILL — no graceful shutdown), then restart: the flushed
# rows are in Iceberg, so they survive the loss of the process.
buf_pid=$(cat "$BUF_PID_FILE")
kill -9 "$buf_pid" 2>/dev/null || fail "could not SIGKILL buffered server"
for _ in $(seq 1 20); do kill -0 "$buf_pid" 2>/dev/null || break; sleep 0.25; done
kill -0 "$buf_pid" 2>/dev/null && fail "buffered server survived SIGKILL"
rm -f "$BUF_PID_FILE"
"$BIN" serve --host "$PG_HOST" --port "$BUF_PORT" --write-buffer-ms "$BUF_MS" \
  >>"$BUF_LOG" 2>&1 &
echo $! >"$BUF_PID_FILE"
buf_ready=0
for _ in $(seq 1 60); do
  if psql -h "$PG_HOST" -p "$BUF_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
    buf_ready=1; break
  fi
  sleep 0.5
done
[[ "$buf_ready" == 1 ]] || { tail -n 20 "$BUF_LOG" >&2; fail "buffered server not ready after unclean restart"; }
assert_eq "committed burst survived the unclean kill + restart" "$total_rows" \
  "$("${BQ[@]}" -c "select count(*) from demo.trips where trip_id between $buf_base and $((buf_base + total_rows - 1))")"
# The main (synchronous, :$PG_PORT) server sees them too — cross-server
# freshness after the flush cadence.
assert_eq "burst visible on the default-mode server (cross-server = commit cadence)" "$total_rows" \
  "$(q "select count(*) from demo.trips where trip_id between $buf_base and $((buf_base + total_rows - 1))")"

# Ordering fence: with rows pending in the buffer, an UPDATE must see them
# (buffered INSERT then immediate UPDATE behaves exactly like sync mode).
fence_id=$((buf_base + total_rows))
psql -h "$PG_HOST" -p "$BUF_PORT" -U postgres -d icegres -q -c \
  "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($fence_id, 'E2E Fence', 1.0, 1.0, TIMESTAMP '2026-07-06 00:00:00')" \
  || fail "fence INSERT failed"
fence_tag=$(psql -h "$PG_HOST" -p "$BUF_PORT" -U postgres -d icegres -c \
  "update demo.trips set fare = 42.0 where trip_id = $fence_id" | tail -n 1)
assert_eq "UPDATE right after a buffered INSERT (flush fence)" "UPDATE 1" "$fence_tag"
assert_eq "fenced row readable with the updated value" "$fence_id|42.0" \
  "$("${BQ[@]}" -c "select trip_id, fare from demo.trips where trip_id = $fence_id")"

stop_pidfile_generic "$BUF_PID_FILE"

# ---------------------------------------------------------------------------
# (m) Zero-copy branches (SPEC D6, Neon branch-per-endpoint model): a branch
#     is a named Iceberg snapshot ref — creating one copies NO data. Two
#     servers, one per branch, write to their own ref with full isolation:
#     writes on dev never appear on main and vice versa, single copy of the
#     shared history in the lake.
# ---------------------------------------------------------------------------
BR_PORT=5440
BR_PID_FILE="$E2E_DIR/serve-branch.pid"
BR_LOG="$E2E_DIR/serve-branch.log"
BR_NAME=e2e_dev
log "(m) zero-copy branches: main on :$PG_PORT, branch '$BR_NAME' on :$BR_PORT"
stop_pidfile_generic "$BR_PID_FILE"
if psql -h "$PG_HOST" -p "$BR_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  fail "something is already listening on :$BR_PORT — stop it first"
fi
# Idempotency: a crashed earlier run may have left the ref behind.
"$BIN" branch drop demo.trips "$BR_NAME" >/dev/null 2>&1 || true

# m1: create = one metadata commit, zero data copied; both refs at one head.
"$BIN" branch create demo.trips "$BR_NAME" >"$E2E_DIR/branch-create.log" 2>&1 \
  || { cat "$E2E_DIR/branch-create.log" >&2; fail "branch create failed"; }
grep -q "created branch $BR_NAME" "$E2E_DIR/branch-create.log" \
  || fail "branch create output unexpected: $(cat "$E2E_DIR/branch-create.log")"
pass "branch create $BR_NAME (zero-copy snapshot ref)"
branch_list=$("$BIN" branch list demo.trips 2>&1)
main_head=$(awk -F'\t' '$1=="main"{print $2}' <<<"$branch_list")
dev_head=$(awk -F'\t' -v b="$BR_NAME" '$1==b{print $2}' <<<"$branch_list")
[[ -n "$main_head" && "$main_head" == "$dev_head" ]] \
  || fail "freshly created branch does not share main's head: main=$main_head dev=$dev_head ($branch_list)"
pass "branch list shows $BR_NAME at main's head ($main_head)"
if "$BIN" branch create demo.trips "$BR_NAME" >/dev/null 2>&1; then
  fail "duplicate branch create was ACCEPTED (assert-ref-snapshot-id null must reject it)"
fi
pass "duplicate branch create rejected (atomic create via assert-ref-snapshot-id=null)"

# m2: serve the branch on its own port (Neon: one endpoint per branch).
: >"$BR_LOG"
"$BIN" serve --host "$PG_HOST" --port "$BR_PORT" --branch "$BR_NAME" >>"$BR_LOG" 2>&1 &
echo $! >"$BR_PID_FILE"
br_ready=0
for _ in $(seq 1 60); do
  if psql -h "$PG_HOST" -p "$BR_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
    br_ready=1; break
  fi
  sleep 0.5
done
[[ "$br_ready" == 1 ]] || { tail -n 20 "$BR_LOG" >&2; fail "--branch server not ready on :$BR_PORT"; }
BRQ=(psql -h "$PG_HOST" -p "$BR_PORT" -U postgres -d icegres -tA)
assert_eq "both endpoints serve the shared history (same count)" \
  "$(q 'select count(*) from demo.trips')" \
  "$("${BRQ[@]}" -c 'select count(*) from demo.trips')"

# m3: write to dev -> main unchanged (zero-copy isolation, direction 1).
br_base=$(( $(q 'select coalesce(max(trip_id), 0) from demo.trips') + 100 ))
(( br_base >= 900000 )) || br_base=970000
DEV_ID=$br_base
MAIN_ID=$((br_base + 1))
main_total_before=$(q 'select count(*) from demo.trips')
dev_ins=$("${BRQ[@]}" -c "insert into demo.trips (trip_id, city, distance_km, fare, ts)
  values ($DEV_ID, 'E2E DevBranch', 1.0, 10.0, TIMESTAMP '2026-07-06 00:00:00')" 2>&1 | tail -n 1)
assert_eq "INSERT on the dev endpoint" "INSERT 0 1" "$dev_ins"
assert_eq "dev endpoint sees its row (new connection)" 1 \
  "$("${BRQ[@]}" -c "select count(*) from demo.trips where trip_id = $DEV_ID")"
assert_eq "main endpoint does NOT see the dev row" 0 \
  "$(q "select count(*) from demo.trips where trip_id = $DEV_ID")"
assert_eq "main total unchanged by the dev write" "$main_total_before" \
  "$(q 'select count(*) from demo.trips')"

# m4: write to main -> dev unchanged (direction 2).
"${PSQL[@]}" -q -c "insert into demo.trips (trip_id, city, distance_km, fare, ts)
  values ($MAIN_ID, 'E2E MainSide', 1.0, 20.0, TIMESTAMP '2026-07-06 00:00:00')" \
  || fail "INSERT on the main endpoint failed"
assert_eq "main endpoint sees its row" 1 \
  "$(q "select count(*) from demo.trips where trip_id = $MAIN_ID")"
assert_eq "dev endpoint does NOT see the main row" 0 \
  "$("${BRQ[@]}" -c "select count(*) from demo.trips where trip_id = $MAIN_ID")"

# m5: the full write engine works ON the branch (UPDATE + txn), still isolated.
dev_upd=$("${BRQ[@]}" -c "update demo.trips set fare = 99.5 where trip_id = $DEV_ID" 2>&1 | tail -n 1)
assert_eq "UPDATE on the dev endpoint" "UPDATE 1" "$dev_upd"
assert_eq "updated fare visible on dev" "99.5" \
  "$("${BRQ[@]}" -c "select fare from demo.trips where trip_id = $DEV_ID")"
txn_out=$(psql -h "$PG_HOST" -p "$BR_PORT" -U postgres -d icegres -v ON_ERROR_STOP=1 2>&1 <<EOF
BEGIN;
insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($((DEV_ID + 2)), 'E2E DevTxn', 1.0, 1.0, TIMESTAMP '2026-07-06 00:00:00');
COMMIT;
EOF
) || fail "transaction on the dev endpoint failed: $txn_out"
assert_eq "txn row committed to the branch, invisible on main" "1|0" \
  "$("${BRQ[@]}" -c "select count(*) from demo.trips where trip_id = $((DEV_ID + 2))")|$(q "select count(*) from demo.trips where trip_id = $((DEV_ID + 2))")"
assert_eq "seeded rows intact on BOTH endpoints" "280|280" \
  "$(q 'select count(*) from demo.trips where trip_id between 1 and 280')|$("${BRQ[@]}" -c 'select count(*) from demo.trips where trip_id between 1 and 280')"

# m6: heads have diverged; reading a table without the ref fails loudly.
branch_list=$("$BIN" branch list demo.trips 2>&1)
main_head2=$(awk -F'\t' '$1=="main"{print $2}' <<<"$branch_list")
dev_head2=$(awk -F'\t' -v b="$BR_NAME" '$1==b{print $2}' <<<"$branch_list")
[[ -n "$main_head2" && -n "$dev_head2" && "$main_head2" != "$dev_head2" ]] \
  || fail "branch heads did not diverge after writes: main=$main_head2 dev=$dev_head2"
pass "branch heads diverged (main=$main_head2, $BR_NAME=$dev_head2) with shared history below the fork"
no_ref_out=$("${BRQ[@]}" -c 'select count(*) from demo.cities' 2>&1) || true
echo "$no_ref_out" | grep -q "does not exist on this table" \
  || fail "reading a table without the branch ref did not fail loudly: $no_ref_out"
pass "table without the branch ref fails loudly (no silent fallback to main)"

# m7: drop the branch — ref-only removal; main untouched; 'main' is protected.
stop_pidfile_generic "$BR_PID_FILE"
if "$BIN" branch drop demo.trips main >/dev/null 2>&1; then
  fail "'branch drop main' was ACCEPTED — main must be protected"
fi
pass "dropping 'main' is refused"
"$BIN" branch drop demo.trips "$BR_NAME" >"$E2E_DIR/branch-drop.log" 2>&1 \
  || { cat "$E2E_DIR/branch-drop.log" >&2; fail "branch drop failed"; }
branch_list=$("$BIN" branch list demo.trips 2>&1)
if grep -q "^$BR_NAME	" <<<"$branch_list"; then
  fail "branch $BR_NAME still listed after drop: $branch_list"
fi
pass "branch drop removed the ref"
assert_eq "main state fully intact after the branch lifecycle (dev row|main row|seeded)" "0|1|280" \
  "$(q "select count(*) from demo.trips where trip_id = $DEV_ID")|$(q "select count(*) from demo.trips where trip_id = $MAIN_ID")|$(q 'select count(*) from demo.trips where trip_id between 1 and 280')"

# ---------------------------------------------------------------------------
# (n) icegresd control plane (SPEC D5/D7): wake-on-connect scale-to-zero,
#     branch-endpoint routing by pgwire database name, supervised computes.
#     icegresd listens on :$PXY_PORT; the main compute lives on :$PXY_MAIN
#     (spawned on demand, --idle-shutdown-secs 2), branch computes on
#     ephemeral localhost ports.
# ---------------------------------------------------------------------------
PXY_PORT=5444
PXY_MAIN=5445
DBIN="$ICEGRES_DIR/target/debug/icegresd"
PXY_LOG="$E2E_DIR/icegresd.log"
PXY_STATUS="$E2E_DIR/icegresd-status.json"
PXY_BRANCH=e2e_pxy
log "(n) icegresd control plane on :$PXY_PORT (main compute :$PXY_MAIN, idle 2s)"
[[ -x "$DBIN" ]] || fail "icegresd binary not found at $DBIN (cargo build builds both bins)"
stop_icegresd
for p in "$PXY_PORT" "$PXY_MAIN"; do
  if psql -h "$PG_HOST" -p "$p" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
    fail "something is already listening on :$p — stop it first"
  fi
done
"$BIN" branch drop demo.trips "$PXY_BRANCH" >/dev/null 2>&1 || true

: >"$PXY_LOG"
rm -f "$PXY_STATUS"
# --pool-size 0: n1-n6 test the BARE wake/splice/supervision path (a warm
# pool would hold sessions on the compute and mask the idle exits n2/n3
# assert on); the session pool gets its own section n7 below.
"$DBIN" serve --host "$PG_HOST" --port "$PXY_PORT" --main-port "$PXY_MAIN" \
  --icegres-bin "$BIN" --idle-shutdown-secs 2 --pool-size 0 --status-file "$PXY_STATUS" \
  >>"$PXY_LOG" 2>&1 &
echo $! >"$E2E_DIR/icegresd.pid"
pxy_up=0
for _ in $(seq 1 40); do
  if (exec 3<>"/dev/tcp/$PG_HOST/$PXY_PORT") 2>/dev/null; then exec 3>&- 3<&-; pxy_up=1; break; fi
  sleep 0.25
done
[[ "$pxy_up" == 1 ]] || { tail -n 20 "$PXY_LOG" >&2; fail "icegresd not listening on :$PXY_PORT"; }
pass "icegresd listening on :$PXY_PORT"
PXQ=(psql -h "$PG_HOST" -p "$PXY_PORT" -U postgres -d icegres -tA)

# helper: read a field of one compute from the status file
pxy_status() { # key jq-expr
  jq -r --arg k "$1" ".computes[] | select(.key == \$k) | $2" "$PXY_STATUS" 2>/dev/null
}

# n1: wake-on-connect from cold — the compute does not exist yet; the FIRST
#     client connection spawns it, waits for readiness, and splices.
assert_eq "first connection through icegresd wakes the compute and answers" 20 \
  "$("${PXQ[@]}" -c 'select count(*) from demo.cities')"
main_cpid=$(pxy_status main .pid)
[[ "$main_cpid" =~ ^[0-9]+$ ]] || fail "status file has no main compute pid: $(cat "$PXY_STATUS" 2>/dev/null)"
[[ "$(ps -o comm= -p "$main_cpid" 2>/dev/null)" == icegres ]] \
  || fail "status pid $main_cpid is not a live icegres process"
pass "status file reports the main compute (pid $main_cpid on :$(pxy_status main .port))"

# n2: scale-to-zero — with --idle-shutdown-secs 2 the compute exits on its
#     own; icegresd reaps it and marks the slot stopped.
compute_gone=0
for _ in $(seq 1 40); do
  if ! kill -0 "$main_cpid" 2>/dev/null && [[ "$(pxy_status main .state)" == "stopped" ]]; then
    compute_gone=1; break
  fi
  sleep 0.25
done
[[ "$compute_gone" == 1 ]] || fail "compute did not idle-exit (pid $main_cpid state=$(pxy_status main .state))"
if psql -h "$PG_HOST" -p "$PXY_MAIN" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  fail "compute port :$PXY_MAIN still answering after idle exit"
fi
pass "compute idle-exited (scale-to-zero): process gone, slot marked stopped"

# n3: wake-after-idle — the next connection through icegresd re-spawns the
#     compute transparently; measure the first-connection-after-idle latency.
t0=$(($(date +%s%N) / 1000000))
wake_out=$("${PXQ[@]}" -c 'select 1' 2>&1)
wake_after_idle_ms=$(( $(date +%s%N) / 1000000 - t0 ))
assert_eq "reconnect after idle auto-wakes the compute" 1 "$wake_out"
(( wake_after_idle_ms < 10000 )) || fail "wake-after-idle took ${wake_after_idle_ms}ms (>10s)"
pass "wake-after-idle latency: ${wake_after_idle_ms}ms (cold start + splice setup, incl. psql overhead)"

# n4: branch-endpoint routing — dbname 'icegres@<branch>' routes to a
#     per-branch compute spawned on demand with --branch <branch>.
"$BIN" branch create demo.trips "$PXY_BRANCH" >/dev/null 2>&1 \
  || fail "branch create $PXY_BRANCH failed"
PXB=(psql -h "$PG_HOST" -p "$PXY_PORT" -U postgres -d "icegres@$PXY_BRANCH" -tA)
pxb_base=$(( $(q 'select coalesce(max(trip_id), 0) from demo.trips') + 200 ))
(( pxb_base >= 900000 )) || pxb_base=980000
assert_eq "branch endpoint INSERT via icegresd" "INSERT 0 1" \
  "$(psql -h "$PG_HOST" -p "$PXY_PORT" -U postgres -d "icegres@$PXY_BRANCH" -c \
     "insert into demo.trips (trip_id, city, distance_km, fare, ts)
      values ($pxb_base, 'E2E ProxyBranch', 1.0, 5.0, TIMESTAMP '2026-07-06 00:00:00')" 2>&1 | tail -n 1)"
assert_eq "branch endpoint sees its row (new connection via icegresd)" 1 \
  "$("${PXB[@]}" -c "select count(*) from demo.trips where trip_id = $pxb_base")"
assert_eq "main endpoint (dbname icegres) does NOT see the branch row" 0 \
  "$("${PXQ[@]}" -c "select count(*) from demo.trips where trip_id = $pxb_base")"
br_state=$(pxy_status "branch:$PXY_BRANCH" .state)
br_port=$(pxy_status "branch:$PXY_BRANCH" .port)
[[ "$br_state" == "running" || "$br_state" == "stopped" ]] \
  || fail "branch compute missing from status: state='$br_state'"
pass "per-branch compute spawned on demand (branch:$PXY_BRANCH on ephemeral :$br_port, state $br_state)"

# n5: supervision — kill -9 the main compute while a session is open; the
#     supervisor must restart it (capped backoff) WITHOUT a new connection.
"${PXQ[@]}" -c 'select 1' >/dev/null 2>&1 || fail "pre-kill wake failed"
( sleep 30 | psql -h "$PG_HOST" -p "$PXY_PORT" -U postgres -d icegres >/dev/null 2>&1 ) &
HOLD_PID=$!
held=0
for _ in $(seq 1 40); do
  if [[ "$(pxy_status main .active_connections)" == 1 ]]; then held=1; break; fi
  sleep 0.1
done
[[ "$held" == 1 ]] || fail "held session never showed up in active_connections"
main_cpid=$(pxy_status main .pid)
restarts_before=$(pxy_status main .restarts)
kill -9 "$main_cpid" 2>/dev/null || fail "could not SIGKILL compute pid $main_cpid"
recovered=0
for _ in $(seq 1 100); do
  if [[ "$(pxy_status main .restarts)" -gt "$restarts_before" ]] \
      && [[ "$(pxy_status main .state)" == "running" ]]; then
    recovered=1; break
  fi
  sleep 0.1
done
kill "$HOLD_PID" 2>/dev/null || true
[[ "$recovered" == 1 ]] || { tail -n 20 "$PXY_LOG" >&2; fail "supervisor did not restart the killed compute (restarts=$(pxy_status main .restarts), state=$(pxy_status main .state))"; }
pass "kill -9 mid-session: supervisor restarted the compute (restarts $restarts_before -> $(pxy_status main .restarts))"
assert_eq "next connection after the crash answers" 20 \
  "$("${PXQ[@]}" -c 'select count(*) from demo.cities')"
grep -q "exited UNCLEANLY" "$PXY_LOG" || fail "unclean exit was not logged loudly"
pass "unclean exit logged loudly by the supervisor"

# n6: shutdown — SIGTERM to icegresd terminates its computes; ports free.
main_cpid=$(pxy_status main .pid)
stop_icegresd
sleep 0.5
if [[ "$main_cpid" =~ ^[0-9]+$ ]] && kill -0 "$main_cpid" 2>/dev/null; then
  fail "compute pid $main_cpid survived icegresd shutdown"
fi
if psql -h "$PG_HOST" -p "$PXY_MAIN" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  fail "compute port :$PXY_MAIN still answering after icegresd shutdown"
fi
pass "icegresd shutdown terminated its computes (no leftovers)"
"$BIN" branch drop demo.trips "$PXY_BRANCH" >/dev/null 2>&1 || true

# n7: SESSION POOLING — a fresh icegresd with a warm pool (--pool-size 4,
#     --pool-idle-secs 3). Contract under test:
#       * many short-lived sequential client connections (the API pattern)
#         are all served, most from WARM pooled conns (no compute-side
#         handshake);
#       * session state does NOT leak between clients — every warm conn
#         serves exactly ONE client session and dies with it (SET and an
#         abandoned transaction from one session are invisible to the next);
#       * overflow: identity-mismatched clients (different user) go DIRECT
#         and still work;
#       * scale-to-zero survives pooling: with no clients the pool
#         idle-drains, the compute idle-exits, and the next connection
#         re-wakes AND re-warms.
log "(n7) icegresd session pooling on :$PXY_PORT (pool 4, pool-idle 3s, compute idle 2s)"
: >"$PXY_LOG"
rm -f "$PXY_STATUS"
"$DBIN" serve --host "$PG_HOST" --port "$PXY_PORT" --main-port "$PXY_MAIN" \
  --icegres-bin "$BIN" --idle-shutdown-secs 2 --pool-size 4 --pool-idle-secs 3 \
  --status-file "$PXY_STATUS" >>"$PXY_LOG" 2>&1 &
echo $! >"$E2E_DIR/icegresd.pid"
pxy_up=0
for _ in $(seq 1 40); do
  if (exec 3<>"/dev/tcp/$PG_HOST/$PXY_PORT") 2>/dev/null; then exec 3>&- 3<&-; pxy_up=1; break; fi
  sleep 0.25
done
[[ "$pxy_up" == 1 ]] || { tail -n 20 "$PXY_LOG" >&2; fail "pooled icegresd not listening on :$PXY_PORT"; }

# n7a: the first connection wakes the compute (direct — pool still empty)
#      and triggers background warming to --pool-size.
assert_eq "first pooled-proxy connection wakes the compute and answers" 1 \
  "$("${PXQ[@]}" -c 'select 1')"
pool_warm=0
for _ in $(seq 1 40); do
  if [[ "$(pxy_status main .pool.warm)" == 4 ]]; then pool_warm=1; break; fi
  sleep 0.25
done
[[ "$pool_warm" == 1 ]] || { tail -n 20 "$PXY_LOG" >&2; fail "pool did not warm to 4 (warm=$(pxy_status main .pool.warm))"; }
pass "pool warmed to 4 spare backend conns after the wake"

# n7b: API pattern — 15 short-lived sequential client connections, all
#      served; the bulk must have come from warm pooled handouts.
pooled_before=$(pxy_status main .pool.pooled_sessions)
for i in $(seq 1 15); do
  r=$("${PXQ[@]}" -c 'select 1' 2>&1)
  [[ "$r" == 1 ]] || fail "pooled sequential connection $i failed: $r"
done
pooled_after=$(pxy_status main .pool.pooled_sessions)
(( pooled_after - pooled_before >= 10 )) \
  || fail "expected >=10 of 15 sequential sessions to be pooled handouts (got $((pooled_after - pooled_before)); status: $(cat "$PXY_STATUS"))"
pass "15 sequential short-lived connections served ($((pooled_after - pooled_before)) from the warm pool, rest direct overflow)"

# n7c: session isolation — SET in one client session is invisible to the
#      next (a warm conn serves exactly one client; no reuse).
assert_eq "SET applies inside its own pooled session" "5555ms" \
  "$("${PXQ[@]}" -c 'SET statement_timeout = 5555' -c 'SHOW statement_timeout' | tail -n 1)"
assert_eq "SET does NOT leak into the next pooled session" "0" \
  "$("${PXQ[@]}" -c 'SHOW statement_timeout')"

# n7d: session isolation — an abandoned transaction (BEGIN + INSERT, then
#      disconnect without COMMIT) is rolled back with its session and its
#      row is invisible to the next client.
N7_ID=956789
"${PXQ[@]}" -c 'BEGIN' -c "insert into demo.trips (trip_id, city, distance_km, fare, ts)
  values ($N7_ID, 'E2E PoolIso', 1.0, 5.0, TIMESTAMP '2026-07-06 00:00:00')" >/dev/null 2>&1 \
  || fail "BEGIN+INSERT in pooled session failed"
assert_eq "abandoned txn from the previous pooled session is invisible (implicit rollback)" 0 \
  "$("${PXQ[@]}" -c "select count(*) from demo.trips where trip_id = $N7_ID")"

# n7e: identity mismatch overflows to a direct connection and still works.
direct_before=$(pxy_status main .pool.direct_sessions)
assert_eq "client with a different user bypasses the pool and answers" 1 \
  "$(psql -h "$PG_HOST" -p "$PXY_PORT" -U pool_bypass_user -d icegres -tA -c 'select 1')"
direct_after=$(pxy_status main .pool.direct_sessions)
(( direct_after > direct_before )) \
  || fail "different-user session was not counted as direct ($direct_before -> $direct_after)"
pass "different-user client served via direct (non-pooled) connection"

# n7f: scale-to-zero with pooling — no clients for --pool-idle-secs drains
#      the warm pool, which frees the compute to idle-exit as usual.
drained=0
for _ in $(seq 1 60); do
  if [[ "$(pxy_status main .pool.warm)" == 0 && "$(pxy_status main .state)" == "stopped" ]]; then
    drained=1; break
  fi
  sleep 0.25
done
[[ "$drained" == 1 ]] \
  || fail "pool did not drain / compute did not idle-exit (warm=$(pxy_status main .pool.warm), state=$(pxy_status main .state))"
pass "pool idle-drained and the compute idle-exited (scale-to-zero preserved under pooling)"

# n7g: the next connection re-wakes the compute and re-warms the pool.
assert_eq "connection after drain re-wakes the compute" 1 "$("${PXQ[@]}" -c 'select 1')"
rewarmed=0
for _ in $(seq 1 40); do
  if [[ "$(pxy_status main .pool.warm)" == 4 ]]; then rewarmed=1; break; fi
  sleep 0.25
done
[[ "$rewarmed" == 1 ]] || fail "pool did not re-warm after the wake (warm=$(pxy_status main .pool.warm))"
pass "wake after drain re-warmed the pool"

stop_icegresd
sleep 0.5
if psql -h "$PG_HOST" -p "$PXY_MAIN" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  fail "compute port :$PXY_MAIN still answering after pooled icegresd shutdown"
fi
pass "pooled icegresd shutdown terminated its computes (no leftovers)"

# ---------------------------------------------------------------------------
# (o) real ORM/driver clients — SPEC A8 (bench/clients/a8_orm_probe.py)
# ---------------------------------------------------------------------------
# Runs the headless ORM compatibility probe (psycopg2 + pg8000 +
# SQLAlchemy 2.x + pandas: connect, inspect(), reflection of demo.trips,
# ORM filter/aggregate, pandas join, prepared-statement reuse,
# BEGIN/COMMIT/ROLLBACK) against the main server. The probe's writes use
# trip_id >= 930000 and clean up after themselves. Server-side (named)
# cursors are a documented XFAIL inside the probe, not a failure.
log "(o) ORM/driver compatibility probe (bench/clients/a8_orm_probe.py)"
if ! command -v python3 >/dev/null 2>&1 \
    || ! python3 -c 'import sqlalchemy, psycopg2, pg8000, pandas' 2>/dev/null; then
  log "    SKIPPED: python3 with sqlalchemy/psycopg2/pg8000/pandas not available" \
      "(pip install sqlalchemy psycopg2-binary pg8000 pandas)"
else
  A8_OUT=$(env ICEGRES_PROBE_HOST="$PG_HOST" ICEGRES_PROBE_PORT="$PG_PORT" \
      python3 "$REPO_DIR/bench/clients/a8_orm_probe.py" 2>&1) \
    || { echo "$A8_OUT" | tail -n 25 >&2; fail "A8 ORM/driver probe reported failures"; }
  echo "$A8_OUT" | sed 's/^/    /'
  echo "$A8_OUT" | grep -q '^A8 RESULT: .*fail=0' \
    || fail "A8 ORM/driver probe summary is not fail=0"
  pass "ORM/driver clients green ($(echo "$A8_OUT" | grep '^A8 RESULT:'))"
fi

# ---------------------------------------------------------------------------
# (p) ADBC first-class — SPEC A11 (bench/clients/a11_adbc_probe.py)
# ---------------------------------------------------------------------------
# Two lanes: (1) `icegres flight-serve` (Arrow Flight SQL, adbc_driver_
# flightsql): query/metadata/prepared+bind/DML counts/BULK INGEST (one
# Iceberg commit per stream, asserted via $snapshots) + basic-auth variants
# against a second flight server with --auth-file; (2) adbc_driver_
# postgresql against the main pgwire server (COPY ... TO STDOUT binary
# reads, params, get_objects, DML). The probe's writes use trip_id >=
# 940000 / demo.adbc_ingest and clean up; two documented XFAILs inside the
# probe (pg-lane COPY FROM ingest, in-transaction extended SELECT).
FLIGHT_PORT=50051
FLIGHT_SECURE_PORT=50052
FLIGHT_PID_FILE="$E2E_DIR/flight.pid"
FLIGHT_SECURE_PID_FILE="$E2E_DIR/flight-secure.pid"

flight_port_open() { # $1 = port
  python3 -c "import socket,sys; s=socket.socket(); s.settimeout(0.3);
sys.exit(0 if s.connect_ex(('127.0.0.1', $1))==0 else 1)" 2>/dev/null
}

log "(p) ADBC probe: flight-serve on :$FLIGHT_PORT (+auth on :$FLIGHT_SECURE_PORT), pgwire COPY lane on :$PG_PORT"
if ! command -v python3 >/dev/null 2>&1 \
    || ! python3 -c 'import adbc_driver_flightsql, adbc_driver_postgresql, pyarrow' 2>/dev/null; then
  log "    SKIPPED: python3 with ADBC drivers not available" \
      "(pip install adbc-driver-flightsql adbc-driver-postgresql pyarrow)"
else
  stop_pidfile_generic "$FLIGHT_PID_FILE"
  stop_pidfile_generic "$FLIGHT_SECURE_PID_FILE"
  if flight_port_open "$FLIGHT_PORT"; then
    fail "something is already listening on :$FLIGHT_PORT — stop it first (not started by this harness)"
  fi
  "$BIN" flight-serve --host 127.0.0.1 --port "$FLIGHT_PORT" \
    >"$E2E_DIR/flight.log" 2>&1 &
  echo $! >"$FLIGHT_PID_FILE"
  printf 'e2e_flight_user:e2e-flight-pw\n' >"$E2E_DIR/flight-auth.conf"
  "$BIN" flight-serve --host 127.0.0.1 --port "$FLIGHT_SECURE_PORT" \
    --auth-file "$E2E_DIR/flight-auth.conf" >"$E2E_DIR/flight-secure.log" 2>&1 &
  echo $! >"$FLIGHT_SECURE_PID_FILE"
  for _ in $(seq 1 60); do
    flight_port_open "$FLIGHT_PORT" && flight_port_open "$FLIGHT_SECURE_PORT" && break
    if ! kill -0 "$(cat "$FLIGHT_PID_FILE")" 2>/dev/null \
        || ! kill -0 "$(cat "$FLIGHT_SECURE_PID_FILE")" 2>/dev/null; then
      tail -n 20 "$E2E_DIR/flight.log" "$E2E_DIR/flight-secure.log" >&2
      fail "icegres flight-serve exited during startup"
    fi
    sleep 0.5
  done
  flight_port_open "$FLIGHT_PORT" || fail "flight-serve did not open :$FLIGHT_PORT in 30s"
  flight_port_open "$FLIGHT_SECURE_PORT" || fail "flight-serve (auth) did not open :$FLIGHT_SECURE_PORT in 30s"
  pass "flight-serve up on :$FLIGHT_PORT and :$FLIGHT_SECURE_PORT (basic auth)"

  A11_OUT=$(env ICEGRES_PROBE_FLIGHT_HOST=127.0.0.1 \
      ICEGRES_PROBE_FLIGHT_PORT="$FLIGHT_PORT" \
      ICEGRES_PROBE_FLIGHT_SECURE_PORT="$FLIGHT_SECURE_PORT" \
      ICEGRES_PROBE_FLIGHT_SECURE_USER=e2e_flight_user \
      ICEGRES_PROBE_FLIGHT_SECURE_PASSWORD=e2e-flight-pw \
      ICEGRES_PROBE_PG_HOST="$PG_HOST" ICEGRES_PROBE_PG_PORT="$PG_PORT" \
      python3 "$REPO_DIR/bench/clients/a11_adbc_probe.py" 2>&1) \
    || { echo "$A11_OUT" | tail -n 25 >&2; fail "A11 ADBC probe reported failures"; }
  echo "$A11_OUT" | sed 's/^/    /'
  echo "$A11_OUT" | grep -q '^A11 RESULT: .*fail=0' \
    || fail "A11 ADBC probe summary is not fail=0"
  echo "$A11_OUT" | grep -q '^PASS flight: basic auth handshake' \
    || fail "A11 basic-auth step did not run/pass (secure server env was set)"
  pass "ADBC first-class green ($(echo "$A11_OUT" | grep '^A11 RESULT:'))"

  # (p2) Flight-NATIVE authorization (SPEC A12 on the Flight lane / production-
  #      readiness blocker #1): the ReBAC policy that gates pgwire MUST gate the
  #      Flight endpoint too — otherwise the Flight port is a total authz bypass.
  #      Grant e2e_flight_user read on demo.trips ONLY; assert trips allowed but
  #      demo.cities SELECT and any write denied.
  FLIGHT_AUTHZ_PORT=50053
  printf 'grant e2e_flight_user read demo.trips\n' >"$E2E_DIR/flight-authz.conf"
  "$BIN" flight-serve --host 127.0.0.1 --port "$FLIGHT_AUTHZ_PORT" \
    --auth-file "$E2E_DIR/flight-auth.conf" --authz-file "$E2E_DIR/flight-authz.conf" \
    >"$E2E_DIR/flight-authz.log" 2>&1 &
  echo $! >"$E2E_DIR/flight-authz.pid"
  for _ in $(seq 1 60); do
    flight_port_open "$FLIGHT_AUTHZ_PORT" && break
    kill -0 "$(cat "$E2E_DIR/flight-authz.pid")" 2>/dev/null \
      || { tail -n 20 "$E2E_DIR/flight-authz.log" >&2; fail "flight-serve (authz) exited during startup"; }
    sleep 0.5
  done
  flight_port_open "$FLIGHT_AUTHZ_PORT" || fail "flight-serve (authz) did not open :$FLIGHT_AUTHZ_PORT in 30s"
  AZF_OUT=$(env FA_PORT="$FLIGHT_AUTHZ_PORT" python3 - <<'PYEOF' 2>&1
import os
from adbc_driver_flightsql import dbapi as fl
port = os.environ["FA_PORT"]
cn = fl.connect(f"grpc://127.0.0.1:{port}",
                db_kwargs={"username": "e2e_flight_user", "password": "e2e-flight-pw"})
cur = cn.cursor()
cur.execute("select count(*) from demo.trips")
assert cur.fetchone()[0] >= 0
print("OK granted read demo.trips")
denied = False
try:
    cur.execute("select count(*) from demo.cities"); cur.fetchone()
except Exception as e:
    denied = "permission denied" in str(e) or "cannot SELECT" in str(e)
assert denied, "ungranted demo.cities SELECT was NOT denied"
print("OK denied read demo.cities")
wdenied = False
try:
    cur.execute("insert into demo.cities (city,country,population) values ('z','z',1)")
except Exception as e:
    wdenied = "permission denied" in str(e) or "cannot write" in str(e)
assert wdenied, "ungranted demo.cities write was NOT denied"
print("OK denied write demo.cities")
cur.close(); cn.close()
print("FLIGHT_AUTHZ_OK")
PYEOF
) || { echo "$AZF_OUT" | tail -n 15 >&2; fail "Flight-native authorization probe failed"; }
  echo "$AZF_OUT" | grep -q FLIGHT_AUTHZ_OK \
    || { echo "$AZF_OUT" | tail -n 15 >&2; fail "Flight authz probe did not confirm all denials"; }
  pass "Flight-native authorization enforced (granted read allowed; ungranted table SELECT + write denied 42501)"
  stop_pidfile_generic "$E2E_DIR/flight-authz.pid"

  stop_pidfile_generic "$FLIGHT_PID_FILE"
  stop_pidfile_generic "$FLIGHT_SECURE_PID_FILE"
  pass "flight-serve servers stopped"
fi

# ---------------------------------------------------------------------------
# (q) JDBC client — SPEC A9 (bench/clients/a9_jdbc_probe.sh)
# ---------------------------------------------------------------------------
# Runs the pgjdbc compatibility probe (DriverManager connect with pgjdbc's
# startup parameters, DatabaseMetaData getTables/getColumns of demo,
# Statement + PreparedStatement with typed parameters, executeUpdate INSERT
# with a proper `INSERT 0 n` tag on the extended protocol, and a
# setAutoCommit(false) rollback/commit cycle) against the main server. The
# probe's writes use trip_id >= 940000 and clean up after themselves. Skips
# gracefully when no JDK is installed (exit 3 from the wrapper).
log "(q) JDBC client probe (bench/clients/a9_jdbc_probe.sh)"
if ! command -v java >/dev/null 2>&1 || ! command -v javac >/dev/null 2>&1; then
  log "    SKIPPED: java/javac not available (apt install openjdk-21-jdk-headless)"
else
  A9_OUT=$(env ICEGRES_PROBE_HOST="$PG_HOST" ICEGRES_PROBE_PORT="$PG_PORT" \
      bash "$REPO_DIR/bench/clients/a9_jdbc_probe.sh" 2>&1)
  A9_RC=$?
  if [[ $A9_RC -eq 3 ]]; then
    log "    SKIPPED: $(echo "$A9_OUT" | tail -n 1)"
  else
    echo "$A9_OUT" | sed 's/^/    /'
    [[ $A9_RC -eq 0 ]] || fail "A9 JDBC probe reported failures (exit $A9_RC)"
    echo "$A9_OUT" | grep -q '^A9 RESULT: .*fail=0' \
      || fail "A9 JDBC probe summary is not fail=0"
    pass "JDBC client green ($(echo "$A9_OUT" | grep '^A9 RESULT:'))"
  fi
fi

# ---------------------------------------------------------------------------
# (r) ODBC client — SPEC A10 (bench/clients/a10_odbc_probe.sh)
# ---------------------------------------------------------------------------
# Runs the psqlODBC (unixODBC) compatibility probe against the main server:
# connect (psqlODBC's version/type probes), SQLTables/SQLColumns metadata,
# qmark-parameterized query, INSERT/readback/DELETE with rowcount (autocommit),
# and a read inside an explicit transaction. Writes use trip_id >= 970000 and
# clean up after themselves. Skips gracefully when pyodbc / the psqlODBC driver
# is not installed (exit 3; apt install unixodbc odbc-postgresql + pip pyodbc,
# or run infra/scripts/odbc-setup.sh).
log "(r) ODBC client probe (bench/clients/a10_odbc_probe.sh)"
if ! command -v python3 >/dev/null 2>&1 || ! python3 -c 'import pyodbc' 2>/dev/null; then
  log "    SKIPPED: pyodbc not available (apt install unixodbc odbc-postgresql; pip install pyodbc)"
else
  A10_OUT=$(env ICEGRES_PROBE_HOST="$PG_HOST" ICEGRES_PROBE_PORT="$PG_PORT" \
      bash "$REPO_DIR/bench/clients/a10_odbc_probe.sh" 2>&1)
  A10_RC=$?
  if [[ $A10_RC -eq 3 ]]; then
    log "    SKIPPED: $(echo "$A10_OUT" | tail -n 1)"
  else
    echo "$A10_OUT" | sed 's/^/    /'
    [[ $A10_RC -eq 0 ]] || fail "A10 ODBC probe reported failures (exit $A10_RC)"
    echo "$A10_OUT" | grep -q '^A10 RESULT: .*fail=0' \
      || fail "A10 ODBC probe summary is not fail=0"
    pass "ODBC client green ($(echo "$A10_OUT" | grep '^A10 RESULT:'))"
  fi
fi

# ---------------------------------------------------------------------------
# (s) Authorization — SPEC A12 (bench/clients/authz_probe.sh)
# ---------------------------------------------------------------------------
# Runs the ReBAC enforcement probe (managed add-on): it starts its own icegres
# with --auth-file + --authz-file and verifies namespace-grant inheritance,
# table-scoped grants, warehouse ownership, roles, per-statement 42501 denial,
# JOIN-checks-every-table, and pg_catalog metadata staying free. Skips
# gracefully when psql is missing or the binary lacks the `managed` feature.
log "(s) authorization probe (bench/clients/authz_probe.sh)"
if ! command -v psql >/dev/null 2>&1; then
  log "    SKIPPED: psql not available"
else
  AZ_OUT=$(ICEGRES_BIN="$BIN" bash "$REPO_DIR/bench/clients/authz_probe.sh" 2>&1)
  AZ_RC=$?
  if [[ $AZ_RC -eq 3 ]]; then
    log "    SKIPPED: $(echo "$AZ_OUT" | tail -n 1)"
  else
    echo "$AZ_OUT" | sed 's/^/    /'
    [[ $AZ_RC -eq 0 ]] || fail "A12 authz probe reported failures (exit $AZ_RC)"
    echo "$AZ_OUT" | grep -q '^A12 RESULT: .*fail=0' \
      || fail "A12 authz probe summary is not fail=0"
    pass "authorization enforced ($(echo "$AZ_OUT" | grep '^A12 RESULT:'))"
  fi
fi

# ---------------------------------------------------------------------------
# (t) Snapshot expiry (SPEC lifecycle): `icegres maintain expire-snapshots`
# ---------------------------------------------------------------------------
# Every write to an Iceberg table adds a snapshot forever; expiry trims the
# metadata to the newest N + everything still reachable from a ref, without
# touching the current data. Uses the main permissive server on :$PG_PORT.
log "(t) snapshot expiry (maintain expire-snapshots)"
# count(*) on Iceberg metadata tables hits a DataFusion logical/physical schema
# mismatch (documented in parity probe C5); a *bare* projection over the
# metadata table trips the same mismatch in the pg row encoder. Selecting with
# an ORDER BY inserts a sort that re-establishes the schema, which is the shape
# the C5 probe uses — count snapshots that way.
count_snaps() {
  q "select snapshot_id, committed_at from demo.\"$1\$snapshots\" order by committed_at" \
    | grep -c '|'
}
q 'drop table if exists demo.e2e_expire' >/dev/null 2>&1 || true
q 'create table demo.e2e_expire (id bigint, v text)' >/dev/null
for i in 1 2 3 4 5; do
  q "insert into demo.e2e_expire (id, v) values ($i, 'r$i')" >/dev/null
done
snap_before=$(count_snaps e2e_expire)
assert_eq "five writes make five snapshots" "5" "$snap_before"
head_before=$(q 'select snapshot_id from demo."e2e_expire$snapshots" order by committed_at desc limit 1')
"$BIN" maintain expire-snapshots demo.e2e_expire --keep 2 >"$E2E_DIR/expire.log" 2>&1 \
  || { cat "$E2E_DIR/expire.log" >&2; fail "expire-snapshots failed"; }
grep -q 'expired 3 snapshot' "$E2E_DIR/expire.log" \
  || { cat "$E2E_DIR/expire.log" >&2; fail "expire-snapshots did not remove 3 snapshots"; }
snap_after=$(count_snaps e2e_expire)
assert_eq "expiry keeps exactly the newest two snapshots" "2" "$snap_after"
# A WHERE filter on the metadata table trips a separate DataFusion type-inference
# quirk, so check head survival by listing surviving ids and matching in the shell.
survivors=$(q 'select snapshot_id, committed_at from demo."e2e_expire$snapshots" order by committed_at' | cut -d'|' -f1)
if echo "$survivors" | grep -qx "$head_before"; then
  pass "current head survives expiry (never expired out from under a reader)"
else
  fail "current head $head_before missing after expiry; survivors: $(echo "$survivors" | tr '\n' ' ')"
fi
assert_eq "data is intact after expiry (metadata-only op)" "5" \
  "$(q 'select count(*) from demo.e2e_expire')"
# Idempotent: nothing older than the kept window remains to expire.
"$BIN" maintain expire-snapshots demo.e2e_expire --keep 2 >"$E2E_DIR/expire2.log" 2>&1 \
  || { cat "$E2E_DIR/expire2.log" >&2; fail "second expire-snapshots failed"; }
grep -q 'expired 0 snapshot' "$E2E_DIR/expire2.log" \
  || { cat "$E2E_DIR/expire2.log" >&2; fail "second expire should be a no-op"; }
pass "expire-snapshots trims to newest-N, keeps head, is idempotent"
q 'drop table demo.e2e_expire' >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# (u) Strict transaction mode (SPEC B4 hardening): on a catalog WITH the
# multi-table transaction endpoint (Lakekeeper), ICEGRES_TXN_STRICT is
# satisfied by atomicity — a multi-table COMMIT succeeds as ONE atomic
# request. Only when the catalog LACKS the endpoint (simulated with the
# test-only ICEGRES_TXN_DISABLE_ATOMIC knob) does strict mode refuse up
# front (0A000, nothing applied) instead of committing per-table.
# ---------------------------------------------------------------------------
log "(u) strict transaction mode (ICEGRES_TXN_STRICT) on :$STRICT_PORT"
strict_start() { # strict_start [EXTRA_ENV=...]
  stop_pidfile_generic "$E2E_DIR/serve-strict.pid"
  : >"$E2E_DIR/serve-strict.log"
  env "$@" ICEGRES_TXN_STRICT=true "$BIN" serve --host "$PG_HOST" --port "$STRICT_PORT" \
    >>"$E2E_DIR/serve-strict.log" 2>&1 &
  echo $! >"$E2E_DIR/serve-strict.pid"
  local ready=0
  for _ in $(seq 1 60); do
    if psql -h "$PG_HOST" -p "$STRICT_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
      ready=1; break
    fi
    sleep 0.5
  done
  [[ "$ready" == 1 ]] \
    || { tail -n 20 "$E2E_DIR/serve-strict.log" >&2; fail "strict server not ready on :$STRICT_PORT"; }
}
SQ=(psql -h "$PG_HOST" -p "$STRICT_PORT" -U postgres -d icegres)
strict_start
"${SQ[@]}" -tA -c 'drop table if exists demo.e2e_strict_a' >/dev/null 2>&1 || true
"${SQ[@]}" -tA -c 'drop table if exists demo.e2e_strict_b' >/dev/null 2>&1 || true
"${SQ[@]}" -tA -c 'create table demo.e2e_strict_a (id bigint)' >/dev/null
"${SQ[@]}" -tA -c 'create table demo.e2e_strict_b (id bigint)' >/dev/null
# u1: strict + supported catalog: the two-table COMMIT succeeds ATOMICALLY.
strict_out=$("${SQ[@]}" -v ON_ERROR_STOP=1 2>&1 <<'EOF'
BEGIN;
insert into demo.e2e_strict_a values (1);
insert into demo.e2e_strict_b values (2);
COMMIT;
EOF
) || { echo "$strict_out" >&2; fail "strict multi-table COMMIT failed on a catalog WITH transactions/commit"; }
echo "$strict_out" | grep -q "COMMIT" || fail "strict COMMIT tag missing: $strict_out"
grep -q 'transaction committed atomically via transactions/commit' "$E2E_DIR/serve-strict.log" \
  || fail "strict multi-table COMMIT did not use the atomic endpoint"
assert_eq "strict mode + Lakekeeper: multi-table COMMIT applied atomically" "1|1" \
  "$("${SQ[@]}" -tA -c 'select (select count(*) from demo.e2e_strict_a) as a, (select count(*) from demo.e2e_strict_b) as b')"
# u2: catalog without the endpoint (simulated): strict refuses the COMMIT
# up front and applies nothing.
strict_start ICEGRES_TXN_DISABLE_ATOMIC=1
strict_out=$("${SQ[@]}" -v VERBOSITY=verbose 2>&1 <<'EOF'
BEGIN;
insert into demo.e2e_strict_a values (3);
insert into demo.e2e_strict_b values (4);
COMMIT;
EOF
) || true
echo "$strict_out" | grep -q '0A000' \
  || fail "strict multi-table COMMIT not refused with 0A000 without the endpoint: $strict_out"
pass "strict mode refuses multi-table COMMIT when the catalog lacks the endpoint (0A000)"
assert_eq "strict refusal applied nothing to either table (atomic rollback)" "1|1" \
  "$("${SQ[@]}" -tA -c 'select (select count(*) from demo.e2e_strict_a) as a, (select count(*) from demo.e2e_strict_b) as b')"
# u3: single-table transaction still commits normally under strict mode.
"${SQ[@]}" 2>&1 <<'EOF' >/dev/null
BEGIN;
insert into demo.e2e_strict_a values (9);
insert into demo.e2e_strict_a values (10);
COMMIT;
EOF
assert_eq "strict mode still commits single-table transactions" "3" \
  "$("${SQ[@]}" -tA -c 'select count(*) from demo.e2e_strict_a')"
"${SQ[@]}" -tA -c 'drop table demo.e2e_strict_a' >/dev/null 2>&1 || true
"${SQ[@]}" -tA -c 'drop table demo.e2e_strict_b' >/dev/null 2>&1 || true
stop_pidfile_generic "$E2E_DIR/serve-strict.pid"

# ---------------------------------------------------------------------------
# (v) Buffered-write durability contract: an acked-but-UNFLUSHED row is LOST on
# an UNCLEAN kill (the documented trade), but SURVIVES a CLEAN SIGTERM (the
# shutdown-flush hardening). A 10-minute cadence guarantees the background
# flusher never auto-commits within the test, so the only commit that can
# happen is the shutdown flush — proving the behavior end to end.
# ---------------------------------------------------------------------------
VBUF_PID_FILE="$E2E_DIR/serve-vbuf.pid"
VBUF_LOG="$E2E_DIR/serve-vbuf.log"
VBUF_MS=600000
VBQ=(psql -h "$PG_HOST" -p "$VBUF_PORT" -U postgres -d icegres -tA)
: >"$VBUF_LOG"
vbuf_start() {
  stop_pidfile_generic "$VBUF_PID_FILE"
  "$BIN" serve --host "$PG_HOST" --port "$VBUF_PORT" --write-buffer-ms "$VBUF_MS" >>"$VBUF_LOG" 2>&1 &
  echo $! >"$VBUF_PID_FILE"
  for _ in $(seq 1 60); do
    if "${VBQ[@]}" -c 'select 1' >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  tail -n 20 "$VBUF_LOG" >&2; fail "buffered durability server not ready on :$VBUF_PORT"
}
log "(v) buffered durability: kill-loss vs clean-shutdown-flush on :$VBUF_PORT"
if "${VBQ[@]}" -c 'select 1' >/dev/null 2>&1; then fail "something is already listening on :$VBUF_PORT"; fi
vbuf_start

vbase=$(( $(q 'select coalesce(max(trip_id), 0) from demo.trips') + 1 ))
(( vbase >= 990000 )) || vbase=990000
KL_ID=$vbase        # kill-loss sentinel (never committed -> lost)
CS_ID=$((vbase + 1)) # clean-shutdown sentinel (flushed on SIGTERM -> survives)

# --- kill-loss: an acked-but-unflushed row is lost on SIGKILL ---
"${VBQ[@]}" -c "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($KL_ID, 'E2E KillLoss', 1.0, 1.0, TIMESTAMP '2026-07-06 00:00:00')" >/dev/null \
  || fail "kill-loss INSERT failed"
assert_eq "unflushed acked row readable on the buffering server (union view)" "1" \
  "$("${VBQ[@]}" -c "select count(*) from demo.trips where trip_id = $KL_ID")"
assert_eq "unflushed acked row NOT yet committed (invisible to the sync server)" "0" \
  "$(q "select count(*) from demo.trips where trip_id = $KL_ID")"
vpid=$(cat "$VBUF_PID_FILE")
kill -9 "$vpid" 2>/dev/null || fail "could not SIGKILL buffered durability server"
for _ in $(seq 1 20); do kill -0 "$vpid" 2>/dev/null || break; sleep 0.25; done
kill -0 "$vpid" 2>/dev/null && fail "buffered durability server survived SIGKILL"
rm -f "$VBUF_PID_FILE"
vbuf_start
assert_eq "unflushed acked row LOST after the unclean kill (durability contract is real)" "0" \
  "$("${VBQ[@]}" -c "select count(*) from demo.trips where trip_id = $KL_ID")"
pass "kill-loss: an acked-but-unflushed write is genuinely lost on SIGKILL"

# --- clean-shutdown-flush: an acked-but-unflushed row survives a graceful stop ---
"${VBQ[@]}" -c "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($CS_ID, 'E2E CleanFlush', 1.0, 1.0, TIMESTAMP '2026-07-06 00:00:00')" >/dev/null \
  || fail "clean-flush INSERT failed"
assert_eq "clean-flush row is unflushed pre-SIGTERM (invisible to the sync server)" "0" \
  "$(q "select count(*) from demo.trips where trip_id = $CS_ID")"
vpid=$(cat "$VBUF_PID_FILE")
kill -TERM "$vpid" 2>/dev/null || fail "could not SIGTERM buffered durability server"
for _ in $(seq 1 120); do kill -0 "$vpid" 2>/dev/null || break; sleep 0.25; done
if kill -0 "$vpid" 2>/dev/null; then kill -9 "$vpid" 2>/dev/null; fail "server did not exit within 30s of SIGTERM"; fi
rm -f "$VBUF_PID_FILE"
grep -q "flushing write buffer before clean shutdown" "$VBUF_LOG" \
  || fail "clean shutdown did not attempt a buffer flush (log: $VBUF_LOG)"
grep -q "write buffer flushed on shutdown; no acked rows lost" "$VBUF_LOG" \
  || fail "shutdown flush did not report success (log: $VBUF_LOG)"
pass "clean SIGTERM flushed the buffer before exit (log confirms)"
assert_eq "clean-flush row COMMITTED to the lake by the shutdown flush (sync server sees it)" "1" \
  "$(q "select count(*) from demo.trips where trip_id = $CS_ID")"
vbuf_start
assert_eq "clean-flush row survives the restart (durably in Iceberg)" "1" \
  "$("${VBQ[@]}" -c "select count(*) from demo.trips where trip_id = $CS_ID")"
pass "clean-shutdown-flush: an acked-but-unflushed write survives a graceful stop"
stop_pidfile_generic "$VBUF_PID_FILE"

# ---------------------------------------------------------------------------
# (w) Observability + security hardening: per-query duration metrics +
# slow-query WARN correlated to a per-connection span (audit #9/#11), per-peer
# failed-auth backoff (#4), and Flight SQL in-process TLS (#13).
# ---------------------------------------------------------------------------
log "(w) observability + hardening (#9/#11/#4/#13)"

# --- w1: query metrics + slow-query WARN + correlation span ---
OBS_PORT=5454
OBS_HEALTH=8091
OBS_PID="$E2E_DIR/serve-obs.pid"
OBS_LOG="$E2E_DIR/serve-obs.log"
stop_pidfile_generic "$OBS_PID"
: >"$OBS_LOG"
# ICEGRES_SLOW_QUERY_MS=1 makes any query "slow" (deterministic WARN); JSON logs
# so the correlation span is assertable without ANSI escapes.
ICEGRES_LOG_FORMAT=json ICEGRES_SLOW_QUERY_MS=1 \
  "$BIN" serve --host "$PG_HOST" --port "$OBS_PORT" --health-port "$OBS_HEALTH" >>"$OBS_LOG" 2>&1 &
echo $! >"$OBS_PID"
obs_ready=0
for _ in $(seq 1 60); do
  if psql -h "$PG_HOST" -p "$OBS_PORT" -U postgres -d icegres -tAc 'select 1' >/dev/null 2>&1; then obs_ready=1; break; fi
  sleep 0.5
done
[[ "$obs_ready" == 1 ]] || { tail -n 20 "$OBS_LOG" >&2; fail "observability server not ready on :$OBS_PORT"; }
psql -h "$PG_HOST" -p "$OBS_PORT" -U postgres -d icegres -tAc 'select count(*) from demo.trips' >/dev/null
if command -v curl >/dev/null 2>&1; then
  OBS_M=$(curl -s "http://$PG_HOST:$OBS_HEALTH/metrics")
  echo "$OBS_M" | grep -q '^icegres_queries_in_flight ' || fail "queries_in_flight metric missing"
  echo "$OBS_M" | grep -q '^icegres_query_duration_ms_total ' || fail "query_duration_ms_total metric missing"
  obs_slow=$(echo "$OBS_M" | awk '/^icegres_queries_slow_total /{print $2}')
  [[ -n "$obs_slow" && "$obs_slow" -ge 1 ]] || fail "queries_slow_total not incremented (got ${obs_slow:-none})"
  pass "new query metrics exposed (in_flight/slow_total=$obs_slow/duration)"
else
  log "    SKIPPED /metrics assertions: curl not available"
fi
# Correlation: the slow-query WARN line carries the per-connection span
# (name=conn, id, peer) so concurrent-connection logs de-multiplex.
slow_line=$(grep '"message":"slow query"' "$OBS_LOG" | head -1)
[[ -n "$slow_line" ]] || fail "no slow-query WARN emitted"
echo "$slow_line" | grep -q '"name":"conn"' || fail "slow-query WARN not inside a conn span: $slow_line"
echo "$slow_line" | grep -q '"peer"' || fail "conn span missing peer: $slow_line"
pass "query timing WARNs correlated to a per-connection span (conn id + peer)"
stop_pidfile_generic "$OBS_PID"

# --- w2: per-peer failed-auth backoff ---
THR_PORT=5455
THR_PID="$E2E_DIR/serve-thr.pid"
THR_LOG="$E2E_DIR/serve-thr.log"
THR_AUTH="$E2E_DIR/thr-auth.conf"
printf 'thruser:right-pw\n' >"$THR_AUTH"; chmod 600 "$THR_AUTH"
stop_pidfile_generic "$THR_PID"
: >"$THR_LOG"
"$BIN" serve --host "$PG_HOST" --port "$THR_PORT" --auth-file "$THR_AUTH" >>"$THR_LOG" 2>&1 &
echo $! >"$THR_PID"
thr_ready=0
for _ in $(seq 1 60); do
  if PGPASSWORD=right-pw psql "host=$PG_HOST port=$THR_PORT user=thruser dbname=icegres" -tAc 'select 1' >/dev/null 2>&1; then thr_ready=1; break; fi
  sleep 0.5
done
[[ "$thr_ready" == 1 ]] || { tail -n 20 "$THR_LOG" >&2; fail "throttle server not ready on :$THR_PORT"; }
# First wrong attempt is ~baseline (no prior failures); after a couple more the
# escalating backoff makes a later attempt visibly slower.
# These are expected to FAIL (wrong password) — guard against `set -e`.
t0=$(date +%s%N)
PGPASSWORD=nope psql "host=$PG_HOST port=$THR_PORT user=thruser dbname=icegres connect_timeout=30" -tAc 'select 1' >/dev/null 2>&1 || true
first_ms=$(( ($(date +%s%N) - t0) / 1000000 ))
for _ in 1 2; do PGPASSWORD=nope psql "host=$PG_HOST port=$THR_PORT user=thruser dbname=icegres connect_timeout=30" -tAc 'select 1' >/dev/null 2>&1 || true; done
t0=$(date +%s%N)
PGPASSWORD=nope psql "host=$PG_HOST port=$THR_PORT user=thruser dbname=icegres connect_timeout=30" -tAc 'select 1' >/dev/null 2>&1 || true
later_ms=$(( ($(date +%s%N) - t0) / 1000000 ))
grep -q 'throttling this peer' "$THR_LOG" || fail "failed-auth throttle did not fire (log: $THR_LOG)"
(( later_ms > first_ms + 100 )) || fail "no backoff escalation: first=${first_ms}ms later=${later_ms}ms"
pass "per-peer failed-auth backoff escalates (first=${first_ms}ms -> later=${later_ms}ms)"
assert_eq "correct password still authenticates despite the throttle" "1" \
  "$(PGPASSWORD=right-pw psql "host=$PG_HOST port=$THR_PORT user=thruser dbname=icegres connect_timeout=30" -tAc 'select 1')"
stop_pidfile_generic "$THR_PID"

# --- w3: Flight SQL in-process TLS ---
FTLS_PORT=50056
FTLS_PID="$E2E_DIR/flight-tls.pid"
FTLS_LOG="$E2E_DIR/flight-tls.log"
bash "$REPO_DIR/infra/scripts/gen-dev-cert.sh" >/dev/null 2>&1 || true
FCRT="$REPO_DIR/infra/.data/tls/dev.crt"
FKEY="$REPO_DIR/infra/.data/tls/dev.key"
if ! command -v python3 >/dev/null 2>&1 || ! python3 -c 'import adbc_driver_flightsql' >/dev/null 2>&1; then
  log "    SKIPPED w3 Flight TLS: python3/adbc_driver_flightsql not available"
elif [[ ! -f "$FCRT" || ! -f "$FKEY" ]]; then
  log "    SKIPPED w3 Flight TLS: dev cert not available ($FCRT)"
else
  stop_pidfile_generic "$FTLS_PID"
  : >"$FTLS_LOG"
  "$BIN" flight-serve --host "$PG_HOST" --port "$FTLS_PORT" --tls-cert "$FCRT" --tls-key "$FKEY" >>"$FTLS_LOG" 2>&1 &
  echo $! >"$FTLS_PID"
  ftls_ready=0
  for _ in $(seq 1 60); do grep -q 'flight-serve ready' "$FTLS_LOG" && { ftls_ready=1; break; }; sleep 0.5; done
  [[ "$ftls_ready" == 1 ]] || { tail -n 20 "$FTLS_LOG" >&2; fail "Flight TLS server not ready on :$FTLS_PORT"; }
  FT_OUT=$(FT_PORT="$FTLS_PORT" python3 - <<'PY' 2>&1
import os
import adbc_driver_flightsql.dbapi as f
from adbc_driver_flightsql import DatabaseOptions
p = os.environ["FT_PORT"]
tls_ok = False
plain_rejected = False
try:
    c = f.connect(f"grpc+tls://localhost:{p}", db_kwargs={DatabaseOptions.TLS_SKIP_VERIFY.value: "true"})
    cur = c.cursor(); cur.execute("select count(*) from demo.trips"); cur.fetchone()
    tls_ok = True; cur.close(); c.close()
except Exception as e:
    print("TLS-FAIL", type(e).__name__, str(e)[:140])
try:
    c = f.connect(f"grpc://localhost:{p}"); c.cursor().execute("select 1")
except Exception:
    plain_rejected = True
print(f"RESULT tls_ok={tls_ok} plain_rejected={plain_rejected}")
PY
)
  echo "$FT_OUT" | sed 's/^/    /'
  echo "$FT_OUT" | grep -q 'tls_ok=True' || fail "ADBC over grpc+tls failed"
  echo "$FT_OUT" | grep -q 'plain_rejected=True' || fail "plaintext client not rejected on the TLS Flight port"
  pass "Flight in-process TLS: ADBC grpc+tls query works, plaintext client rejected"
  stop_pidfile_generic "$FTLS_PID"
fi

# ---------------------------------------------------------------------------
# (x) Keyed tail upserts (roadmap Phase 2, docs/sota-roadmap.md §4): on a
# table with icegres.tail-upsert=true + icegres.primary-key, an exact-PK
# UPDATE acks from the durable tail instead of a synchronous COW commit.
# Proven here: 20 sequential UPDATEs to ONE hot row ack fast, produce ZERO
# intermediate snapshots, and net exactly ONE composed commit at the flush;
# a mid-window SELECT sees the newest value through the union read; time
# travel to the pre-update snapshot still shows the old value (never
# overlaid). Port 5457 (5456 belongs to tail_durability.sh).
# ---------------------------------------------------------------------------
KY_PORT=5457
KY_PID="$E2E_DIR/serve-keyed.pid"
KY_LOG="$E2E_DIR/serve-keyed.log"
KY_TAIL="$E2E_DIR/keyed-tail-wal"
KY_MS=600000 # only fences flush: every commit below is one the test forced
KYQ=(psql -h "$PG_HOST" -p "$KY_PORT" -U postgres -d icegres -v ON_ERROR_STOP=1 -tA)
ky_snap_count() {
  curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/e2e_keyed"     | jq '[.metadata.snapshots[]?] | length'
}
# The fence: any non-keyed DML forces a synchronous flush first (this one
# then matches nothing and commits nothing itself).
ky_flush() { "${KYQ[@]}" -c 'delete from demo.e2e_keyed where id < -1' >/dev/null; }
log "(x) keyed tail upserts (Phase 2) on :$KY_PORT"
stop_pidfile_generic "$KY_PID"
if "${KYQ[@]}" -c 'select 1' >/dev/null 2>&1; then fail "something is already listening on :$KY_PORT"; fi
rm -rf "$KY_TAIL"
: >"$KY_LOG"
curl -sf -X DELETE "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/e2e_keyed?purgeRequested=true" >/dev/null 2>&1 || true
curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces/demo/tables" \
  -H 'Content-Type: application/json' -d @- <<'JSON' >/dev/null
{
  "name": "e2e_keyed",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "id", "required": false, "type": "long"},
      {"id": 2, "name": "val", "required": false, "type": "string"}
    ]
  },
  "properties": {"icegres.primary-key": "id", "icegres.tail-upsert": "true"}
}
JSON
"$BIN" serve --host "$PG_HOST" --port "$KY_PORT" --write-buffer-ms "$KY_MS" \
  --tail-dir "$KY_TAIL" >>"$KY_LOG" 2>&1 &
echo $! >"$KY_PID"
ky_ready=0
for _ in $(seq 1 60); do
  if "${KYQ[@]}" -c 'select 1' >/dev/null 2>&1; then ky_ready=1; break; fi
  sleep 0.5
done
[[ "$ky_ready" == 1 ]] || { tail -n 20 "$KY_LOG" >&2; fail "keyed server not ready on :$KY_PORT"; }

# Seed one committed row, note the pre-update snapshot for time travel.
"${KYQ[@]}" -c "insert into demo.e2e_keyed values (1, 'before')" >/dev/null
ky_flush
assert_eq "seed flush produced the first snapshot" "1" "$(ky_snap_count)"
KY_SNAP1=$(curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/e2e_keyed" \
  | jq -r '.metadata."current-snapshot-id"')

# 20 sequential hot-row UPDATEs: each acks UPDATE 1 without a commit.
ky_t0=$(date +%s%N)
for i in $(seq 1 20); do
  ky_tag=$(psql -h "$PG_HOST" -p "$KY_PORT" -U postgres -d icegres -c \
    "update demo.e2e_keyed set val = 'v$i' where id = 1" | tr -d '[:space:]')
  [[ "$ky_tag" == "UPDATE1" ]] || fail "keyed UPDATE $i answered [$ky_tag], expected UPDATE 1"
done
ky_ms=$(( ($(date +%s%N) - ky_t0) / 1000000 ))
pass "20 sequential keyed UPDATEs acked (total ${ky_ms} ms ≈ $((ky_ms / 20)) ms/stmt incl. psql startup)"
assert_eq "mid-window SELECT sees the NEWEST value (union read)" "v20" \
  "$("${KYQ[@]}" -c 'select val from demo.e2e_keyed where id = 1')"
assert_eq "no per-statement snapshots: still only the seed commit" "1" "$(ky_snap_count)"

# One flush -> ONE coalesced commit carrying the final value.
ky_flush
assert_eq "one flush window = ONE snapshot for 20 updates" "2" "$(ky_snap_count)"
assert_eq "post-flush value is the last write" "v20" \
  "$("${KYQ[@]}" -c 'select val from demo.e2e_keyed where id = 1')"
assert_eq "exactly one row for the key (no duplicates)" "1" \
  "$("${KYQ[@]}" -c 'select count(*) from demo.e2e_keyed')"

# (L2) Ack order is the total order for a key: a keyed DELETE followed by a
# plain INSERT of the SAME key in the SAME window leaves the row PRESENT
# with the inserted values — the later insert becomes the key's newest
# version instead of being folded away by the coalesced delete.
ky_tag=$(psql -h "$PG_HOST" -p "$KY_PORT" -U postgres -d icegres -c \
  "delete from demo.e2e_keyed where id = 1" | tr -d '[:space:]')
[[ "$ky_tag" == "DELETE1" ]] || fail "keyed DELETE answered [$ky_tag], expected DELETE 1"
assert_eq "keyed DELETE hides the row mid-window (union read)" "0" \
  "$("${KYQ[@]}" -c 'select count(*) from demo.e2e_keyed where id = 1')"
"${KYQ[@]}" -c "insert into demo.e2e_keyed values (1, 'reborn')" >/dev/null
assert_eq "same-window re-INSERT after the keyed DELETE is visible (union)" "reborn|1" \
  "$("${KYQ[@]}" -c 'select val from demo.e2e_keyed where id = 1')|$("${KYQ[@]}" \
    -c 'select count(*) from demo.e2e_keyed where id = 1')"
assert_eq "delete-then-reinsert made no mid-window snapshots" "2" "$(ky_snap_count)"
ky_flush
assert_eq "flush committed the delete-then-reinsert as ONE snapshot" "3" "$(ky_snap_count)"
assert_eq "committed row survives the same-window delete-then-reinsert, exactly once" "reborn|1" \
  "$("${KYQ[@]}" -c 'select val from demo.e2e_keyed where id = 1')|$("${KYQ[@]}" \
    -c 'select count(*) from demo.e2e_keyed where id = 1')"

# Time travel predates the updates and never sees the buffer.
assert_eq "time travel to the pre-update snapshot shows the OLD value" "before" \
  "$("${KYQ[@]}" -c "select val from demo.\"e2e_keyed@$KY_SNAP1\" where id = 1")"

stop_pidfile_generic "$KY_PID"
rm -rf "$KY_TAIL"
curl -sf -X DELETE "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/e2e_keyed?purgeRequested=true" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# (y) Orphan-file GC (roadmap Phase 4, docs/sota-roadmap.md §6):
# `icegres maintain remove-orphans` reclaims the files snapshot expiry
# strands. Recipe: 4 one-row INSERTs make 4 small data files; TWO full-table
# COW UPDATEs then strand them — the first rewrites all four into one file,
# the second rewrites again AND drops the first UPDATE's DELETED manifest
# entries (spec: DELETED entries live only in the snapshot that deleted
# them), so after `expire-snapshots --keep 1` the four insert-era Parquet
# files (and the expired snapshots' manifests/manifest lists) are referenced
# by NOTHING. Live after expiry: the newest rewrite + the previous rewrite
# (still named by a DELETED entry in the retained snapshot's manifest) = 2
# Parquet files, plus the retained manifest list/manifests and every
# metadata JSON in the metadata log. Proven here: dry run reports the
# orphans and deletes NOTHING; --execute with a 0-hour grace window is
# REFUSED (fail closed) until --unsafe-grace asserts the table is
# quiescent; --execute --unsafe-grace deletes exactly the reported set
# (verified via aws CLI object counts) while the table stays queryable with
# correct rows; a rerun reports zero. Uses --older-than-hours 0 with
# --unsafe-grace on every step (the table is quiescent, and the flag also
# drops the 15-minute clock-skew allowance that would otherwise hide the
# seconds-old orphans); production keeps the default 72h grace window and
# never passes --unsafe-grace. The --execute step also exercises the
# clock-skew probe (write/stat/delete under metadata/), which must pass
# against the local store.
# ---------------------------------------------------------------------------
log "(y) orphan-file GC (maintain remove-orphans)"
q 'drop table if exists demo.e2e_orphan' >/dev/null 2>&1 || true
q 'create table demo.e2e_orphan (id bigint, v text)' >/dev/null
for i in 1 2 3 4; do
  q "insert into demo.e2e_orphan (id, v) values ($i, 'r$i')" >/dev/null
done
q "update demo.e2e_orphan set v = 'u1' where id >= 1" >/dev/null
q "update demo.e2e_orphan set v = 'u2' where id >= 1" >/dev/null
orphan_loc=$(curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/e2e_orphan" | jq -r '.metadata.location')
[[ "$orphan_loc" == s3://lakehouse/* ]] || fail "unexpected table location for demo.e2e_orphan: $orphan_loc"
orphan_key=${orphan_loc#s3://lakehouse/}
count_orphan_parquet() {
  aws --endpoint-url "$S3_ENDPOINT" s3 ls --recursive "s3://lakehouse/$orphan_key/data/" \
    | grep -c '\.parquet$' || true
}
count_orphan_objects() {
  aws --endpoint-url "$S3_ENDPOINT" s3 ls --recursive "s3://lakehouse/$orphan_key/" \
    | grep -c . || true
}
"$BIN" maintain expire-snapshots demo.e2e_orphan --keep 1 >"$E2E_DIR/orphan-expire.log" 2>&1 \
  || { cat "$E2E_DIR/orphan-expire.log" >&2; fail "expire-snapshots before GC failed"; }
grep -q 'expired 5 snapshot' "$E2E_DIR/orphan-expire.log" \
  || { cat "$E2E_DIR/orphan-expire.log" >&2; fail "expected 5 expired snapshots (4 inserts + 1 update)"; }
assert_eq "stranded data files present before GC (4 inserts + 2 rewrites)" "6" "$(count_orphan_parquet)"
orphan_obj_before=$(count_orphan_objects)

# --- dry run: reports the orphans, deletes NOTHING (--unsafe-grace only
# --- drops the skew allowance so the seconds-old orphans are visible;
# --- dry runs never need it to be *allowed*) ---
"$BIN" maintain remove-orphans demo.e2e_orphan --older-than-hours 0 --unsafe-grace >"$E2E_DIR/orphan-dry.log" 2>&1 \
  || { cat "$E2E_DIR/orphan-dry.log" >&2; fail "remove-orphans dry run failed"; }
grep -q 'DRY RUN — nothing deleted' "$E2E_DIR/orphan-dry.log" \
  || { cat "$E2E_DIR/orphan-dry.log" >&2; fail "dry run did not announce itself"; }
orphan_n=$(sed -n 's/^found \([0-9]\{1,\}\) orphan file(s) totaling.*/\1/p' "$E2E_DIR/orphan-dry.log")
[[ -n "$orphan_n" && "$orphan_n" -ge 4 ]] \
  || { cat "$E2E_DIR/orphan-dry.log" >&2; fail "dry run found [$orphan_n] orphans, expected >= 4 (the insert-era Parquet files)"; }
orphan_dry_parquet=$(grep -c '^  s3://lakehouse/.*/data/.*\.parquet$' "$E2E_DIR/orphan-dry.log" || true)
assert_eq "dry run names exactly the 4 insert-era Parquet files as orphans" "4" "$orphan_dry_parquet"
assert_eq "dry run deleted NOTHING (object count unchanged)" "$orphan_obj_before" "$(count_orphan_objects)"
assert_eq "dry run left every data file in place" "6" "$(count_orphan_parquet)"
pass "dry run reports $orphan_n orphan(s) and deletes nothing"

# --- refusal (fail closed): --execute with a sub-1h grace window must be
# --- refused WITHOUT --unsafe-grace, deleting nothing ---
if "$BIN" maintain remove-orphans demo.e2e_orphan --older-than-hours 0 --execute >"$E2E_DIR/orphan-refuse.log" 2>&1; then
  cat "$E2E_DIR/orphan-refuse.log" >&2
  fail "--execute with --older-than-hours 0 must be refused without --unsafe-grace"
fi
grep -q 'refusing --execute' "$E2E_DIR/orphan-refuse.log" \
  || { cat "$E2E_DIR/orphan-refuse.log" >&2; fail "refusal did not name the grace-window guard"; }
assert_eq "refused execute deleted NOTHING (object count unchanged)" \
  "$orphan_obj_before" "$(count_orphan_objects)"

# --- execute (+--unsafe-grace: quiescent table): deletes exactly the
# --- reported set; live files + rows intact; clock-skew probe passes ---
"$BIN" maintain remove-orphans demo.e2e_orphan --older-than-hours 0 --execute --unsafe-grace >"$E2E_DIR/orphan-exec.log" 2>&1 \
  || { cat "$E2E_DIR/orphan-exec.log" >&2; fail "remove-orphans --execute failed"; }
grep -q "deleted $orphan_n orphan file(s) totaling" "$E2E_DIR/orphan-exec.log" \
  || { cat "$E2E_DIR/orphan-exec.log" >&2; fail "--execute did not delete the $orphan_n orphans the dry run found"; }
assert_eq "execute removed exactly the orphan set from the object store" \
  "$((orphan_obj_before - orphan_n))" "$(count_orphan_objects)"
assert_eq "live data files survive the GC (newest rewrite + its DELETED-entry predecessor)" "2" \
  "$(count_orphan_parquet)"
assert_eq "table fully readable after GC (rows + last update intact)" "4|u2|u2" \
  "$(q 'select count(*), min(v), max(v) from demo.e2e_orphan')"

# --- rerun: idempotent, zero orphans left ---
"$BIN" maintain remove-orphans demo.e2e_orphan --older-than-hours 0 --unsafe-grace >"$E2E_DIR/orphan-rerun.log" 2>&1 \
  || { cat "$E2E_DIR/orphan-rerun.log" >&2; fail "remove-orphans rerun failed"; }
grep -q 'found 0 orphan file(s)' "$E2E_DIR/orphan-rerun.log" \
  || { cat "$E2E_DIR/orphan-rerun.log" >&2; fail "rerun should find zero orphans"; }
pass "orphan GC: dry-run/execute/rerun contract holds ($orphan_n orphans reclaimed)"
q 'drop table demo.e2e_orphan' >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
log "all assertions passed ($PASS_COUNT)"
