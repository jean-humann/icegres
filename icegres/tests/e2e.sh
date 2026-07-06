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
"$DBIN" serve --host "$PG_HOST" --port "$PXY_PORT" --main-port "$PXY_MAIN" \
  --icegres-bin "$BIN" --idle-shutdown-secs 2 --status-file "$PXY_STATUS" \
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
log "all assertions passed ($PASS_COUNT)"
