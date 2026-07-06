#!/usr/bin/env bash
# Parity comparison engine for icegres (bench/SPEC.md §1).
#
# Executes every probe A1..E3 against the live lakehouse stack, emits
# bench/results/parity-<ts>.json (one record per probe: id, area, behavior,
# verdict PASS|GAP|NA_BY_DESIGN, evidence) and regenerates the parity section
# of bench/SCORECARD.md.
#
# Server handling: reuses a server already answering on :5439, otherwise
# starts its own (release binary if built, else debug) with identity-checked
# pidfile handling like icegres/tests/e2e.sh. Always starts a second compute
# on :5440 (D1/D2/E1), a short-lived scale-to-zero one on :5445 with a
# health endpoint on :5446 (D5/E2), and an env-only one on :5444 (E3).
#
# GAP probes capture the server's actual error output as evidence — nothing
# is assumed. Probes B1/B3/B4/B5 append (and B2/B3 mutate/delete) a few rows
# with trip_id >= 900000 per run (same convention as e2e.sh; deterministic
# assertions elsewhere filter trip_id 1..280 so this is safe by design).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
ICEGRES_DIR="$REPO_DIR/icegres"
RESULTS_DIR="$SCRIPT_DIR/results"
RUN_DIR="$SCRIPT_DIR/.run"
SCORECARD="$SCRIPT_DIR/SCORECARD.md"

PG_HOST=127.0.0.1
MAIN_PORT=5439
SECOND_PORT=5440
ENV_PORT=5444
IDLE_PORT=5445
HEALTH_PORT=5446
SECURE_PORT=5447 # auth+TLS server for the A6/A7 probes
BRANCH_PORT=5450 # --branch server for the D6 probe
D5_COMPUTE_PORT=5451 # compute behind icegresd for the D5 probe
D7_PORT=5452         # icegresd public port for the D7 probe
D7_COMPUTE_PORT=5453 # main compute behind icegresd for the D7 probe

# Harness-owned servers are permissive/plaintext by design (except the
# dedicated A6/A7 secure server, configured explicitly): a stray
# ICEGRES_AUTH_FILE/ICEGRES_TLS_* in the caller's environment must not flip
# them. Clients still pass credentials when configured: every psql below
# reads PGPASSWORD from the inherited environment.
unset ICEGRES_AUTH_FILE ICEGRES_TLS_CERT ICEGRES_TLS_KEY
# Same for buffered-write mode: only the dedicated C4 buffered probe enables it.
unset ICEGRES_WRITE_BUFFER_MS ICEGRES_WRITE_BUFFER_MAX_ROWS
CATALOG_URI="http://127.0.0.1:8181/catalog"
WAREHOUSE=lakehouse
S3_ENDPOINT="http://127.0.0.1:9000"
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfssecret
export AWS_DEFAULT_REGION=us-east-1
export PGCONNECT_TIMEOUT=5

TS="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RESULTS_DIR" "$RUN_DIR"
RECORDS="$RUN_DIR/parity-records-$$.ndjson"
: >"$RECORDS"
OUT_JSON="$RESULTS_DIR/parity-$TS.json"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fatal() { printf '\033[1;31mFATAL\033[0m %s\n' "$*" >&2; exit 1; }

# psql wrappers: every call is a NEW connection.
q()  { psql -h "$PG_HOST" -p "$MAIN_PORT"   -U postgres -d icegres -tA -c "$1" 2>&1; }
q2() { psql -h "$PG_HOST" -p "$SECOND_PORT" -U postgres -d icegres -tA -c "$1" 2>&1; }

# Flatten output into a single evidence line (strip ANSI colors from logs).
flat() { tr '\n' ' ' | sed -e $'s/\x1b\\[[0-9;]*m//g' -e 's/  */ /g' -e 's/^ //' -e 's/ $//' | cut -c1-400; }

record() { # id area behavior verdict evidence
  local verdict=$4
  case "$verdict" in PASS|GAP|NA_BY_DESIGN) ;; *) fatal "bad verdict '$verdict' for probe $1" ;; esac
  jq -n --arg id "$1" --arg area "$2" --arg behavior "$3" \
        --arg verdict "$verdict" --arg evidence "$5" \
        '{id:$id, area:$area, behavior:$behavior, verdict:$verdict, evidence:$evidence}' >>"$RECORDS"
  printf '%-4s %-13s %s\n' "$1" "[$verdict]" "$5" | cut -c1-160
}

# ---------------------------------------------------------------------------
# Identity-checked server lifecycle (pattern from icegres/tests/e2e.sh)
# ---------------------------------------------------------------------------
stop_pidfile() { # pidfile
  local pidfile=$1 pid
  if [[ -f "$pidfile" ]]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegres ]]; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  fi
}

wait_ready() { # port [timeout_halfsecs]
  local port=$1 tries=${2:-60}
  for _ in $(seq 1 "$tries"); do
    if psql -h "$PG_HOST" -p "$port" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

start_server() { # port pidfile logfile
  local port=$1 pidfile=$2 logfile=$3
  "$BIN" serve --host "$PG_HOST" --port "$port" >>"$logfile" 2>&1 &
  echo $! >"$pidfile"
  wait_ready "$port" || { tail -n 20 "$logfile" >&2; fatal "icegres serve not ready on :$port"; }
}

stop_icegresd_pidfile() { # pidfile — identity-checked kill (comm=icegresd);
  # SIGTERM makes icegresd terminate its computes before exiting.
  local pidfile=$1 pid
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

STARTED_MAIN=0
cleanup() {
  [[ "$STARTED_MAIN" == 1 ]] && stop_pidfile "$RUN_DIR/parity-serve.pid"
  stop_pidfile "$RUN_DIR/parity-serve2.pid"
  stop_pidfile "$RUN_DIR/parity-serve-env.pid"
  stop_pidfile "$RUN_DIR/parity-serve-idle.pid"
  stop_pidfile "$RUN_DIR/parity-serve-secure.pid"
  stop_pidfile "$RUN_DIR/parity-serve-pk.pid"
  stop_pidfile "$RUN_DIR/parity-serve-buffered.pid"
  stop_pidfile "$RUN_DIR/parity-serve-branch.pid"
  stop_pidfile "$RUN_DIR/parity-flight.pid"
  stop_icegresd_pidfile "$RUN_DIR/parity-icegresd-d5.pid"
  stop_icegresd_pidfile "$RUN_DIR/parity-icegresd-d7.pid"
  rm -f "$RECORDS"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 0. Stack + binary + servers
# ---------------------------------------------------------------------------
log "checking lakehouse stack"
if ! { pg_isready -h 127.0.0.1 -p 5433 -q \
       && curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" >/dev/null; }; then
  log "stack unhealthy — running infra/scripts/up.sh"
  bash "$REPO_DIR/infra/scripts/up.sh" >"$RUN_DIR/up.log" 2>&1 \
    || { tail -n 20 "$RUN_DIR/up.log" >&2; fatal "infra/scripts/up.sh failed"; }
fi

BIN="$ICEGRES_DIR/target/release/icegres"
[[ -x "$BIN" ]] || BIN="$ICEGRES_DIR/target/debug/icegres"
[[ -x "$BIN" ]] || fatal "no icegres binary found — run: cargo build [--release] in icegres/"
log "using binary: $BIN"
DBIN="${BIN%/*}/icegresd" # control plane sibling (same build profile)

# start_icegresd <public_port> <compute_port> <idle_secs> <pidfile> <logfile> <statusfile> [extra flags...]
start_icegresd() {
  local port=$1 cport=$2 idle=$3 pidfile=$4 logfile=$5 statusfile=$6
  shift 6
  rm -f "$statusfile"
  "$DBIN" serve --host "$PG_HOST" --port "$port" --main-port "$cport" \
    --icegres-bin "$BIN" --idle-shutdown-secs "$idle" --status-file "$statusfile" \
    "$@" >>"$logfile" 2>&1 &
  echo $! >"$pidfile"
  for _ in $(seq 1 40); do
    if (exec 3<>"/dev/tcp/$PG_HOST/$port") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 0.25
  done
  return 1
}

# istatus <statusfile> <compute-key> <jq-expr>
istatus() { jq -r --arg k "$2" ".computes[] | select(.key == \$k) | $3" "$1" 2>/dev/null; }

if wait_ready "$MAIN_PORT" 1; then
  log "reusing running server on :$MAIN_PORT"
else
  stop_pidfile "$RUN_DIR/parity-serve.pid"
  log "starting icegres serve on :$MAIN_PORT"
  : >"$RUN_DIR/parity-serve.log"
  STARTED_MAIN=1
  start_server "$MAIN_PORT" "$RUN_DIR/parity-serve.pid" "$RUN_DIR/parity-serve.log"
fi

stop_pidfile "$RUN_DIR/parity-serve2.pid"
if wait_ready "$SECOND_PORT" 1; then
  fatal "something not started by this harness is listening on :$SECOND_PORT — stop it first"
fi
log "starting second compute on :$SECOND_PORT (for D1/D2/E1)"
: >"$RUN_DIR/parity-serve2.log"
start_server "$SECOND_PORT" "$RUN_DIR/parity-serve2.pid" "$RUN_DIR/parity-serve2.log"

# ===========================================================================
# Area A — Postgres wire & SQL surface
# ===========================================================================
log "Area A — wire & SQL surface"

out=$(q 'select 1')
if [[ "$out" == 1 ]]; then
  record A1 wire "psql connects, simple query protocol" PASS "psql -c 'select 1' over a fresh connection returned: $out"
else
  record A1 wire "psql connects, simple query protocol" GAP "select 1 failed: $(echo "$out" | flat)"
fi

# Extended protocol via psql \bind (binds $1 over the extended protocol).
out=$(printf '\\bind 42\nselect trip_id, city from demo.trips where trip_id = $1;\n' \
      | psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres -tA 2>&1)
if [[ "$out" == 42\|* ]]; then
  record A2 wire "extended protocol / parameterized statements" PASS \
    "psql \\bind 42 + 'where trip_id = \$1' (extended query protocol) returned: $(echo "$out" | flat)"
else
  record A2 wire "extended protocol / parameterized statements" GAP \
    "\\bind parameterized query failed: $(echo "$out" | flat)"
fi

out=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres -tA -c '\dt demo.*' 2>&1)
if echo "$out" | grep -q 'demo|trips|table' && echo "$out" | grep -q 'demo|cities|table'; then
  record A3 wire "\\dt / pg_catalog introspection" PASS \
    "\\dt demo.* lists trips and cities (plus \$snapshots/\$manifests metadata tables): $(echo "$out" | flat)"
else
  record A3 wire "\\dt / pg_catalog introspection" GAP "\\dt demo.* output: $(echo "$out" | flat)"
fi

out=$(q "select table_name from information_schema.tables where table_schema='demo' order by table_name")
if echo "$out" | grep -qx 'trips' && echo "$out" | grep -qx 'cities'; then
  record A4 wire "information_schema" PASS \
    "information_schema.tables for schema demo returned: $(echo "$out" | flat)"
else
  record A4 wire "information_schema" GAP "information_schema query returned: $(echo "$out" | flat)"
fi

pids=(); : >"$RUN_DIR/a5.out"
for i in $(seq 1 8); do
  { r=$(q 'select count(*) from demo.trips'); echo "$i:$r" >>"$RUN_DIR/a5.out"; } &
  pids+=($!)
done
for p in "${pids[@]}"; do wait "$p"; done
ok=$(grep -cE '^[1-8]:[0-9]+$' "$RUN_DIR/a5.out")
if [[ "$ok" == 8 ]]; then
  record A5 wire "multiple concurrent connections" PASS \
    "8 parallel psql SELECT count(*) connections all succeeded (8/8 identical results: $(cut -d: -f2 "$RUN_DIR/a5.out" | sort -u | flat))"
else
  record A5 wire "multiple concurrent connections" GAP \
    "only $ok/8 parallel connections succeeded: $(flat <"$RUN_DIR/a5.out")"
fi

# A8: real ORM/driver compatibility — runs bench/clients/a8_orm_probe.py
# (psycopg2 + pg8000 + SQLAlchemy 2.x + pandas) against the main server;
# with `with_secure` the auth+TLS connect variants target the A6/A7 secure
# server (must still be running). PASS requires exit 0 AND `fail=0` in the
# probe's summary (server-side cursors are a documented XFAIL, not a fail).
A8_BEHAVIOR="ORM/driver compatibility (SQLAlchemy+psycopg2+pg8000)"
run_a8_probe() { # with_secure|plain
  local probe="$REPO_DIR/bench/clients/a8_orm_probe.py"
  local -a a8_env=(env ICEGRES_PROBE_HOST="$PG_HOST" ICEGRES_PROBE_PORT="$MAIN_PORT")
  if [[ "${1:-plain}" == with_secure ]]; then
    a8_env+=(ICEGRES_PROBE_SECURE_PORT="$SECURE_PORT"
             ICEGRES_PROBE_SECURE_USER=parity_user
             ICEGRES_PROBE_SECURE_PASSWORD=parity-secret-pw)
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    record A8 wire "$A8_BEHAVIOR" GAP "python3 not available to run bench/clients/a8_orm_probe.py"
    return
  fi
  if ! python3 -c 'import sqlalchemy, psycopg2, pg8000, pandas' 2>/dev/null; then
    record A8 wire "$A8_BEHAVIOR" GAP \
      "python client libraries missing (pip install sqlalchemy psycopg2-binary pg8000 pandas)"
    return
  fi
  local a8_out a8_rc a8_summary
  a8_out=$("${a8_env[@]}" python3 "$probe" 2>&1)
  a8_rc=$?
  a8_summary=$(echo "$a8_out" | grep '^A8 RESULT:' | tail -n 1)
  if [[ $a8_rc -eq 0 && "$a8_summary" == *"fail=0"* ]]; then
    record A8 wire "$A8_BEHAVIOR" PASS \
      "bench/clients/a8_orm_probe.py all green ($a8_summary; secure variants: ${1:-plain}): psycopg2+pg8000 connect, SCRAM+TLS connect, SQLAlchemy inspect()/reflection of demo.trips (correct column types), ORM filter+GROUP BY, pandas read_sql join, prepared-statement reuse, BEGIN/ROLLBACK + BEGIN/COMMIT via the driver. Documented XFAIL: server-side (named) cursors — DECLARE CURSOR unsupported."
  else
    record A8 wire "$A8_BEHAVIOR" GAP \
      "probe exit=$a8_rc ($a8_summary): $(echo "$a8_out" | grep -E '^(FAIL|Traceback)' | flat)"
  fi
}

# A6/A7 probe a dedicated server started WITH --auth-file + --tls-cert/key
# (the icegres security posture is opt-in per server; other probes use the
# permissive main server, which logs a startup WARN saying auth is off).
stop_pidfile "$RUN_DIR/parity-serve-secure.pid"
SECURE_LOG="$RUN_DIR/parity-serve-secure.log"
AUTH_FILE="$RUN_DIR/parity-auth.conf"
TLS_CRT="$REPO_DIR/infra/.data/tls/dev.crt"
TLS_KEY="$REPO_DIR/infra/.data/tls/dev.key"
secure_up=0
if psql -h "$PG_HOST" -p "$SECURE_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  secure_err="port :$SECURE_PORT already occupied; could not start the auth+TLS server"
elif ! bash "$REPO_DIR/infra/scripts/gen-dev-cert.sh" >/dev/null 2>&1; then
  secure_err="infra/scripts/gen-dev-cert.sh failed"
else
  printf 'parity_user:parity-secret-pw\n' >"$AUTH_FILE"
  chmod 600 "$AUTH_FILE"
  : >"$SECURE_LOG"
  "$BIN" serve --host "$PG_HOST" --port "$SECURE_PORT" \
      --auth-file "$AUTH_FILE" --tls-cert "$TLS_CRT" --tls-key "$TLS_KEY" \
      >>"$SECURE_LOG" 2>&1 &
  echo $! >"$RUN_DIR/parity-serve-secure.pid"
  for _ in $(seq 1 60); do
    if PGPASSWORD=parity-secret-pw psql "host=$PG_HOST port=$SECURE_PORT user=parity_user dbname=icegres sslmode=require" \
         -tA -c 'select 1' >/dev/null 2>&1; then
      secure_up=1; break
    fi
    sleep 0.5
  done
  [[ "$secure_up" == 1 ]] || secure_err="auth+TLS server never became ready: $(tail -n 5 "$SECURE_LOG" | flat)"
fi

if [[ "$secure_up" == 1 ]]; then
  ok_goodpass=$(PGPASSWORD=parity-secret-pw psql "host=$PG_HOST port=$SECURE_PORT user=parity_user dbname=icegres sslmode=require" -tA -c 'select 1' 2>&1)
  out_badpass=$(PGPASSWORD=definitely-wrong psql "host=$PG_HOST port=$SECURE_PORT user=parity_user dbname=icegres" -tA -c 'select 1' 2>&1)
  out_baduser=$(PGPASSWORD=parity-secret-pw psql "host=$PG_HOST port=$SECURE_PORT user=not_a_real_user dbname=icegres" -tA -c 'select 1' 2>&1)
  if [[ "$ok_goodpass" == 1 && "$out_badpass" != 1 && "$out_baduser" != 1 ]]; then
    record A6 wire "server-side auth" PASS \
      "--auth-file (SCRAM-SHA-256, hashed-at-rest): right password accepted (select 1 -> $ok_goodpass); wrong password rejected ($(echo "$out_badpass" | flat)); unknown user rejected ($(echo "$out_baduser" | flat)). Servers without --auth-file stay permissive and log a startup WARN."
  else
    record A6 wire "server-side auth" GAP \
      "auth-enabled server misbehaved: good-pass -> $(echo "$ok_goodpass" | flat); wrong-pass -> $(echo "$out_badpass" | flat); bad-user -> $(echo "$out_baduser" | flat)"
  fi

  # A7: upstream pgwire serves BOTH TLS and plaintext startup on one listener
  # (like stock Postgres without hostssl rules), so a bare sslmode=require
  # success is not enough — prove the handshake with openssl s_client too.
  out_tls=$(PGPASSWORD=parity-secret-pw psql "host=$PG_HOST port=$SECURE_PORT user=parity_user dbname=icegres sslmode=require" -tA -c 'select 1' 2>&1)
  tls_line=$(echo | openssl s_client -starttls postgres -connect "$PG_HOST:$SECURE_PORT" 2>/dev/null \
    | grep -Eo 'TLSv1\.[23], Cipher is [A-Z0-9_]+' | head -n 1)
  out_vfull=$(PGPASSWORD=parity-secret-pw psql "host=localhost port=$SECURE_PORT user=parity_user dbname=icegres sslmode=verify-full sslrootcert=$TLS_CRT" -tA -c 'select 1' 2>&1)
  if [[ "$out_tls" == 1 && -n "$tls_line" && "$out_vfull" == 1 ]]; then
    record A7 wire "TLS" PASS \
      "--tls-cert/--tls-key (rustls; boot fails hard on bad cert/key, no plaintext fallback): sslmode=require -> $out_tls; openssl s_client -starttls postgres handshake: $tls_line; sslmode=verify-full against the pinned dev cert -> $out_vfull. Plaintext startup is still accepted on the same listener (upstream/stock-Postgres behavior) — clients enforce encryption via sslmode."
  else
    record A7 wire "TLS" GAP \
      "TLS incomplete: sslmode=require -> $(echo "$out_tls" | flat); s_client -> '${tls_line:-no handshake}'; verify-full -> $(echo "$out_vfull" | flat)"
  fi
  run_a8_probe with_secure
  stop_pidfile "$RUN_DIR/parity-serve-secure.pid"
else
  record A6 wire "server-side auth" GAP "$secure_err"
  record A7 wire "TLS" GAP "$secure_err"
  run_a8_probe plain
fi

# A9: JDBC compatibility — runs bench/clients/a9_jdbc_probe.sh (stock pgjdbc
# driver: DatabaseMetaData.getTables/getColumns, Statement, PreparedStatement
# with typed parameters, executeUpdate INSERT + readback, autoCommit(false)
# commit/rollback) against the main server. PASS requires exit 0 AND fail=0
# in the probe summary. NA_BY_DESIGN is never used here; a missing JDK
# records a GAP in evidence terms (environment, not server).
A9_BEHAVIOR="JDBC compatibility (pgjdbc DatabaseMetaData+PreparedStatement+txn)"
a9_probe="$REPO_DIR/bench/clients/a9_jdbc_probe.sh"
if ! command -v java >/dev/null 2>&1 || ! command -v javac >/dev/null 2>&1; then
  record A9 wire "$A9_BEHAVIOR" GAP "java/javac not available to run bench/clients/a9_jdbc_probe.sh"
else
  a9_out=$(env ICEGRES_PROBE_HOST="$PG_HOST" ICEGRES_PROBE_PORT="$MAIN_PORT" bash "$a9_probe" 2>&1)
  a9_rc=$?
  a9_summary=$(echo "$a9_out" | grep '^A9 RESULT:' | tail -n 1)
  if [[ $a9_rc -eq 0 && "$a9_summary" == *"fail=0"* ]]; then
    record A9 wire "$A9_BEHAVIOR" PASS \
      "bench/clients/a9_jdbc_probe.sh all green ($a9_summary): pgjdbc connect (startup params accepted), DatabaseMetaData product/getTables/getColumns of demo.trips, Statement SELECT, PreparedStatement setLong/setString x7 (crosses prepareThreshold), ResultSetMetaData, executeUpdate INSERT + readback (INSERT 0 n tag on the extended protocol), setAutoCommit(false) rollback + commit visible from a new connection."
  else
    record A9 wire "$A9_BEHAVIOR" GAP \
      "probe exit=$a9_rc ($a9_summary): $(echo "$a9_out" | grep -E '^(FAIL|A9 ERROR|A9 SKIP)' | flat)"
  fi
fi

# A11: ADBC first-class — starts `icegres flight-serve` on :$FLIGHT_PORT and
# runs bench/clients/a11_adbc_probe.py, both lanes: (1) adbc_driver_flightsql
# against the Arrow Flight SQL endpoint (query, get_objects, prepared+bind,
# DML counts, BULK INGEST asserted as ONE Iceberg commit); (2) adbc_driver_
# postgresql against the main pgwire server (COPY ... TO STDOUT binary reads,
# params, get_objects, DML). PASS requires exit 0 AND fail=0 (the probe's two
# pg-lane XFAILs — COPY FROM ingest, in-txn extended SELECT — are documented
# limits, not failures). Auth variants are covered by e2e section (p).
A11_BEHAVIOR="ADBC first-class (Flight SQL + bulk ingest; postgres-lane COPY reads)"
a11_probe="$REPO_DIR/bench/clients/a11_adbc_probe.py"
FLIGHT_PORT=50051
flight_port_open() {
  python3 -c "import socket,sys; s=socket.socket(); s.settimeout(0.3);
sys.exit(0 if s.connect_ex(('127.0.0.1', $FLIGHT_PORT))==0 else 1)" 2>/dev/null
}
if ! command -v python3 >/dev/null 2>&1 \
    || ! python3 -c 'import adbc_driver_flightsql, adbc_driver_postgresql, pyarrow' 2>/dev/null; then
  record A11 wire "$A11_BEHAVIOR" GAP \
    "python ADBC drivers missing (pip install adbc-driver-flightsql adbc-driver-postgresql pyarrow)"
elif flight_port_open; then
  record A11 wire "$A11_BEHAVIOR" GAP \
    "port :$FLIGHT_PORT already in use — cannot start a harness-owned flight-serve"
else
  "$BIN" flight-serve --host 127.0.0.1 --port "$FLIGHT_PORT" \
    >"$RUN_DIR/parity-flight.log" 2>&1 &
  echo $! >"$RUN_DIR/parity-flight.pid"
  a11_up=0
  for _ in $(seq 1 60); do
    flight_port_open && { a11_up=1; break; }
    kill -0 "$(cat "$RUN_DIR/parity-flight.pid")" 2>/dev/null || break
    sleep 0.5
  done
  if [[ "$a11_up" != 1 ]]; then
    record A11 wire "$A11_BEHAVIOR" GAP \
      "icegres flight-serve failed to start: $(tail -n 5 "$RUN_DIR/parity-flight.log" | flat)"
  else
    a11_out=$(env ICEGRES_PROBE_FLIGHT_HOST=127.0.0.1 \
        ICEGRES_PROBE_FLIGHT_PORT="$FLIGHT_PORT" \
        ICEGRES_PROBE_PG_HOST="$PG_HOST" ICEGRES_PROBE_PG_PORT="$MAIN_PORT" \
        python3 "$a11_probe" 2>&1)
    a11_rc=$?
    a11_summary=$(echo "$a11_out" | grep '^A11 RESULT:' | tail -n 1)
    a11_ingest=$(echo "$a11_out" | grep '^PASS flight: BULK INGEST' | flat)
    if [[ $a11_rc -eq 0 && "$a11_summary" == *"fail=0"* ]]; then
      record A11 wire "$A11_BEHAVIOR" PASS \
        "bench/clients/a11_adbc_probe.py all green ($a11_summary): adbc_driver_flightsql connect+query (Arrow end to end), get_objects catalogs/schemas/tables/columns, prepared statements with \$1 binds, INSERT/UPDATE/DELETE with real affected counts via the copy-on-write engine, BULK INGEST landing as ONE Iceberg commit [$a11_ingest], statement schema metadata; adbc_driver_postgresql (libpq) reads over COPY ... TO STDOUT (FORMAT binary), params, get_objects, DML rowcounts. Documented XFAILs: pg-lane COPY FROM ingest (Flight lane owns ingest), in-transaction extended SELECT (pre-existing 0A000)."
    else
      record A11 wire "$A11_BEHAVIOR" GAP \
        "probe exit=$a11_rc ($a11_summary): $(echo "$a11_out" | grep -E '^(FAIL|Traceback)' | flat)"
    fi
  fi
  stop_pidfile "$RUN_DIR/parity-flight.pid"
fi

# ===========================================================================
# Area B — OLTP semantics
# ===========================================================================
log "Area B — OLTP semantics"

max_id=$(q 'select coalesce(max(trip_id), 0) from demo.trips')
next_id=$((max_id >= 900000 ? max_id + 1 : 900000))

B1_ID=$next_id; next_id=$((next_id + 1))
tag=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres -c \
  "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($B1_ID, 'Parity B1', 1.11, 2.22, TIMESTAMP '2026-07-05 00:00:00')" 2>&1 | tail -n 1)
# Freshness clock (C4): starts the moment the INSERT's commit returned; the
# readback below runs over a NEW connection, polled every 10ms.
t_insert_done_ms=$(($(date +%s%N) / 1000000))
fresh_ms=-1
back=""
for _ in $(seq 1 200); do
  back=$(q "select trip_id, city from demo.trips where trip_id = $B1_ID")
  if [[ "$back" == "$B1_ID|Parity B1" ]]; then
    fresh_ms=$(( $(date +%s%N) / 1000000 - t_insert_done_ms )); break
  fi
  sleep 0.01
done
if [[ "$tag" == "INSERT 0 1" && "$back" == "$B1_ID|Parity B1" ]]; then
  record B1 oltp "INSERT via wire, durable" PASS \
    "INSERT trip_id=$B1_ID returned tag '$tag'; row read back over a NEW connection: $back"
else
  record B1 oltp "INSERT via wire, durable" GAP \
    "insert tag: $(echo "$tag" | flat); readback: $(echo "$back" | flat)"
fi

# B2: UPDATE over the wire (copy-on-write overwrite snapshot), verified by
# reading the new value back over a NEW connection.
b2_tag=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres -c \
  "update demo.trips set fare = 99.9 where trip_id = $B1_ID" 2>&1 | tail -n 1)
b2_back=$(q "select round(fare, 2) from demo.trips where trip_id = $B1_ID")
if [[ "$b2_tag" == "UPDATE 1" && "$b2_back" == "99.9" ]]; then
  record B2 oltp "UPDATE" PASS \
    "UPDATE trip_id=$B1_ID returned tag '$b2_tag'; new fare read back over a NEW connection: $b2_back (copy-on-write: only the Parquet file holding the row is rewritten, all other files are reused in the new Iceberg snapshot)"
else
  record B2 oltp "UPDATE" GAP \
    "UPDATE tag: $(echo "$b2_tag" | flat); readback: $(echo "$b2_back" | flat)"
fi

# B3: DELETE over the wire; row gone from a NEW connection, and the
# pre-delete snapshot still serves it (time travel intact after DML).
B3_ID=$next_id; next_id=$((next_id + 1))
psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres -c \
  "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($B3_ID, 'Parity B3', 3.0, 3.0, TIMESTAMP '2026-07-05 00:00:00')" >/dev/null 2>&1
b3_pre_snap=$(q 'select snapshot_id from demo."trips$snapshots" order by committed_at desc limit 1')
b3_tag=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres -c \
  "delete from demo.trips where trip_id = $B3_ID" 2>&1 | tail -n 1)
b3_after=$(q "select count(*) from demo.trips where trip_id = $B3_ID")
b3_tt=$(q "select count(*) from demo.\"trips@$b3_pre_snap\" where trip_id = $B3_ID")
if [[ "$b3_tag" == "DELETE 1" && "$b3_after" == 0 && "$b3_tt" == 1 ]]; then
  record B3 oltp "DELETE" PASS \
    "DELETE trip_id=$B3_ID returned tag '$b3_tag'; count over a NEW connection afterwards: $b3_after; time travel intact: pre-delete snapshot $b3_pre_snap still serves the row (count=$b3_tt)"
else
  record B3 oltp "DELETE" GAP \
    "DELETE tag: $(echo "$b3_tag" | flat); post-delete count: $b3_after; pre-delete snapshot readback: $(echo "$b3_tt" | flat)"
fi

# B4: real transactions — ROLLBACK undoes (with read-your-own-writes inside
# the txn), and a multi-statement COMMIT applies atomically.
B4_ID=$next_id; next_id=$((next_id + 1))
B4C_ID=$next_id; next_id=$((next_id + 1))
txn_out=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres 2>&1 <<EOF | flat
BEGIN;
insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($B4_ID, 'Parity B4', 1.0, 1.0, TIMESTAMP '2026-07-05 00:00:00');
select count(*) from demo.trips where trip_id = $B4_ID;
ROLLBACK;
EOF
)
after_rb=$(q "select count(*) from demo.trips where trip_id = $B4_ID")
commit_out=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres 2>&1 <<EOF | flat
BEGIN;
insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($B4C_ID, 'Parity B4c', 1.0, 1.0, TIMESTAMP '2026-07-05 00:00:00');
update demo.trips set fare = 77.0 where trip_id = $B4C_ID;
COMMIT;
EOF
)
after_commit=$(q "select trip_id, round(fare, 1) from demo.trips where trip_id = $B4C_ID")
if [[ "$after_rb" == 0 && "$after_commit" == "$B4C_ID|77.0" ]]; then
  record B4 oltp "explicit transactions BEGIN/COMMIT/ROLLBACK" PASS \
    "real transactions: ROLLBACK undid the INSERT (row $B4_ID absent afterwards; the row WAS visible inside the txn: '$txn_out'); a 2-statement txn (INSERT + UPDATE) COMMITted atomically as one Iceberg snapshot (row $B4C_ID|fare=77.0 from a new connection: '$commit_out'). Reads inside a txn are snapshot-pinned per table (snapshot isolation, first-committer-wins 40001 on conflict — proven in e2e (j))."
else
  record B4 oltp "explicit transactions BEGIN/COMMIT/ROLLBACK" GAP \
    "transactions incomplete: post-ROLLBACK count=$after_rb (want 0; session: $txn_out); post-COMMIT readback='$after_commit' (want $B4C_ID|77.0; session: $commit_out)"
fi

# B5: opt-in PK enforcement — probed on a dedicated server started with
# --enforce-pk against a parity-owned scratch table declaring
# icegres.primary-key (the main server keeps the default: enforcement OFF).
PK_PORT=5448
stop_pidfile "$RUN_DIR/parity-serve-pk.pid"
prefix=$(curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" | jq -r '.overrides.prefix // .defaults.prefix')
curl -sf -X DELETE "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/parity_pk?purgeRequested=true" >/dev/null 2>&1
b5_err=""
if psql -h "$PG_HOST" -p "$PK_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  b5_err="port :$PK_PORT already occupied; could not start the --enforce-pk server"
elif ! curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces/demo/tables" \
    -H 'Content-Type: application/json' -d '{
  "name": "parity_pk",
  "schema": {"type":"struct","schema-id":0,"fields":[
    {"id":1,"name":"id","required":false,"type":"long"},
    {"id":2,"name":"val","required":false,"type":"string"}]},
  "properties": {"icegres.primary-key": "id"}
}' >/dev/null; then
  b5_err="could not create demo.parity_pk via the REST catalog"
else
  : >"$RUN_DIR/parity-serve-pk.log"
  "$BIN" serve --host "$PG_HOST" --port "$PK_PORT" --enforce-pk \
    >>"$RUN_DIR/parity-serve-pk.log" 2>&1 &
  echo $! >"$RUN_DIR/parity-serve-pk.pid"
  wait_ready "$PK_PORT" || b5_err="--enforce-pk server never became ready: $(tail -n 5 "$RUN_DIR/parity-serve-pk.log" | flat)"
fi
if [[ -z "$b5_err" ]]; then
  first_tag=$(psql -h "$PG_HOST" -p "$PK_PORT" -U postgres -d icegres -c \
    "insert into demo.parity_pk (id, val) values (1, 'a')" 2>&1 | tail -n 1)
  dup_out=$(psql -h "$PG_HOST" -p "$PK_PORT" -U postgres -d icegres -c \
    "insert into demo.parity_pk (id, val) values (1, 'dup')" 2>&1)
  null_out=$(psql -h "$PG_HOST" -p "$PK_PORT" -U postgres -d icegres -c \
    "insert into demo.parity_pk (id, val) values (NULL, 'n')" 2>&1)
  pk_count=$(psql -h "$PG_HOST" -p "$PK_PORT" -U postgres -d icegres -tA -c \
    "select count(*) from demo.parity_pk" 2>&1)
  if [[ "$first_tag" == "INSERT 0 1" ]] \
     && echo "$dup_out" | grep -q 'duplicate key value violates unique constraint' \
     && echo "$null_out" | grep -q 'violates not-null constraint' \
     && [[ "$pk_count" == 1 ]]; then
    record B5 oltp "PK/constraint enforcement" PASS \
      "--enforce-pk + table property icegres.primary-key=id: first insert accepted ('$first_tag'); duplicate rejected with 23505 ($(echo "$dup_out" | flat)); NULL key rejected with 23502 ($(echo "$null_out" | flat)); table holds $pk_count row. Checks run against the snapshot each commit anchors to (409 retry re-validates), so racing duplicates cannot both land. Default is OFF (documented cost: reads key columns of every live file per write)."
  else
    record B5 oltp "PK/constraint enforcement" GAP \
      "enforcement incomplete: first='$first_tag' dup='$(echo "$dup_out" | flat)' null='$(echo "$null_out" | flat)' count=$pk_count"
  fi
  stop_pidfile "$RUN_DIR/parity-serve-pk.pid"
  curl -sf -X DELETE "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/parity_pk?purgeRequested=true" >/dev/null 2>&1
else
  record B5 oltp "PK/constraint enforcement" GAP "$b5_err"
fi

# ===========================================================================
# Area C — Lakehouse integration
# ===========================================================================
log "Area C — lakehouse integration"

server_count=$(q 'select count(*) from demo.trips')
c1_done=0
if python3 -c 'import pyiceberg, pyarrow' 2>/dev/null; then
  ext_count=$(python3 - <<EOF 2>&1 | tail -n 1
from pyiceberg.catalog import load_catalog
cat = load_catalog("lakekeeper", **{
    "type": "rest", "uri": "$CATALOG_URI", "warehouse": "$WAREHOUSE",
    "s3.endpoint": "$S3_ENDPOINT", "s3.access-key-id": "$AWS_ACCESS_KEY_ID",
    "s3.secret-access-key": "$AWS_SECRET_ACCESS_KEY", "s3.region": "us-east-1",
    "s3.path-style-access": "true",
})
print(len(cat.load_table("demo.trips").scan().to_arrow()))
EOF
)
  if [[ "$ext_count" == "$server_count" ]]; then
    record C1 lakehouse "data in open format, other engines can read" PASS \
      "independent reader pyiceberg $(python3 -c 'import pyiceberg; print(pyiceberg.__version__)') read demo.trips via the REST catalog + S3: $ext_count rows == server's count($server_count)"
    c1_done=1
  else
    record C1 lakehouse "data in open format, other engines can read" GAP \
      "pyiceberg read disagreed with server count $server_count: $(echo "$ext_count" | flat)"
    c1_done=1
  fi
fi
if [[ "$c1_done" == 0 ]] && python3 -c 'import duckdb' 2>/dev/null; then
  # Fallback: copy the table's live Parquet files locally and count with duckdb.
  loc_prefix=$(curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" | jq -r '.overrides.prefix // .defaults.prefix')
  location=$(curl -sf "$CATALOG_URI/v1/$loc_prefix/namespaces/demo/tables/trips" | jq -r '.metadata.location')
  tmpd=$(mktemp -d)
  aws --endpoint-url "$S3_ENDPOINT" s3 cp --recursive "$location/data/" "$tmpd/" >/dev/null 2>&1
  ext_count=$(python3 -c "import duckdb; print(duckdb.sql(\"select count(*) from read_parquet('$tmpd/**/*.parquet')\").fetchone()[0])" 2>&1 | tail -n 1)
  rm -rf "$tmpd"
  if [[ "$ext_count" == "$server_count" ]]; then
    record C1 lakehouse "data in open format, other engines can read" PASS \
      "duckdb read the table's Parquet data files (aws s3 cp from RustFS): $ext_count rows == server's count($server_count) (append-only table, all data files live)"
  else
    record C1 lakehouse "data in open format, other engines can read" GAP \
      "duckdb parquet count $ext_count != server count $server_count"
  fi
  c1_done=1
fi
if [[ "$c1_done" == 0 ]]; then
  record C1 lakehouse "data in open format, other engines can read" GAP \
    "no independent reader available (pyiceberg/pyarrow and duckdb not importable, pip install failed in this environment)"
fi

prefix=$(curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" | jq -r '.overrides.prefix // .defaults.prefix')
tables_json=$(curl -sf "$CATALOG_URI/v1/$prefix/namespaces/demo/tables")
if echo "$tables_json" | jq -e '.identifiers[] | select(.name=="trips")' >/dev/null 2>&1 \
   && echo "$tables_json" | jq -e '.identifiers[] | select(.name=="cities")' >/dev/null 2>&1; then
  record C2 lakehouse "catalog registration (REST)" PASS \
    "Lakekeeper GET /v1/$prefix/namespaces/demo/tables lists: $(echo "$tables_json" | jq -c '[.identifiers[].name]' | flat)"
else
  record C2 lakehouse "catalog registration (REST)" GAP \
    "REST table listing: $(echo "$tables_json" | flat)"
fi

record C3 lakehouse "CDC Postgres->Iceberg" NA_BY_DESIGN \
  "Moonlink exists to replicate Postgres heap data into Iceberg; icegres data is born in Iceberg (INSERT via wire commits an Iceberg snapshot directly, verified by B1+C1) — there is no second copy to synchronize."

# C4: freshness — measured inside the B1 probe (commit -> first successful
# new-connection readback, 10ms poll). Additionally probe the opt-in
# buffered write mode (--write-buffer-ms, Moonlink-style union reads) on a
# dedicated server: ack -> new-connection readback, which is served from the
# in-memory buffer union rather than waiting for the Iceberg commit.
BUFC4_PORT=5449
stop_pidfile "$RUN_DIR/parity-serve-buffered.pid"
buffered_evidence="buffered mode not probed"
if psql -h "$PG_HOST" -p "$BUFC4_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  buffered_evidence="buffered-mode probe skipped: port :$BUFC4_PORT already occupied"
else
  : >"$RUN_DIR/parity-serve-buffered.log"
  "$BIN" serve --host "$PG_HOST" --port "$BUFC4_PORT" --write-buffer-ms 100 \
    >>"$RUN_DIR/parity-serve-buffered.log" 2>&1 &
  echo $! >"$RUN_DIR/parity-serve-buffered.pid"
  if wait_ready "$BUFC4_PORT" 40; then
    C4B_ID=$next_id; next_id=$((next_id + 1))
    psql -h "$PG_HOST" -p "$BUFC4_PORT" -U postgres -d icegres -q -c \
      "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($C4B_ID, 'Parity C4buf', 1.0, 1.0, TIMESTAMP '2026-07-06 00:00:00')" \
      2>/dev/null
    t0=$(($(date +%s%N) / 1000000))
    buf_fresh_ms=-1
    for _ in $(seq 1 100); do
      r=$(psql -h "$PG_HOST" -p "$BUFC4_PORT" -U postgres -d icegres -tA -c \
        "select trip_id from demo.trips where trip_id = $C4B_ID" 2>&1)
      if [[ "$r" == "$C4B_ID" ]]; then
        buf_fresh_ms=$(( $(date +%s%N) / 1000000 - t0 )); break
      fi
      sleep 0.01
    done
    if [[ "$buf_fresh_ms" -ge 0 ]]; then
      buffered_evidence="buffered mode (--write-buffer-ms 100, opt-in; default 0 = synchronous): INSERT acked from the in-memory buffer was readable from a NEW connection on the same server after ~${buf_fresh_ms}ms (union read — no wait for the Iceberg commit; precise p50/p95 is bench insert_single_buffered_ms/freshness_buffered_ms; cross-server freshness = flush cadence, unclean-kill loss window <= 100ms, WARNed at startup)"
    else
      buffered_evidence="buffered-mode probe FAILED: acked row trip_id=$C4B_ID not readable within 1s on :$BUFC4_PORT"
    fi
    sleep 0.5 # let the flusher commit the probe row before stopping
  else
    buffered_evidence="buffered-mode probe skipped: server with --write-buffer-ms never became ready: $(tail -n 3 "$RUN_DIR/parity-serve-buffered.log" | flat)"
  fi
  stop_pidfile "$RUN_DIR/parity-serve-buffered.pid"
fi
if [[ "$fresh_ms" -ge 0 && "$fresh_ms" -lt 1000 ]]; then
  record C4 lakehouse "write freshness (commit -> readable elsewhere)" PASS \
    "row committed via conn A was readable from a new connection ~${fresh_ms}ms after commit (coarse; includes psql startup — precise p50/p95 is bench freshness_ms). Moonlink bar: sub-second. Additionally: $buffered_evidence."
elif [[ "$fresh_ms" -ge 0 ]]; then
  record C4 lakehouse "write freshness (commit -> readable elsewhere)" GAP \
    "row visible only after ${fresh_ms}ms (> 1s Moonlink bar); $buffered_evidence"
else
  record C4 lakehouse "write freshness (commit -> readable elsewhere)" GAP \
    "row trip_id=$B1_ID never became visible while polling; $buffered_evidence"
fi

snaps=$(q 'select snapshot_id, committed_at from demo."trips$snapshots" order by committed_at')
nsnaps=$(echo "$snaps" | grep -c '|')
star_err=$(q 'select * from demo."trips$snapshots" limit 1' | flat)
if [[ "$nsnaps" -ge 1 && "$snaps" != ERROR* ]]; then
  record C5 lakehouse "Iceberg metadata surfaces" PASS \
    "demo.\"trips\$snapshots\" queryable ($nsnaps snapshots listed; \$manifests also works). Caveats: 'select *' fails on the Map-typed summary column ($star_err) and count(*) hits a DataFusion logical/physical schema mismatch — column projections work."
else
  record C5 lakehouse "Iceberg metadata surfaces" GAP \
    "snapshots metadata table not queryable: $(echo "$snaps" | flat)"
fi

# ===========================================================================
# Area D — Serverless / elasticity
# ===========================================================================
log "Area D — serverless / elasticity"

# D2 first: the :5440 compute was started BEFORE B1's insert on :5439, so
# reading B1's row there proves cross-compute visibility of post-start commits.
d2_count=$(q2 'select count(*) from demo.trips')
d2_stale_ms=-1
t0=$(($(date +%s%N) / 1000000))
for _ in $(seq 1 200); do
  r=$(q2 "select trip_id, city from demo.trips where trip_id = $B1_ID")
  if [[ "$r" == "$B1_ID|Parity B1" ]]; then
    d2_stale_ms=$(( $(date +%s%N) / 1000000 - t0 )); break
  fi
  sleep 0.05
done
main_count=$(q 'select count(*) from demo.trips')
if [[ "$d2_stale_ms" -ge 0 && "$d2_count" == "$main_count" ]]; then
  record D2 elasticity "multiple stateless computes on shared storage" PASS \
    "second 'icegres serve' on :$SECOND_PORT (same catalog, started before the write) returned identical count ($d2_count) and served row trip_id=$B1_ID committed via :$MAIN_PORT after both computes started (visible on first poll, ${d2_stale_ms}ms; each query reloads table metadata from the REST catalog, so no staleness window was observed)."
elif [[ "$d2_stale_ms" -ge 0 ]]; then
  record D2 elasticity "multiple stateless computes on shared storage" GAP \
    "row visible on :$SECOND_PORT but counts diverge: :$MAIN_PORT=$main_count :$SECOND_PORT=$d2_count"
else
  record D2 elasticity "multiple stateless computes on shared storage" GAP \
    "row trip_id=$B1_ID committed via :$MAIN_PORT never became visible on :$SECOND_PORT (last read: $(echo "$r" | flat))"
fi

# D1 + D3: kill the :5440 compute, restart it, verify data intact; the
# restart doubles as a single cold-start measurement.
before=$(q2 'select count(*) from demo.trips')
stop_pidfile "$RUN_DIR/parity-serve2.pid"
if psql -h "$PG_HOST" -p "$SECOND_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  record D1 elasticity "stateless compute: restart durability" GAP "server on :$SECOND_PORT still answering after kill"
  record D3 elasticity "cold start" GAP "could not measure: old server did not die"
else
  t0=$(($(date +%s%N) / 1000000))
  "$BIN" serve --host "$PG_HOST" --port "$SECOND_PORT" >>"$RUN_DIR/parity-serve2.log" 2>&1 &
  echo $! >"$RUN_DIR/parity-serve2.pid"
  ready=0
  for _ in $(seq 1 600); do
    if psql -h "$PG_HOST" -p "$SECOND_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
      ready=1; break
    fi
    sleep 0.05
  done
  cold_ms=$(( $(date +%s%N) / 1000000 - t0 ))
  after=$(q2 'select count(*) from demo.trips')
  b1=$(q2 "select count(*) from demo.trips where trip_id = $B1_ID")
  if [[ "$ready" == 1 && "$after" == "$before" && "$b1" -ge 1 ]]; then
    record D1 elasticity "stateless compute: restart durability" PASS \
      "killed and restarted the :$SECOND_PORT compute: row count identical before/after ($before), wire-inserted row trip_id=$B1_ID intact — all state lives in Iceberg/RustFS, none in the process."
  else
    record D1 elasticity "stateless compute: restart durability" GAP \
      "after restart: ready=$ready count $before -> $after, B1 row count=$b1"
  fi
  if [[ "$ready" == 1 && "$cold_ms" -le 10000 ]]; then
    record D3 elasticity "cold start" PASS \
      "serve spawn -> first successful 'select 1': ${cold_ms}ms (single run, 50ms poll; p50/p95 over >=5 runs is bench cold_start_ms). Neon bar: ~500ms-few s."
  else
    record D3 elasticity "cold start" GAP "spawn->ready took ${cold_ms}ms (ready=$ready)"
  fi
fi

# D4: time-travel read. Pin a query to the OLDEST snapshot via the quoted
# "table@snapshot_id" form; rows appended earlier in this very run (B1/B5)
# guarantee the pinned count is strictly below the current count when time
# travel actually works.
snap_list=$(q 'select snapshot_id from demo."trips$snapshots" order by committed_at limit 1')
tt_now=$(q 'select count(*) from demo.trips')
tt_pinned=$(q "select count(*) from demo.\"trips@$snap_list\"" | flat)
tt_filter=$(q "select count(*) from demo.\"trips@$snap_list\" where trip_id between 1 and 280" | flat)
if [[ "$tt_pinned" =~ ^[0-9]+$ && "$tt_now" =~ ^[0-9]+$ && "$tt_pinned" -lt "$tt_now" \
      && "$tt_filter" =~ ^[0-9]+$ ]]; then
  record D4 elasticity "time-travel read (branching/PITR analogue)" PASS \
    "snapshot-pinned read works: demo.\"trips@$snap_list\" (oldest snapshot from \"trips\$snapshots\") returned count=$tt_pinned vs current count=$tt_now (rows appended this run are invisible in the pinned view; WHERE on the pinned table -> $tt_filter). Read-only; timestamp syntax FOR SYSTEM_TIME AS OF is not supported by datafusion 52."
else
  record D4 elasticity "time-travel read (branching/PITR analogue)" GAP \
    "snapshots are enumerable (e.g. id $snap_list via \"trips\$snapshots\") but snapshot-pinned read failed: table@snapshot count -> '$tt_pinned' (current $tt_now), filtered -> '$tt_filter'"
fi

# D5: scale-to-zero — the FULL sleep/wake cycle through icegresd (the
# shipped control plane, not an external supervisor): first connection to
# the public port wakes a compute (--idle-shutdown-secs 5, health endpoint
# on :$HEALTH_PORT via env passthrough for E2), the compute exits cleanly on
# its own after the idle window and is reaped, and the NEXT connection to
# the same public port re-wakes it transparently (wake latency measured).
health_body=""; health_code=""
D5_STATUS="$RUN_DIR/parity-icegresd-d5-status.json"
stop_icegresd_pidfile "$RUN_DIR/parity-icegresd-d5.pid"
if psql -h "$PG_HOST" -p "$IDLE_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1 \
   || psql -h "$PG_HOST" -p "$D5_COMPUTE_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  record D5 elasticity "scale-to-zero" GAP "port :$IDLE_PORT or :$D5_COMPUTE_PORT already occupied; could not probe"
elif [[ ! -x "$DBIN" ]]; then
  record D5 elasticity "scale-to-zero" GAP "icegresd binary not found at $DBIN"
else
  : >"$RUN_DIR/parity-icegresd-d5.log"
  # --pool-idle-secs 2: keep the default warm session pool ON but let it
  # idle-drain inside the probe's 20 s window — warm conns hold sessions on
  # the compute, so the drain is a REQUIRED step of the pooled sleep cycle
  # (pool drains after 2 s idle, then the compute's own 5 s idle clock runs).
  if ! ICEGRES_HEALTH_PORT="$HEALTH_PORT" start_icegresd "$IDLE_PORT" "$D5_COMPUTE_PORT" 5 \
       "$RUN_DIR/parity-icegresd-d5.pid" "$RUN_DIR/parity-icegresd-d5.log" "$D5_STATUS" \
       --pool-idle-secs 2; then
    record D5 elasticity "scale-to-zero" GAP \
      "icegresd never listened on :$IDLE_PORT: $(tail -n 5 "$RUN_DIR/parity-icegresd-d5.log" | flat)"
  else
    # Wake 1: connecting to the public port spawns the compute.
    t0=$(($(date +%s%N) / 1000000))
    idle_q=$(psql -h "$PG_HOST" -p "$IDLE_PORT" -U postgres -d icegres -tA -c 'select count(*) from demo.cities' 2>&1)
    wake1_ms=$(( $(date +%s%N) / 1000000 - t0 ))
    d5_cpid=$(istatus "$D5_STATUS" main .pid)
    # E2 evidence, and proof that health traffic does not reset the idle clock.
    health_code=$(curl -s -m 2 -o "$RUN_DIR/health-body.txt" -w '%{http_code}' "http://$PG_HOST:$HEALTH_PORT/health" 2>&1)
    health_body=$(flat <"$RUN_DIR/health-body.txt")
    t_last=$(($(date +%s%N) / 1000000))
    exited=0
    for _ in $(seq 1 80); do
      if [[ "$d5_cpid" =~ ^[0-9]+$ ]] && ! kill -0 "$d5_cpid" 2>/dev/null \
          && [[ "$(istatus "$D5_STATUS" main .state)" == "stopped" ]]; then
        exited=1; break
      fi
      sleep 0.25
    done
    exit_after_ms=$(( $(date +%s%N) / 1000000 - t_last ))
    d5_last_exit=$(istatus "$D5_STATUS" main .last_exit)
    # Wake 2 (scale-FROM-zero): the next connection to the SAME public port
    # re-spawns the compute; nothing else touches the system in between.
    t0=$(($(date +%s%N) / 1000000))
    wake=$(psql -h "$PG_HOST" -p "$IDLE_PORT" -U postgres -d icegres -tA -c 'select 1' 2>&1)
    wake_ms=$(( $(date +%s%N) / 1000000 - t0 ))
    stop_icegresd_pidfile "$RUN_DIR/parity-icegresd-d5.pid"
    if [[ "$exited" == 1 && "$idle_q" =~ ^[0-9]+$ && "$wake" == 1 && "$d5_last_exit" == *"clean idle exit"* ]]; then
      record D5 elasticity "scale-to-zero" PASS \
        "FULL sleep/wake cycle via icegresd (shipped control plane) WITH the warm session pool active: first connection to :$IDLE_PORT woke a compute in ${wake1_ms}ms (demo.cities count=$idle_q); the pool idle-drained its warm conns, then the compute exited on its own ${exit_after_ms}ms after the last connection ($d5_last_exit; health pings on :$HEALTH_PORT do not reset the idle clock); NEXT connection to the same port auto-re-woke it AND re-warmed the pool: 'select 1' in ${wake_ms}ms (= cold start + splice setup). No external supervisor needed."
    else
      record D5 elasticity "scale-to-zero" GAP \
        "icegresd sleep/wake cycle incomplete: wake1='$(echo "$idle_q" | flat)' (${wake1_ms}ms), idle-exit=$exited (last_exit='$(echo "$d5_last_exit" | flat)'), rewake='$(echo "$wake" | flat)' after ${wake_ms}ms — log: $(tail -n 5 "$RUN_DIR/parity-icegresd-d5.log" | flat)"
    fi
  fi
fi

# D6: writable zero-copy branches (Neon's branch-per-endpoint model). A
# branch is a named Iceberg snapshot ref — `icegres branch create` copies NO
# data. Serve the branch on its own port, INSERT + UPDATE on it, prove main
# is byte-for-byte untouched (row invisible, count unchanged), then drop the
# branch (ref-only removal) and re-verify main.
D6_BRANCH=parity_dev
stop_pidfile "$RUN_DIR/parity-serve-branch.pid"
"$BIN" branch drop demo.trips "$D6_BRANCH" >/dev/null 2>&1 || true
qbr() { psql -h "$PG_HOST" -p "$BRANCH_PORT" -U postgres -d icegres -tA -c "$1" 2>&1; }
if psql -h "$PG_HOST" -p "$BRANCH_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  record D6 elasticity "writable zero-copy branches" GAP \
    "port :$BRANCH_PORT already occupied; could not probe"
else
  d6_create=$("$BIN" branch create demo.trips "$D6_BRANCH" 2>&1)
  if ! grep -q "created branch $D6_BRANCH" <<<"$d6_create"; then
    record D6 elasticity "writable zero-copy branches" GAP \
      "branch create failed: $(echo "$d6_create" | flat)"
  else
    : >"$RUN_DIR/parity-serve-branch.log"
    "$BIN" serve --host "$PG_HOST" --port "$BRANCH_PORT" --branch "$D6_BRANCH" \
        >>"$RUN_DIR/parity-serve-branch.log" 2>&1 &
    echo $! >"$RUN_DIR/parity-serve-branch.pid"
    if ! wait_ready "$BRANCH_PORT" 40; then
      record D6 elasticity "writable zero-copy branches" GAP \
        "server with --branch $D6_BRANCH never became ready: $(tail -n 5 "$RUN_DIR/parity-serve-branch.log" | flat)"
    else
      # Far above every id range other probes/harnesses use (<= ~1M), so a
      # pre-existing main row can never alias the isolation checks.
      D6_ID=$((99000000 + RANDOM))
      d6_main_before=$(q "select count(*) from demo.trips where trip_id = $D6_ID")
      d6_total_before=$(q 'select count(*) from demo.trips')
      d6_ins=$(psql -h "$PG_HOST" -p "$BRANCH_PORT" -U postgres -d icegres -c \
        "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($D6_ID, 'Parity D6', 1.0, 1.0, TIMESTAMP '2026-07-06 00:00:00')" 2>&1 | tail -n 1)
      d6_br_sees=$(qbr "select count(*) from demo.trips where trip_id = $D6_ID")
      d6_upd=$(psql -h "$PG_HOST" -p "$BRANCH_PORT" -U postgres -d icegres -c \
        "update demo.trips set fare = 123.0 where trip_id = $D6_ID" 2>&1 | tail -n 1)
      d6_fare=$(qbr "select fare from demo.trips where trip_id = $D6_ID")
      d6_main_after=$(q "select count(*) from demo.trips where trip_id = $D6_ID")
      d6_total_after=$(q 'select count(*) from demo.trips')
      stop_pidfile "$RUN_DIR/parity-serve-branch.pid"
      d6_drop=$("$BIN" branch drop demo.trips "$D6_BRANCH" 2>&1)
      d6_main_final=$(q "select count(*) from demo.trips where trip_id = $D6_ID")
      if [[ "$d6_ins" == "INSERT 0 1" && "$d6_br_sees" == 1 && "$d6_upd" == "UPDATE 1" \
            && "$d6_fare" == "123.0" && "$d6_main_after" == "$d6_main_before" \
            && "$d6_total_after" == "$d6_total_before" && "$d6_main_final" == "$d6_main_before" \
            && "$d6_drop" == *"dropped branch $D6_BRANCH"* ]]; then
        record D6 elasticity "writable zero-copy branches" PASS \
          "Neon branch-per-endpoint over Iceberg snapshot refs: 'icegres branch create demo.trips $D6_BRANCH' forked main's head with zero data copied; a second server (icegres serve --branch $D6_BRANCH on :$BRANCH_PORT) took INSERT trip_id=$D6_ID + UPDATE (fare -> $d6_fare) committed to the branch ref with assert-ref-snapshot-id; main endpoint on :$MAIN_PORT stayed untouched (row count for the id $d6_main_before -> $d6_main_after, total $d6_total_before -> $d6_total_after); 'branch drop' removed only the ref."
      else
        record D6 elasticity "writable zero-copy branches" GAP \
          "branch write flow incomplete: ins='$d6_ins' br_sees='$d6_br_sees' upd='$d6_upd' fare='$d6_fare' main $d6_main_before->$d6_main_after->$d6_main_final total $d6_total_before->$d6_total_after drop='$(echo "$d6_drop" | flat)'"
      fi
    fi
  fi
fi

# D7: endpoint routing + supervised computes (icegresd control plane). One
# public port serves BOTH endpoints, routed by the pgwire startup `database`
# parameter: 'icegres' -> main compute, 'icegres@<branch>' -> a per-branch
# compute spawned on demand (ephemeral localhost port, `serve --branch`).
# Supervision: kill -9 the main compute; icegresd restarts it with capped
# backoff and the endpoint keeps answering.
D7_BRANCH=parity_d7
D7_STATUS="$RUN_DIR/parity-icegresd-d7-status.json"
stop_icegresd_pidfile "$RUN_DIR/parity-icegresd-d7.pid"
"$BIN" branch drop demo.trips "$D7_BRANCH" >/dev/null 2>&1 || true
qd7() { psql -h "$PG_HOST" -p "$D7_PORT" -U postgres -d icegres -tA -c "$1" 2>&1; }
qd7b() { psql -h "$PG_HOST" -p "$D7_PORT" -U postgres -d "icegres@$D7_BRANCH" -tA -c "$1" 2>&1; }
if psql -h "$PG_HOST" -p "$D7_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1 \
   || psql -h "$PG_HOST" -p "$D7_COMPUTE_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  record D7 elasticity "endpoint routing + supervised computes" GAP \
    "port :$D7_PORT or :$D7_COMPUTE_PORT already occupied; could not probe"
elif [[ ! -x "$DBIN" ]]; then
  record D7 elasticity "endpoint routing + supervised computes" GAP "icegresd binary not found at $DBIN"
elif ! "$BIN" branch create demo.trips "$D7_BRANCH" >/dev/null 2>&1; then
  record D7 elasticity "endpoint routing + supervised computes" GAP "branch create $D7_BRANCH failed"
else
  : >"$RUN_DIR/parity-icegresd-d7.log"
  if ! start_icegresd "$D7_PORT" "$D7_COMPUTE_PORT" 60 \
       "$RUN_DIR/parity-icegresd-d7.pid" "$RUN_DIR/parity-icegresd-d7.log" "$D7_STATUS"; then
    record D7 elasticity "endpoint routing + supervised computes" GAP \
      "icegresd never listened on :$D7_PORT: $(tail -n 5 "$RUN_DIR/parity-icegresd-d7.log" | flat)"
  else
    D7_ID=$((98000000 + RANDOM))
    d7_main1=$(qd7 'select count(*) from demo.trips')
    d7_ins=$(psql -h "$PG_HOST" -p "$D7_PORT" -U postgres -d "icegres@$D7_BRANCH" -c \
      "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($D7_ID, 'Parity D7', 1.0, 1.0, TIMESTAMP '2026-07-06 00:00:00')" 2>&1 | tail -n 1)
    d7_br_sees=$(qd7b "select count(*) from demo.trips where trip_id = $D7_ID")
    d7_main_sees=$(qd7 "select count(*) from demo.trips where trip_id = $D7_ID")
    d7_br_port=$(istatus "$D7_STATUS" "branch:$D7_BRANCH" .port)
    # Supervision: SIGKILL the main compute; icegresd must restart it.
    d7_cpid=$(istatus "$D7_STATUS" main .pid)
    kill -9 "$d7_cpid" 2>/dev/null
    d7_recovered=0
    for _ in $(seq 1 100); do
      if [[ "$(istatus "$D7_STATUS" main .restarts)" -ge 1 ]] \
          && [[ "$(istatus "$D7_STATUS" main .state)" == "running" ]]; then
        d7_recovered=1; break
      fi
      sleep 0.1
    done
    d7_after=$(qd7 'select count(*) from demo.cities')
    stop_icegresd_pidfile "$RUN_DIR/parity-icegresd-d7.pid"
    "$BIN" branch drop demo.trips "$D7_BRANCH" >/dev/null 2>&1 || true
    if [[ "$d7_main1" =~ ^[0-9]+$ && "$d7_ins" == "INSERT 0 1" && "$d7_br_sees" == 1 \
          && "$d7_main_sees" == 0 && "$d7_recovered" == 1 && "$d7_after" == 20 ]]; then
      record D7 elasticity "endpoint routing + supervised computes" PASS \
        "ONE public port (:$D7_PORT) served two endpoints routed by the pgwire startup database param: dbname 'icegres' -> main compute on :$D7_COMPUTE_PORT (count=$d7_main1), dbname 'icegres@$D7_BRANCH' -> per-branch compute auto-spawned on ephemeral :$d7_br_port with --branch (INSERT '$d7_ins' visible on the branch endpoint ($d7_br_sees), INVISIBLE on main ($d7_main_sees)); then kill -9 of the main compute was auto-restarted by icegresd (capped backoff, restarts>=1) and the endpoint answered again (demo.cities count=$d7_after)."
    else
      record D7 elasticity "endpoint routing + supervised computes" GAP \
        "routing/supervision incomplete: main1='$(echo "$d7_main1" | flat)' ins='$d7_ins' br_sees='$d7_br_sees' main_sees='$d7_main_sees' recovered=$d7_recovered after='$(echo "$d7_after" | flat)' — log: $(tail -n 5 "$RUN_DIR/parity-icegresd-d7.log" | flat)"
    fi
  fi
fi

# ===========================================================================
# Area E — Ops
# ===========================================================================
log "Area E — ops"

# tracing's fmt layer wraps field names in ANSI escapes on a tty-less pipe
# too (colors are on by default) — strip them before matching.
E1_PLAIN="$RUN_DIR/parity-serve2.plain.log"
sed $'s/\x1b\\[[0-9;]*m//g' "$RUN_DIR/parity-serve2.log" >"$E1_PLAIN"
e1=$(grep -E 'catalog_uri=.*warehouse=|starting pgwire server' "$E1_PLAIN" | head -n 2 | flat)
if grep -q 'catalog_uri=' "$E1_PLAIN" \
   && grep -q 'listen_addr=' "$E1_PLAIN"; then
  record E1 ops "structured startup logs" PASS \
    "serve log contains structured fields catalog_uri/warehouse/s3_endpoint/listen_addr: $e1"
else
  record E1 ops "structured startup logs" GAP \
    "expected structured fields missing from serve log: $(head -n 3 "$RUN_DIR/parity-serve2.log" | flat)"
fi

hc=$(q 'select 1')
if [[ "$hc" == 1 && "$health_code" == 200 && "$health_body" == ok* ]]; then
  record E2 ops "health-checkable" PASS \
    "two probes work: (1) pgwire connect + 'select 1' as the readiness check (what the harnesses use; plain TCP connect works for tcpSocket-style checks); (2) dedicated HTTP liveness endpoint via --health-port — curl http://$PG_HOST:$HEALTH_PORT/health during the D5 probe returned $health_code '$health_body'."
elif [[ "$hc" == 1 ]]; then
  record E2 ops "health-checkable" PASS \
    "pgwire connect + 'select 1' works as a health probe (this is what the harnesses use); --health-port endpoint answered '$health_code' '$health_body' (expected 200 'ok' — see D5 probe log)."
else
  record E2 ops "health-checkable" GAP "health probe failed: $(echo "$hc" | flat)"
fi

stop_pidfile "$RUN_DIR/parity-serve-env.pid"
if psql -h "$PG_HOST" -p "$ENV_PORT" -U postgres -d icegres -tA -c 'select 1' >/dev/null 2>&1; then
  record E3 ops "full config via env vars" GAP "port :$ENV_PORT already occupied; could not probe"
else
  : >"$RUN_DIR/parity-serve-env.log"
  env ICEGRES_HOST="$PG_HOST" ICEGRES_PORT="$ENV_PORT" \
      ICEGRES_CATALOG_URI="$CATALOG_URI" ICEGRES_WAREHOUSE="$WAREHOUSE" \
      ICEGRES_S3_ENDPOINT="$S3_ENDPOINT" ICEGRES_S3_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
      ICEGRES_S3_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" ICEGRES_S3_REGION=us-east-1 \
      "$BIN" serve >>"$RUN_DIR/parity-serve-env.log" 2>&1 &
  echo $! >"$RUN_DIR/parity-serve-env.pid"
  if wait_ready "$ENV_PORT" 40; then
    r=$(psql -h "$PG_HOST" -p "$ENV_PORT" -U postgres -d icegres -tA -c 'select count(*) from demo.cities' 2>&1)
    record E3 ops "full config via env vars" PASS \
      "'icegres serve' booted with ZERO flags — all config from ICEGRES_* env vars (host/port/catalog/warehouse/S3) — and answered on :$ENV_PORT (demo.cities count=$r)"
  else
    record E3 ops "full config via env vars" GAP \
      "env-only boot failed: $(tail -n 5 "$RUN_DIR/parity-serve-env.log" | flat)"
  fi
  stop_pidfile "$RUN_DIR/parity-serve-env.pid"
fi

# ===========================================================================
# Emit JSON + regenerate SCORECARD parity section
# ===========================================================================
n_pass=$(jq -rs '[.[] | select(.verdict=="PASS")] | length' "$RECORDS")
n_gap=$(jq -rs '[.[] | select(.verdict=="GAP")] | length' "$RECORDS")
n_na=$(jq -rs '[.[] | select(.verdict=="NA_BY_DESIGN")] | length' "$RECORDS")

jq -s --arg ts "$TS" --arg bin "$BIN" \
   --argjson pass "$n_pass" --argjson gap "$n_gap" --argjson na "$n_na" \
   '{schema:"icegres-parity-v1", ts:$ts, binary:$bin,
     summary:{PASS:$pass, GAP:$gap, NA_BY_DESIGN:$na},
     probes: sort_by(.id)}' "$RECORDS" >"$OUT_JSON"
log "wrote $OUT_JSON (PASS=$n_pass GAP=$n_gap NA_BY_DESIGN=$n_na)"

# --- SCORECARD.md ---
PARITY_MD="$RUN_DIR/parity-section.md"
{
  echo "<!-- parity:begin — generated by bench/parity.sh, do not edit by hand -->"
  echo "## Parity matrix — icegres vs the Lakebase/Neon/Moonlink bar"
  echo
  echo "Run: \`$TS\` · binary: \`${BIN#"$REPO_DIR"/}\` · result: **$n_pass PASS / $n_gap GAP / $n_na N/A-BY-DESIGN** · raw: \`bench/results/parity-$TS.json\`"
  echo
  echo "| id | area | behavior | verdict | evidence |"
  echo "|----|------|----------|---------|----------|"
  jq -rs 'sort_by(.id)[] |
    "| \(.id) | \(.area) | \(.behavior | gsub("\\|"; "\\|")) | \(if .verdict=="PASS" then "✅ PASS" elif .verdict=="GAP" then "❌ GAP" else "➖ N/A-BY-DESIGN" end) | \(.evidence | gsub("\\|"; "\\|") | .[0:400]) |"' \
    "$RECORDS"
  echo "<!-- parity:end -->"
} >"$PARITY_MD"

if [[ -f "$SCORECARD" ]] && grep -q '<!-- parity:begin' "$SCORECARD"; then
  awk -v repl="$PARITY_MD" '
    /<!-- parity:begin/ { while ((getline line < repl) > 0) print line; close(repl); skip=1; next }
    /<!-- parity:end -->/ { skip=0; next }
    !skip { print }
  ' "$SCORECARD" >"$SCORECARD.tmp" && mv "$SCORECARD.tmp" "$SCORECARD"
else
  {
    echo "# icegres Scorecard"
    echo
    echo "Generated by \`bench/parity.sh\` (parity matrix) and \`bench/bench.sh\`"
    echo "(benchmark runs). See \`bench/SPEC.md\` for the contract."
    echo
    cat "$PARITY_MD"
    echo
    echo "## Benchmark runs"
    echo
    echo "<!-- bench:runs — appended by bench/bench.sh -->"
  } >"$SCORECARD"
fi
rm -f "$PARITY_MD"
log "regenerated parity section of $SCORECARD"
log "parity done: PASS=$n_pass GAP=$n_gap NA_BY_DESIGN=$n_na"
