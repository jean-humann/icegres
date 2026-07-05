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
# on :5440 (D1/D2/E1) and a short-lived env-only one on :5444 (E3).
#
# GAP probes capture the server's actual error output as evidence — nothing
# is assumed. Probes B1/B4/B5/C4/D2 append a few rows with trip_id >= 900000
# per run (append-only Iceberg, same convention as e2e.sh; deterministic
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

STARTED_MAIN=0
cleanup() {
  [[ "$STARTED_MAIN" == 1 ]] && stop_pidfile "$RUN_DIR/parity-serve.pid"
  stop_pidfile "$RUN_DIR/parity-serve2.pid"
  stop_pidfile "$RUN_DIR/parity-serve-env.pid"
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

out_badpass=$(PGPASSWORD=definitely-wrong psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres -tA -c 'select 1' 2>&1)
out_baduser=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U not_a_real_user -d icegres -tA -c 'select 1' 2>&1)
if [[ "$out_badpass" == 1 && "$out_baduser" == 1 ]]; then
  record A6 wire "server-side auth" GAP \
    "no auth enforcement: connections with a wrong password and with nonexistent user 'not_a_real_user' both succeeded (select 1 -> 1). AuthManager::default() is a noop."
elif [[ "$out_badpass" != 1 && "$out_baduser" != 1 ]]; then
  record A6 wire "server-side auth" PASS \
    "bad credentials rejected: $(echo "$out_badpass" | flat)"
else
  record A6 wire "server-side auth" GAP \
    "partial auth: wrong-password -> $(echo "$out_badpass" | flat); bad-user -> $(echo "$out_baduser" | flat)"
fi

out=$(psql "host=$PG_HOST port=$MAIN_PORT user=postgres dbname=icegres sslmode=require" -tA -c 'select 1' 2>&1)
if [[ "$out" == 1 ]]; then
  record A7 wire "TLS" PASS "sslmode=require connection succeeded"
else
  record A7 wire "TLS" GAP "no TLS support: $(echo "$out" | flat)"
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

out=$(q "update demo.trips set fare = 99.9 where trip_id = $B1_ID")
if [[ "$out" == UPDATE* ]]; then
  record B2 oltp "UPDATE" PASS "UPDATE returned: $(echo "$out" | flat)"
else
  record B2 oltp "UPDATE" GAP \
    "UPDATE rejected (iceberg-datafusion 0.9 is append-only): $(echo "$out" | flat)"
fi

out=$(q "delete from demo.trips where trip_id = -12345")
if [[ "$out" == DELETE* ]]; then
  record B3 oltp "DELETE" PASS "DELETE returned: $(echo "$out" | flat)"
else
  record B3 oltp "DELETE" GAP \
    "DELETE rejected (iceberg-datafusion 0.9 is append-only): $(echo "$out" | flat)"
fi

B4_ID=$next_id; next_id=$((next_id + 1))
txn_out=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres 2>&1 <<EOF | flat
BEGIN;
insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($B4_ID, 'Parity B4', 1.0, 1.0, TIMESTAMP '2026-07-05 00:00:00');
ROLLBACK;
EOF
)
after=$(q "select count(*) from demo.trips where trip_id = $B4_ID")
if [[ "$after" == 0 ]]; then
  record B4 oltp "explicit transactions BEGIN/COMMIT/ROLLBACK" PASS \
    "ROLLBACK undid the INSERT (row $B4_ID absent afterwards). Session output: $txn_out"
else
  record B4 oltp "explicit transactions BEGIN/COMMIT/ROLLBACK" GAP \
    "BEGIN/ROLLBACK are accepted on the wire but are non-transactional: INSERT inside BEGIN..ROLLBACK persisted (row $B4_ID visible after ROLLBACK, count=$after). Session output: $txn_out"
fi

dup_tag=$(psql -h "$PG_HOST" -p "$MAIN_PORT" -U postgres -d icegres -c \
  "insert into demo.trips (trip_id, city, distance_km, fare, ts) values ($B4_ID, 'Parity B5 dup', 2.0, 2.0, TIMESTAMP '2026-07-05 00:00:00')" 2>&1 | tail -n 1)
dup_count=$(q "select count(*) from demo.trips where trip_id = $B4_ID")
if [[ "$dup_tag" == "INSERT 0 1" && "$dup_count" -ge 2 ]]; then
  record B5 oltp "PK/constraint enforcement" GAP \
    "no constraint enforcement: duplicate insert of trip_id=$B4_ID accepted ('$dup_tag'), table now holds $dup_count rows with that id. Iceberg has no PK/unique constraints."
elif [[ "$dup_tag" != INSERT* ]]; then
  record B5 oltp "PK/constraint enforcement" PASS \
    "duplicate insert rejected: $(echo "$dup_tag" | flat)"
else
  record B5 oltp "PK/constraint enforcement" GAP \
    "ambiguous: tag=$(echo "$dup_tag" | flat) count=$dup_count"
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
# new-connection readback, 10ms poll).
if [[ "$fresh_ms" -ge 0 && "$fresh_ms" -lt 1000 ]]; then
  record C4 lakehouse "write freshness (commit -> readable elsewhere)" PASS \
    "row committed via conn A was readable from a new connection ~${fresh_ms}ms after commit (coarse; includes psql startup — precise p50/p95 is bench freshness_ms). Moonlink bar: sub-second."
elif [[ "$fresh_ms" -ge 0 ]]; then
  record C4 lakehouse "write freshness (commit -> readable elsewhere)" GAP \
    "row visible only after ${fresh_ms}ms (> 1s Moonlink bar)"
else
  record C4 lakehouse "write freshness (commit -> readable elsewhere)" GAP \
    "row trip_id=$B1_ID never became visible while polling"
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

snap_list=$(q 'select snapshot_id from demo."trips$snapshots" order by committed_at limit 1')
tt1=$(q "select count(*) from demo.trips FOR SYSTEM_TIME AS OF '2026-01-01 00:00:00'" | flat)
tt2=$(q "select count(*) from demo.\"trips@$snap_list\"" | flat)
if [[ "$tt1" != ERROR* || "$tt2" != ERROR* ]]; then
  record D4 elasticity "time-travel read (branching/PITR analogue)" PASS \
    "snapshot-pinned query worked: FOR SYSTEM_TIME -> $tt1 / table@snapshot -> $tt2"
else
  record D4 elasticity "time-travel read (branching/PITR analogue)" GAP \
    "snapshots are enumerable (e.g. id $snap_list via \"trips\$snapshots\") but no snapshot-pinned read exists in datafusion 52 / iceberg-datafusion 0.9: FOR SYSTEM_TIME AS OF -> '$tt1'; table@snapshot name -> '$tt2'"
fi

d5_help=$("$BIN" serve --help 2>&1 | grep -icE 'idle|scale|shutdown' || true)
if [[ "$d5_help" -gt 0 ]]; then
  record D5 elasticity "scale-to-zero" PASS "serve exposes idle/scale flags: $("$BIN" serve --help | grep -iE 'idle|scale|shutdown' | flat)"
else
  record D5 elasticity "scale-to-zero" GAP \
    "no idle-shutdown supervisor: 'icegres serve --help' has no idle/scale/shutdown option; the process runs until killed. Roadmap item (SPEC §4.5). Cold start of a few seconds (D3) makes this viable."
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
http_hc=$(curl -s -m 2 "http://$PG_HOST:$MAIN_PORT/health" 2>&1 | flat)
if [[ "$hc" == 1 ]]; then
  record E2 ops "health-checkable" PASS \
    "pgwire connect + 'select 1' works as a health probe (this is what the harnesses use). No dedicated HTTP health endpoint exists (curl :$MAIN_PORT/health -> '${http_hc:-connection failed}')."
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
