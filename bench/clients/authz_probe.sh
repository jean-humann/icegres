#!/usr/bin/env bash
# A12 authorization probe (bench/SPEC.md A12): starts a dedicated icegres
# server with SCRAM auth (--auth-file) AND Lakekeeper-style ReBAC authorization
# (--authz-file), then verifies the enforcement matrix over the wire with psql:
# namespace-grant inheritance, table-scoped grants, warehouse ownership, roles,
# per-statement 42501 denial, JOIN checks every table, and pg_catalog metadata
# staying free. Cleans up its server and temp files on exit.
#
# Self-contained: writes its own auth/policy files, runs against the shared
# catalog (ICEGRES_* env, same defaults as `icegres serve`). Skips gracefully
# (exit 3) when psql is unavailable.
#
# Exit: 0 = all green (fail=0), 1 = failures, 2 = infra error, 3 = skip.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="${ICEGRES_BIN:-$REPO_DIR/icegres/target/release/icegres}"
PORT="${ICEGRES_AUTHZ_PORT:-5448}"
HOST=127.0.0.1

if ! command -v psql >/dev/null 2>&1; then
  echo "A12 SKIP: psql not available" >&2
  echo "A12 RESULT: pass=0 fail=0 skip=1"
  exit 3
fi
if [[ ! -x "$BIN" ]]; then
  echo "A12 SKIP: icegres binary not built ($BIN)" >&2
  echo "A12 RESULT: pass=0 fail=0 skip=1"
  exit 3
fi

WORK="$(mktemp -d)"
USERS="$WORK/users"
POLICY="$WORK/policy"
LOG="$WORK/serve.log"
PIDFILE="$WORK/serve.pid"

cat > "$USERS" <<'EOF'
alice:secret1
bob:secret2
admin:secret3
EOF
# Lakekeeper-style grants: read/write/own on warehouse(*)/namespace/table,
# inherited down the hierarchy; roles with membership.
cat > "$POLICY" <<'EOF'
grant analyst read demo
grant writer  write demo.trips
grant admin   own  *
member alice analyst
member bob   writer
EOF

cleanup() {
  if [[ -f "$PIDFILE" ]]; then
    local pid; pid=$(cat "$PIDFILE" 2>/dev/null || true)
    if [[ -n "${pid:-}" ]] && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegres ]]; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 10); do kill -0 "$pid" 2>/dev/null || break; sleep 0.3; done
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

port_open() { bash -c "exec 3<>/dev/tcp/$HOST/$PORT" 2>/dev/null; }
if port_open; then
  echo "A12 SKIP: port $PORT already in use" >&2
  echo "A12 RESULT: pass=0 fail=0 skip=1"
  exit 3
fi

"$BIN" serve --host "$HOST" --port "$PORT" \
  --auth-file "$USERS" --authz-file "$POLICY" >"$LOG" 2>&1 &
echo $! > "$PIDFILE"

up=0
for _ in $(seq 1 40); do
  port_open && { up=1; break; }
  kill -0 "$(cat "$PIDFILE")" 2>/dev/null || break
  sleep 0.5
done
if [[ "$up" != 1 ]]; then
  echo "A12 ERROR: server did not start" >&2
  tail -5 "$LOG" >&2
  echo "A12 RESULT: pass=0 fail=1 skip=0"
  exit 2
fi

PASS=0
FAIL=0
q() { # user password sql   -> stdout (errors folded in)
  PGPASSWORD="$2" psql -h "$HOST" -p "$PORT" -U "$1" -d icegres -w -tAc "$3" 2>&1
}
check() { # description  expect-regex  actual
  if echo "$3" | grep -qE "$2"; then
    echo "    PASS authz: $1"; PASS=$((PASS + 1))
  else
    echo "    FAIL authz: $1 -- got: $(echo "$3" | head -1)"; FAIL=$((FAIL + 1))
  fi
}

check "alice (read demo ns) SELECT trips allowed" '^[0-9]+$' "$(q alice secret1 'select count(*) from demo.trips')"
check "alice INSERT denied (42501)" '42501|permission denied' "$(q alice secret1 "insert into demo.trips values (990001,'x',1,2,timestamp '2026-01-01')")"
check "alice SELECT cities allowed (namespace grant inherits)" '^[0-9]+$' "$(q alice secret1 'select count(*) from demo.cities')"
check "bob (write demo.trips) INSERT allowed" 'INSERT 0 1' "$(q bob secret2 "insert into demo.trips values (990002,'x',1,2,timestamp '2026-01-01')")"
check "bob SELECT cities denied (only trips granted)" '42501|permission denied' "$(q bob secret2 'select count(*) from demo.cities')"
check "bob JOIN trips+cities denied on cities" '42501|permission denied' "$(q bob secret2 'select count(*) from demo.trips t join demo.cities c on t.city=c.city')"
check "admin (own warehouse) reads both tables" '^[0-9]+$' "$(q admin secret3 'select (select count(*) from demo.trips)+(select count(*) from demo.cities)')"
check "pg_catalog/information_schema metadata is free" '^[0-9]+$' "$(q alice secret1 "select count(*) from information_schema.tables where table_schema='demo'")"
# cleanup the row bob inserted
q bob secret2 'delete from demo.trips where trip_id=990002' >/dev/null 2>&1 || true

echo "A12 RESULT: pass=$PASS fail=$FAIL skip=0"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
