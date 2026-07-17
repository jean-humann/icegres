#!/usr/bin/env bash
# Durable-tail durability contract (--tail-dir, icegres/src/tail.rs): with a
# local WAL tail attached to buffered-write mode, an acked-but-UNFLUSHED row
# SURVIVES an unclean kill (SIGKILL) — the inversion of the documented loss
# window — while plain buffered mode (no --tail-dir) still loses it (the
# contrast case, proving the tail is what closes the window).
#
#   bash icegres/tests/tail_durability.sh
#
# What it proves, end to end:
#   1. --tail-dir without --write-buffer-ms is refused at boot (fail loudly).
#   2. On a tailed server with a 10-minute flush cadence (so the background
#      flusher can never commit within the test), acked INSERTs are readable
#      via the union view but NOT committed; after kill -9 + restart with the
#      same --tail-dir, the log reports a tail replay and every acked row is
#      back (pending again, then committed by a fence-forced flush).
#   3. Exactly-once across the crash: after the flush commits the rows (and
#      stamps the icegres.tail-seq.<tail-id> watermark), ANOTHER kill -9 +
#      restart replays nothing and the count stays exact — no double-apply.
#   4. Sequence-floor regression (post-flush restart): after the watermark is
#      stamped and the tail truncated, a restart + NEW inserts + kill -9 must
#      keep BOTH generations — the new rows' sequences must not restart under
#      the persisted watermark (or the second replay would drop them).
#   5. Contrast: the same kill -9 sequence WITHOUT --tail-dir loses the acked
#      rows (today's documented behavior, unchanged).
#   6-8. The POSTGRES tail backend (--tail-url, icegres/src/tail_pg.rs) holds
#      the same contract with the tail living in the stack's Postgres
#      (database icegres_test, schema icegres_tail): distinct startup WARN,
#      frames durable in the tail database, kill -9 recovery (6); flush +
#      watermark exactly-once across a second crash, sidecar row written,
#      covered frames truncated (7); post-flush-restart sequence floor (8).
#      Section 1 also proves --tail-url without buffered writes and
#      --tail-dir + --tail-url together are refused at boot.
#   10. The QUORUM tail backend (--tail-quorum, icegres/src/tail_quorum.rs +
#      src/quorum/ + icekeeperd): three acceptor daemons on 5471-5473; the
#      consensus-class durability contract — distinct startup WARN; acked
#      rows survive kill -9 of the COMPUTE (replay from the acceptors);
#      writes CONTINUE after kill -9 of ONE ACCEPTOR (quorum 2/3);
#      exactly-once across commit -> kill -9 -> replay (watermark records
#      honored); FENCING: a second icegres on the same quorum supersedes the
#      first (its next INSERT fails with "superseded by a newer server"
#      while the second serves — and recovers the first's unflushed acked
#      rows). Section 1 also proves --tail-quorum without buffered writes
#      and --tail-quorum + --tail-dir together are refused at boot.
#
# Non-destructive, self-contained, idempotent: creates/purges its own scratch
# table demo.tail_durability_scratch via the REST catalog (never touches
# demo.trips), its own tail dir under .e2e, its own icegres_tail schema in
# icegres_test, and its own icekeeper data dirs under .e2e. Uses port 5456
# for the main server, 5458 for the fencing second server, and 5471-5473 for
# the icekeeperd acceptors (left free: 5452 belongs to bench/parity.sh D7,
# 5455 to e2e.sh THR_PORT, 5457 to e2e.sh KY_PORT, 5459 to e2e.sh FR_PORT).
# Standalone by design — NOT wired into e2e.sh (keep the gate stable).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ICEGRES_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$ICEGRES_DIR")"
RUN_DIR="$ICEGRES_DIR/.e2e"
BIN="${BIN:-$ICEGRES_DIR/target/release/icegres}"

PG_HOST=127.0.0.1
PG_PORT=5456
PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d icegres -v ON_ERROR_STOP=1)
export PGCONNECT_TIMEOUT=5

# This harness owns its server config: a stray environment must not flip it.
unset ICEGRES_AUTH_FILE ICEGRES_TLS_CERT ICEGRES_TLS_KEY
unset ICEGRES_WRITE_BUFFER_MS ICEGRES_WRITE_BUFFER_MAX_ROWS ICEGRES_TAIL_DIR
unset ICEGRES_TAIL_URL ICEGRES_TAIL_QUORUM ICEGRES_TAIL_QUORUM_TIMEOUT_MS
unset ICEKEEPER_HOST ICEKEEPER_PORT ICEKEEPER_DATA_DIR ICEKEEPER_NODE_ID

CATALOG_URI="http://127.0.0.1:8181/catalog"
WAREHOUSE=lakehouse
TABLE=tail_durability_scratch

# 10 minutes: the background flusher can never auto-commit during the test,
# so the ONLY paths to the lake are the mechanisms under test (tail replay +
# fence-forced flush) — or nothing at all (the contrast case).
BUF_MS=600000

TAIL_DIR="$RUN_DIR/tail-durability-wal"
SERVE_LOG="$RUN_DIR/tail-durability-serve.log"
PID_FILE="$RUN_DIR/tail-durability.pid"
mkdir -p "$RUN_DIR"

# The Postgres tail backend (--tail-url, sections 6-8): the stack's own
# Postgres (infra/scripts/pg-start.sh creates icegres_test OWNED by the
# lakekeeper role), schema icegres_tail auto-created by the server.
TAIL_PG_URL="postgresql://lakekeeper:lakekeeper@127.0.0.1:5433/icegres_test"
pg_tail() { psql "$TAIL_PG_URL" -v ON_ERROR_STOP=1 -tA -c "$1"; }
drop_pg_tail_schema() {
  psql "$TAIL_PG_URL" -c 'DROP SCHEMA IF EXISTS icegres_tail CASCADE' \
    >/dev/null 2>&1 || true
}

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

q() { "${PSQL[@]}" -tA -c "$1"; }

strip_ansi() { sed 's/\x1b\[[0-9;]*m//g'; }

catalog_prefix() {
  curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" | jq -r '.defaults.prefix'
}

drop_scratch() {
  local prefix="$1"
  curl -sf -X DELETE \
    "$CATALOG_URI/v1/$prefix/namespaces/demo/tables/$TABLE?purgeRequested=true" \
    >/dev/null 2>&1 || true
}

create_scratch() {
  local prefix="$1"
  curl -sf -X POST "$CATALOG_URI/v1/$prefix/namespaces/demo/tables" \
    -H 'Content-Type: application/json' -d @- <<'JSON' >/dev/null
{
  "name": "tail_durability_scratch",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "id", "required": false, "type": "long"},
      {"id": 2, "name": "note", "required": false, "type": "string"}
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

# kill_9: the unclean kill under test (never through stop_server's TERM,
# which would trigger the clean-shutdown flush and defeat the point).
kill_9() {
  local pid; pid=$(cat "$PID_FILE")
  kill -9 "$pid" 2>/dev/null || fail "could not SIGKILL server (pid $pid)"
  for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
  kill -0 "$pid" 2>/dev/null && fail "server survived SIGKILL"
  rm -f "$PID_FILE"
}

# start_server [extra serve flags...]
start_server() {
  stop_server
  "$BIN" serve --host "$PG_HOST" --port "$PG_PORT" --write-buffer-ms "$BUF_MS" "$@" \
    >>"$SERVE_LOG" 2>&1 &
  echo $! >"$PID_FILE"
  for _ in $(seq 1 60); do
    q "select 1" >/dev/null 2>&1 && return 0
    kill -0 "$(cat "$PID_FILE")" 2>/dev/null \
      || { tail -n 30 "$SERVE_LOG" >&2; fail "server exited during startup"; }
    sleep 0.5
  done
  tail -n 30 "$SERVE_LOG" >&2
  fail "server not ready on :$PG_PORT within 30s"
}

# --------------------------- quorum tail helpers ---------------------------
# Three icekeeperd acceptors (section 10) on ports 5471-5473, data dirs and
# pid files under RUN_DIR. Identity-checked like stop_server.
KBIN="${ICEKEEPERD:-$ICEGRES_DIR/target/release/icekeeperd}"
KEEPER_PORTS=(5471 5472 5473)
KEEPER_BASE="$RUN_DIR/icekeeper"
QUORUM_ADDRS="127.0.0.1:5471,127.0.0.1:5472,127.0.0.1:5473"

keeper_ready() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && exec 3>&- 3<&-; }

start_keeper() { # start_keeper <n: 1..3>
  local n=$1 port=${KEEPER_PORTS[$((n - 1))]}
  "$KBIN" serve --host 127.0.0.1 --port "$port" \
    --data-dir "$KEEPER_BASE-$n" --node-id "$n" \
    >>"$KEEPER_BASE-$n.log" 2>&1 &
  echo $! >"$KEEPER_BASE-$n.pid"
  for _ in $(seq 1 40); do
    keeper_ready "$port" && return 0
    kill -0 "$(cat "$KEEPER_BASE-$n.pid")" 2>/dev/null \
      || { tail -n 30 "$KEEPER_BASE-$n.log" >&2; fail "icekeeperd $n exited during startup"; }
    sleep 0.25
  done
  tail -n 30 "$KEEPER_BASE-$n.log" >&2
  fail "icekeeperd $n not accepting on :$port within 10s"
}

stop_keeper() { # graceful, identity-checked
  local n=$1 pidfile="$KEEPER_BASE-$1.pid" pid
  if [[ -f "$pidfile" ]]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icekeeperd ]]; then
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

kill_keeper_9() { # the unclean acceptor kill under test
  local n=$1 pid
  pid=$(cat "$KEEPER_BASE-$n.pid")
  kill -9 "$pid" 2>/dev/null || fail "could not SIGKILL icekeeperd $n (pid $pid)"
  for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
  kill -0 "$pid" 2>/dev/null && fail "icekeeperd $n survived SIGKILL"
  rm -f "$KEEPER_BASE-$n.pid"
}

# The fencing second server (section 10e) on its own port + pid file.
PG_PORT2=5458
PID_FILE2="$RUN_DIR/tail-durability-2.pid"
SERVE_LOG2="$RUN_DIR/tail-durability-serve-2.log"
PSQL2=(psql -h "$PG_HOST" -p "$PG_PORT2" -U postgres -d icegres -v ON_ERROR_STOP=1)
q2() { "${PSQL2[@]}" -tA -c "$1"; }

stop_server2() {
  if [[ -f "$PID_FILE2" ]]; then
    local pid; pid=$(cat "$PID_FILE2")
    if kill -0 "$pid" 2>/dev/null \
        && [[ "$(ps -o comm= -p "$pid" 2>/dev/null)" == icegres ]]; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE2"
  fi
}

PREFIX=""
cleanup() {
  stop_server
  stop_server2
  for n in 1 2 3; do stop_keeper "$n"; done
  [[ -n "$PREFIX" ]] && drop_scratch "$PREFIX"
  [[ -n "$PREFIX" ]] && curl -sf -X DELETE \
    "$CATALOG_URI/v1/$PREFIX/namespaces/demo/tables/tail_keyed_scratch?purgeRequested=true" \
    >/dev/null 2>&1 || true
  rm -rf "$TAIL_DIR" "$RUN_DIR/tail-keyed-wal" \
    "$KEEPER_BASE-1" "$KEEPER_BASE-2" "$KEEPER_BASE-3"
  drop_pg_tail_schema
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 0. Stack + binary + port
# ---------------------------------------------------------------------------
log "checking lakehouse stack"
if ! curl -sf "$CATALOG_URI/v1/config?warehouse=$WAREHOUSE" >/dev/null; then
  bash "$REPO_DIR/infra/scripts/up.sh" >"$RUN_DIR/tail-durability-up.log" 2>&1 \
    || fail "infra/scripts/up.sh failed (log: $RUN_DIR/tail-durability-up.log)"
fi
[[ -x "$BIN" ]] || fail "binary not found at $BIN (build with: cargo build --release)"
PREFIX=$(catalog_prefix)
[[ -n "$PREFIX" && "$PREFIX" != null ]] || fail "could not resolve catalog prefix"
if q "select 1" >/dev/null 2>&1; then
  fail "something else is listening on $PG_HOST:$PG_PORT — stop it first"
fi
pass "stack healthy (prefix $PREFIX), binary $BIN, port :$PG_PORT free"

drop_scratch "$PREFIX"
create_scratch "$PREFIX" || fail "could not create demo.$TABLE via REST catalog"
rm -rf "$TAIL_DIR"
: >"$SERVE_LOG"

# ---------------------------------------------------------------------------
# 1. --tail-dir without buffered writes is refused at boot
# ---------------------------------------------------------------------------
log "(1) --tail-dir without --write-buffer-ms fails loudly"
# timeout guards a regression: if the flag were accepted, the server would
# sit listening forever instead of failing this assertion.
set +e
noop_out=$(timeout 10 "$BIN" serve --host "$PG_HOST" --port "$PG_PORT" \
  --write-buffer-ms 0 --tail-dir "$TAIL_DIR" 2>&1)
noop_rc=$?
set -e
[[ $noop_rc -ne 0 ]] || fail "--tail-dir with --write-buffer-ms 0 was accepted"
grep -q "tail-dir requires buffered writes" <<<"$noop_out" \
  || fail "unexpected refusal message: $noop_out"
pass "--tail-dir without buffered writes refused at boot (exit $noop_rc)"

log "(1b) --tail-url without --write-buffer-ms fails loudly"
set +e
noop_out=$(timeout 10 "$BIN" serve --host "$PG_HOST" --port "$PG_PORT" \
  --write-buffer-ms 0 --tail-url "$TAIL_PG_URL" 2>&1)
noop_rc=$?
set -e
[[ $noop_rc -ne 0 ]] || fail "--tail-url with --write-buffer-ms 0 was accepted"
grep -q "tail-url requires buffered writes" <<<"$noop_out" \
  || fail "unexpected refusal message: $noop_out"
pass "--tail-url without buffered writes refused at boot (exit $noop_rc)"

log "(1c) --tail-dir and --tail-url together are refused (one process, ONE tail)"
set +e
noop_out=$(timeout 10 "$BIN" serve --host "$PG_HOST" --port "$PG_PORT" \
  --write-buffer-ms "$BUF_MS" --tail-dir "$TAIL_DIR" --tail-url "$TAIL_PG_URL" 2>&1)
noop_rc=$?
set -e
[[ $noop_rc -ne 0 ]] || fail "--tail-dir together with --tail-url was accepted"
grep -q "cannot be used with" <<<"$noop_out" \
  || fail "unexpected refusal message: $noop_out"
pass "--tail-dir + --tail-url refused at boot (exit $noop_rc)"
rm -rf "$TAIL_DIR"

log "(1d) --tail-quorum without --write-buffer-ms fails loudly"
set +e
noop_out=$(timeout 10 "$BIN" serve --host "$PG_HOST" --port "$PG_PORT" \
  --write-buffer-ms 0 --tail-quorum "$QUORUM_ADDRS" 2>&1)
noop_rc=$?
set -e
[[ $noop_rc -ne 0 ]] || fail "--tail-quorum with --write-buffer-ms 0 was accepted"
grep -q "tail-quorum requires buffered writes" <<<"$noop_out" \
  || fail "unexpected refusal message: $noop_out"
pass "--tail-quorum without buffered writes refused at boot (exit $noop_rc)"

log "(1e) --tail-quorum and --tail-dir together are refused (one process, ONE tail)"
set +e
noop_out=$(timeout 10 "$BIN" serve --host "$PG_HOST" --port "$PG_PORT" \
  --write-buffer-ms "$BUF_MS" --tail-quorum "$QUORUM_ADDRS" --tail-dir "$TAIL_DIR" 2>&1)
noop_rc=$?
set -e
[[ $noop_rc -ne 0 ]] || fail "--tail-quorum together with --tail-dir was accepted"
grep -q "cannot be used with" <<<"$noop_out" \
  || fail "unexpected refusal message: $noop_out"
pass "--tail-quorum + --tail-dir refused at boot (exit $noop_rc)"
rm -rf "$TAIL_DIR"

# ---------------------------------------------------------------------------
# 2. Tail replay: acked-but-unflushed rows SURVIVE kill -9
# ---------------------------------------------------------------------------
log "(2) tailed server: acked rows survive an unclean kill"
start_server --tail-dir "$TAIL_DIR"
strip_ansi <"$SERVE_LOG" | grep -q "durable local tail" \
  || fail "startup WARN does not announce the durable tail (log: $SERVE_LOG)"
pass "startup WARN announces the durable-tail variant"

for i in 1 2 3; do
  q "insert into demo.$TABLE (id, note) values ($i, 'tail-survivor')" >/dev/null \
    || fail "tailed INSERT $i failed"
done
assert_eq "acked rows readable via the union view (buffered, uncommitted)" "3" \
  "$(q "select count(*) from demo.$TABLE")"
ls "$TAIL_DIR"/demo.$TABLE/*.seg >/dev/null 2>&1 \
  || fail "no tail segments on disk after acked INSERTs ($TAIL_DIR)"
pass "tail segments exist on disk before the kill"

kill_9
start_server --tail-dir "$TAIL_DIR"
strip_ansi <"$SERVE_LOG" | grep -q "recovered .* rows for .* tables from the" \
  || fail "restart log does not report a tail replay (log: $SERVE_LOG)"
assert_eq "ALL acked rows present after kill -9 + tail replay" "3" \
  "$(q "select count(*) from demo.$TABLE")"
pass "unclean kill lost NOTHING with --tail-dir"

# ---------------------------------------------------------------------------
# 3. Fence-forced flush commits the replayed rows; watermark prevents
#    double-apply on the NEXT crash
# ---------------------------------------------------------------------------
log "(3) flush + watermark: exactly-once across a second crash"
# A DELETE is an ordering fence: it forces flush_now() first (commit +
# icegres.tail-seq.<tail-id> watermark + tail truncation), then deletes
# nothing.
q "delete from demo.$TABLE where id < 0" >/dev/null || fail "fence DELETE failed"
assert_eq "rows COMMITTED by the fence-forced flush" "3" \
  "$(q "select count(*) from demo.$TABLE")"
kill_9
start_server --tail-dir "$TAIL_DIR"
assert_eq "no double-apply after commit + crash (watermark honored)" "3" \
  "$(q "select count(*) from demo.$TABLE")"
pass "exactly-once held across commit -> kill -9 -> replay"

# ---------------------------------------------------------------------------
# 4. Sequence-floor regression: rows acked AFTER a flushed generation and a
#    restart must survive the next crash. The trap: the flush in (3) stamped
#    watermark 3 and truncated the tail, so a restarted server that numbered
#    new frames from 1 would put this generation's sequences UNDER the
#    watermark — and the replay below would silently discard the acked rows
#    as "already covered". The seq floor (watermark + 1 at boot) prevents it.
# ---------------------------------------------------------------------------
log "(4) seq floor: a post-flush restart + new inserts survive the next crash"
# The server currently running was booted AFTER the (3) flush, i.e. it is
# exactly the restarted-over-a-truncated-tail process the bug bites.
for i in 11 12 13; do
  q "insert into demo.$TABLE (id, note) values ($i, 'second-generation')" >/dev/null \
    || fail "second-generation INSERT $i failed"
done
assert_eq "both generations readable pre-kill (union view)" "6" \
  "$(q "select count(*) from demo.$TABLE")"
kill_9
start_server --tail-dir "$TAIL_DIR"
assert_eq "BOTH generations present after the second crash" "6" \
  "$(q "select count(*) from demo.$TABLE")"
pass "post-restart sequences cleared the persisted watermark (no silent drop)"
# Commit generation two so the contrast case below starts from a clean lake.
q "delete from demo.$TABLE where id < 0" >/dev/null || fail "fence DELETE failed"
assert_eq "second generation COMMITTED by the fence-forced flush" "6" \
  "$(q "select count(*) from demo.$TABLE")"

# ---------------------------------------------------------------------------
# 5. Contrast: WITHOUT --tail-dir the same sequence still loses the rows
# ---------------------------------------------------------------------------
log "(5) contrast: no --tail-dir = the documented loss window is real"
stop_server
start_server # buffered, NO tail
for i in 101 102 103; do
  q "insert into demo.$TABLE (id, note) values ($i, 'kill-loss')" >/dev/null \
    || fail "untailed INSERT $i failed"
done
assert_eq "untailed acked rows readable pre-kill (union view)" "9" \
  "$(q "select count(*) from demo.$TABLE")"
kill_9
start_server # restart, still no tail
assert_eq "untailed acked rows LOST after kill -9 (unchanged trade-off)" "6" \
  "$(q "select count(*) from demo.$TABLE")"
pass "without --tail-dir the unclean-kill loss window is unchanged"

# ---------------------------------------------------------------------------
# 6. POSTGRES tail backend (--tail-url): acked rows survive kill -9 with the
#    tail living in the stack's Postgres — the node-loss-durable backend.
#    Fresh scratch table + fresh icegres_tail schema so counts start at 0.
# ---------------------------------------------------------------------------
log "(6) postgres tail: acked rows survive an unclean kill"
stop_server
drop_scratch "$PREFIX"
create_scratch "$PREFIX" || fail "could not re-create demo.$TABLE via REST catalog"
drop_pg_tail_schema
: >"$SERVE_LOG"
start_server --tail-url "$TAIL_PG_URL"
strip_ansi <"$SERVE_LOG" | grep -q "durable Postgres tail" \
  || fail "startup WARN does not announce the Postgres tail backend (log: $SERVE_LOG)"
pass "startup WARN announces the Postgres tail backend (node-loss durability class)"

for i in 1 2 3; do
  q "insert into demo.$TABLE (id, note) values ($i, 'pg-tail-survivor')" >/dev/null \
    || fail "pg-tailed INSERT $i failed"
done
assert_eq "acked rows readable via the union view (buffered, uncommitted)" "3" \
  "$(q "select count(*) from demo.$TABLE")"
assert_eq "frames durable in the tail DATABASE before the kill" "3" \
  "$(pg_tail "select count(*) from icegres_tail.frames")"

kill_9
start_server --tail-url "$TAIL_PG_URL"
strip_ansi <"$SERVE_LOG" | grep -q "recovered .* rows for .* tables from the" \
  || fail "restart log does not report a tail replay (log: $SERVE_LOG)"
assert_eq "ALL acked rows present after kill -9 + pg-tail replay" "3" \
  "$(q "select count(*) from demo.$TABLE")"
pass "unclean kill lost NOTHING with --tail-url"

# ---------------------------------------------------------------------------
# 7. PG tail: fence-forced flush commits the replayed rows, stamps the
#    watermark, writes the sidecar row, truncates the covered frames — and
#    the NEXT crash double-applies nothing (exactly-once).
# ---------------------------------------------------------------------------
log "(7) pg tail: flush + watermark = exactly-once across a second crash"
q "delete from demo.$TABLE where id < 0" >/dev/null || fail "fence DELETE failed"
assert_eq "rows COMMITTED by the fence-forced flush" "3" \
  "$(q "select count(*) from demo.$TABLE")"
assert_eq "watermark sidecar row records the covered seq" "3" \
  "$(pg_tail "select seq from icegres_tail.watermarks")"
assert_eq "covered frames truncated from the tail database" "0" \
  "$(pg_tail "select count(*) from icegres_tail.frames")"
kill_9
start_server --tail-url "$TAIL_PG_URL"
assert_eq "no double-apply after commit + crash (watermark honored)" "3" \
  "$(q "select count(*) from demo.$TABLE")"
pass "exactly-once held across commit -> kill -9 -> replay (pg tail)"

# ---------------------------------------------------------------------------
# 8. PG tail sequence floor: rows acked AFTER a flushed generation and a
#    restart must survive the next crash — post-restart numbering must not
#    duck under the persisted watermark (same trap as section 4; here the
#    floor is seeded from the watermarks table at open).
# ---------------------------------------------------------------------------
log "(8) pg tail seq floor: post-flush restart + new inserts survive a crash"
for i in 11 12 13; do
  q "insert into demo.$TABLE (id, note) values ($i, 'pg-second-generation')" >/dev/null \
    || fail "pg second-generation INSERT $i failed"
done
assert_eq "both generations readable pre-kill (union view)" "6" \
  "$(q "select count(*) from demo.$TABLE")"
kill_9
start_server --tail-url "$TAIL_PG_URL"
assert_eq "BOTH generations present after the second crash" "6" \
  "$(q "select count(*) from demo.$TABLE")"
pass "post-restart sequences cleared the persisted watermark (pg tail)"
q "delete from demo.$TABLE where id < 0" >/dev/null || fail "fence DELETE failed"
assert_eq "second generation COMMITTED by the fence-forced flush" "6" \
  "$(q "select count(*) from demo.$TABLE")"
stop_server

# ---------------------------------------------------------------------------
# 9. KEYED tail upserts (roadmap Phase 2): on a table with
#    icegres.tail-upsert=true + icegres.primary-key, an exact-PK UPDATE or
#    DELETE acks from the durable tail (no synchronous COW commit) and its
#    keyed frame SURVIVES kill -9 — replay rebuilds the keyed map and the
#    flush commits the FINAL value exactly once. Contrast: the same UPDATE
#    on a table WITHOUT the property still takes the fence+sync path (a
#    snapshot per statement).
# ---------------------------------------------------------------------------
log "(9) keyed tail: UPDATE/DELETE ack from the tail and survive kill -9"
KTABLE=tail_keyed_scratch
kt_snap_count() {
  curl -sf "$CATALOG_URI/v1/$PREFIX/namespaces/demo/tables/$KTABLE" \
    | jq '[.metadata.snapshots[]?] | length'
}
drop_keyed_scratch() {
  curl -sf -X DELETE \
    "$CATALOG_URI/v1/$PREFIX/namespaces/demo/tables/$KTABLE?purgeRequested=true" \
    >/dev/null 2>&1 || true
}
drop_keyed_scratch
curl -sf -X POST "$CATALOG_URI/v1/$PREFIX/namespaces/demo/tables" \
  -H 'Content-Type: application/json' -d @- <<'JSON' >/dev/null
{
  "name": "tail_keyed_scratch",
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
KTAIL_DIR="$RUN_DIR/tail-keyed-wal"
rm -rf "$KTAIL_DIR"
start_server --tail-dir "$KTAIL_DIR"

# Seed a committed row (fence = a non-keyed DML that matches nothing).
q "insert into demo.$KTABLE values (1, 'before'), (2, 'other')" >/dev/null
q "delete from demo.$KTABLE where id < -1" >/dev/null
assert_eq "seed committed (one snapshot)" "1" "$(kt_snap_count)"

# (9a) keyed UPDATE: fast ack, NO commit, survives kill -9.
kt_tag=$("${PSQL[@]}" -c "update demo.$KTABLE set val = 'after' where id = 1" \
  | tr -d '[:space:]')
assert_eq "keyed UPDATE answers the standard tag" "UPDATE1" "$kt_tag"
assert_eq "keyed UPDATE made NO snapshot (acked from the tail)" "1" "$(kt_snap_count)"
assert_eq "union read sees the updated value mid-window" "after" \
  "$(q "select val from demo.$KTABLE where id = 1")"
kill_9
start_server --tail-dir "$KTAIL_DIR"
assert_eq "UPDATED value survives kill -9 + replay, exactly once" "after|1" \
  "$(q "select val from demo.$KTABLE where id = 1")|$(q \
    "select count(*) from demo.$KTABLE where id = 1")"
q "delete from demo.$KTABLE where id < -1" >/dev/null # flush the replayed op
assert_eq "flush committed the update as ONE snapshot" "2" "$(kt_snap_count)"
assert_eq "committed value is the update, exactly once" "after|1" \
  "$(q "select val from demo.$KTABLE where id = 1")|$(q \
    "select count(*) from demo.$KTABLE where id = 1")"
pass "keyed UPDATE: tail-acked, crash-safe, exactly-once"

# (9b) keyed DELETE: fast ack, crash, replay -> row gone.
kt_tag=$("${PSQL[@]}" -c "delete from demo.$KTABLE where id = 2" | tr -d '[:space:]')
assert_eq "keyed DELETE answers the standard tag" "DELETE1" "$kt_tag"
assert_eq "keyed DELETE made NO snapshot" "2" "$(kt_snap_count)"
assert_eq "union read hides the deleted row mid-window" "0" \
  "$(q "select count(*) from demo.$KTABLE where id = 2")"
kill_9
start_server --tail-dir "$KTAIL_DIR"
assert_eq "deleted row STAYS gone after kill -9 + replay" "0" \
  "$(q "select count(*) from demo.$KTABLE where id = 2")"
q "delete from demo.$KTABLE where id < -1" >/dev/null
assert_eq "flush committed the delete" "3" "$(kt_snap_count)"
assert_eq "row gone from the lake" "0" \
  "$(q "select count(*) from demo.$KTABLE where id = 2")"
pass "keyed DELETE: tail-acked, crash-safe, exactly-once"

# (9c) contrast: WITHOUT icegres.tail-upsert the same statement shape still
# takes the fence+synchronous path — one snapshot per UPDATE, immediately.
before=$(curl -sf "$CATALOG_URI/v1/$PREFIX/namespaces/demo/tables/$TABLE" \
  | jq '[.metadata.snapshots[]?] | length')
q "update demo.$TABLE set note = 'sync-path' where id = 11" >/dev/null
after=$(curl -sf "$CATALOG_URI/v1/$PREFIX/namespaces/demo/tables/$TABLE" \
  | jq '[.metadata.snapshots[]?] | length')
assert_eq "table WITHOUT the property commits synchronously (fence path)" \
  "$((before + 1))" "$after"
pass "keyed routing is opt-in: no property = unchanged synchronous UPDATE"

stop_server
rm -rf "$KTAIL_DIR"
drop_keyed_scratch

# ---------------------------------------------------------------------------
# 10. QUORUM tail backend (--tail-quorum, roadmap §3 backend 3): three
#     icekeeperd acceptors, consensus-class durability. Fresh scratch table
#     and fresh acceptor data dirs so counts start at 0.
# ---------------------------------------------------------------------------
log "(10) quorum tail: 3 icekeeperd acceptors + --tail-quorum"
[[ -x "$KBIN" ]] || fail "icekeeperd not found at $KBIN (build with: cargo build --release)"
for port in "${KEEPER_PORTS[@]}"; do
  keeper_ready "$port" && fail "something else is listening on :$port — stop it first"
done
drop_scratch "$PREFIX"
create_scratch "$PREFIX" || fail "could not re-create demo.$TABLE via REST catalog"
rm -rf "$KEEPER_BASE-1" "$KEEPER_BASE-2" "$KEEPER_BASE-3"
: >"$SERVE_LOG"
for n in 1 2 3; do start_keeper "$n"; done
pass "3 icekeeperd acceptors up on ${KEEPER_PORTS[*]}"

start_server --tail-quorum "$QUORUM_ADDRS"
strip_ansi <"$SERVE_LOG" | grep -q "durable quorum tail" \
  || fail "startup WARN does not announce the quorum tail backend (log: $SERVE_LOG)"
pass "startup WARN announces the quorum tail (consensus durability class)"

# (10a) acked rows survive kill -9 of the COMPUTE: replay from the acceptors.
log "(10a) quorum tail: acked rows survive an unclean COMPUTE kill"
for i in 1 2 3; do
  q "insert into demo.$TABLE (id, note) values ($i, 'quorum-survivor')" >/dev/null \
    || fail "quorum-tailed INSERT $i failed"
done
assert_eq "acked rows readable via the union view (buffered, uncommitted)" "3" \
  "$(q "select count(*) from demo.$TABLE")"
ls "$KEEPER_BASE-1"/wal/*.seg >/dev/null 2>&1 \
  || fail "no acceptor log segments on disk after acked INSERTs ($KEEPER_BASE-1/wal)"
pass "acceptor log segments exist on disk before the kill"

kill_9
start_server --tail-quorum "$QUORUM_ADDRS"
strip_ansi <"$SERVE_LOG" | grep -q "recovered .* rows for .* tables from the" \
  || fail "restart log does not report a tail replay (log: $SERVE_LOG)"
assert_eq "ALL acked rows present after kill -9 + quorum replay" "3" \
  "$(q "select count(*) from demo.$TABLE")"
pass "unclean COMPUTE kill lost NOTHING with --tail-quorum"

# (10b) acked rows survive kill -9 of ONE ACCEPTOR: writes continue on 2/3.
log "(10b) quorum tail: writes continue after an unclean ACCEPTOR kill (2/3)"
kill_keeper_9 3
for i in 11 12 13; do
  q "insert into demo.$TABLE (id, note) values ($i, 'two-of-three')" >/dev/null \
    || fail "INSERT $i failed with one acceptor down (quorum 2/3 must carry it)"
done
assert_eq "rows acked by the surviving 2/3 quorum" "6" \
  "$(q "select count(*) from demo.$TABLE")"
kill_9
start_server --tail-quorum "$QUORUM_ADDRS"
assert_eq "ALL acked rows survive compute kill WITH an acceptor down" "6" \
  "$(q "select count(*) from demo.$TABLE")"
pass "one acceptor down: writes continued AND acked rows survived kill -9"
start_keeper 3 # bring it back; the proposer catches it up in the background

# (10c) exactly-once: fence-forced flush commits + stamps the watermark;
# the NEXT crash double-applies nothing.
log "(10c) quorum tail: flush + watermark = exactly-once across a crash"
q "delete from demo.$TABLE where id < 0" >/dev/null || fail "fence DELETE failed"
assert_eq "rows COMMITTED by the fence-forced flush" "6" \
  "$(q "select count(*) from demo.$TABLE")"
kill_9
start_server --tail-quorum "$QUORUM_ADDRS"
assert_eq "no double-apply after commit + crash (watermark honored)" "6" \
  "$(q "select count(*) from demo.$TABLE")"
pass "exactly-once held across commit -> kill -9 -> replay (quorum tail)"

# (10d) sequence floor: post-flush restart + new inserts survive a crash
# (same trap as sections 4/8; the floor is seeded from the recovered
# watermark records + the in-commit property).
log "(10d) quorum tail seq floor: post-flush restart + new inserts survive"
for i in 21 22 23; do
  q "insert into demo.$TABLE (id, note) values ($i, 'quorum-second-gen')" >/dev/null \
    || fail "quorum second-generation INSERT $i failed"
done
kill_9
start_server --tail-quorum "$QUORUM_ADDRS"
assert_eq "BOTH generations present after the second crash" "9" \
  "$(q "select count(*) from demo.$TABLE")"
pass "post-restart sequences cleared the persisted watermark (quorum tail)"
q "delete from demo.$TABLE where id < 0" >/dev/null || fail "fence DELETE failed"
assert_eq "second generation COMMITTED by the fence-forced flush" "9" \
  "$(q "select count(*) from demo.$TABLE")"

# (10e) FENCING: a second icegres on the SAME quorum runs a higher-term
# election; the first server's next INSERT fails with the superseded error
# while the second owns the log — and recovers the first's unflushed rows.
log "(10e) quorum tail fencing: a second server supersedes the first"
for i in 31 32; do
  q "insert into demo.$TABLE (id, note) values ($i, 'pre-fence')" >/dev/null \
    || fail "pre-fence INSERT $i failed"
done
: >"$SERVE_LOG2"
"$BIN" serve --host "$PG_HOST" --port "$PG_PORT2" --write-buffer-ms "$BUF_MS" \
  --tail-quorum "$QUORUM_ADDRS" >>"$SERVE_LOG2" 2>&1 &
echo $! >"$PID_FILE2"
for _ in $(seq 1 60); do
  q2 "select 1" >/dev/null 2>&1 && break
  kill -0 "$(cat "$PID_FILE2")" 2>/dev/null \
    || { tail -n 30 "$SERVE_LOG2" >&2; fail "second server exited during startup"; }
  sleep 0.5
done
q2 "select 1" >/dev/null 2>&1 || fail "second server not ready on :$PG_PORT2"
strip_ansi <"$SERVE_LOG2" | grep -q "recovered .* rows for .* tables from the" \
  || fail "second server did not replay the first's unflushed acked rows"
assert_eq "second server recovered the first's acked rows" "11" \
  "$(q2 "select count(*) from demo.$TABLE")"

set +e
fence_out=$("${PSQL[@]}" -c \
  "insert into demo.$TABLE (id, note) values (99, 'must-fail')" 2>&1)
fence_rc=$?
set -e
[[ $fence_rc -ne 0 ]] || fail "the fenced first server accepted an INSERT: $fence_out"
grep -q "superseded by a newer server" <<<"$fence_out" \
  || fail "unexpected fencing error: $fence_out"
pass "first server's INSERT failed with the superseded error (fenced, no lock files)"

q2 "insert into demo.$TABLE (id, note) values (41, 'post-fence')" >/dev/null \
  || fail "the NEW owner's INSERT failed"
q2 "delete from demo.$TABLE where id < 0" >/dev/null || fail "fence DELETE failed"
assert_eq "the new owner serves and commits (old rows + its own)" "12" \
  "$(q2 "select count(*) from demo.$TABLE")"
pass "the second server owns the quorum log end to end"

stop_server
stop_server2
for n in 1 2 3; do stop_keeper "$n"; done

log "all assertions passed ($PASS_COUNT)"
