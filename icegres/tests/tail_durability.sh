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
#
# Non-destructive, self-contained, idempotent: creates/purges its own scratch
# table demo.tail_durability_scratch via the REST catalog (never touches
# demo.trips) and its own tail dir under .e2e. Uses port 5456 (left free:
# 5452 belongs to bench/parity.sh D7, 5455 to e2e.sh THR_PORT).
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

PREFIX=""
cleanup() {
  stop_server
  [[ -n "$PREFIX" ]] && drop_scratch "$PREFIX"
  rm -rf "$TAIL_DIR"
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

log "all assertions passed ($PASS_COUNT)"
