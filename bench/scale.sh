#!/usr/bin/env bash
# icegres single-node SCALE bench (P6 / Half A).
#
# Generates a deterministic demo.trips_scale table at several row counts and
# measures the SAME four icegres query classes at each — point lookup,
# selective join, filtered count (= COMPARISON q6), full GROUP BY aggregation
# (= COMPARISON q5) — recording p50/p95, peak RSS (VmHWM) and effective
# rows/sec per class. This EXTENDS bench/compare (four engines @ 5M) with the
# icegres-only scale curve; the cross-engine Trino/Spark columns are cited
# from bench/COMPARISON.md at 5M and NOT re-run here (documented scope).
#
# Honest single-node labeling throughout: one 4-core box, everything
# (client, engine, catalog, S3, PG) colocated. See bench/COMPARISON.md caveats.
#
# Disk is the binding constraint. This script:
#   * preflight-frees icegres/target/debug (stale for the release bench),
#   * refuses to start a size whose Parquet footprint + a 3 GB margin would
#     not fit in currently-available space,
#   * watches df DURING generation and kills the generator if free space
#     drops below FLOOR_GB, and
#   * ALWAYS purges demo.trips_scale (trap on every exit path) so no
#     generated data is left behind.
#
# Sizes are generated ONE AT A TIME (purged before the next), so peak disk is
# max(size)+margin, never the sum.
#
# Usage:
#   bench/scale.sh                 # default curve: 5M 50M 300M 500M
#   bench/scale.sh 5M 50M 300M     # committed curve only (300M = max w/ 2nd-copy margin)
#   bench/scale.sh smoke           # quick 100k validation (CI-safe)
#
# FOREGROUND / long-running: 300M generation ~27 min, 500M ~45 min. Do not
# background-and-stop; if backgrounded, monitor to completion.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
COMPARE_DIR="$SCRIPT_DIR/compare"
ICEGRES_BIN="$REPO_DIR/icegres/target/release/icegres"
DATA_DIR="$REPO_DIR/infra/.data"
RESULTS_DIR="$SCRIPT_DIR/results"
GEN="$COMPARE_DIR/make_trips_big.py"
SCALE_TABLE="trips_scale"
PORT=5439
PIDFILE="$DATA_DIR/scale-icegres.pid"
FLOOR_GB=2                 # abort/kill generation if avail drops below this
BYTES_PER_ROW=14.2369      # recon-measured zstd Parquet bytes/row
TS="$(date -u +%Y%m%dT%H%M%SZ)"

mkdir -p "$RESULTS_DIR" "$DATA_DIR"

# ------------------------------------------------------------ size specs
# label -> "rows batches scan_warmup scan_iters query_only"
declare -A SPEC=(
  [smoke]="100000 2 3 8 0"
  [5M]="5000000 10 3 15 0"
  [50M]="50000000 5 3 15 0"
  [300M]="300000000 30 2 6 0"
  [500M]="500000000 50 2 6 1"   # query-only stretch (100x demo_big; no compaction)
)

SIZES=("$@")
[ ${#SIZES[@]} -gt 0 ] || SIZES=(5M 50M 300M 500M)

avail_gb()  { df -PB1G / | awk 'NR==2{print $4}'; }
avail_kb()  { df -Pk  / | awk 'NR==2{print $4}'; }

log() { echo "[$(date -u +%H:%M:%S)] $*"; }

# ------------------------------------------------------------ cleanup
purge_scale() {
  python3 - "$SCALE_TABLE" <<'PY' 2>/dev/null || true
import sys
from pyiceberg.catalog import load_catalog
cat = load_catalog("lake", **{
    "type": "rest", "uri": "http://127.0.0.1:8181/catalog",
    "warehouse": "lakehouse", "s3.endpoint": "http://127.0.0.1:9000",
    "s3.access-key-id": "rustfsadmin", "s3.secret-access-key": "rustfssecret",
    "s3.region": "us-east-1", "s3.path-style-access": "true"})
ident = ("demo", sys.argv[1])
if cat.table_exists(ident):
    cat.drop_table(ident, purge_requested=True)
    print(f"purged demo.{sys.argv[1]}")
else:
    print(f"demo.{sys.argv[1]} absent — nothing to purge")
PY
}

stop_icegres() {
  local pid
  pid=$(cat "$PIDFILE" 2>/dev/null || true)
  [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null || true
  rm -f "$PIDFILE"
  # also sweep any stray scale server on our port
  for p in $(pgrep -f "icegres serve --port $PORT" 2>/dev/null); do
    kill "$p" 2>/dev/null || true
  done
}

cleanup() {
  log "cleanup: stopping icegres + purging demo.$SCALE_TABLE"
  stop_icegres
  purge_scale
  log "cleanup done; df avail=$(avail_gb)G"
}
trap cleanup EXIT INT TERM

# ------------------------------------------------------------ preflight
log "=== icegres scale bench $TS ==="
log "df before preflight: avail=$(avail_gb)G"

if [ -d "$REPO_DIR/icegres/target/debug" ]; then
  log "preflight: rm -rf icegres/target/debug (stale for release bench)"
  rm -rf "$REPO_DIR/icegres/target/debug"
  log "df after debug cleanup: avail=$(avail_gb)G"
fi

# stack health
if ! curl -fsS -o /dev/null http://127.0.0.1:8181/health 2>/dev/null; then
  log "Lakekeeper not healthy — restoring stack (infra/scripts/up.sh)"
  bash "$REPO_DIR/infra/scripts/up.sh" || { log "up.sh FAILED"; exit 1; }
fi
[ -x "$ICEGRES_BIN" ] || { log "release binary missing: $ICEGRES_BIN"; exit 1; }

# ------------------------------------------------------------ engine mgmt
start_icegres() {
  stop_icegres
  nohup "$ICEGRES_BIN" serve --host 127.0.0.1 --port "$PORT" \
    >>"$DATA_DIR/scale-icegres.log" 2>&1 &
  echo $! > "$PIDFILE"
  for _ in $(seq 1 100); do
    python3 - "$PORT" <<'PY' && return 0
import socket, sys
s = socket.socket(); s.settimeout(0.3)
sys.exit(0 if s.connect_ex(("127.0.0.1", int(sys.argv[1]))) == 0 else 1)
PY
    sleep 0.2
  done
  log "ERROR: icegres did not become ready on :$PORT"
  return 1
}

# ------------------------------------------------------------ generation
# generate demo.trips_scale with a background df floor-guard.
generate() {  # $1 rows  $2 batches
  local rows=$1 batches=$2
  local floor_kb=$((FLOOR_GB * 1024 * 1024))
  ( python3 "$GEN" --table "$SCALE_TABLE" --rows "$rows" --batches "$batches" \
      --force >>"$DATA_DIR/scale-gen.log" 2>&1 ) &
  local gpid=$!
  # watch df; kill the generator if free space crosses the floor
  while kill -0 "$gpid" 2>/dev/null; do
    if [ "$(avail_kb)" -lt "$floor_kb" ]; then
      log "FLOOR GUARD: avail<${FLOOR_GB}G during generation — killing generator"
      kill "$gpid" 2>/dev/null || true
      wait "$gpid" 2>/dev/null
      return 2
    fi
    sleep 5
  done
  wait "$gpid"
}

# ------------------------------------------------------------ per-size run
FAILED=()
RAN=()
for label in "${SIZES[@]}"; do
  spec="${SPEC[$label]:-}"
  if [ -z "$spec" ]; then log "unknown size '$label' — skipping"; FAILED+=("$label(unknown)"); continue; fi
  read -r rows batches scan_warmup scan_iters query_only <<<"$spec"

  echo
  log "=================== $label ($rows rows) ==================="
  # fit check: footprint + 3 GB margin must fit in current avail
  need_kb=$(python3 -c "print(int($rows*$BYTES_PER_ROW/1024)+3*1024*1024)")
  have_kb=$(avail_kb)
  log "fit check: need~$((need_kb/1024/1024))G (footprint+3G margin), have $((have_kb/1024/1024))G"
  if [ "$have_kb" -lt "$need_kb" ]; then
    log "SKIP $label: insufficient disk (would breach ${FLOOR_GB}G floor)"
    FAILED+=("$label(disk)"); continue
  fi

  # ensure a clean slate for this size
  purge_scale >/dev/null

  log "generating $label ..."
  gen_t0=$(date +%s)
  if ! generate "$rows" "$batches"; then
    log "ERROR: generation failed for $label (see scale-gen.log)"
    FAILED+=("$label(gen)"); purge_scale >/dev/null; continue
  fi
  log "$label generated in $(( $(date +%s) - gen_t0 ))s; df avail=$(avail_gb)G"

  # fresh engine so VmHWM is this size's peak only
  if ! start_icegres; then FAILED+=("$label(engine)"); purge_scale >/dev/null; continue; fi
  pid=$(cat "$PIDFILE")

  out="$RESULTS_DIR/scale-$label-$TS.json"
  if python3 "$COMPARE_DIR/scale.py" --table "$SCALE_TABLE" --rows "$rows" \
        --size-label "$label" --pid "$pid" \
        --scan-warmup "$scan_warmup" --scan-iters "$scan_iters" \
        --out "$out"; then
    RAN+=("$label:$out")
    [ "$query_only" = "1" ] && log "NOTE: $label is a QUERY-ONLY stretch point (no compaction headroom)."
  else
    log "ERROR: scale.py failed for $label"
    FAILED+=("$label(bench)")
  fi

  stop_icegres
  purge_scale >/dev/null
  log "$label done; df avail=$(avail_gb)G"
done

# ------------------------------------------------------------ scorecard block
echo
log "=== SCORECARD-ready scale table ==="
python3 - "$TS" "${RAN[@]}" <<'PY'
import json, sys
ts = sys.argv[1]
paths = [a.split(":", 1)[1] for a in sys.argv[2:]]
rows = []
for p in paths:
    with open(p) as f:
        rows.append(json.load(f))
rows.sort(key=lambda r: r["rows"])
def cell(m):
    return f"{m['p50']} / {m['p95']}" if m else "—"
print(f"<!-- scale bench {ts} -->")
print("| size | rows | point p50/p95 | sel-join p50/p95 | "
      "filtered-count p50/p95 | full-agg p50/p95 | full-agg rows/s | peak RSS |")
print("|---|---|---|---|---|---|---|---|")
for r in rows:
    m = r["metrics"]
    agg = m.get("full_agg_ms", {})
    rss = m.get("rss_peak_mb", {}).get("value", "—")
    print(f"| {r['size_label']} | {r['rows']:,} | "
          f"{cell(m.get('point_lookup_ms'))} | {cell(m.get('selective_join_ms'))} | "
          f"{cell(m.get('filtered_count_ms'))} | {cell(m.get('full_agg_ms'))} | "
          f"{agg.get('eff_rows_per_s','—'):,} | {rss} MB |")
PY

echo
log "ran: ${RAN[*]:-none}"
if [ ${#FAILED[@]} -gt 0 ]; then
  log "FAILED/SKIPPED: ${FAILED[*]}"
fi
log "=== scale bench complete; df avail=$(avail_gb)G ==="
# cleanup trap purges the scale table + stops the engine on exit.
