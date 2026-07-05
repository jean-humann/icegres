#!/usr/bin/env bash
# Regression gate for icegres improvements (bench/SPEC.md §3).
#
#   bash bench/gate.sh <baseline-bench.json> <candidate-bench.json> \
#        [<baseline-parity.json> <candidate-parity.json>] [--skip-e2e]
#
# FAILS if:
#   - any latency metric p50 worsens by more than 20% vs baseline;
#   - qps_8conn drops by more than 10%;
#   - resource footprint regresses: rss_peak_mb or rss_idle_mb worsens by
#     more than 25%, or binary_size_mb by more than 10% (performance must be
#     traded explicitly against memory and binary size);
#   - icegres/tests/e2e.sh is not green (skippable with --skip-e2e, e.g. when
#     the caller has just run it);
#   - any parity verdict downgrades from PASS (when two parity JSONs given).
#
# Prints per-metric deltas and a final PASS/FAIL verdict; exit code 0/1.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

SKIP_E2E=0
args=()
for a in "$@"; do
  if [[ "$a" == "--skip-e2e" ]]; then SKIP_E2E=1; else args+=("$a"); fi
done
if [[ ${#args[@]} -ne 2 && ${#args[@]} -ne 4 ]]; then
  echo "usage: $0 <baseline-bench.json> <candidate-bench.json> [<baseline-parity.json> <candidate-parity.json>] [--skip-e2e]" >&2
  exit 2
fi
BASE=${args[0]}; CAND=${args[1]}
PBASE=${args[2]:-}; PCAND=${args[3]:-}
for f in "$BASE" "$CAND" ${PBASE:+"$PBASE"} ${PCAND:+"$PCAND"}; do
  [[ -f "$f" ]] || { echo "no such file: $f" >&2; exit 2; }
done

FAILURES=0
note_fail() { FAILURES=$((FAILURES + 1)); }

LATENCY_METRICS=(connect_ms point_lookup_ms filtered_scan_ms aggregate_ms join_ms
                 insert_single_ms insert_batch100_ms freshness_ms cold_start_ms)

echo "=== bench gate: $(basename "$BASE") -> $(basename "$CAND") ==="
printf '%-20s %10s %10s %8s  %s\n' "metric (p50 ms)" "baseline" "candidate" "delta" "verdict"

for m in "${LATENCY_METRICS[@]}"; do
  b=$(jq -r --arg m "$m" '.metrics[$m].p50 // "null"' "$BASE")
  c=$(jq -r --arg m "$m" '.metrics[$m].p50 // "null"' "$CAND")
  if [[ "$b" == null || "$c" == null ]]; then
    printf '%-20s %10s %10s %8s  %s\n' "$m" "$b" "$c" "—" "MISSING"
    note_fail
    continue
  fi
  # delta% and >20%-worse check, done in jq for float math
  verdict=$(jq -rn --argjson b "$b" --argjson c "$c" \
    'if $c > $b * 1.2 then "FAIL" else "ok" end')
  delta=$(jq -rn --argjson b "$b" --argjson c "$c" \
    'if $b == 0 then "n/a" else ((($c - $b) / $b * 1000 | round) / 10 | tostring) + "%" end')
  printf '%-20s %10s %10s %8s  %s\n' "$m" "$b" "$c" "$delta" "$verdict"
  [[ "$verdict" == FAIL ]] && note_fail
done

bq=$(jq -r '.metrics.qps_8conn.value // "null"' "$BASE")
cq=$(jq -r '.metrics.qps_8conn.value // "null"' "$CAND")
if [[ "$bq" == null || "$cq" == null ]]; then
  printf '%-20s %10s %10s %8s  %s\n' "qps_8conn" "$bq" "$cq" "—" "MISSING"
  note_fail
else
  verdict=$(jq -rn --argjson b "$bq" --argjson c "$cq" \
    'if $c < $b * 0.9 then "FAIL" else "ok" end')
  delta=$(jq -rn --argjson b "$bq" --argjson c "$cq" \
    'if $b == 0 then "n/a" else ((($c - $b) / $b * 1000 | round) / 10 | tostring) + "%" end')
  printf '%-20s %10s %10s %8s  %s\n' "qps_8conn (qps)" "$bq" "$cq" "$delta" "$verdict"
  [[ "$verdict" == FAIL ]] && note_fail
fi

# --- resource rules (memory & binary size are first-class gated metrics) ------
# metric:allowed-worsening-% — value may grow at most this much vs baseline.
RESOURCE_RULES=(rss_peak_mb:25 rss_idle_mb:25 binary_size_mb:10)

echo
echo "=== resource gate (rss_peak/rss_idle +25% max, binary_size +10% max) ==="
printf '%-20s %10s %10s %8s  %s\n' "metric (value)" "baseline" "candidate" "delta" "verdict"
for rule in "${RESOURCE_RULES[@]}"; do
  m=${rule%%:*}; allow=${rule##*:}
  b=$(jq -r --arg m "$m" '.metrics[$m].value // "null"' "$BASE")
  c=$(jq -r --arg m "$m" '.metrics[$m].value // "null"' "$CAND")
  if [[ "$b" == null || "$c" == null ]]; then
    printf '%-20s %10s %10s %8s  %s\n' "$m" "$b" "$c" "—" "MISSING"
    note_fail
    continue
  fi
  verdict=$(jq -rn --argjson b "$b" --argjson c "$c" --argjson a "$allow" \
    'if $c > $b * (1 + $a / 100) then "FAIL" else "ok" end')
  delta=$(jq -rn --argjson b "$b" --argjson c "$c" \
    'if $b == 0 then "n/a" else ((($c - $b) / $b * 1000 | round) / 10 | tostring) + "%" end')
  printf '%-20s %10s %10s %8s  %s (max +%s%%)\n' "$m" "$b" "$c" "$delta" "$verdict" "$allow"
  [[ "$verdict" == FAIL ]] && note_fail
done

# --- parity no-downgrade -----------------------------------------------------
if [[ -n "$PBASE" ]]; then
  echo
  echo "=== parity gate: $(basename "$PBASE") -> $(basename "$PCAND") ==="
  downgrades=$(jq -n --slurpfile b "$PBASE" --slurpfile c "$PCAND" '
    ($b[0].probes | map({key:.id, value:.verdict}) | from_entries) as $bv |
    ($c[0].probes | map({key:.id, value:.verdict}) | from_entries) as $cv |
    [ $bv | to_entries[] | select(.value == "PASS" and ($cv[.key] // "MISSING") != "PASS")
      | "\(.key): PASS -> \($cv[.key] // "MISSING")" ] | .[]' -r)
  if [[ -n "$downgrades" ]]; then
    echo "$downgrades" | sed 's/^/DOWNGRADE /'
    while IFS= read -r _; do note_fail; done <<<"$downgrades"
  else
    echo "no parity downgrades (every baseline PASS is still PASS)"
  fi
fi

# --- e2e ----------------------------------------------------------------------
echo
if [[ "$SKIP_E2E" == 1 ]]; then
  echo "=== e2e: SKIPPED (--skip-e2e) ==="
else
  echo "=== e2e: running icegres/tests/e2e.sh ==="
  if bash "$REPO_DIR/icegres/tests/e2e.sh" >"$SCRIPT_DIR/.run/gate-e2e.log" 2>&1; then
    echo "e2e green ($(grep -c '^' <"$SCRIPT_DIR/.run/gate-e2e.log") log lines, log: bench/.run/gate-e2e.log)"
  else
    echo "e2e FAILED — tail of bench/.run/gate-e2e.log:"
    tail -n 15 "$SCRIPT_DIR/.run/gate-e2e.log"
    note_fail
  fi
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "GATE: PASS"
  exit 0
else
  echo "GATE: FAIL ($FAILURES check(s) failed)"
  exit 1
fi
