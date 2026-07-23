#!/usr/bin/env bash
# tests/browser-flight.sh — the browser-direct Flight SQL gate.
#
# Proves the frontend data path end to end in a REAL browser against a live
# icegres: the @icegres/flight-web client's own tests, plus a headless
# Chromium smoke run that drives all four data lanes and asserts each decodes
# the right rows (bench/clients/js/bench/smoke.mjs). This is the CI guard the
# docs/frontend-dashboards.md numbers and the client package depend on.
#
# Like tests/helm.sh, it SKIPs loudly (exit 0) when its prerequisites are
# absent so it never blocks a machine that cannot run it — CI runs it where
# node, Chromium, and the lakehouse stack are all present.
#
# Prereqs, each a loud SKIP if missing:
#   - node on PATH
#   - a Chromium at CHROMIUM_PATH (default /opt/pw-browsers/chromium)
#   - the base lakehouse stack reachable (ICEGRES_CATALOG_URI, default the
#     local dev catalog) with the demo seed loaded
#   - the icegres release binary (built if cargo is available, else SKIP)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JS_DIR="$ROOT/bench/clients/js"
PKG_DIR="$ROOT/clients/flight-web"
BIN="$ROOT/icegres/target/release/icegres"
CATALOG_URI="${ICEGRES_CATALOG_URI:-http://127.0.0.1:8181/catalog}"
CHROMIUM="${CHROMIUM_PATH:-/opt/pw-browsers/chromium}"
FLIGHT_PORT="${BROWSER_FLIGHT_PORT:-50060}"
PROXY_PORT="${BROWSER_PROXY_PORT:-8092}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfssecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

skip() { echo "SKIP tests/browser-flight.sh: $1 (runs where node + Chromium + the stack are present)"; exit 0; }
# A hard failure (exit 1), distinct from a SKIP: used once the prerequisites
# are all present, so a real regression cannot masquerade as a loud SKIP.
die()  { echo "FAIL tests/browser-flight.sh: $1"; exit 1; }

command -v node >/dev/null 2>&1 || skip "node not on PATH"
[ -x "$CHROMIUM" ] || skip "no Chromium at $CHROMIUM (set CHROMIUM_PATH)"
curl -sf -o /dev/null "$CATALOG_URI/v1/config?warehouse=${ICEGRES_WAREHOUSE:-lakehouse}" 2>/dev/null \
  || skip "Iceberg catalog not reachable at $CATALOG_URI"
if [ ! -x "$BIN" ]; then
  command -v cargo >/dev/null 2>&1 || skip "icegres binary missing and no cargo to build it"
  echo "building icegres release binary ..."
  (cd "$ROOT/icegres" && cargo build --release) || skip "icegres build failed"
fi

PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "==> ensuring the demo seed is present"
ICEGRES_CATALOG_URI="$CATALOG_URI" "$BIN" seed >/dev/null 2>&1 || true

echo "==> starting flight-serve --grpc-web on :$FLIGHT_PORT (with resource limits)"
ICEGRES_CATALOG_URI="$CATALOG_URI" "$BIN" flight-serve \
  --host 127.0.0.1 --port "$FLIGHT_PORT" --grpc-web \
  --flight-max-result-bytes 8388608 --flight-statement-timeout-ms 30000 \
  --flight-health-port "$((FLIGHT_PORT + 1))" >/tmp/browser-flight-serve.log 2>&1 &
PIDS+=($!)
for _ in $(seq 1 50); do
  node -e "require('net').connect($FLIGHT_PORT,'127.0.0.1').on('connect',()=>process.exit(0)).on('error',()=>process.exit(1))" 2>/dev/null && break
  sleep 0.2
done

echo "==> npm ci (root client workspace + bench harness)"
# A hard FAIL, not a SKIP: node/Chromium/stack are all present by here, so an
# install failure is a real regression (broken lockfile, a bad file: link, a
# native-addon build breaking) — exactly what this gate must catch, not mask.
(cd "$ROOT" && npm ci --no-audit --no-fund --ignore-scripts) >/dev/null 2>&1 \
  || die "npm ci failed in the root workspace"
(cd "$JS_DIR" && npm ci --no-audit --no-fund --ignore-scripts) >/dev/null 2>&1 \
  || die "npm ci failed in $JS_DIR"

FAIL=0

echo "==> @icegres/flight-web unit + live integration tests"
if (cd "$PKG_DIR" && ICEGRES_GRPCWEB_URL="http://127.0.0.1:$FLIGHT_PORT" npm test); then
  echo "ok  client package tests"
else
  echo "FAIL client package tests"; FAIL=1
fi

echo "==> bundling the browser pages"
(cd "$JS_DIR" && node build.mjs) >/dev/null 2>&1 || { echo "FAIL bundle"; FAIL=1; }

echo "==> starting the bench proxy on :$PROXY_PORT"
(cd "$JS_DIR" && PORT="$PROXY_PORT" ICEGRES_FLIGHT_ADDR="127.0.0.1:$FLIGHT_PORT" \
  ICEGRES_PG="${ICEGRES_PG:-postgres://bench:bench@127.0.0.1:5439/icegres}" \
  node proxy/server.js) >/tmp/browser-flight-proxy.log 2>&1 &
PIDS+=($!)
sleep 1

echo "==> headless Chromium smoke across all four data lanes"
if (cd "$JS_DIR" && BENCH_BASE="http://127.0.0.1:$PROXY_PORT" \
    GRPCWEB_PORT="$FLIGHT_PORT" CHROMIUM_PATH="$CHROMIUM" node bench/smoke.mjs); then
  echo "ok  browser smoke"
else
  echo "FAIL browser smoke"; FAIL=1
fi

echo "---- tests/browser-flight.sh: $([ $FAIL -eq 0 ] && echo PASS || echo FAIL)"
exit $FAIL
