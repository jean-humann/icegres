#!/usr/bin/env bash
# A10 ODBC probe wrapper (bench/SPEC.md A10): runs bench/clients/a10_odbc_probe.py
# against a live icegres server using the stock PostgreSQL ODBC driver
# (psqlODBC via unixODBC), then surfaces its PASS/FAIL/XFAIL summary.
#
# The probe is self-contained: it connects with a DRIVER= string, so it needs
# only the psqlODBC driver registered in odbcinst.ini (apt: unixodbc
# odbc-postgresql) — no /etc/odbc.ini DSN. See infra/scripts/odbc-setup.sh to
# additionally register a named DSN "icegres".
#
# Usage:
#   bench/clients/a10_odbc_probe.sh              # probe of 127.0.0.1:5439
#   ICEGRES_PROBE_PORT=5440 bench/clients/a10_odbc_probe.sh
#
# Exit codes: 0 = all green (fail=0), 1 = probe failures,
#             3 = pyodbc/driver not available (skip).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "A10 SKIP: python3 not available" >&2
  echo "A10 RESULT: pass=0 fail=0 xfail=0 skip=1"
  exit 3
fi
if ! python3 -c 'import pyodbc' 2>/dev/null; then
  echo "A10 SKIP: pyodbc not installed (pip install pyodbc)" >&2
  echo "A10 RESULT: pass=0 fail=0 xfail=0 skip=1"
  exit 3
fi

exec python3 "$SCRIPT_DIR/a10_odbc_probe.py"
