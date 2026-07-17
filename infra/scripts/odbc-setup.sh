#!/usr/bin/env bash
# Register the stock PostgreSQL ODBC driver (psqlODBC / unixODBC) and a named
# DSN "icegres" pointing at a local icegres server, so `isql icegres` and
# DSN-based clients work. The A10 probe does NOT need this (it uses a DRIVER=
# connection string); this is for interactive isql use and DSN-style clients.
#
# Idempotent. Requires apt (ubuntu main). Run as root.
#
#   bash infra/scripts/odbc-setup.sh            # DSN -> 127.0.0.1:5439/icegres
#   ICEGRES_ODBC_PORT=5440 bash infra/scripts/odbc-setup.sh
set -euo pipefail

PORT="${ICEGRES_ODBC_PORT:-5439}"
HOST="${ICEGRES_ODBC_HOST:-127.0.0.1}"
DB="${ICEGRES_ODBC_DB:-icegres}"

if ! command -v isql >/dev/null 2>&1 || ! ls /usr/lib/*/odbc/psqlodbcw.so >/dev/null 2>&1; then
  echo "==> installing unixodbc + odbc-postgresql (apt)"
  apt-get install -y -qq unixodbc odbc-postgresql
fi

# psqlODBC registers [PostgreSQL Unicode] in /etc/odbcinst.ini automatically.
odbcinst -q -d | grep -q 'PostgreSQL Unicode' || {
  echo "ERROR: psqlODBC driver not registered in odbcinst.ini after install" >&2
  exit 1
}

cat > /etc/odbc.ini <<EOF
[icegres]
Description=icegres lakehouse (Postgres wire)
Driver=PostgreSQL Unicode
Servername=$HOST
Port=$PORT
Database=$DB
Username=postgres
Password=
SSLmode=disable
UseDeclareFetch=0
EOF

echo "==> DSN 'icegres' written to /etc/odbc.ini -> $HOST:$PORT/$DB"
echo "    test: echo 'select count(*) from demo.trips;' | isql -v icegres"
