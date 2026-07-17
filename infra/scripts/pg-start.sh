#!/usr/bin/env bash
# Idempotent start script for the local PostgreSQL 16 cluster (Lakekeeper metadata).
# Works from any cwd. Safe to re-run: no-op if the server is already running.
set -euo pipefail

PGBIN=/usr/lib/postgresql/16/bin
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
PGDATA="$INFRA_DIR/.data/pg"
PGLOG="$INFRA_DIR/.data/pg.log"
PGPORT=5433
PGUSER_OS=postgres

run_as_pg() {
  if [ "$(id -un)" = "$PGUSER_OS" ]; then
    "$@"
  else
    su -s /bin/bash "$PGUSER_OS" -c "$(printf '%q ' "$@")"
  fi
}

# --- initdb if the cluster does not exist yet ---
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  mkdir -p "$PGDATA"
  chown "$PGUSER_OS:$PGUSER_OS" "$INFRA_DIR/.data" "$PGDATA"
  chmod 700 "$PGDATA"
  run_as_pg "$PGBIN/initdb" -D "$PGDATA" -U postgres -A trust
  cat >> "$PGDATA/postgresql.conf" <<EOF

# --- lakekeeper infra overrides ---
listen_addresses = '127.0.0.1'
port = $PGPORT
unix_socket_directories = '$PGDATA'
EOF
  chown "$PGUSER_OS:$PGUSER_OS" "$PGDATA/postgresql.conf"
fi

# --- start if not already running ---
if run_as_pg "$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
  echo "PostgreSQL already running (data dir: $PGDATA, port $PGPORT)"
else
  run_as_pg "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGLOG" -w start
fi

# --- ensure role + databases exist (idempotent) ---
PSQL=(psql -h 127.0.0.1 -p "$PGPORT" -U postgres -qAt)
if [ "$("${PSQL[@]}" -c "SELECT 1 FROM pg_roles WHERE rolname='lakekeeper'")" != "1" ]; then
  "${PSQL[@]}" -c "CREATE ROLE lakekeeper LOGIN PASSWORD 'lakekeeper'"
fi
for db in lakekeeper icegres_test; do
  if [ "$("${PSQL[@]}" -c "SELECT 1 FROM pg_database WHERE datname='$db'")" != "1" ]; then
    "${PSQL[@]}" -c "CREATE DATABASE $db OWNER lakekeeper"
  fi
done

echo "PostgreSQL ready: postgresql://lakekeeper:lakekeeper@127.0.0.1:$PGPORT/lakekeeper"
