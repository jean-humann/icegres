#!/usr/bin/env bash
# Idempotent stop script for the local PostgreSQL 16 cluster.
# Works from any cwd. No-op if the server is not running.
set -euo pipefail

PGBIN=/usr/lib/postgresql/16/bin
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
PGDATA="$INFRA_DIR/.data/pg"
PGUSER_OS=postgres

run_as_pg() {
  if [ "$(id -un)" = "$PGUSER_OS" ]; then
    "$@"
  else
    su -s /bin/bash "$PGUSER_OS" -c "$(printf '%q ' "$@")"
  fi
}

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "No cluster at $PGDATA; nothing to stop."
  exit 0
fi

if run_as_pg "$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
  run_as_pg "$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
  echo "PostgreSQL stopped."
else
  echo "PostgreSQL not running; nothing to stop."
fi
