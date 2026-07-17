#!/usr/bin/env bash
# Tear down the local lakehouse stack in reverse order: Lakekeeper -> RustFS -> Postgres.
# Idempotent: safe to re-run. Data is preserved in infra/.data/.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Stopping Lakekeeper"
"$SCRIPT_DIR/lakekeeper-stop.sh"

echo "==> Stopping RustFS"
"$SCRIPT_DIR/rustfs-stop.sh"

echo "==> Stopping Postgres"
"$SCRIPT_DIR/pg-stop.sh"

echo "Stack is down. State preserved under infra/.data/ (delete it to reset)."
