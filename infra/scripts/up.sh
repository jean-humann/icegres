#!/usr/bin/env bash
# Bring up the whole local lakehouse stack: Postgres -> RustFS -> Lakekeeper.
# Idempotent: safe to re-run at any time. Ends with health checks.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Starting Postgres (127.0.0.1:5433)"
"$SCRIPT_DIR/pg-start.sh"

echo "==> Starting RustFS (127.0.0.1:9000)"
"$SCRIPT_DIR/rustfs-start.sh"

# Ensure the "lakehouse" bucket exists (idempotent; needs awscli).
if command -v aws >/dev/null 2>&1; then
  AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfssecret AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url http://127.0.0.1:9000 s3api head-bucket --bucket lakehouse 2>/dev/null || \
  AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfssecret AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://lakehouse
fi

echo "==> Starting Lakekeeper (127.0.0.1:8181)"
"$SCRIPT_DIR/lakekeeper-start.sh"

# Bootstrap (only succeeds once; ignore failures on re-run).
curl -sf -X POST http://127.0.0.1:8181/management/v1/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}' >/dev/null 2>&1 || true

# Create warehouse "lakehouse" if it does not exist yet.
if ! curl -sf "http://127.0.0.1:8181/catalog/v1/config?warehouse=lakehouse" >/dev/null 2>&1; then
  echo "==> Creating warehouse 'lakehouse'"
  curl -sf -X POST http://127.0.0.1:8181/management/v1/warehouse \
    -H 'Content-Type: application/json' -d '{
      "warehouse-name": "lakehouse",
      "project-id": "00000000-0000-0000-0000-000000000000",
      "storage-profile": {
        "type": "s3",
        "bucket": "lakehouse",
        "key-prefix": "warehouse",
        "endpoint": "http://127.0.0.1:9000",
        "region": "us-east-1",
        "path-style-access": true,
        "flavor": "s3-compat",
        "sts-enabled": false
      },
      "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "access-key-id": "rustfsadmin",
        "secret-access-key": "rustfssecret"
      }
    }' >/dev/null
fi

echo "==> Health checks"
FAIL=0

if psql -h 127.0.0.1 -p 5433 -U lakekeeper -d lakekeeper -tAc 'select 1' 2>/dev/null | grep -q 1; then
  echo "  [ok] postgres      postgresql://lakekeeper:lakekeeper@127.0.0.1:5433/lakekeeper"
else
  echo "  [FAIL] postgres on 127.0.0.1:5433"; FAIL=1
fi

if curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9000 | grep -qE '^(200|403)$'; then
  echo "  [ok] rustfs s3     http://127.0.0.1:9000 (rustfsadmin/rustfssecret, path-style)"
else
  echo "  [FAIL] rustfs on 127.0.0.1:9000"; FAIL=1
fi

if curl -sf -o /dev/null http://127.0.0.1:8181/health; then
  echo "  [ok] lakekeeper    http://127.0.0.1:8181 (health)"
else
  echo "  [FAIL] lakekeeper on 127.0.0.1:8181"; FAIL=1
fi

if curl -sf "http://127.0.0.1:8181/catalog/v1/config?warehouse=lakehouse" >/dev/null; then
  PREFIX=$(curl -sf "http://127.0.0.1:8181/catalog/v1/config?warehouse=lakehouse" | sed -n 's/.*"prefix":"\([^"]*\)".*/\1/p')
  echo "  [ok] iceberg rest  http://127.0.0.1:8181/catalog (warehouse=lakehouse, prefix=$PREFIX)"
else
  echo "  [FAIL] iceberg rest catalog config for warehouse=lakehouse"; FAIL=1
fi

exit $FAIL
