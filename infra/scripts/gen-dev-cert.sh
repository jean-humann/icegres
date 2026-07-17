#!/usr/bin/env bash
# Generate a self-signed development TLS certificate for `icegres serve
# --tls-cert/--tls-key` (SAN: localhost + 127.0.0.1).
#
#   bash infra/scripts/gen-dev-cert.sh [--force]
#
# Writes infra/.data/tls/dev.crt and infra/.data/tls/dev.key (PKCS#8, chmod
# 600) and is idempotent: existing files are kept unless --force is given.
#
# Client usage against a server started with these files:
#   psql "host=127.0.0.1 port=5439 user=... dbname=icegres sslmode=require"
#   # full verification (dev cert is self-signed, so pin it as the root):
#   psql "... sslmode=verify-full sslrootcert=infra/.data/tls/dev.crt host=localhost"
#
# DEV ONLY — production deployments should use real CA-issued certificates.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
TLS_DIR="$INFRA_DIR/.data/tls"
CRT="$TLS_DIR/dev.crt"
KEY="$TLS_DIR/dev.key"

FORCE=0
[[ "${1:-}" == "--force" ]] && FORCE=1

if [[ -s "$CRT" && -s "$KEY" && "$FORCE" == 0 ]]; then
  echo "dev cert already present: $CRT (use --force to regenerate)"
  exit 0
fi

command -v openssl >/dev/null || { echo "openssl not found" >&2; exit 1; }
mkdir -p "$TLS_DIR"

# -newkey rsa:2048 with OpenSSL 3 writes a PKCS#8 'BEGIN PRIVATE KEY' block,
# which is what icegres/rustls-pemfile expects.
openssl req -x509 -newkey rsa:2048 -sha256 -days 825 -nodes \
  -keyout "$KEY" -out "$CRT" \
  -subj "/CN=icegres-dev" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  >/dev/null 2>&1

chmod 600 "$KEY"
chmod 644 "$CRT"
echo "wrote $CRT and $KEY (self-signed, CN=icegres-dev, SAN localhost/127.0.0.1, valid 825 days)"
