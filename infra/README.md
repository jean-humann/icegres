# Local Lakehouse Stack

A self-contained, source-built lakehouse running entirely on localhost:

| Component  | What                              | Endpoint                      |
|------------|-----------------------------------|-------------------------------|
| PostgreSQL 16 | Lakekeeper metadata store (+ `icegres_test` db) | `127.0.0.1:5433` |
| RustFS     | S3-compatible object store        | `http://127.0.0.1:9000`       |
| Lakekeeper v0.13.1 | Apache Iceberg REST catalog | `http://127.0.0.1:8181`     |

On top of this stack, **icegres** serves the lakehouse over the Postgres wire
protocol on `127.0.0.1:5439` — see [`../icegres/README.md`](../icegres/README.md).

## Prerequisites

The scripts and test harness rely on these CLI tools being on `PATH`:
`curl`, `psql`, `aws` (awscli — bucket creation in `up.sh`; `up.sh` fails
loudly if it is missing), and `jq` (used by `icegres/tests/e2e.sh`).

## Quick start

```bash
infra/scripts/up.sh     # start everything (idempotent), ends with health checks
infra/scripts/down.sh   # stop everything (data preserved)
```

Individual services: `pg-start.sh`/`pg-stop.sh`, `rustfs-start.sh`/`rustfs-stop.sh`,
`lakekeeper-start.sh`/`lakekeeper-stop.sh` (all idempotent, cwd-independent).

## Endpoints and credentials

### PostgreSQL (port 5433)
- URL: `postgresql://lakekeeper:lakekeeper@127.0.0.1:5433/lakekeeper`
- Extra db: `postgresql://lakekeeper:lakekeeper@127.0.0.1:5433/icegres_test`
- Superuser: `postgresql://postgres@127.0.0.1:5433/postgres`
- TCP auth is `trust` locally, so the password is cosmetic.
- Cluster is owned by OS user `postgres`; scripts wrap `pg_ctl` in `su` when run as root.

### RustFS S3 (port 9000)
- Endpoint: `http://127.0.0.1:9000` — **path-style addressing required**
  (`force_path_style=true` / `s3.path-style-access=true`); virtual-hosted style is not routed.
- Credentials: access key `rustfsadmin`, secret `rustfssecret`, region `us-east-1`.
- Bucket: `lakehouse` (created automatically by `up.sh` if missing; requires
  the AWS CLI — `up.sh` errors out when `aws` is not installed).
- Binary (built from source, gitignored): `infra/.data/bin/rustfs`.

### Lakekeeper — Iceberg REST catalog (port 8181)
- Catalog base URI: `http://127.0.0.1:8181/catalog`
- Warehouse name: `lakehouse` (clients pass `warehouse=lakehouse`)
- The server returns the prefix from `GET /catalog/v1/config?warehouse=lakehouse`
  (`defaults.prefix` is the warehouse UUID). Clients that honor the config endpoint
  (pyiceberg, iceberg-rust, Spark) discover it automatically.
- Management API: `http://127.0.0.1:8181/management/v1/...`;
  Swagger UI: `http://127.0.0.1:8181/swagger-ui`;
  health: `http://127.0.0.1:8181/health`; Prometheus metrics on port `9090`.
- Auth is **disabled** (no OIDC configured, authz backend = allowall). Local dev only.
- Binary (built from source): `infra/src/lakekeeper/target/release/lakekeeper`.
- Warehouse storage: `s3://lakehouse/warehouse` on RustFS, remote signing enabled.

## Client configuration (example: pyiceberg / iceberg-rust RestCatalog)

```text
uri                  = http://127.0.0.1:8181/catalog
warehouse            = lakehouse
# S3 props — the catalog vends credentials via remote signing / the
# /credentials endpoint, but if your client reads data directly it needs:
s3.endpoint          = http://127.0.0.1:9000
s3.access-key-id     = rustfsadmin
s3.secret-access-key = rustfssecret
s3.region            = us-east-1
s3.path-style-access = true      # REQUIRED for RustFS
```

pyiceberg example:

```python
from pyiceberg.catalog.rest import RestCatalog
catalog = RestCatalog(
    "lakehouse",
    uri="http://127.0.0.1:8181/catalog",
    warehouse="lakehouse",
    **{
        "s3.endpoint": "http://127.0.0.1:9000",
        "s3.access-key-id": "rustfsadmin",
        "s3.secret-access-key": "rustfssecret",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    },
)
```

Note: reserved namespace names in Lakekeeper: `system`, `examples`, `information_schema`.

## Layout

```
infra/
  scripts/       start/stop scripts + up.sh/down.sh (committed)
  src/lakekeeper Lakekeeper source checkout + release binary (large; not for commit)
  .data/         runtime state — gitignored (".data/" in infra/.gitignore)
    pg/          postgres cluster        pg.log
    rustfs/      object store data       rustfs.log, rustfs.pid
    bin/rustfs   rustfs binary
    lakekeeper.log, lakekeeper.pid
```

## Resetting state

```bash
infra/scripts/down.sh
rm -rf infra/.data/pg infra/.data/rustfs infra/.data/*.log infra/.data/*.pid
infra/scripts/up.sh    # re-inits postgres, re-bootstraps lakekeeper, recreates bucket+warehouse
```

(Keep `infra/.data/bin/rustfs` unless you want to rebuild RustFS from source, ~20 min.)

To wipe only the catalog: stop lakekeeper, then `dropdb lakekeeper` via the
superuser on port 5433 and re-run `up.sh` — `pg-start.sh` recreates the
database with `OWNER lakekeeper`, which the migrations need on PostgreSQL 16.
(If you recreate it manually instead, use `createdb -O lakekeeper lakekeeper`;
a database owned by `postgres` makes `lakekeeper migrate` fail with
"permission denied for schema public".)

## Rebuild notes

- Lakekeeper: `cd infra/src/lakekeeper && RUSTUP_TOOLCHAIN=stable SQLX_OFFLINE=true cargo build --release --bin lakekeeper -p lakekeeper-bin`
- RustFS: crates.io package is a placeholder; build from git master (needs rust >= 1.96 and `protobuf-compiler`), see `rustfs-start.sh` header.
- Lakekeeper env config lives inline in `lakekeeper-start.sh` (`LAKEKEEPER__*` vars). The
  metrics port is moved to 9090 because the default (9000) collides with RustFS.
