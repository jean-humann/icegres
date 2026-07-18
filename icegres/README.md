# icegres

A Postgres wire endpoint over an Iceberg lakehouse — the Phase-0
"serve-in-place" system from `docs/lakebase-lakegres-architecture-study.md`.

`icegres` connects to an Iceberg REST catalog (Lakekeeper), mounts every
namespace/table into a DataFusion session, and serves that session over the
Postgres wire protocol with `datafusion-postgres`. Any Postgres client
(`psql`, drivers, BI tools) can then query — and modify — Iceberg tables
whose data lives as Parquet on S3-compatible storage (RustFS). There is
exactly **one copy of the data**, in open Iceberg format on the lake;
every feature below is zero-copy on top of it.

For how to assemble these pieces in production — which tier serves OLTP
vs API vs BI, with the measured latencies and the honest anti-patterns —
see the CQRS reference topology: **`../docs/cqrs-topology.md`**. All
measurements live in `../bench/SCORECARD.md`.

**Where it stands vs real engines** (measured, not marketed — full matrix
and caveats in `../bench/COMPARISON.md`): on a single 4-core box against
Trino 446 and Spark 3.5.8 Thrift reading the *same* Iceberg tables through
the same REST catalog, icegres is the clear interactive-serving winner —
7–10 ms small-query p50s vs 115–436 ms (16–43× faster), 51.7 qps at 8
connections vs 12.2/7.9, ~0.3 s startup vs 10–14 s, and 8–10× less peak
RSS — because it pays no JVM, coordinator, or per-query task-scheduling
overhead. It is **not** a distributed analytics engine: Trino already
beats it on the largest full-table aggregation measured (5M rows: 336 vs
404 ms p50), and that gap would widen with data volume or a real cluster.
Honest fit: sub-second point/filtered/join queries, Postgres-protocol
compatibility, and scale-to-zero economics on lakehouse data — leave
100 GB+ distributed scans to Trino/Spark.

### Features

| feature | mechanism | flag / syntax |
|---|---|---|
| SELECT (full SQL via DataFusion) | snapshot-aware metadata cache, exact freshness (no TTL) | default |
| INSERT | Iceberg `fast_append` commit per statement | default |
| UPDATE / DELETE | copy-on-write overwrite snapshots — only files containing matched rows are rewritten (`src/overwrite.rs`) | default |
| Transactions | BEGIN/COMMIT/ROLLBACK; snapshot-pinned reads, read-your-own-writes, COMMIT = ONE Iceberg snapshot per table — atomic ACROSS tables via the catalog's multi-table `transactions/commit` endpoint (Lakekeeper), first-committer-wins (40001) (`src/txn.rs`) | default |
| Primary-key enforcement | opt-in NOT NULL + uniqueness checks (23502/23505) anchored to the commit snapshot | `--enforce-pk` + table property `icegres.primary-key` |
| Authentication | SCRAM-SHA-256 (salted hashes in memory, 28P01 on failure) — managed add-on | `--auth-file` |
| Authorization | Lakekeeper-style ReBAC: warehouse/namespace/table grants, roles, per-statement 42501 — managed add-on | `--authz-file` |
| TLS | rustls on the pgwire listener; misconfig aborts boot | `--tls-cert`/`--tls-key` |
| Time travel | read-only snapshot-pinned queries | `demo."trips@<snapshot_id>"` |
| Zero-copy branches | Neon-style branch-per-endpoint over Iceberg snapshot refs (`src/branch.rs`) | `icegres branch create/list/drop`, `serve --branch` |
| Buffered writes (opt-in) | Moonlink-style group commit: ~1.5 ms INSERT ack, union reads, ≤N ms durability window, WARN on enable (`src/buffer.rs`) | `--write-buffer-ms N` (default 0 = synchronous) |
| Keyed tail upserts (opt-in) | Hot-row `UPDATE`/`DELETE` by exact PK ack from the durable tail (~5.2 ms p50 with `--freshness-ms 25`, ~7.0 ms without, vs ~47.5 ms synchronous COW UPDATE), coalesced per key into ONE commit per flush window (`src/keyed.rs`, `src/buffer.rs`) | table properties `icegres.primary-key` + `icegres.tail-upsert=true`, with `--write-buffer-ms > 0` and a tail backend |
| Bounded-staleness reads (opt-in) | Freshness refresher + plan cache: scans skip the per-scan catalog check (point lookup ~7.4 → ~4.4 ms p50, repeated statements ~3.6/~2.8 ms via the physical-plan cache); own writes stay read-your-own-writes exact, foreign commits visible within ~N ms + one refresh round trip — tables refresh concurrently, so a slow table delays only itself (per-table timeout min(4·N, 2 s)); WARN on enable, staleness gauge on `/metrics` (`src/freshness.rs`, `src/plancache.rs`) | `--freshness-ms N` (default 0 = exact freshness) |
| Scale-to-zero | clean exit after N idle seconds; stateless compute | `--idle-shutdown-secs` |
| Wake-on-connect control plane | `icegresd`: pgwire-aware proxy that spawns computes on connect, routes `icegres@<branch>` dbnames to per-branch computes, supervises crashes with capped backoff, keeps a warm session pool (`--pool-size`, sub-ms connects; session pooling only — no transaction pooling, no cross-client reuse) | `icegresd serve` / `icegresd status` |
| Health endpoint | HTTP 200 liveness | `--health-port` |
| ORM/BI compatibility | pg_catalog shims: coherent oids, `version()`, ORM introspection rewrites (`src/compat.rs`) — SQLAlchemy/psycopg2/pg8000/pandas verified | default |
| ADBC / Arrow Flight SQL | second first-class wire protocol (`src/flight.rs`): Arrow end to end, catalog metadata (`get_objects`), prepared statements with binds, DML with real affected counts, BULK INGEST (`adbc_ingest` → ONE Iceberg commit per stream), basic-auth handshake | `icegres flight-serve` (default `:50051`) |
| ADBC postgres driver | `COPY ... TO STDOUT (FORMAT binary\|text\|csv)` on both protocols (`ops.rs::CopyOutHook`) — `adbc_driver_postgresql` reads/params/DML verified | default (`serve`) |
| ODBC | stock psqlODBC (unixODBC) — `SQLTables`/`SQLColumns` metadata, params, DML, read-in-txn verified (no icegres code changes; reuses the ORM/JDBC `pg_catalog` shims) | default (`serve`); DSN via `infra/scripts/odbc-setup.sh` |
| JDBC | stock pgjdbc 42.7 — `DatabaseMetaData`, `PreparedStatement`, `executeUpdate`, txn cycles verified | default (`serve`) |

```
psql ──pgwire──▶ icegres (DataFusion) ──REST──▶ Lakekeeper ──▶ Postgres (metadata)
                        │
                        └───────s3───────▶ RustFS (Parquet data files)
```

## Build

```sh
cd icegres
cargo build            # dev profile; add --release for optimized builds
```

The crate pins a verified compatibility matrix (iceberg 0.9.1, datafusion
52.5.0, datafusion-postgres 0.15.0, arrow 57.3.1) and toolchain
(`rust-toolchain.toml`, 1.96.1). Do not bump versions independently — see
comments in `Cargo.toml`. `build.rs` stamps the commit SHA into `icegres
--version`.

**Deploying to production?** See **`../docs/deployment.md`** (container image,
health/readiness/metrics probes, resource limits, graceful shutdown, security,
snapshot-expiry maintenance) and **`../docs/limitations.md`** (what icegres
deliberately does not do). A multi-stage, non-root **`../Dockerfile`** builds
the GA image.

## Prerequisites

The local lakehouse stack must be running:

```sh
bash ../infra/scripts/up.sh   # Postgres + RustFS + Lakekeeper (idempotent)
```

CLI tools: `up.sh` needs `curl`, `psql` and `aws` (awscli); the e2e harness
additionally needs `jq`.

## Usage

```sh
# 1. Seed demo data (idempotent — safe to re-run)
./target/debug/icegres seed

# 2. Serve on the Postgres wire protocol
./target/debug/icegres serve

# 3. Query from any Postgres client (permissive by default — any
#    user/password accepted; see "Authentication" below to require SCRAM)
psql -h 127.0.0.1 -p 5439 -U postgres -d icegres
```

### Subcommands

| Command | Description |
|---|---|
| `icegres serve` | Serve the lakehouse over pgwire (default `0.0.0.0:5439`). |
| `icegres flight-serve` | Serve the same lakehouse over Arrow Flight SQL / gRPC (default `0.0.0.0:50051`) — the ADBC endpoint. See "ADBC (Arrow Flight SQL)" below. |
| `icegres seed`  | Create namespace `demo` + tables `trips`/`cities` and insert demo rows. Rows are inserted only when the seeded data is absent (row count 0), so re-seeding never duplicates data and repairs a table left empty by an interrupted earlier run. |
| `icegres branch create <table> <name> [--at-snapshot <id>]` | Create a zero-copy branch: ONE metadata commit adding a snapshot ref at main's head (or `--at-snapshot`); no data copied. Fails if the branch exists (atomic via `assert-ref-snapshot-id = null`). |
| `icegres branch list <table>` | List all snapshot refs (branches/tags) of a table with their head snapshot ids. |
| `icegres branch drop <table> <name>` | Remove the ref only (`main` is refused); the branch's snapshots stay time-travel-readable until expiry. |
| `icegres branch create-all <name>` | Whole-lakehouse branch: set the ref on EVERY table in the catalog in ONE atomic multi-table transaction (`POST /v1/{prefix}/transactions/commit`) — a consistent cross-table cut. Per-table `assert-ref-snapshot-id = null` guards make it all-or-nothing: if any table already has the branch, nothing is applied. Snapshot-less tables cannot hold a ref and are skipped with a loud warning. Requires a catalog implementing the endpoint (Lakekeeper does); errors cleanly otherwise. |
| `icegres branch drop-all <name>` | Remove the ref from every table that has it in ONE atomic multi-table transaction (`main` refused; tables without the ref are skipped; errors if no table has it). |
| `icegres maintain expire-snapshots <table> [--keep N]` | Trim table metadata to the newest `N` snapshots (default 10) **plus every snapshot still reachable from a branch/tag ref** — a metadata-only, live-safe REST commit (anchored with `assert-table-uuid` + `assert-ref-snapshot-id main=<head>`). Data/manifest files of the expired snapshots are left for `maintain remove-orphans` to reclaim. Long-lived tables need this so `$snapshots` and the per-open metadata JSON stop growing unbounded. |
| `icegres maintain remove-orphans <table> [--older-than-hours N] [--execute] [--unsafe-grace]` | Orphan-file GC — the storage half of expiry: lists the table's S3 prefix, subtracts the LIVE set (every data file, manifest, and manifest list reachable from EVERY retained snapshot — all branches/tags, DELETED manifest entries included — plus the current metadata JSON, the metadata log, and statistics files), and reports the rest. **Dry-run by default** (count + bytes + up to 20 sample paths); `--execute` deletes. Only objects older than `--older-than-hours` (default 72) plus a fixed 15-minute clock-skew allowance are eligible — the grace window is THE guard for in-flight commits, ours or a foreign writer's; `--execute` also verifies real host-vs-store skew with a write/stat/delete probe under `metadata/` (abort beyond the allowance). `--execute` with a sub-1h window is refused without `--unsafe-grace` (quiescent tables only — concurrent writers WILL lose in-flight files). Fails closed: unreadable table metadata or any unreadable manifest aborts the whole run, a recorded path outside the listed bucket aborts the whole run (liveness unverifiable), unknown-age objects are never deleted, unrecognized objects under the prefix are skipped with a WARN, and a mid-run commit re-derives the live set (a UUID change aborts). |
| `icegres maintain compact --table <table> [--target-file-mb N] [--min-input-files N] [--execute]` | Bin-pack compaction: rewrite each partition's data files under `--target-file-mb` (default 128 MiB) into ~target-size files as ONE `replace` snapshot — row set identical, old files time-travel-readable until expiry + GC. **Dry-run by default** (plan: candidates per partition, projected outputs); `--execute` commits. First-committer-wins: anchored to the snapshot the plan was computed against, so a concurrent commit aborts it cleanly with nothing changed (re-run). Refuses loudly on foreign merge-on-read (delete-manifest) tables and partitioned tables. |
| `icegres sql -e '<query>'` | One-shot local execution against the catalog (debugging aid; no server involved). Honors `--enforce-pk`. |

### Configuration

All flags have working defaults for the local stack and can also be set via
environment variables. The common flags are below;
**[`docs/configuration.md`](../docs/configuration.md) is the complete reference**
— every flag and `ICEGRES_*` / `ICEGRESD_*` / `ICEKEEPER_*` env var (including the
scan, cache, and DataFusion tuning knobs), grouped with defaults and meanings.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--catalog-uri` | `ICEGRES_CATALOG_URI` | `http://127.0.0.1:8181/catalog` | Iceberg REST catalog base URI (Lakekeeper) |
| `--warehouse` | `ICEGRES_WAREHOUSE` | `lakehouse` | Warehouse name in the catalog |
| `--s3-endpoint` | `ICEGRES_S3_ENDPOINT` | `http://127.0.0.1:9000` | S3-compatible endpoint (RustFS; path-style is forced) |
| `--s3-access-key` | `ICEGRES_S3_ACCESS_KEY` | `rustfsadmin` | S3 access key id |
| `--s3-secret-key` | `ICEGRES_S3_SECRET_KEY` | `rustfssecret` | S3 secret access key |
| `--s3-region` | `ICEGRES_S3_REGION` | `us-east-1` | S3 region |
| `--host` (serve) | `ICEGRES_HOST` | `0.0.0.0` | pgwire bind address |
| `--port` (serve) | `ICEGRES_PORT` | `5439` | pgwire bind port |
| `--idle-shutdown-secs` (serve) | `ICEGRES_IDLE_SHUTDOWN_SECS` | off | Scale-to-zero: exit cleanly after N seconds with no client connections |
| `--health-port` (serve) | `ICEGRES_HEALTH_PORT` | off | Serve an HTTP 200 `ok` liveness endpoint on this port |
| `--tls-cert` (serve) | `ICEGRES_TLS_CERT` | off | PEM certificate (chain) enabling TLS on the pgwire listener (requires `--tls-key`) |
| `--tls-key` (serve) | `ICEGRES_TLS_KEY` | off | PEM private key for `--tls-cert` (PKCS#8/RSA/SEC1) |
| `--auth-file` (serve) | `ICEGRES_AUTH_FILE` | off | Require SCRAM-SHA-256 auth against a `user:password` credentials file |
| `--enforce-pk` (serve, sql) | `ICEGRES_ENFORCE_PK` | off | Enforce `icegres.primary-key` table properties: NOT NULL (23502) + uniqueness (23505) checks on INSERT and PK-assigning UPDATE, anchored to the commit snapshot |
| `--branch` (serve) | `ICEGRES_BRANCH` | `main` | Serve a zero-copy branch: reads pin to the ref's head, all writes commit to the ref with `assert-ref-snapshot-id` (never touching other branches) |
| `--freshness-ms` (serve) | `ICEGRES_FRESHNESS_MS` | `0` (exact) | Opt-in bounded-staleness reads: scans serve the cached snapshot with no per-scan catalog check; ONE background task polls the catalog every N ms (up to 8 tables refreshed concurrently) and swaps changed snapshots. Own writes stay read-your-own-writes exact (synchronous invalidation); foreign commits visible within ~N ms + one refresh round trip — a slow table delays only itself (retry-free per-table refresh timeout min(4·N, 2 s); the next pass retries), never other tables (WARN on enable). Also enables the physical-plan cache. During a catalog outage reads keep serving the last refreshed snapshot (`ICEGRES_STALE_READ_ON_CATALOG_ERROR=0` fails loudly instead); worst-case age = `icegres_freshness_age_ms` on `/metrics`, sampled at refresher pass start (healthy ≈ N) |
|  | `ICEGRES_PLAN_CACHE_ENTRIES` | `256` | LRU capacity of the physical-plan cache (active only with `--freshness-ms > 0`; `0` disables it) |
|  | `ICEGRES_RESULT_CACHE_BYTES` | `0` (off) | Byte budget for the opt-in result cache: repeated identical queries at an unchanged snapshot are served from cached result batches with no execution or IO (freshness mode only; same version invalidation as the plan cache) |
| `--write-buffer-ms` (serve) | `ICEGRES_WRITE_BUFFER_MS` | `0` (sync) | Opt-in buffered writes: INSERTs ack from an in-memory buffer, group-committed every N ms; unclean kill loses ≤N ms of acked writes (WARN on enable) |
|  | `ICEGRES_WRITE_BUFFER_MAX_ROWS` | `50000` | Row threshold that forces an early flush in buffered mode |
| `--tail-dir` (serve) | `ICEGRES_TAIL_DIR` | off | Durable local tail for buffered writes (requires `--write-buffer-ms > 0`): fsync'd per-table WAL appended BEFORE each buffered ack, replayed on boot — closes the unclean-kill loss window (node/disk loss still loses the tail) |
| `--tail-url` (serve) | `ICEGRES_TAIL_URL` | off | Durable Postgres-backed tail for buffered writes (requires `--write-buffer-ms > 0`, mutually exclusive with `--tail-dir`): each buffered INSERT commits to a frames table in this Postgres database BEFORE its ack, replayed on boot — survives losing the compute node (durability = the tail database's fsync/replication); an unreachable tail database blocks buffered writes (statement errors, never silent loss) |
| `--tail-quorum` (serve) | `ICEGRES_TAIL_QUORUM` | off | Quorum-replicated durable tail for buffered writes (requires `--write-buffer-ms > 0`, mutually exclusive with `--tail-dir`/`--tail-url`): exactly three `host:port` addresses of `icekeeperd` acceptors; each buffered INSERT is fsynced by 2 of 3 acceptors BEFORE its ack (Neon SafeKeeper's consensus, adapted — see NOTICE) — survives losing ANY single node incl. this one; fewer than 2 live acceptors blocks buffered writes; a competing server fences this one ("superseded by a newer server") |
| `--tail-api-port` (serve) | `ICEGRES_TAIL_API_PORT` | off | Open tail read API ([`docs/open-tail-protocol.md`](../docs/open-tail-protocol.md)): serve the buffer's durable un-flushed tail window over Arrow Flight (read-only listener; requires buffered mode + a durable tail). Auth rides `--auth-file` (Flight basic-auth handshake) |
| `--peer-tail` (serve) | `ICEGRES_PEER_TAILS` | off | Fleet overlays: comma-separated tail APIs of buffering peer computes to mirror; scans union each peer's un-flushed rows under the exactly-once watermark rule (best-effort — a dead/silent peer falls back to commit cadence with one WARN per outage) |
|  | `ICEGRES_PEER_TAIL_USER` / `ICEGRES_PEER_TAIL_PASSWORD` | off | Credentials the `--peer-tail` subscriber presents to peers secured with `--auth-file` (one identity for every configured peer; standard Flight basic-auth handshake per connection). Without them, an authed peer rejects the subscriber (Unauthenticated; reads stay on commit cadence) |
|  | `ICEGRES_TXN_STRICT` | off | Only relevant on catalogs WITHOUT the multi-table `transactions/commit` endpoint (with it — e.g. Lakekeeper — multi-table COMMITs are always atomic and strict mode never bites): refuse a multi-table `COMMIT` up front with `0A000` (nothing applied) instead of best-effort ordered per-table commits (where a partial apply reports `40003`, not the retryable `40001`). |

Logging uses `tracing` with an env filter: `RUST_LOG=debug icegres serve`.
Every connection runs inside a correlation span (`conn` id + peer) so
interleaved concurrent-connection logs de-multiplex. The remaining operational
knobs — memory pool (`ICEGRES_MEMORY_LIMIT_MB`), connection cap
(`ICEGRES_MAX_CONNECTIONS`), catalog timeout/retry, slow-query threshold,
`ICEGRES_LOG_FORMAT=json`, scan and cache tuning, and the `/metrics` series — are
listed with their defaults in
[`docs/configuration.md`](../docs/configuration.md). When `--auth-file` is set, a
per-source-IP **failed-auth backoff** slows credential brute-forcing (failures
decay after 60 s; a successful login is never delayed beyond the current
penalty). The **Flight SQL** listener supports in-process TLS via
`flight-serve --tls-cert/--tls-key` (advertises the `h2` ALPN; `grpc+tls://`
clients connect directly, no front proxy needed).

### Authentication (`--auth-file`)

Without `--auth-file` the server is **permissive** (any user/password
accepted — the historical behavior) and logs a startup `WARN` saying so.
With it, every connection must complete a SCRAM-SHA-256 exchange
(RFC 5802/7677 — the password never crosses the wire in cleartext, even
without TLS); a wrong password or unknown user is rejected with the standard
Postgres `28P01` error. The file holds one `user:password` per line (`#`
comments and blank lines ignored; the username must not contain `:`, the
password may):

```sh
printf 'app_user:s3cret\n' > auth.conf && chmod 600 auth.conf   # protect like .pgpass
icegres serve --auth-file auth.conf
PGPASSWORD=s3cret psql -h 127.0.0.1 -p 5439 -U app_user -d icegres
```

In memory the server keeps only the SCRAM salted hash (random per-user
16-byte salt from `/dev/urandom`, 4096 iterations), never the cleartext.
The file itself is cleartext on disk — `chmod 600` it. Clients too old for
SCRAM (pre-libpq-10) are rejected, not downgraded.

### TLS (`--tls-cert` / `--tls-key`)

```sh
bash ../infra/scripts/gen-dev-cert.sh   # self-signed dev cert -> infra/.data/tls/
icegres serve --tls-cert ../infra/.data/tls/dev.crt --tls-key ../infra/.data/tls/dev.key

psql "host=127.0.0.1 port=5439 user=postgres dbname=icegres sslmode=require"
# full verification against the pinned dev cert (SAN covers localhost):
psql "host=localhost port=5439 dbname=icegres sslmode=verify-full sslrootcert=../infra/.data/tls/dev.crt"
```

Any TLS setup error (missing file, bad PEM, mismatched pair) **aborts
startup** — icegres never falls back to plaintext when asked to serve TLS
(upstream datafusion-postgres's `serve_with_handlers` would only warn).
Like stock Postgres without `hostssl` rules, a TLS-enabled listener still
*accepts* plaintext startup: encryption is enforced from the client side
with `sslmode=require`/`verify-full`. Combine with `--auth-file` for
encrypted + authenticated serving; TLS protects the SCRAM exchange against
active MITM downgrade in addition to encrypting query traffic.

### ADBC (Arrow Flight SQL) — `icegres flight-serve`

The second first-class wire protocol (SPEC A11, `src/flight.rs`): the same
lakehouse, engine wiring (snapshot-aware caches from `cache.rs`, the
copy-on-write DML engine from `overwrite.rs`), served over Arrow Flight SQL
on gRPC. Everything stays Arrow end to end — no row-format round trip —
which is exactly what ADBC clients (pandas 2.x, polars, DuckDB, Go/Rust/
Java ADBC) consume natively.

```sh
icegres flight-serve                  # grpc://0.0.0.0:50051
```

```python
import adbc_driver_flightsql.dbapi as fs
import pyarrow as pa

conn = fs.connect("grpc://127.0.0.1:50051")
cur = conn.cursor()
cur.execute("SELECT city, count(*) FROM demo.trips GROUP BY city")
print(cur.fetch_arrow_table())                        # Arrow, zero conversion
cur.execute("SELECT * FROM demo.trips WHERE trip_id = $1", parameters=(7,))
conn.adbc_get_objects(depth="all")                    # catalogs/schemas/tables/columns

# THE killer feature — bulk ingest: the whole Arrow stream lands as ONE
# Iceberg fast-append commit (rolling Parquet writer, default target file
# size). 100k rows ≈ one snapshot + a handful of properly-sized files,
# vs. one commit per statement on the INSERT path.
cur.adbc_ingest("adbc_ingest", arrow_table, mode="append", db_schema_name="demo")
```

Surface: queries (`CommandStatementQuery`), catalog metadata
(GetCatalogs/GetDbSchemas/GetTables with `%`/`_` filters + Arrow schemas/
GetTableTypes/GetSqlInfo), prepared statements with `$n` binds,
INSERT/UPDATE/DELETE with real affected counts (UPDATE/DELETE route through
the same copy-on-write engine and scope rules as the pgwire `DmlHook`), and
bulk ingest (`CommandStatementIngest`, append into an existing table;
`mode="create"`/`"replace"`, temporary tables and ingest transactions are
rejected loudly). Verified end-to-end against `adbc_driver_flightsql`
(bench/clients/a11_adbc_probe.py; e2e section (p); parity probe A11).

Auth: `--auth-file` (same `user:password` file as `serve`) enables the
Flight SQL basic-auth handshake — credentials are verified against the
stored SCRAM verifier (never kept in cleartext) and exchanged for a
per-boot bearer token. NOTE basic auth sends the password itself (unlike
pgwire SCRAM), so front the listener with TLS: gRPC TLS termination is not
built in; use any gRPC-aware proxy/LB (nginx `grpc_pass`, envoy) — ADBC
clients connect with `grpc+tls://`. Without `--auth-file` the endpoint is
permissive and logs a startup WARN, matching `serve`.

The `adbc_driver_postgresql` (libpq) lane also works against plain
`icegres serve`: reads run over `COPY (query) TO STDOUT (FORMAT binary)`
(implemented in `ops.rs::CopyOutHook` on both wire protocols, also usable
from psql with text/csv/binary formats), plus parameterized queries,
`get_objects` and DML rowcounts — use `autocommit=True` (documented limit:
extended-protocol SELECT inside an explicit transaction, 0A000). Its bulk
ingest issues `COPY ... FROM STDIN`, which is out of scope by design —
ingest belongs to the Flight lane.

### Scale-to-zero (`--idle-shutdown-secs`)

icegres computes are stateless — all durable state lives in the Iceberg
catalog + object store — so an idle server can simply exit. With
`--idle-shutdown-secs N` the server exits cleanly (code 0) once no client
connection has been open for `N` consecutive seconds (the countdown also
starts at boot). Pair it with a supervisor that restarts on demand to get
the scale-from-zero half; cold start is a few hundred ms:

```sh
# simplest supervisor loop: exits when idle, next client wakes it up
while :; do icegres serve --idle-shutdown-secs 300; done
```

```ini
# systemd: clean idle exit (code 0) does not restart with Restart=on-failure;
# use a socket-activated unit to respawn on the next connection.
[Service]
ExecStart=/usr/local/bin/icegres serve --idle-shutdown-secs 300
Restart=on-failure
```

Health-endpoint connections (below) do not count as client activity, so
liveness probes never keep an idle server alive.

For a shipped supervisor that also completes the scale-from-zero half (and
adds branch routing), use `icegresd` below.

### icegresd — the minimal control plane (`icegresd serve` / `icegresd status`)

`icegresd` (a second, ~3 MB binary in this crate) is the missing OSS piece
of the Neon-style loop: a pgwire-aware proxy/supervisor that makes
scale-to-zero fully transparent to clients and routes one public port to
per-branch computes.

```sh
# quickstart: public endpoint on :5432, computes spawned on demand
./target/release/icegresd serve --port 5432 --idle-shutdown-secs 300

# first connection wakes the main compute (on :5439), then splices bytes
psql -h 127.0.0.1 -p 5432 -U postgres -d icegres -c 'select 1'

# branch endpoint: dbname 'icegres@<branch>' routes to a per-branch compute
# spawned with `icegres serve --branch <branch>` on an ephemeral local port
./target/release/icegres branch create demo.trips dev
psql -h 127.0.0.1 -p 5432 -U postgres -d 'icegres@dev'

# inspect computes, branches, ports, PIDs, restart counts
./target/release/icegresd status
```

* **Wake-on-connect.** If the target compute is not running, the connection
  spawns it, waits for TCP readiness, then forwards the client's ORIGINAL
  startup bytes and splices. Computes idle-exit (`--idle-shutdown-secs`,
  default 300) and are reaped; the next connection re-wakes them —
  measured wake-after-idle is ~85 ms end to end on the dev box
  (`cold_start_via_proxy_ms` in the bench).
* **Routing.** Only the plaintext pgwire `StartupMessage` is parsed (the
  `database` parameter, before any auth). `icegres` → main compute;
  `icegres@<branch>` → that branch's compute (`[A-Za-z0-9_-]+`). Create the
  branch first (`icegres branch create`), or queries fail loudly on the
  branch endpoint.
* **Session pooling (warm backend connections).** icegresd keeps up to
  `--pool-size` (default 8) warm, pre-handshaked pgwire connections per
  compute. A client whose startup matches the pool identity
  (`user == --pool-user` (default `postgres`), `database` == the compute's
  canonical name (`icegres` / `icegres@<branch>`), no `options` parameter)
  is handed a warm connection: icegresd replays the cached backend greeting
  and the client reaches ReadyForQuery in well under a millisecond, even
  though its session is a brand-new backend session. Everything else —
  pool empty (overflow), different user/database, `options` present, or
  `ICEGRES_AUTH_FILE` set (SCRAM cannot be pre-answered; pooling disables
  itself) — falls through to a direct compute connection with the client's
  original startup forwarded verbatim. Non-identity startup parameters of
  pooled clients (e.g. `application_name`) are ignored, like PgBouncer's
  `ignore_startup_parameters`.

  **One client per backend connection — no reuse, and no transaction
  pooling.** icegres sessions carry real state (transaction buffers, `SET`
  variables, prepared statements) and there is no `DISCARD ALL`-style
  reset in datafusion-postgres, so a returned connection could leak one
  client's state into the next. Every warm connection therefore serves
  exactly ONE client session and is closed with it; the pool is a
  warm-SPARE pool, refilled in the background (correctness over reuse).
  Transaction pooling (PgBouncer `pool_mode=transaction`) is deliberately
  NOT implemented: it would hop statements across backend sessions between
  transactions and silently lose `SET` state, prepared statements, and
  buffered-write ordering — session state makes it unsafe here by
  construction.

  Pooling coexists with scale-to-zero: warm conns are active sessions on
  the compute, so after `--pool-idle-secs` (default 60) with zero client
  sessions the pool drains itself, the compute's `--idle-shutdown-secs`
  clock runs, and the next connection re-wakes and re-warms. The pool is
  also cleared/re-warmed around compute crashes and restarts.
  `icegresd status` shows per-compute `pool` stats (warm spares, pooled vs
  direct sessions). Bench evidence: `connect_via_proxy_ms` (client connect
  -> ReadyForQuery via a warm handout) vs `cold_start_via_proxy_ms` in
  `bench/SCORECARD.md`; `--pool-size 0` disables pooling entirely.
* **Supervision.** Clean idle exits are scale-to-zero; UNCLEAN compute
  exits (crash, `kill -9`) are restarted with capped exponential backoff
  (0.5 s/1 s/2 s, max 3 per crash episode) and logged loudly.
* **TLS terminates at the compute.** icegresd answers `SSLRequest` with `N`
  (plaintext at the proxy; libpq's default `sslmode=prefer` falls back
  automatically) and talks plain TCP to computes on localhost. Clients that
  require TLS should connect directly to a compute started with
  `--tls-cert/--tls-key`. `CancelRequest` is not routed (no backend-key
  tracking).
* **Config.** Computes inherit icegresd's environment, so every `ICEGRES_*`
  variable (catalog, S3, `ICEGRES_AUTH_FILE`, `ICEGRES_WRITE_BUFFER_MS`,
  `ICEGRES_HEALTH_PORT`, ...) applies to spawned computes; `--host/--port/
  --branch/--idle-shutdown-secs` are always passed as flags by icegresd and
  win over env. Flags: `--port` (public, default 5432), `--main-port`
  (default 5439), `--icegres-bin` (default: sibling binary), `--compute-host`
  (default 127.0.0.1), `--wake-timeout-ms` (default 10000), `--status-file`
  (default `<tmpdir>/icegresd-status.json`), `--pool-size` (default 8, 0 =
  off), `--pool-user` (default `postgres`), `--pool-idle-secs` (default
  60); env `ICEGRESD_*` equivalents exist for all.

* **Kubernetes mode (`--k8s-compute` / `--k8s-scale`, opt-in).** In a
  cluster icegresd never forks: `--k8s-compute` makes the main compute a
  REMOTE pod dialed at `--compute-host:--main-port` (the writer Service),
  and `--k8s-scale deployments/<name>|statefulsets/<name>` (implies
  `--k8s-compute`) adds wake-on-connect (scale 0 → 1, then the normal
  readiness poll) and idle scale-to-zero (zero proxied sessions for
  `--idle-shutdown-secs` → scale to 0) by patching that workload's
  apps/v1 scale subresource with the pod serviceaccount — RBAC needed:
  `get`+`patch` on exactly that one object's scale. Process-mode-only
  features are refused loudly in k8s mode (`--health-check-ms` — the
  kubelet's liveness probe owns compute replacement; `--read-replicas-max`
  — use a Deployment + HPA; branch endpoints — deploy per-branch
  computes). The Helm chart at `deploy/helm/icegres` wires all of this
  (see `docs/deployment.md` §11).

### Health checks (`--health-port`)

The pgwire port itself is health-checkable with a plain TCP connect (e.g.
Kubernetes `tcpSocket`, `nc -z host 5439`) or, for full readiness, a real
round trip: `psql -h host -p 5439 -U postgres -d icegres -c 'select 1'` —
this is what the bench/parity/e2e harnesses use. With `--health-port P` the
server additionally exposes a dedicated HTTP *liveness* endpoint: any
request to that port (e.g. `curl http://host:P/health`) is answered with
`HTTP/1.1 200 OK` and body `ok`. It asserts the process is up and accepting
connections; it does not probe the catalog.

### Time travel (`table@snapshot_id`)

Every Iceberg snapshot retained in table metadata is queryable. List
snapshots via the `$snapshots` metadata table, then pin any query to a
snapshot with the quoted `table@snapshot_id` form (read-only):

```sql
select snapshot_id, committed_at from demo."trips$snapshots" order by committed_at;
select count(*) from demo."trips@4436304835314641572";  -- e.g. the seed snapshot
```

### Transactions

`BEGIN` / `COMMIT` / `ROLLBACK` are real (`src/txn.rs`): reads inside a
transaction are snapshot-pinned per table (snapshot isolation), writes are
buffered in the session with read-your-own-writes overlays, and `COMMIT`
composes everything into **one** Iceberg snapshot per table anchored at the
pinned snapshot. Concurrency is first-committer-wins: if another writer
committed to a touched table since the pin, `COMMIT` fails with SQLSTATE
`40001` (retry the transaction). A statement error inside a transaction
poisons it (`25P02`) and `COMMIT` then rolls back, like stock Postgres.

**Multi-table COMMITs are atomic across tables** when the catalog
implements the Iceberg REST multi-table transaction endpoint
(`POST /v1/{prefix}/transactions/commit`) — verified against Lakekeeper,
the assumed catalog: the whole COMMIT is ONE all-or-nothing catalog request
carrying every table's `assert-ref-snapshot-id` pin, so every table commits
or none does, and any conflict is a clean, retryable `40001` with nothing
applied. Endpoint support is read from `GET /v1/config`'s capability list
(or probed once on first use) and cached. On a catalog without the
endpoint, multi-table COMMITs fall back to the documented ordered per-table
path (a partial apply reports `40003` — see `docs/limitations.md`), and
`ICEGRES_TXN_STRICT=true` refuses them up front (`0A000`, nothing applied);
with the endpoint, strict mode is satisfied by atomicity and never refuses.

```sql
begin;
insert into demo.trips values (900001, 'Ghent', 12.5, 21.0, now());
update demo.trips set fare = 22.0 where trip_id = 900001;  -- sees the insert
commit;  -- exactly one new Iceberg snapshot
```

### Primary keys (`--enforce-pk`)

Iceberg has no constraint concept; icegres adds opt-in enforcement. Declare
a key as a table property (`icegres.primary-key = "col1[,col2…]"`), serve
with `--enforce-pk`, and every INSERT (and PK-assigning UPDATE) gets
NOT NULL (`23502`) and uniqueness (`23505`) checks validated against the
very snapshot the commit anchors to — including a transaction's own
buffered rows. Off by default because enforcement reads the key columns of
every live data file per write; it is racy-free per commit but adds a
read-before-write cost.

### Bounded-staleness reads (`--freshness-ms N`, opt-in)

By default every scan performs one catalog `load_table` round trip (~2–3 ms
against local Lakekeeper) purely to detect snapshot changes — exact
freshness, and the single largest line item of the ~7 ms read path (measured
with `ICEGRES_QUERY_TIMING=1`). With `N > 0` that trade is made explicit and
bounded instead:

- ONE background task per server polls the catalog for every mounted table
  each `N` ms — tables refresh concurrently (up to 8 in flight), each with a
  retry-free per-table timeout of min(4·`N`, 2 s) (the next pass is the
  retry), so a slow or stalled table delays only itself — and swaps the
  cached provider on metadata change; scans serve the cached snapshot with
  **no catalog round trip**. The refresher runs under a supervisor that
  respawns it (budgeted, loudly) if it ever dies.
- **Read-your-own-writes stays exact.** Every local write path — sync DML,
  PK-enforced INSERT, transaction COMMIT, buffer flush, plain INSERT —
  synchronously invalidates the touched table, so the next read on this
  server loads fresh metadata and observes the write immediately (locked by
  a live unit test and e2e §(z)). Buffered/keyed rows are additionally
  readable pre-commit through the per-scan buffer overlay, unchanged.
- **Foreign writers** (other servers, Spark, anything committing through
  the catalog) become visible within ~`N` ms plus one refresh round trip —
  bounded staleness, per table: a slow table delays only itself (up to its
  per-table refresh timeout), never other tables' visibility. Time travel
  and branch-pinned reads are unaffected (snapshot-addressed reads are
  immutable). Enabling the mode logs a WARN stating this bound.
- **Catalog outage honesty:** reads keep serving the last refreshed
  snapshot (set `ICEGRES_STALE_READ_ON_CATALOG_ERROR=0` to fail loudly
  instead); the refresher WARNs (rate-limited) and exports the worst-case
  staleness age as the `icegres_freshness_age_ms` gauge on `/metrics` —
  sampled at each refresher pass START, so a healthy gauge reads ≈ `N` and
  it keeps growing through an outage (or if the refresher itself dies; its
  supervisor's watchdog keeps bumping it).
- **Physical-plan cache** (`src/plancache.rs`): with the freshness mode on,
  repeated *identical* simple-protocol SELECTs reuse their physical plan
  (LRU, `ICEGRES_PLAN_CACHE_ENTRIES`, default 256), keyed on statement text
  + search path + time zone and validated against each referenced table's
  metadata version — a snapshot change, local write, or DDL makes the entry
  miss and re-plan. Overlay-bearing (buffered) tables, time-travel/metadata
  tables, and non-immutable expressions (`now()`, `random()`, …) are never
  cached.

**Measured on the dev box** (release build, local stack, 30-query p50 per
leg, `ICEGRES_QUERY_TIMING=1`): point lookups with distinct literals drop
from ~7.4 ms to **~4.4 ms** (the per-scan freshness check disappears from
physical planning: 3.46 → 0.49 ms); repeated identical statements
additionally skip planning entirely via the plan cache — repeated point
lookup **~3.6 ms**, repeated filtered aggregate 6.9 → **~2.8 ms**. Default
mode (`--freshness-ms 0`) is byte-identical to the historical exact-freshness
path.

### Buffered writes (`--write-buffer-ms N`, opt-in)

Moonlink-style group commit. With `N > 0`, INSERTs acknowledge after
appending to an in-memory buffer (~1.5 ms p50 vs ~50–60 ms synchronous)
and a background task commits the buffer to Iceberg every `N` ms (or at
`ICEGRES_WRITE_BUFFER_MAX_ROWS`). Every read on the same server unions the
committed table with the buffer, so read-your-writes holds across all
local connections instantly; other servers/readers see rows at the commit
cadence (≤ N ms after ack). UPDATE/DELETE/BEGIN/DDL/PK-checked INSERTs
flush first (ordering fences). **Trade-off, stated plainly:** an *unclean*
kill (SIGKILL, power loss) loses up to N ms of acked-but-uncommitted
writes. A *clean* shutdown (SIGTERM/SIGINT — rolling deploys) flushes the
buffer before exiting, so a graceful stop loses nothing. That is why the
default is `0` — fully synchronous, semantics identical to not having the
feature — and enabling it logs a WARN. Both halves of the contract are
locked by e2e (kill-loss vs clean-shutdown-flush), and the union-read flush
race is covered by `buffer.rs` unit tests.

**Durable tail — three backends.** Buffered mode can attach a durable tail
that every INSERT is appended to BEFORE its ack, replayed into the buffer
on the next boot; the three backends hold the identical exactly-once
protocol (the `icegres.tail-seq.<tail-id>` watermark stamped into each
flush commit) and differ only in where the tail lives — i.e. in the
durability class:

| Backend | Flag | The tail lives in | Survives unclean kill | Survives node/disk loss | Ack cost |
|---|---|---|---|---|---|
| Local WAL | `--tail-dir <dir>` | fsync'd segments on this node's disk | yes | **no** — the tail dies with the disk | one local fsync, group-committed under concurrency (3.6 ms p50 single-writer, bench `durable_ack_dir_ms`; ~2.4–2.5 ms statement-level probe) |
| Postgres | `--tail-url <postgres url>` | a `frames` table (schema `icegres_tail`, auto-created) in any Postgres database — the natural target is a dedicated DB on the instance already backing Lakekeeper | yes | **yes** — durability = the tail database's own fsync/replication (a delegated single system) | one INSERT round trip + commit (2.7 ms p50 to a same-box database, bench `durable_ack_pg_ms`; 2.2 ms statement-level probe) |
| Quorum (consensus) | `--tail-quorum h:p,h:p,h:p` | a replicated record log fsynced by three `icekeeperd` acceptor daemons (Neon SafeKeeper's proposer–acceptor consensus, adapted — see NOTICE and `src/quorum/`) | yes | **yes — any single node**, acceptor or compute, with no delegated single system: an ack means 2 of 3 independent disks hold the record | one LAN round trip + the slower of 2 acceptor fsyncs (4.1 ms p50 on localhost, bench `durable_ack_quorum_ms`; 2.5 ms statement-level probe) |

**The write-latency ladder, measured end to end** (dev box, local
Lakekeeper 0.13.1 + RustFS + PG16; single-row INSERT ack p50 unless noted;
`bench/bench.sh` reports the tail rungs as `durable_ack_{dir,pg,quorum}_ms`).
Physics framing: an Iceberg commit is several object-store PUTs + a catalog
POST and can never ack in single-digit milliseconds on real object storage —
sub-10 ms durable writes MEAN the tail path; the sync path's honest floor
is a few tens of milliseconds:

| Path | Durable-ack p50 | Durability class | Cross-server/engine visibility | Semantics trade |
|---|---|---|---|---|
| Synchronous `INSERT` (default) | ~46 ms (batch-100 ~40 ms — batching is nearly free) | Iceberg snapshot (object storage + catalog) | global, immediately | none |
| Buffered `INSERT` | ~1.4 ms | process memory ONLY | this server instantly; global at the flush (≤ N ms) | unclean kill loses ≤ N ms of acked rows |
| + `--tail-dir` | ~3.6 ms bench (statement-level probe ~2.4–2.5 ms); concurrent writers share fsyncs (8 writers: p50 ~6.1 ms vs ~9–10 ms serialized) | this node's disk, fsync before EVERY ack | as buffered | node/disk loss |
| + `--tail-url` | ~2.7 ms bench (probe ~2.2 ms) | tail database (its replication) | as buffered | tail DB on the write path |
| + `--tail-quorum` | ~4.1 ms bench (probe ~2.5 ms) | 2-of-3 acceptor disks (consensus) | as buffered | three acceptors, 2 live required |
| Keyed `UPDATE`/`DELETE` (`icegres.tail-upsert`) | ~5.2 ms with `--freshness-ms 25`, ~7.0 ms without | as the attached tail | union read instant; ONE coalesced commit per window | exact-PK shapes; per-key last-writer-wins window |
| Explicit transaction `COMMIT` | synchronous (~50 ms+) | Iceberg snapshot | global, immediately | stays sync BY DESIGN — a tail-staged COMMIT would ack before conflict detection and break `40001` (docs/limitations.md) |

The flags are mutually exclusive (one server writes ONE tail), and all
require `--write-buffer-ms > 0`. For `--tail-url`: the identity behind the
watermark key is minted once into the schema's `meta` table (same URL =
same logical tail across restarts), and an unreachable tail database fails
startup / fails the INSERT statement mid-flight — backpressure, never
silent loss, exactly like a failing tail disk. A session advisory lock
refuses a second `serve` on the same URL/schema at boot — best-effort
boot-time mutual exclusion, NOT the correctness guard: the lock releases
with its session, and exactly-once is enforced by the in-commit watermark
property + the catalog's `assert-ref-snapshot-id` CAS + the fresh metadata
reload before every flush attempt (`buffer.rs`), so even a replacement
server overlapping a half-dead predecessor cannot double-apply. The URL
must be a direct connection or session-pooled (a transaction-mode pooler
would silently void the session lock; boot verifies and refuses). TLS URLs
are not yet supported (keep the tail database
on localhost or a trusted segment), and fleet-SHARED tails — several
computes overlaying one tail — are the roadmap's next increment, not this
backend (docs/sota-roadmap.md §3).

**The quorum backend in detail (`--tail-quorum h:p,h:p,h:p` + `icekeeperd`).**
Consensus-class durability with no delegated single system: run exactly
three acceptor daemons (`icekeeperd serve --port P --data-dir D`, the third
binary in this crate) on independent nodes/disks, and every buffered
INSERT's record is fsynced by 2 of the 3 BEFORE the client ack. The
protocol is Neon SafeKeeper's proposer–acceptor algorithm adapted for the
generic tail log (terms, persist-before-respond voting, term-history
reconciliation, divergence truncation, the Raft commit rule — `src/quorum/`,
attribution in NOTICE); the record framing is the same crc-framed segment
machinery the local tail proved (`src/segment.rs`). Boot = an election:
the server adopts the quorum's tail identity, wins a vote from 2 of 3,
recovers the unfinished committed suffix from the most advanced acceptor,
reconciles every acceptor's log to it, and replays the recovered rows into
the buffer — so acked-but-uncommitted rows survive `kill -9` of the
compute, loss of the compute NODE, or loss of any ONE acceptor. Two live
acceptors = writes proceed; one live = statement errors and the tail
poisons itself until restart (backpressure, never silent loss — and a
timed-out record that may still commit later is never re-numbered).
**Fencing replaces lock files:** a second icegres opening the same quorum
wins a higher-term election, the first server's next INSERT fails with
"superseded by a newer server (term X)", and the second recovers its
unflushed acked rows — split-brain is structurally impossible, not
advisory-locked away. Honest scope (docs/limitations.md): static 3-node
membership (a replacement acceptor joins empty only via a fresh election),
no TLS/auth between proposer and acceptors (trusted network segment only),
proposer-driven catch-up only (no acceptor gossip), and the acceptors'
log truncation (horizon) lags flushes by design — bounded, and the latest
per-table watermark record is always retained as the replay sidecar.

**The local backend in detail (`--tail-dir <dir>`)** closes the unclean-kill
window without giving up the buffered ack (measured on the dev box: 3.6 ms
p50 ack with the tail (bench `durable_ack_dir_ms`; ~2.4–2.5 ms statement-level
probe) vs ~1.4 ms untailed and ~46 ms synchronous — the
fsync is the price of the closed window, and concurrent statements share it:
frames are staged under the lock, the fsync runs group-committed outside it,
so 8 concurrent writers see p50 ~6.1 ms instead of ~9–10 ms serialized): every
buffered INSERT is appended
to an fsync'd per-table WAL segment under `<dir>` BEFORE the client ack (a
tail write failure is the statement's error — never a silent downgrade), and
on the next boot with the same `--tail-dir` acked-but-uncommitted rows are
replayed into the buffer and committed by the normal flusher, so SIGKILL /
power loss of the process loses nothing. Exactly-once across crashes is
anchored in the lake: each flush commit records the highest drained tail
sequence as a table property namespaced by the tail's persistent identity
(`icegres.tail-seq.<tail-id>`, minted once into `<dir>/identity`) in the same
atomic commit, plus a best-effort local sidecar (`<dir>/<table>/watermark`),
and boot replay drops frames at or below `max(property, sidecar)` (a crash
between commit and tail truncation cannot double-apply; several buffered
writers on one table keep independent cursors). **Honest scope:** durability
is THIS node's disk — losing the node or the disk still loses the tail; this
is a strict upgrade over in-memory buffering, not node-loss durability
(`src/tail.rs`). Like the pending buffer it mirrors, the tail dir grows
without bound while the catalog is unreachable (nothing truncates until a
flush commits), and boot replay materializes the whole surviving tail in
memory before the flusher drains it. One residual double-apply window
remains: a crash between the commit and the sidecar write COMBINED with a
foreign writer dropping the table property. The tail dir is single-writer
(exclusive `flock`; a second server on the same dir is refused at boot).
Requires `--write-buffer-ms > 0` (refused at boot otherwise); verified
standalone by `icegres/tests/tail_durability.sh` (kill -9 with the tail =
zero loss, without = the documented loss, plus the no-double-apply and
post-flush-restart sequence-floor cases — proven for ALL THREE backends:
sections 2–4 local, 6–8 Postgres, 10 quorum incl. acceptor-kill and
fencing; the quorum consensus core is additionally covered by in-process
integration tests in `src/tail_quorum.rs` — 3 real acceptors on ephemeral
ports, no shell needed). The Postgres backend's unit tests
(`src/tail_pg.rs`) run live against `ICEGRES_TEST_PG_URL` (the local
stack's `postgresql://lakekeeper:lakekeeper@127.0.0.1:5433/icegres_test`)
and skip cleanly when it is unset.

### Hot rows: keyed tail upserts (`icegres.tail-upsert`, opt-in)

Roadmap Phase 2 (docs/sota-roadmap.md §4). On an opted-in table, an
autocommit `UPDATE ... WHERE <exact PK equality>` (literal SET values) or
`DELETE ... WHERE <exact PK equality>` skips the synchronous copy-on-write
commit entirely: the statement resolves the key's current row through the
same union view a scan sees, fsyncs ONE keyed frame to the durable tail,
and acks. The flusher coalesces every keyed op of the window per key
(last-writer-wins) and applies them as ONE composed COW commit — N updates
to a hot row become one file rewrite per flush window instead of N
serialized ~55–70 ms commits with `40001` storms, and an acked keyed op is
never exposed to a client-visible `40001` (flush conflicts with foreign
writers retry internally; the rows stay tail-durable meanwhile).

**Measured on the dev box** (`--write-buffer-ms 250 --tail-dir`, 50
sequential UPDATEs to one hot committed row, psql, autocommit):

| Path | UPDATE ack p50 | p95 | Snapshots produced |
|---|---|---|---|
| Keyed tail + `--freshness-ms 25` | **5.2 ms** | 7.8 ms | one per flush window |
| Keyed tail (exact freshness) | **7.0 ms** | 10.4 ms | one per flush window |
| Synchronous COW UPDATE (same table shape, no property) | ~47.5 ms | ~59 ms | one per statement |

The keyed ack budget (per-stage `ICEGRES_QUERY_TIMING` p50s): activation
gate ~2.4 ms as one catalog `load_table` under exact freshness, ~0 when
`--freshness-ms` serves it from the freshness cache; current-row resolution
~0 on a keyed-map hit (a hot key already in the window) or one union-view
read otherwise (which itself rides the freshness/plan caches when enabled);
DataFusion row fold ~2.3 ms; tail fsync ~1 ms — the documented
read-modify-write cost of the ack (`docs/limitations.md`).

**Activation matrix — ALL of these, or the statement silently takes the
unchanged fence-then-synchronous path (never an error):**

| Requirement | How |
|---|---|
| Declared primary key | table property `icegres.primary-key = "col[,col...]"` |
| Keyed-tail opt-in | table property `icegres.tail-upsert = "true"` |
| Buffered writes | `--write-buffer-ms N` with `N > 0` |
| A durable tail | `--tail-dir <dir>` or `--tail-url <postgres url>` |
| Statement shape | exact equality on ALL PK columns with literals (AND-composed for composite keys), no other predicates, no `RETURNING`/joins/subqueries/bind parameters; UPDATE additionally: literal SET values, PK columns not assigned |
| PK column types | Iceberg `int`/`long`/`string`/`boolean`/`date` |
| Key cardinality | the key currently matches at most ONE row (duplicate keys fall back to the sync path) |

Durability and crash recovery ride the existing tail machinery: one keyed
statement = one fsync'd tail frame (op-discriminated payload, format v2),
replayed in sequence order into the keyed map on boot, truncated by the
same watermark protocol as inserts. Reads on this server merge lake + tail
by key: committed rows (and older buffered layers) whose key was updated or
deleted are suppressed and the newest buffered version unions in;
time-travel (`table@snapshot`) and metadata tables stay point-in-time pure.
**Semantics shift, stated plainly (docs/limitations.md):** within a flush
window a keyed table trades snapshot-isolation-per-statement for per-key
last-writer-wins **in ack (tail-sequence) order** — a plain INSERT of a
key acked after a keyed delete/update of that key becomes its newest
version (delete-then-reinsert in one window leaves the row present with
the inserted values), and one acked before it loses, exactly as wall-clock
ack order suggests. Explicit `BEGIN…COMMIT` keeps today's
fence-flush-then-sync path (serialized per table against in-flight keyed
statements, so a committed synchronous write can never be clobbered by a
keyed statement's stale row image), and `--enforce-pk` composes: enforced
INSERTs still fence, keyed updates still work (a keyed upsert is
delete-then-insert of one key, so uniqueness is inherent). **Flight SQL is
out of scope:** `flight-serve` is a separate process with no write buffer
or tail — it executes DML synchronously, keyed routing never applies
there, its counts are sync-exact, and it sees another server's in-window
keyed ops at the commit cadence like any cross-server reader. Verified end
to end by `icegres/tests/tail_durability.sh`
section 9 (keyed UPDATE/DELETE ack fast, survive kill -9, commit exactly
once; fence path proven for tables without the property) and
`icegres/tests/e2e.sh` section (x) (20 hot-row UPDATEs = zero
mid-window snapshots + ONE composed commit; union read sees the newest
value mid-window; delete-then-reinsert of one key in one window leaves the
row present with the inserted values; time travel unaffected).

### Zero-copy branches (`icegres branch`, `serve --branch`)

Neon's branch-per-endpoint model on Iceberg snapshot refs: a branch is a
named ref in table metadata, so creating one is a single metadata commit —
zero data copied, however large the table.

```sh
icegres branch create demo.trips dev        # fork main's head (or --at-snapshot <id>)
icegres serve --branch dev --port 5440 &    # a second endpoint on the branch
psql -h 127.0.0.1 -p 5440 -c "insert into demo.trips values (…)"  # commits to 'dev'
icegres branch list demo.trips              # main + dev, diverged heads
icegres branch drop demo.trips dev          # removes the ref only

icegres branch create-all staging           # whole-lakehouse branch: EVERY table,
                                            # ONE atomic multi-table transaction
icegres branch drop-all staging             # atomic removal everywhere it exists
```

`create-all`/`drop-all` are whole-lakehouse operations: one atomic
`transactions/commit` request sets (or removes) the ref on every table, and
each table's request additionally pins `main` to the head captured when the
table was loaded, so the branch is a **consistent-or-nothing cross-table
cut** — it can never capture half of a concurrent (even atomic multi-table)
commit. If any table already has the branch (create-all), a concurrent
commit races the cut, or the endpoint is missing, nothing is applied (a
raced create-all is safe to just retry). `icegres serve --branch <name>`
then serves that cut as one endpoint.

Reads on a `--branch` server pin to the branch head; all writes (INSERT,
UPDATE, DELETE, transactions) commit with `assert-ref-snapshot-id` on the
branch ref, so endpoints on different branches never conflict and nothing
can leak onto `main`. A table without the ref fails loudly — no silent
fallback. Both branches share every file below the fork point; only new
commits diverge.

### Table maintenance (`icegres maintain`)

Every commit adds a snapshot forever; long-lived tables need three periodic
maintenance passes, all safe against a live serving endpoint:

```sh
# 1. Bin-pack small files: dry-run prints the plan, --execute rewrites them
#    into ~target-size files as ONE `replace` snapshot (row set identical)
icegres maintain compact --table demo.trips
icegres maintain compact --table demo.trips --execute

# 2. Trim metadata: keep the newest 50 snapshots + everything a ref points at
icegres maintain expire-snapshots demo.trips --keep 50

# 3. Reclaim the bytes expiry stranded: dry-run first, then delete
icegres maintain remove-orphans demo.trips                  # report only
icegres maintain remove-orphans demo.trips --execute        # delete (72h grace)
```

`expire-snapshots` is metadata-only (one anchored REST commit); `remove-orphans`
is the storage half: it lists the table's S3 prefix, subtracts everything any
retained snapshot/ref still references (data files, manifests, manifest lists —
including files named by DELETED manifest entries — plus the metadata-JSON log
and statistics files), and deletes the rest. The guard model, plainly: the
grace window (`--older-than-hours`, default 72) is THE guard for in-flight
commits — from icegres or any foreign writer — whose files exist in storage
but not yet in the catalog; a fixed 15-minute clock-skew allowance is folded
into the cutoff, and `--execute` verifies the real host-vs-store skew with a
tiny write/stat/delete probe under `metadata/` (abort beyond the allowance;
probe failure aborts too). `--execute` with a grace window under 1 h is
refused unless `--unsafe-grace` asserts the table is quiescent — that flag is
for quiescent tables only (e.g. tests); concurrent writers WILL lose in-flight
files. It fails closed on anything ambiguous: unreadable metadata or manifests
abort the run, a recorded file path outside the listed bucket aborts the run
(liveness cannot be verified against a listing that cannot see it),
unknown-age objects and unrecognized files are never deleted, and a commit
landing mid-run re-derives the live set.

`compact` bin-packs each partition's data files under `--target-file-mb`
(default 128) into ~target-size files as ONE `replace` snapshot: the row set
is identical, old files stay time-travel-readable until expiry + GC reclaim
them (the GC knows they are not orphans), and the commit is anchored to the
exact snapshot the plan was computed against — a concurrent commit (foreign
writer, DML, buffered flush) makes it abort cleanly with nothing changed;
re-run it. Tables bearing foreign merge-on-read delete manifests are refused
loudly (icegres cannot apply those deletes — `docs/limitations.md`), as are
partitioned tables. Buffered/tail mode already keeps file sizes healthy by
group-committing per window instead of per statement, so compaction is mostly
for tables fed by per-statement commits or foreign micro-batchers.

### Works with your ORM/BI tool

Real clients issue much gnarlier `pg_catalog` introspection than psql, and
icegres serves it (`src/compat.rs`: a coherent-oid `pg_class`/`pg_namespace`/
`pg_attribute` snapshot, a Postgres-parseable `version()`, and AST rewrites
for the `unnest(indkey)`/`generate_subscripts`/nested-correlated-subquery/
regclass constructs ORMs generate). Verified end-to-end by
`bench/clients/a8_orm_probe.py` (e2e section (o), parity probe A8):

| client | verified |
|---|---|
| **psycopg2** 2.9 | connect (plain + SCRAM over TLS), simple-protocol queries, `BEGIN`/`COMMIT`/`ROLLBACK` with read-your-own-writes and cross-connection visibility |
| **pg8000** 1.31 | connect, extended-protocol parameterized queries, prepared-statement reuse across executions |
| **SQLAlchemy** 2.0 | `inspect()` (schemas, tables, columns with correct types), full reflection of `demo.trips` into a `Table`, ORM expression-language SELECT/filter/GROUP BY — over both psycopg2 and pg8000 |
| **pandas** 3.x | `read_sql` of a join through SQLAlchemy |

Known limits (documented failures, not silent corruption):

- **Server-side (named) cursors** — `DECLARE CURSOR`/`FETCH` is not
  implemented by the DataFusion pgwire front-end; use client-side cursors
  (psycopg2's default unnamed cursor works).
- **Extended-protocol SELECT inside an explicit transaction** is rejected
  with a clean `0A000` (transactional SELECT is simple-protocol only).
  psycopg2 uses the simple protocol, so ORM transactions work; for pg8000
  use autocommit (`isolation_level="AUTOCOMMIT"` in SQLAlchemy) or psycopg2.
- `pg_constraint`/`pg_index` never reference user tables (Iceberg has no
  PK/index objects), so ORMs correctly see “no primary key / no indexes”.

### JDBC

The stock PostgreSQL JDBC driver (pgjdbc 42.7) works out of the box —
no custom driver, no URL tricks:

```java
// Standard pgjdbc connection string (any user/password unless --auth-file):
Connection conn = DriverManager.getConnection(
    "jdbc:postgresql://127.0.0.1:5439/icegres", "postgres", "postgres");
// Against a --auth-file + TLS server, the usual pgjdbc SSL params apply:
//   jdbc:postgresql://host:5439/icegres?ssl=true&sslmode=require
```

Verified end-to-end by `bench/clients/A9JdbcProbe.java` (run it via
`bench/clients/a9_jdbc_probe.sh`; e2e section (q), parity probe A9):

- `DriverManager.getConnection` — pgjdbc's startup parameters
  (`extra_float_digits`, `application_name`, client encoding…) accepted;
  `DatabaseMetaData` reports `PostgreSQL 16.6-pgwire-…`.
- `DatabaseMetaData.getTables` / `getColumns` — pgjdbc's `pg_catalog`
  metadata queries (including `select current_catalog`, which
  `src/compat.rs` rewrites: the parenthesis-less
  `current_catalog`/`current_schema` keywords parse to AST shapes
  DataFusion cannot resolve).
- `Statement` and `PreparedStatement` with typed `setLong`/`setString`
  parameters, re-executed past pgjdbc's `prepareThreshold` (server-side
  named statements), `ResultSetMetaData` with correct type names.
- `executeUpdate` INSERT returns the update count — `compat.rs`'s
  `InsertTagHook` answers extended-protocol INSERTs with a proper
  `INSERT 0 n` command tag (upstream streamed a one-row `count` result set,
  which JDBC rejects with "A result was returned when none was expected").
- `setAutoCommit(false)` + `commit()` / `rollback()` cycles (verified from a
  separate connection).

JDBC-specific limit: SELECT inside an explicit transaction is
simple-protocol only (the `0A000` above), and pgjdbc always uses the
extended protocol — so with `autoCommit=false` keep reads on a second
(autocommit) connection, or add `preferQueryMode=simple` to the URL.

### ODBC

The stock PostgreSQL ODBC driver (psqlODBC via unixODBC) works out of the
box — no custom driver. Install once and either use a `DRIVER=` connection
string or register a DSN:

```bash
# driver + a named DSN "icegres" -> 127.0.0.1:5439
apt-get install -y unixodbc odbc-postgresql
bash infra/scripts/odbc-setup.sh          # writes the DSN to /etc/odbc.ini
echo "select count(*) from demo.trips;" | isql -v icegres
```

```python
import pyodbc
# DSN-less (only needs the driver registered in odbcinst.ini):
cn = pyodbc.connect(
    "DRIVER={PostgreSQL Unicode};Server=127.0.0.1;Port=5439;"
    "Database=icegres;UID=postgres;SSLmode=disable;UseDeclareFetch=0")
```

Verified end-to-end by `bench/clients/a10_odbc_probe.py` (run it via
`bench/clients/a10_odbc_probe.sh`; e2e section (r), parity probe A10) —
no icegres code changes were needed, the `pg_catalog`/version shims added
for the ORM and JDBC lanes already cover psqlODBC's probes:

- connect (psqlODBC issues its server-version + `pg_type`/`pg_attribute`
  probes on connect), `SQLTables` (`cursor.tables`) and `SQLColumns`
  (`cursor.columns`) metadata with correct type names.
- qmark-parameterized queries, INSERT/readback/DELETE with `rowcount`
  (autocommit), and a read inside an explicit transaction.

ODBC-specific notes: keep `UseDeclareFetch=0` (server-side `DECLARE`/`FETCH`
cursors are not implemented — the same limit as the other lanes); for
writes prefer autocommit, since DML inside an explicit transaction that
also issues an intervening statement hits the shared `0A000`
extended-protocol limit.

## Open-core model: the managed auth/authz add-on

icegres is open source: the lakehouse SQL server, every wire/driver protocol
(pgwire, Arrow Flight SQL — psql, ODBC, JDBC, ADBC), the copy-on-write write
engine, branching, the control plane, and the **authorization *seam*** (the
`Authorizer` trait, the `AuthzHook` enforcement point, the SQL→action mapping,
and the `--auth-file` / `--authz-file` flags) are always compiled.

The security **backends** — SCRAM authentication (`pgauth::FileAuthSource`) and
the ReBAC authorization policy engine (`authz::FileAuthorizer`) — are the
**managed add-on**, gated behind the `managed` cargo feature:

```bash
cargo build --release                          # managed build (default): auth + authz
cargo build --release --no-default-features    # pure open-source: no auth/authz backends
```

An open-source build runs the server fully, but `--auth-file` / `--authz-file`
return a clear *"managed add-on"* error, and the endpoint is open (any user,
all tables). Because core depends only on the `AuthSource` / `Authorizer` /
`BasicAuthVerifier` traits, the managed backends plug in without touching any
enforcement point — and a future OpenFGA backend that delegates to Lakekeeper's
own authorization can implement the same `Authorizer` trait as a drop-in.

## Authorization (managed add-on) — Lakekeeper-style ReBAC

With `--authz-file` (managed build), every SQL statement is authorized before
it executes, using the relationship-based model from Lakekeeper's
`authz-openfga`: a `warehouse → namespace → table` hierarchy where a grant at a
higher level is inherited by every descendant, and relations ordered
`own ⊇ write ⊇ read` (plus `drop`). Principals are users (from `--auth-file`)
or roles; a user inherits every grant of every role it belongs to.

```bash
icegres serve --auth-file users --authz-file policy    # pair authn + authz
```

Policy file (`#` comments; `grant <principal> <relation> <entity>`):

```text
grant analyst read demo          # read every table in the demo namespace
grant writer  write demo.trips   # write just demo.trips (write implies read)
grant admin   own   *            # warehouse owner — everything
member alice analyst             # alice inherits analyst's grants
member bob   writer
```

A denied statement returns SQLSTATE `42501` (`permission denied: role "…"
cannot … on …`). A `SELECT` is checked against **every** table it references
(joins, subqueries, CTEs); `INSERT`/`UPDATE`/`DELETE` need `write` on the
target; `pg_catalog` / `information_schema` reads, `SET`/`SHOW`, and
transaction control are metadata/session operations and are always allowed —
the same split Lakekeeper draws. Enforced on the pgwire path (psql, ODBC, JDBC,
and the ADBC postgres driver); verified by `bench/clients/authz_probe.sh`
(parity A12, e2e section (s)). Pair `--authz-file` with `--auth-file` so
principals are authenticated, not client-asserted. Flight-SQL-native
enforcement (the ADBC Arrow lane) is the next increment.

## Demo schema

- `demo.cities` — `city STRING, country STRING, population BIGINT` (20 rows)
- `demo.trips` — `trip_id BIGINT, city STRING, distance_km DOUBLE, fare DOUBLE, ts TIMESTAMP` (280 rows, June 2026, deterministic seed)

## Example session

```
$ psql -h 127.0.0.1 -p 5439 -U postgres -d icegres

icegres=> select count(*) from demo.trips;
 count(*)
----------
      280

icegres=> select city, count(*) trips, round(avg(fare), 2) avg_fare
          from demo.trips group by city order by trips desc limit 5;

icegres=> select t.city, c.country, sum(t.distance_km) total_km
          from demo.trips t join demo.cities c using (city)
          group by t.city, c.country order by total_km desc limit 5;

icegres=> insert into demo.cities values ('Prague', 'Czechia', 1309000);
INSERT 0 1
```

`INSERT` over the wire works end-to-end (verified: it appends Parquet data
files to RustFS and commits through the REST catalog). `UPDATE` and `DELETE`
work too, as copy-on-write Iceberg commits (`src/overwrite.rs`): only the
Parquet files that actually contain matching rows are rewritten, every other
file is reused in the new snapshot, and the commit is protected by an
`assert-ref-snapshot-id` requirement with bounded refresh-and-retry on
conflicts. Unsupported DML forms (joins/`USING`, `RETURNING`, subqueries in
predicates, bind parameters) are rejected with clear errors rather than
mis-executed; `INSERT OVERWRITE` is not supported.

## Testing

```sh
bash tests/e2e.sh   # end-to-end test (idempotent; needs psql, curl, jq, aws)
```

The harness starts the stack (`infra/scripts/up.sh`), builds, seeds, serves,
and asserts exact results over psql — **163 assertions** across sections
(a)–(y): seeded row counts, filters/aggregates/joins, `INSERT` over the wire
(verified from new connections), Parquet files on RustFS + catalog
registration via the Lakekeeper REST API, durability across a server
restart, auth + TLS (wrong password/unknown user rejected,
`sslmode=require`/`verify-full`, `openssl s_client` handshake),
UPDATE/DELETE copy-on-write (incl. a 409-conflict retry proven by fault
injection and time-travel-after-DML), transactions (read-your-own-writes,
ROLLBACK, one-snapshot COMMIT, 25P02, live 40001 conflict), PK enforcement
(23505/23502), buffered-write mode (union reads, group commit, SIGKILL
survival of committed rows, flush fences), zero-copy branches (write
isolation both directions, ref-only drop), the icegresd control plane
(wake-on-connect, idle-exit + re-wake, branch routing, kill -9 →
supervised restart) with its warm session pool (sequential API-pattern
connections, SET/txn isolation across pooled sessions, drain →
scale-to-zero → re-warm), and ORM/driver clients (SQLAlchemy reflection,
psycopg2/pg8000 transactions, pandas). Server pid/logs live under
`.e2e/` (gitignored); the server is killed on exit.

The harness is non-destructive: it never drops tables. The deterministic
seeded dataset occupies `trip_id` 1..280, so exact-value assertions filter on
that range; each run appends/updates/deletes a few test rows with unique
`trip_id >= 900000` (leftovers accumulate by design, a few small rows per
run). A sample psql session is in `docs/demo-session.txt`.

## Notes and limitations

- The table *list* is snapshotted when the server starts: tables created
  after startup (e.g. by an external writer) require a server restart to
  appear. Table *data* is refreshed from the catalog on every query.
- `information_schema` and a `pg_catalog` emulation are registered, so
  `\d`, `\dt` and most introspection in psql work.
- Auth and TLS are **off by default** (permissive local-demo configuration,
  announced by a startup WARN); production serving should set
  `--auth-file` + `--tls-cert`/`--tls-key` — see the sections above.
- Unsupported DML forms (joins/`USING` in UPDATE/DELETE, `RETURNING`,
  subqueries in predicates, bind parameters in DML, `INSERT OVERWRITE`)
  are rejected with clear errors rather than mis-executed.
