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
| Transactions | BEGIN/COMMIT/ROLLBACK; snapshot-pinned reads, read-your-own-writes, COMMIT = ONE Iceberg snapshot, first-committer-wins (40001) (`src/txn.rs`) | default |
| Primary-key enforcement | opt-in NOT NULL + uniqueness checks (23502/23505) anchored to the commit snapshot | `--enforce-pk` + table property `icegres.primary-key` |
| Authentication | SCRAM-SHA-256 (salted hashes in memory, 28P01 on failure) — managed add-on | `--auth-file` |
| Authorization | Lakekeeper-style ReBAC: warehouse/namespace/table grants, roles, per-statement 42501 — managed add-on | `--authz-file` |
| TLS | rustls on the pgwire listener; misconfig aborts boot | `--tls-cert`/`--tls-key` |
| Time travel | read-only snapshot-pinned queries | `demo."trips@<snapshot_id>"` |
| Zero-copy branches | Neon-style branch-per-endpoint over Iceberg snapshot refs (`src/branch.rs`) | `icegres branch create/list/drop`, `serve --branch` |
| Buffered writes (opt-in) | Moonlink-style group commit: ~1.5 ms INSERT ack, union reads, ≤N ms durability window, WARN on enable (`src/buffer.rs`) | `--write-buffer-ms N` (default 0 = synchronous) |
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
52.5.0, datafusion-postgres 0.15.0, arrow 57.3.1). Do not bump versions
independently — see comments in `Cargo.toml`.

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
| `icegres sql -e '<query>'` | One-shot local execution against the catalog (debugging aid; no server involved). Honors `--enforce-pk`. |

### Configuration

All flags have working defaults for the local stack and can also be set via
environment variables:

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
| `--write-buffer-ms` (serve) | `ICEGRES_WRITE_BUFFER_MS` | `0` (sync) | Opt-in buffered writes: INSERTs ack from an in-memory buffer, group-committed every N ms; unclean kill loses ≤N ms of acked writes (WARN on enable) |
|  | `ICEGRES_WRITE_BUFFER_MAX_ROWS` | `50000` | Row threshold that forces an early flush in buffered mode |

Logging uses `tracing` with an env filter: `RUST_LOG=debug icegres serve`.

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
composes everything into **one** Iceberg snapshot anchored at the pinned
snapshot. Concurrency is first-committer-wins: if another writer committed
to a touched table since the pin, `COMMIT` fails with SQLSTATE `40001`
(retry the transaction). A statement error inside a transaction poisons it
(`25P02`) and `COMMIT` then rolls back, like stock Postgres.

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

### Buffered writes (`--write-buffer-ms N`, opt-in)

Moonlink-style group commit. With `N > 0`, INSERTs acknowledge after
appending to an in-memory buffer (~1.5 ms p50 vs ~50–60 ms synchronous)
and a background task commits the buffer to Iceberg every `N` ms (or at
`ICEGRES_WRITE_BUFFER_MAX_ROWS`). Every read on the same server unions the
committed table with the buffer, so read-your-writes holds across all
local connections instantly; other servers/readers see rows at the commit
cadence (≤ N ms after ack). UPDATE/DELETE/BEGIN/DDL/PK-checked INSERTs
flush first (ordering fences). **Trade-off, stated plainly:** an unclean
kill (SIGKILL, power loss) loses up to N ms of acked-but-uncommitted
writes. That is why the default is `0` — fully synchronous, semantics
identical to not having the feature — and enabling it logs a WARN.

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
```

Reads on a `--branch` server pin to the branch head; all writes (INSERT,
UPDATE, DELETE, transactions) commit with `assert-ref-snapshot-id` on the
branch ref, so endpoints on different branches never conflict and nothing
can leak onto `main`. A table without the ref fails loudly — no silent
fallback. Both branches share every file below the fork point; only new
commits diverge.

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
and asserts exact results over psql — **104 assertions** across sections
(a)–(o): seeded row counts, filters/aggregates/joins, `INSERT` over the wire
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
