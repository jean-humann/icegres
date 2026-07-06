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

### Features

| feature | mechanism | flag / syntax |
|---|---|---|
| SELECT (full SQL via DataFusion) | snapshot-aware metadata cache, exact freshness (no TTL) | default |
| INSERT | Iceberg `fast_append` commit per statement | default |
| UPDATE / DELETE | copy-on-write overwrite snapshots — only files containing matched rows are rewritten (`src/overwrite.rs`) | default |
| Transactions | BEGIN/COMMIT/ROLLBACK; snapshot-pinned reads, read-your-own-writes, COMMIT = ONE Iceberg snapshot, first-committer-wins (40001) (`src/txn.rs`) | default |
| Primary-key enforcement | opt-in NOT NULL + uniqueness checks (23502/23505) anchored to the commit snapshot | `--enforce-pk` + table property `icegres.primary-key` |
| Authentication | SCRAM-SHA-256 (salted hashes in memory, 28P01 on failure) | `--auth-file` |
| TLS | rustls on the pgwire listener; misconfig aborts boot | `--tls-cert`/`--tls-key` |
| Time travel | read-only snapshot-pinned queries | `demo."trips@<snapshot_id>"` |
| Zero-copy branches | Neon-style branch-per-endpoint over Iceberg snapshot refs (`src/branch.rs`) | `icegres branch create/list/drop`, `serve --branch` |
| Buffered writes (opt-in) | Moonlink-style group commit: ~1.5 ms INSERT ack, union reads, ≤N ms durability window, WARN on enable (`src/buffer.rs`) | `--write-buffer-ms N` (default 0 = synchronous) |
| Scale-to-zero | clean exit after N idle seconds; stateless compute | `--idle-shutdown-secs` |
| Health endpoint | HTTP 200 liveness | `--health-port` |

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
and asserts exact results over psql — **77 assertions** across sections
(a)–(m): seeded row counts, filters/aggregates/joins, `INSERT` over the wire
(verified from new connections), Parquet files on RustFS + catalog
registration via the Lakekeeper REST API, durability across a server
restart, auth + TLS (wrong password/unknown user rejected,
`sslmode=require`/`verify-full`, `openssl s_client` handshake),
UPDATE/DELETE copy-on-write (incl. a 409-conflict retry proven by fault
injection and time-travel-after-DML), transactions (read-your-own-writes,
ROLLBACK, one-snapshot COMMIT, 25P02, live 40001 conflict), PK enforcement
(23505/23502), buffered-write mode (union reads, group commit, SIGKILL
survival of committed rows, flush fences) and zero-copy branches (write
isolation both directions, ref-only drop). Server pid/logs live under
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
