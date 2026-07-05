# icegres

A Postgres wire endpoint over an Iceberg lakehouse — the Phase-0
"serve-in-place" system from `docs/lakebase-lakegres-architecture-study.md`.

`icegres` connects to an Iceberg REST catalog (Lakekeeper), mounts every
namespace/table into a DataFusion session, and serves that session over the
Postgres wire protocol with `datafusion-postgres`. Any Postgres client
(`psql`, drivers, BI tools) can then query — and `INSERT INTO` — Iceberg
tables whose data lives as Parquet on S3-compatible storage (RustFS).

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
| `icegres sql -e '<query>'` | One-shot local execution against the catalog (debugging aid; no server involved). |

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
files to RustFS and commits through the REST catalog). Writes are
append-only; `INSERT OVERWRITE`/`UPDATE`/`DELETE` are not supported by
iceberg-datafusion 0.9.1.

## Testing

```sh
bash tests/e2e.sh   # end-to-end test (idempotent; needs psql, curl, jq, aws)
```

The harness starts the stack (`infra/scripts/up.sh`), builds, seeds, serves,
and asserts exact results over psql: seeded row counts, a WHERE filter, a
GROUP BY aggregate, a JOIN, an `INSERT` over the wire (verified from new
connections), Parquet files on RustFS + catalog registration for both tables
via the Lakekeeper REST API, and durability across a server restart. Server
pid/logs live under `.e2e/` (gitignored); the server is killed on exit.

The harness is non-destructive: it never drops tables. The deterministic
seeded dataset occupies `trip_id` 1..280, so exact-value assertions filter on
that range; each run appends one test row with a unique `trip_id >= 900000`
(append-only storage — these accumulate by design, one small row per run).
A sample psql session is in `docs/demo-session.txt`.

## Notes and limitations

- The table *list* is snapshotted when the server starts: tables created
  after startup (e.g. by an external writer) require a server restart to
  appear. Table *data* is refreshed from the catalog on every query.
- `information_schema` and a `pg_catalog` emulation are registered, so
  `\d`, `\dt` and most introspection in psql work.
- No TLS and no authentication (any user/password accepted) — local demo
  configuration.
