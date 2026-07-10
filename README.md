# icegres

**A Postgres-wire and Arrow Flight SQL endpoint over an Apache Iceberg lakehouse.**

`icegres` connects to an Iceberg REST catalog (Lakekeeper), mounts every
namespace/table into a DataFusion session, and serves that session over **two
first-class wire protocols** — the Postgres wire protocol and Arrow Flight SQL
(ADBC). Any Postgres client (`psql`, JDBC/ODBC drivers, ORMs, BI tools) or ADBC
client can then query **and modify** Iceberg tables whose data lives as Parquet
on S3-compatible storage. There is exactly **one copy of the data**, in open
Iceberg format on the lake; every feature is zero-copy on top of it.

It compiles to a single static binary (plus `icegresd`, a small scale-to-zero
control plane) with no JVM, coordinator, or per-query task scheduler — so it
starts in ~0.3 s and serves interactive queries in single-digit milliseconds.

```
   psql / JDBC / ODBC / ORMs ─┐
                              ├─▶  icegres  ─▶  Iceberg REST catalog (Lakekeeper)
   ADBC / Arrow Flight SQL ───┘       │              │
                                      └─▶  Parquet on S3 (RustFS / MinIO / S3)
```

---

## What it does

| Capability | Summary |
|---|---|
| **Postgres wire** | Simple + extended protocol, `pg_catalog`/`information_schema` emulation, SCRAM-SHA-256 auth, TLS. Verified against psql, psycopg2, pg8000, SQLAlchemy, pgjdbc, psqlODBC. |
| **Arrow Flight SQL / ADBC** | Second first-class protocol: queries stream as Arrow IPC, catalog metadata (`get_objects`), prepared statements, DML, and bulk ingest (one Iceberg commit per stream). In-process TLS (`grpc+tls://`). |
| **OLTP over the lake** | `INSERT`/`UPDATE`/`DELETE` as copy-on-write Iceberg snapshots; explicit `BEGIN/COMMIT/ROLLBACK` with snapshot isolation, first-committer-wins concurrency (`40001`), and atomic multi-table COMMITs via the catalog's `transactions/commit` endpoint (Lakekeeper); opt-in primary-key enforcement. |
| **Time travel & branches** | `table@snapshot_id` reads; Neon-style zero-copy branches (`icegres branch …`, `serve --branch`) — a branch is one metadata commit, no data copied — including whole-lakehouse branches (`branch create-all`/`drop-all`: every table, one atomic transaction, each table pinned to its captured main head — a consistent-or-nothing cross-table cut). |
| **Buffered writes** | Opt-in Moonlink-style group commit (`--write-buffer-ms`): ~1.5 ms INSERT ack, union reads, flushed on clean shutdown; the unclean-kill window is closed by the **durable tail** — `--tail-dir` (local fsync'd WAL, ~3.2 ms ack), `--tail-url` (Postgres backend, survives node loss), or `--tail-quorum` (three `icekeeperd` acceptors, Neon-SafeKeeper-style consensus adapted with attribution — survives any single node with no delegated single system). |
| **Hot-row upserts** | Opt-in keyed tail (`icegres.tail-upsert` + `icegres.primary-key` + a durable tail): exact-PK `UPDATE`/`DELETE` ack in ~9.5 ms p50 (vs ~71 ms synchronous COW), coalesced per key into ONE commit per flush window — no more per-statement snapshots or `40001` storms on a hot row. |
| **Bounded-staleness reads** | Opt-in freshness refresher + plan cache (`--freshness-ms N`): scans skip the per-scan catalog check — point lookups ~7.4 → ~4.4 ms p50, repeated statements ~2.8–3.6 ms. Own writes stay read-your-own-writes exact; foreign commits visible within ~N ms + one refresh round trip (tables refresh concurrently — a slow table delays only itself, bounded by a per-table timeout); staleness gauge on `/metrics`. Default 0 = exact freshness, unchanged. |
| **Scale-to-zero** | `icegresd` wakes computes on connect and idles them to zero; branch-endpoint routing; warm session pooling. |
| **Ops surface** | Graceful drain, bounded memory pool + disk spill, connection cap + per-IP failed-auth backoff, catalog timeouts, catalog-aware `/ready`, Prometheus `/metrics` (incl. in-flight/slow-query), correlation-ID spans, snapshot expiry + fail-closed orphan-file GC (`maintain remove-orphans`, dry-run default). |

## Where it fits

Measured on a single 4-core box against Trino 446 and Spark 3.5.8 Thrift
reading the **same** Iceberg tables through the same REST catalog, `icegres` is
the clear **interactive-serving** winner — small-query p50s of 7–10 ms vs
115–436 ms (16–43× faster), higher qps at 8 connections, ~0.3 s startup vs
10–14 s, and 8–10× less peak RSS. It is **not** a distributed analytics engine:
Trino wins the largest full-table aggregations, and that gap widens with data
volume or a real cluster.

**Honest fit:** sub-second point / filtered / join queries, Postgres-protocol
and ADBC compatibility, and scale-to-zero economics on lakehouse data. Leave
100 GB+ distributed scans to Trino/Spark.

---

## Quick start

Prerequisites: Rust (pinned in `rust-toolchain.toml`), and the local lakehouse
stack (Postgres + RustFS + Lakekeeper) which `infra/scripts/up.sh` provisions.

```sh
# 1. Bring up the local Iceberg lakehouse (Lakekeeper + RustFS + Postgres)
bash infra/scripts/up.sh

# 2. Build and seed demo data
cd icegres && cargo build --release
./target/release/icegres seed

# 3. Serve over the Postgres wire protocol
./target/release/icegres serve --host 127.0.0.1 --port 5439 --health-port 8080

# 4. Connect with any Postgres client
psql "host=127.0.0.1 port=5439 dbname=icegres" \
  -c "select city, count(*) from demo.trips group by city"
```

Serve the same lakehouse over Arrow Flight SQL for ADBC clients:

```sh
./target/release/icegres flight-serve --host 127.0.0.1 --port 50051
```

Run in a container (multi-stage, non-root):

```sh
docker build -t icegres .
docker run --rm -p 5439:5439 -p 8080:8080 \
  -e ICEGRES_CATALOG_URI=https://catalog.example.com/catalog \
  -e ICEGRES_S3_ENDPOINT=https://s3.example.com \
  -e ICEGRES_S3_ACCESS_KEY=... -e ICEGRES_S3_SECRET_KEY=... \
  icegres serve --host 0.0.0.0 --health-port 8080
```

---

## Documentation

| Doc | What's in it |
|---|---|
| [`icegres/README.md`](icegres/README.md) | Full CLI/flag/env reference and per-feature detail (auth, TLS, transactions, PK, branches, buffered writes, ADBC, `icegresd`). |
| [`docs/deployment.md`](docs/deployment.md) | Operator guide: container, health/readiness/metrics probes, graceful shutdown, resource limits, security, snapshot-expiry maintenance, full env-var reference. |
| [`docs/limitations.md`](docs/limitations.md) | Every deliberate non-goal / caveat, with its workaround and why-not-yet. |
| [`docs/cqrs-topology.md`](docs/cqrs-topology.md) | CQRS reference topology — which tier serves OLTP vs API vs BI, with measured latencies. |
| [`docs/production-readiness-audit.md`](docs/production-readiness-audit.md) | Multi-agent pre-GA audit and how each finding was closed. |
| [`bench/SCORECARD.md`](bench/SCORECARD.md) | All benchmark numbers, the parity matrix, and the round-by-round development history. |

## Testing

- `icegres/tests/e2e.sh` — end-to-end suite against the live stack (130+ assertions across every feature and both wire protocols).
- `cargo test` — unit tests (buffer union-read state machine, PK checks, transactions, auth parsing, …).
- `bench/parity.sh` — feature-parity probes vs the Lakebase/Neon/Moonlink bar.
- `bench/bench.sh` + `bench/gate.sh` — the performance harness and no-regression gate.

## Architecture notes

- **Pinned dependency matrix** (do not bump independently — see `icegres/Cargo.toml`): iceberg-rust 0.9.1, DataFusion 52.5.0, arrow 57.3.1, datafusion-postgres 0.15.0 (pgwire 0.38.3), tonic 0.14, sqlparser 0.62.0, toolchain 1.96.1.
- **Open-core split:** the SQL server, the authorization *seam*, and all wire/driver support are open source and always compiled; the auth/authz *backends* live behind the default `managed` cargo feature (`--no-default-features` builds a pure open-source distribution).

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
