# Frontend dashboards over icegres — data-path benchmark & recommendation

**Question.** A browser dashboard needs icegres query results. Which wire
should carry them — and does the frontend need a backend at all?

**Answer.** Keep Arrow end-to-end. Serve dashboards over Flight SQL with the
result staying in Arrow IPC form all the way into `apache-arrow` in the
browser, either through a ~40-line pass-through proxy endpoint
(`arrow-proxy`) or with the browser speaking Flight SQL itself through a
gRPC-web translator (`grpcweb-direct`). Both beat converting to JSON by
**2.5–2.8× end-to-end latency at dashboard-realistic network speeds** and
**5–6× on wire size**, and the gap widens with row count. Below ~1k rows any
path is fine — latency is dominated by query execution, not encoding.

Probes, harness, and the demo dashboard live in
[`bench/clients/js/`](../bench/clients/js/); recorded runs in
[`bench/results/frontend-paths-2026-07-18-local.json`](../bench/results/frontend-paths-2026-07-18-local.json)
and
[`…-net50.json`](../bench/results/frontend-paths-2026-07-18-net50.json).

## The four candidate paths

| Path | Browser wire | Who talks to icegres | Verdict |
|---|---|---|---|
| `arrow-proxy` | Arrow IPC (ZSTD-compressed batches) | Node proxy → Flight SQL `:50051` | **Recommended default** |
| `grpcweb-direct` | gRPC-web FlightData frames | the browser itself (`flight-serve --grpc-web`) | **Recommended when no app backend exists** |
| `flight-json` | JSON | Node proxy → Flight SQL, rows flattened server-side | loses: pays decode + stringify + parse + 5× wire |
| `pg-json` | JSON | Node proxy → node-postgres `:5439` | loses: same JSON tax, plus row-oriented transport |

Two facts frame the comparison:

- **Browsers cannot speak native gRPC** (no HTTP/2 trailer access), so
  "query icegres directly from the frontend" needs a protocol translation.
  icegres now performs it in-process: `flight-serve --grpc-web` makes the
  Flight port itself answer gRPC-web (tonic-web layer + CORS), so the
  benchmark's `grpcweb-direct` lane runs with **no extra process at all**.
  (An Envoy `grpc_web` filter in front of a plain listener remains a valid
  deployment shape; the client works unchanged against either.)
- **There is no official Arrow Flight client for browser JS** — the
  `apache-arrow` npm package covers the IPC format only. icegres ships one:
  [`@icegres/flight-web`](../clients/flight-web/) (~300 lines, no codegen)
  implements the spec-faithful `GetFlightInfo` → `DoGet` flow over gRPC-web,
  reassembles FlightData into an IPC stream, decodes with `apache-arrow`,
  and supports per-RPC Basic auth, AbortController cancellation, and
  progressive per-batch rendering. Node backends have real options today:
  `@grpc/grpc-js` (pure JS, used by the proxy) or ADBC.

## Results — real Chromium 141, headless, 7 reps, 2 warmups, median

Dataset: `demo.dash_trips`, 1M rows × 5 columns (int64, string, 2× float64,
timestamp), seeded through the Flight `adbc_ingest` bulk lane. Full stack on
one 4-core box; catalog + object store latencies identical across paths.

### Localhost (no throttling)

| case | arrow-proxy | grpcweb-direct | flight-json | pg-json |
|---|---|---|---|---|
| agg 8 rows | 63 ms | 64 ms | 63 ms | **61 ms** |
| 1k rows | **107 ms** | 108 ms | 109 ms | 105 ms |
| 10k rows | **152 ms** | 152 ms | 158 ms | 161 ms |
| 100k rows | 666 ms | **581 ms** | 719 ms | 655 ms |
| 1M rows | **5.6 s** | 5.7 s | 7.6 s | 6.7 s |

| wire bytes | arrow lanes | flight-json | pg-json |
|---|---|---|---|
| 10k rows | **162 KiB** | 820 KiB | 966 KiB |
| 100k rows | **1.6 MiB** | 8.1 MiB | 9.5 MiB |
| 1M rows | **18.7 MiB** | 82.0 MiB | 96.3 MiB |

Localhost hides bandwidth, so even here the JSON lanes lose ~20–35% at the
top end — that is pure serialize/parse/GC tax: the proxy pivoting columns to
JS objects and `JSON.stringify`, the browser `JSON.parse`-ing a 100 MB
string into a million heap objects. The Arrow lanes forward server-encoded
bytes untouched (the proxy prepends an 8-byte IPC frame header per batch;
the translator not even that) and `tableFromIPC` wraps the buffers
near-zero-copy.

### 50 Mbit/s, 20 ms latency (a good office connection)

| case | arrow-proxy | grpcweb-direct | flight-json | pg-json |
|---|---|---|---|---|
| agg 8 rows | 89 ms | 115 ms | 88 ms | **85 ms** |
| 1k rows | **131 ms** | 165 ms | 143 ms | 150 ms |
| 10k rows | 209 ms | **204 ms** | 312 ms | 331 ms |
| 100k rows | **851 ms** | 938 ms | 2.36 s | 2.40 s |
| 1M rows | **8.6 s** | 8.8 s | 21.8 s | 22.9 s |

Once bandwidth is finite the wire-size gap converts directly into user-felt
latency: **2.5–2.8× faster at 100k–1M rows**. The ZSTD batch compression
icegres applies on DoGet (`flight_ipc_options`) is doing real work here —
18.7 MiB versus 96 MiB for the same million rows.

`grpcweb-direct` pays one extra round-trip (`GetFlightInfo` before `DoGet`),
visible only on tiny queries (+25 ms at 20 ms RTT); by 10k rows it ties, and
the server's plan-stash makes the second RPC cheap. Re-measured against the
native `--grpc-web` listener (recorded in
`bench/results/frontend-paths-2026-07-18-native-grpcweb.json`), the lane
matches or slightly beats the arrow-proxy lane — the bridged numbers above
are the conservative ones.

## Backend reference (Node, no browser)

For the proxy side, the pure-JS `@grpc/grpc-js` Flight client outruns
node-postgres once results grow — pgwire is a row-oriented text protocol
that must be parsed value-by-value (`bench/node-bench.mjs`, medians):

| case | flight-grpc-js | pg-wire |
|---|---|---|
| agg 8 rows | 64 ms | 70 ms |
| 10k rows | **123 ms** | 139 ms |
| 100k rows | **242 ms** | 410 ms |
| 1M rows | **2.30 s** | 4.37 s |

The Rust-native `@lakehouse-rs/flight-sql-client` (v0.0.10) is **not usable
against icegres today**: its bundled arrow-flight is compiled without the
`zstd` IPC feature, so the first compressed DoGet batch panics a tokio
worker ("zstd IPC decompression requires the zstd feature") and the query
promise never settles. The lane stays in the harness behind `BENCH_NATIVE=1`
for servers that send uncompressed batches.

## Practicalities

- **ZSTD codec registration is mandatory.** icegres compresses DoGet
  batches; `apache-arrow` JS ships only a codec *registry*. Register
  `node:zlib`'s zstd in Node and `fzstd` in the browser — importing
  `@icegres/flight-web` does it for you (the browser codec re-bases fzstd's
  subarray output to byte offset 0, or arrow-js hits typed-array alignment
  errors). Servers can instead run `--result-compression none`.
- **Types survive only on the Arrow lanes.** JSON silently stringifies
  int64/numeric (node-postgres returns `count(*)` as a string) and loses
  timestamp precision; Arrow delivers real typed columns, which is also
  what chart libraries want.
- **Auth over gRPC-web is per-RPC Basic.** The Flight Handshake RPC is a
  bidirectional stream, which the gRPC-web protocol cannot carry; with
  `--auth-file`, browser clients send `authorization: Basic …` on every
  call and the server verifies it per-RPC (successes cached server-side so
  the SCRAM KDF stays off the hot path). Always pair with
  `--tls-cert/--tls-key` and pin `--cors-origin` to the dashboard origin.
- **Security.** `grpcweb-direct` exposes the SQL surface to the browser —
  fine for internal dashboards behind `--auth-file` + TLS + `--authz-file`
  scoping (the Flight endpoint enforces per-principal read scopes); for
  public-facing apps prefer `arrow-proxy` with a fixed query allowlist
  rather than raw SQL pass-through.
- **In production**, either enable `--grpc-web` on the listener (TLS
  in-process, `http/1.1` ALPN added automatically) or terminate with an
  Envoy `grpc_web` filter; `@icegres/flight-web` works unchanged against
  both.

## Bench-environment notes

Run in a sandbox where Lakekeeper/RustFS binaries could not be fetched: the
Iceberg REST catalog was the Apache `RESTCatalogServer` test fixture
(sqlite-backed) and S3 was moto's in-memory server, both on localhost. All
four paths traverse the identical engine/catalog/store, so relative numbers
are meaningful; absolute query-execution floors (~60 ms) are not
representative of a production deployment. One sandbox-specific caveat: the
bench table was created through the REST catalog directly (as `bench.sh`
does for its scratch table) because pgwire `CREATE TABLE` against this
fixture catalog reproducibly wedged the server's async runtime — worth an
upstream look before attributing it to the fixture.
