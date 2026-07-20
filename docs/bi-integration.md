# BI tool integration — deep analysis (Tableau, Power BI, and friends)

**Question.** Can icegres serve as the database behind mainstream BI tools —
Tableau, Power BI, Superset, Metabase, Grafana, Excel — and what stands
between "speaks the Postgres wire protocol" and "works in anger"?

**Answer.** Yes, by design: every mainstream BI tool ships a PostgreSQL
connector, and icegres presents itself as a stock Postgres 16 (`version()`
reports `PostgreSQL 16.6`, `pg_catalog`/`information_schema` shims answer
driver introspection — `icegres/src/compat.rs`). The **driver stacks** those
tools are built on are largely probe-verified today (pgjdbc, psqlODBC,
SQLAlchemy/psycopg2 — `bench/clients/`), so SQLAlchemy-based and JDBC-based
tools are the low-risk path. The two honest gaps are: **no end-to-end run of
any actual BI product has been performed**, and **Npgsql — the driver inside
Power BI's native connector — is the one major Postgres driver with no probe
at all**. This document maps each tool to its driver stack, calls out the
icegres limitations that specifically bite BI workloads (with workarounds),
and ranks the validation work that would turn "by-construction" into
"proven-live".

Every claim below is labeled like `docs/catalog-support.md`:
**proven-live** (a committed probe exercises it), **by-construction** (the
tool rides a driver stack a probe verifies, but the tool itself was not
run), or **unverified** (no probe covers the stack).

Companion docs: [`clients.md`](clients.md) (connection recipes),
[`limitations.md`](limitations.md) (the caveats referenced throughout),
[`frontend-dashboards.md`](frontend-dashboards.md) (the browser/custom-
dashboard counterpart to this document — packaged BI products here, hand-
built dashboards there).

---

## 1. What icegres already brings to the table

Two protocol lanes, both relevant to BI:

- **Postgres wire (`icegres serve`)** — the lane every packaged BI tool can
  use *today* with zero icegres-side work, because they all ship Postgres
  connectors. TLS (rustls, fail-closed), SCRAM-SHA-256 (`--auth-file`),
  per-principal read scoping (`--authz-file`), `COPY … TO STDOUT` for bulk
  reads.
- **Arrow Flight SQL (`icegres flight-serve`)** — the columnar fast lane
  (measured ~1.9× faster than pgwire at 1M rows from Node,
  `frontend-dashboards.md`). No packaged BI tool speaks it natively yet, but
  the Flight SQL **JDBC** driver is probe-verified against icegres
  (`bench/clients/A9FlightJdbcProbe.java`), which opens a "generic JDBC"
  path in tools that allow a custom driver (§6).

The introspection surface — the thing BI tools actually trip on — is
deliberately engineered, not incidental:

- `compat.rs` shims the `pg_catalog` emulation up to what real drivers
  send: a Postgres-parseable `version()`, `pg_get_indexdef`/
  `pg_type_is_visible` stubs, rewrites for `unnest(indkey)`-style array
  introspection, correlated-subquery patches for SQLAlchemy's column
  reflection, and **oid-coherent** `pg_class`/`pg_namespace`/`pg_attribute`
  so joins across them resolve.
- Probe-verified introspection (**proven-live**, `bench/clients/`):
  SQLAlchemy `inspect()` reflection (A8), JDBC `DatabaseMetaData.getTables`/
  `getColumns` (A9), ODBC `SQLTables`/`SQLColumns` including psqlODBC's
  on-connect version/type probes (A10).
- Namespaces surface as schemas; there is one database (`icegres`). BI
  schema pickers work; multi-database pickers show a single entry.

BI workloads are read-dominant, which lands on icegres's strongest surface:
reads stream with bounded memory (a 50M-row extract never materializes
server-side), default reads are exactly fresh, and `--freshness-ms` /
result cache / read replicas exist precisely for dashboard-shaped fleets
(§5).

## 2. Tool-by-tool assessment

| Tool | Connector it would use | Driver stack | Nearest probe | Status |
|---|---|---|---|---|
| Tableau (Desktop/Server/Cloud) | built-in PostgreSQL | pgjdbc (JDBC since ~2020.4) | A9 JDBC | **by-construction** |
| Metabase | built-in PostgreSQL | pgjdbc | A9 JDBC | **by-construction** |
| DBeaver / DataGrip | PostgreSQL | pgjdbc | A9 JDBC | **by-construction** |
| Apache Superset | PostgreSQL | SQLAlchemy + psycopg2 | A8 ORM | **by-construction** (closest to proven) |
| Redash | PostgreSQL | psycopg2 | A8 ORM | **by-construction** |
| Power BI (Import + DirectQuery) | built-in PostgreSQL | **Npgsql** (.NET) | — | **unverified** |
| Power BI / Excel via ODBC | generic ODBC | psqlODBC | A10 ODBC | **by-construction** |
| Excel Get Data → PostgreSQL | Power Query | Npgsql | — | **unverified** |
| Grafana | built-in PostgreSQL | Go pgx | — | **unverified** (low risk: simple-protocol-ish, plain SELECTs) |
| Looker | PostgreSQL dialect | pgjdbc | A9 JDBC | **by-construction** |
| DigDash Enterprise | registered JDBC driver | pgjdbc or Flight SQL JDBC | A9 / A9F | **by-construction** |

### Tableau — by-construction, two known sharp edges

Tableau's PostgreSQL connector rides **pgjdbc**, whose core surface
(connect, `DatabaseMetaData`, prepared statements, txn cycles) is
probe-verified. The two edges to watch:

1. **Result streaming (fetch size).** pgjdbc streams a large result only
   with `autocommit=false` + `setFetchSize(N)` — i.e. an
   extended-protocol `SELECT` inside an explicit transaction, which icegres
   rejects with `0A000` (`limitations.md` §Transactions). If Tableau's
   extract path enables streaming mode, extracts fail at the first fetch.
   Workaround (verified mechanism for pgjdbc generally, not yet against
   Tableau): a datasource properties file / connection property setting
   `preferQueryMode=simple` — simple-protocol reads inside transactions are
   supported. Autocommit reads (pgjdbc's default) need nothing; the risk is
   confined to whatever mode Tableau picks per operation.
2. **Generated SQL breadth (Live connections).** Tableau's live mode
   compiles each viz into Postgres SQL: `date_trunc`, `EXTRACT`, `CAST`,
   string ops, window functions, occasionally correlated subqueries.
   DataFusion 52's Postgres-dialect coverage is broad but not total; the
   honest expectation is that most vizzes plan and a tail of functions
   surface as planning errors. Extract mode sidesteps this entirely — the
   extract is one big scan (streams fine), and all viz SQL runs inside
   Tableau's own engine (Hyper). **Recommendation: lead with Extract mode;
   treat Live mode as a validation-driven follow-up** (Tableau's TDVT
   dialect test kit is the systematic way to enumerate the gap, §7).

Tableau's on-connect `SET` statements ride the existing SetShow hook (the
JDBC probe's startup traffic already exercises the same path). Initial-SQL
is a plain statement — usable for `AS OF` time-travel pins (§5).

### Power BI — the real unknown, with a verified fallback

Power BI's native PostgreSQL connector is built on **Npgsql**, the one
major driver with **no icegres probe**. Npgsql is also the most demanding
introspection client of the lot: on first connect it runs a large
type-loading query joining `pg_type` / `pg_namespace` / `pg_enum` /
`pg_range` and expects a coherent answer *before any user query runs* — if
the emulated catalog can't plan it, the connection fails outright, not
degraded. Npgsql also defaults to the extended protocol with **binary**
result formats everywhere. None of this is known-broken; all of it is
unknown, and it gates both Power BI and Excel's Power Query. This is the
single highest-value probe to add (§7).

Mode-by-mode, once the driver connects:

- **Import mode** (the common case): Power Query folds to relatively tame
  `SELECT` statements and pulls the whole table/query result — lands on the
  bounded streaming read path. Low incremental risk beyond Npgsql itself.
- **DirectQuery**: every visual generates SQL (nested derived tables,
  `LIMIT`-shaped TOP-N, date arithmetic) against icegres per interaction —
  same dialect-breadth exposure as Tableau Live plus a much chattier query
  cadence; wants `--freshness-ms` + the result cache (§5).
- **Power BI Service** reaches a private icegres through the on-premises
  data gateway; the gateway machine is just another Npgsql (or ODBC)
  client, so nothing icegres-specific changes.

**Fallback that works today (by-construction):** Power BI and Excel both
connect through **generic ODBC**, and stock psqlODBC is probe-verified —
with one mandatory setting: `UseDeclareFetch=0` (the probe's own connection
string does this), because declare/fetch mode uses server-side named
cursors, which icegres does not implement.

### DigDash Enterprise — by-construction, JDBC both lanes

DigDash (Java-based) takes any JDBC driver registered in its
`sqldriverrepository.xml`, so both icegres lanes apply: stock pgjdbc against
pgwire, or the Flight SQL JDBC driver against `flight-serve` — the latter
being the recommended one, for two reasons. First, the columnar transport
(the DigDash data-model refresh is an extract-shaped bulk read, exactly
where ADBC/Flight wins 10×+, §6). Second, DigDash's own documentation
steers Postgres sources toward `DEFAULT_FETCH_SIZE` streaming — which on
pgjdbc means `autocommit=false` + `setFetchSize`, i.e. the extended-
protocol-SELECT-in-transaction shape icegres rejects with `0A000` (§3). On
the pgwire lane, leave fetch-size streaming off or set
`preferQueryMode=simple`; on the Flight JDBC lane the issue does not exist.

### Superset / Metabase / Redash / Grafana — the quick wins

These are the shortest path to a live BI deployment story:

- **Superset** and **Redash** sit on SQLAlchemy/psycopg2 — exactly the A8
  probe surface (reflection included). Expect them to largely just work;
  Superset's SQL Lab is autocommit reads.
- **Metabase** sits on pgjdbc (A9). Its query generator is more
  conservative than Tableau's.
- **Grafana** uses Go pgx issuing plain time-series SELECTs; unverified but
  the thinnest introspection footprint of the group.

A docker-compose smoke of these against the dev stack would convert four
tools to proven-live in an afternoon (§7).

## 3. icegres limitations that specifically bite BI

The full catalog is `limitations.md`; this is the BI-shaped cut.

| Limitation | Who it bites | Severity for BI | Workaround |
|---|---|---|---|
| No server-side (named) cursors | psqlODBC declare/fetch mode; anything issuing `DECLARE CURSOR` | High if enabled, zero if not | `UseDeclareFetch=0` (ODBC); default client-side fetch elsewhere |
| Extended-protocol `SELECT` in explicit txn → `0A000` | pgjdbc streaming fetch (Tableau extracts?), any tool wrapping reads in `BEGIN` | The top compatibility risk for JDBC tools | autocommit reads; `preferQueryMode=simple`; document per-tool settings |
| No per-statement timeout on pgwire | Runaway DirectQuery/Live viz queries | Medium — BI is *the* workload that needs it | Memory pool spills-then-errors bounds RAM, not wall-clock; front with Flight (`--flight-statement-timeout-ms`) or add pgwire timeout (§7) |
| pg_catalog emulation breadth | Npgsql type loading; exotic tool introspection | Unknown until probed | Extend `compat.rs` on evidence — the shim architecture is built for exactly this |
| Single database, namespaces-as-schemas | Multi-database pickers | Cosmetic | none needed |
| `$snapshots`/`$manifests` projection quirk | Only dashboards querying Iceberg metadata tables | Low | add `ORDER BY` (`limitations.md`) |
| `AS OF` sugar absent on Flight SQL | Only the Flight JDBC path | Low | use `"t@<snapshot_id>"` |
| Writes: no `COPY FROM`, sync DML latency | Write-back / analytics-app patterns | Irrelevant for read-only BI | Flight `adbc_ingest` for data loading |

Two deployment-posture notes worth stating for BI fleets specifically:

- **Always run `--auth-file` + TLS** for BI: tool connection dialogs store
  credentials, and the permissive no-auth default (any user/password, WARN
  at startup) must never face an office network. `--authz-file` with
  read-only scopes (`CanReadData`) gives BI service accounts an
  engine-enforced read-only guarantee — a misconfigured "write-back" widget
  fails with `42501` instead of committing.
- **Scale-to-zero interacts with gateway keepalives.** Power BI gateways
  and Tableau Server background refreshes hold or re-open connections on
  their own schedules; an `--idle-shutdown-secs` writer behind such a fleet
  may never park (or park and cold-start into a refresh window). Point BI
  at read replicas (`db:ro` routing label) or a dedicated read deployment,
  and keep scale-to-zero for the writer.

## 4. Semantics BI users will notice (mostly good)

- **Freshness**: default reads are *exactly fresh* — a dashboard reflects
  the latest Iceberg commit with no staleness window, which is better than
  most lakehouse-BI stacks (Trino/Spark caching layers). Under
  `--freshness-ms N` the bound is explicit and observable
  (`icegres_freshness_age_ms`).
- **Types**: Arrow-native columns arrive as real Postgres types over the
  wire (int8, float8, text, timestamp) — pgjdbc/psycopg2 probes verify
  round-tripping. BI date/number formatting works off real types, not
  strings.
- **Time travel as a BI feature**: `SELECT … FROM demo.trips AS OF
  TIMESTAMP '2026-07-01 00:00:00'` (or `"trips@<snapshot>"`) in a custom
  SQL / initial SQL box gives point-in-time dashboards and month-end
  reporting snapshots — a genuine differentiator over stock Postgres; no BI
  tool needs to understand it, it is just SQL text.
- **Branches**: a `<db>@<branch>` endpoint (process mode) lets a BI
  workspace point at a zero-copy data branch — staging dashboards over
  production storage.

## 5. Performance & operations for dashboard fleets

- **Extracts / Import**: one large streaming scan; server memory is bounded
  by batch size, not result size (`limitations.md` §Memory). The pgwire
  text protocol is the bottleneck at the million-row scale (Node bench:
  4.37 s vs 2.30 s Flight at 1M rows); acceptable for scheduled extracts,
  and the Flight JDBC path (§6) is the upgrade when it isn't.
- **Live / DirectQuery**: high-cadence small-to-medium queries. Turn on
  `--freshness-ms` (dashboards tolerate seconds of staleness; this removes
  a catalog round trip per scan) — which also enables the physical-plan
  cache and opt-in **result cache**: repeated identical dashboard tiles
  short-circuit entirely. Caveat: the plan cache keys on *identical*
  statement text, and BI tools interpolate literals, so expect result-cache
  hits on refresh-same-dashboard patterns and plan re-use mostly via the
  extended protocol's prepared statements.
- **Concurrency**: many simultaneous viz queries share the DataFusion
  session's FairSpillPool — heavy queries spill then error rather than
  OOM-ing the server; there is no per-query wall-clock bound on pgwire yet
  (§3). Fan reads out with read replicas (`--read-replicas-max` /
  the Helm `-read` Deployment + HPA), which are stateless and BI-shaped by
  design.
- **HA**: BI tools reconnect on error as a matter of course; icegresd
  endpoint failover and writer failover (`limitations.md` §icegresd-ha) sit
  well under typical dashboard retry behavior.

## 6. The ADBC / Arrow Flight SQL angle

ADBC is icegres's best-proven client surface (probe A11 exercises both the
Flight SQL driver and the ADBC postgres driver end to end, introspection
and bulk ingest included), and the measured payoff is large enough to
shape the whole BI recommendation. This section carries the numbers, the
ecosystem status, and the extract patterns they justify.

### Measured: what ADBC buys against icegres

From the recorded driver benchmark
([`bench/results/fetch-compare-summary.md`](../bench/results/fetch-compare-summary.md)
— five clients, same live stack, fetch-to-pandas medians):

| 5M rows → pandas | 5 cols | 15 cols |
|---|--:|--:|
| **ADBC (Flight SQL)** | **959 ms** | **2,367 ms** |
| ADBC (postgres/COPY) | 3,185 ms | 6,082 ms |
| psycopg2 (pgwire rows) | 11,519 ms | 26,222 ms |
| ODBC (psqlODBC) | 19,042 ms | 38,769 ms |

That is **10–16× on full extracts** — the row drivers spend the time
materializing one Python object per cell; ADBC never leaves Arrow. Two
qualifiers that matter for BI routing: below ~50k rows the advantage
*inverts* (Flight's ~3-round-trip gRPC floor loses to the row drivers'
lower per-query floor — keep small interactive dashboard queries on the
tool's native connector), and the ADBC **postgres** driver is the
all-rounder when only the pgwire port is exposed: row-driver floor on
small queries, 4–6× row-driver speed at 5M via `COPY … BINARY`. Rule of
thumb: **interactive → native connector; ≥ ~100k rows to a
DataFrame/extract → ADBC Flight; embedded on the lake files → DuckDB.**

### Ecosystem status (July 2026, labeled)

- **No packaged BI tool ships a generic ADBC/Flight SQL connector yet.**
  Everything below is a bridge or a platform signal.
- **Power BI is adopting ADBC as driver technology**: Microsoft's
  Databricks connector switches to ADBC in **August 2026** (Desktop ≥
  2.145.1105.0). Connector-specific, not a generic Flight SQL target — but
  it makes a future first-party ADBC path plausible and is worth tracking.
- **Flight SQL JDBC driver** — **proven against icegres**
  (`bench/clients/A9FlightJdbcProbe.java`). The bridge for every tool with
  a generic/custom JDBC slot: Tableau "Other Databases (JDBC)", DBeaver,
  Metabase driver plugins, DigDash's driver registry.
- **Flight SQL ODBC driver** (Dremio-built, free download) — the same
  bridge for ODBC-only surfaces; documented by Dremio for Tableau.
  Unverified against icegres.
- **`flightsql-dbapi`** (InfluxData) — Python DB-API 2 + SQLAlchemy
  dialect for Flight SQL, written to ease Superset connections, and its
  primary dialect targets **DataFusion** — which is exactly the engine
  icegres runs. The natural Superset fast lane; SQLAlchemy URI
  `datafusion+flightsql://user:pass@host:50051`.
- **Grafana's FlightSQL datasource plugin exists but is archived**
  (InfluxData, v1.1.1, April 2024, "not under active development"). Still
  installable/signed; treat as best-effort and keep the Postgres
  datasource as the supported lane.
- **Caveat on every Flight bridge**: generic drivers skip the tool's
  Postgres-specific SQL generation and metadata niceties, and `AS OF`
  sugar is pgwire-only — use `"t@<snapshot_id>"` on Flight.

### The Hyper extract pattern (Tableau without waiting for ADBC-in-Tableau)

**Hyper is Tableau's embedded columnar database engine** — every extract
is a `.hyper` file, and in Extract mode Desktop/Server/Cloud run all viz
queries against Hyper, never against icegres. icegres is touched only by
the *refresh*: one bulk pull, which through Tableau's native PostgreSQL
connector rides pgjdbc — the row path that loses 10–16× above.

Nothing requires Tableau's connector to build the file. Tableau publishes
the **Hyper API** for writing `.hyper` directly, and `pantab` wraps it
with Arrow input. So a small scheduled job replaces the refresh:

```
icegres ── Flight SQL/ADBC (2.4 s per 5M×15) ──▶ pyarrow
       └─ native connector (26–39 s, rows) ─┐      │ pantab / Hyper API
                                            ▼      ▼
                                     .hyper extract ──▶ publish (REST) ──▶ Tableau Server
```

Users see identical dashboards, refreshed at a 10–16× lower icegres cost;
time travel composes (`AS OF` on pgwire / `t@snapshot` on Flight) so an
extract can be a reproducible point-in-time artifact. The **Power BI
analogue** is Parquet: the same ADBC pull written as a Parquet file/folder
consumed by Power BI's Parquet connector (or lake-side shortcuts).
Honest label: every piece is standard and the icegres side is proven
(A11 + the recorded bench); the assembled pipelines are **by-construction**
until a live Tableau/Power BI smoke run lands.

### Custom dashboards

Already fully served by the gRPC-web / `@icegres/flight-web` path
([`frontend-dashboards.md`](frontend-dashboards.md)) — that document is
the packaged-BI counterpart of this one.

### Server-side readiness

If/when first-party ADBC or Flight SQL connectors land in the packaged
tools, icegres is already protocol-complete on the server side — queries,
catalog metadata (`GetTables`/`GetDbSchemas`), prepared statements, TLS,
auth, per-RPC authz, timeouts, and result-size caps all exist on the
Flight listener. No server work is on the critical path for BI; the work
is packaging and validation.

## 7. Validation & hardening plan (ranked)

1. **Npgsql probe** (`bench/clients/`, .NET or a recorded-traffic replay):
   connect (type-loading query), reflection, binary-format extended-
   protocol reads, parameterized SELECT. Unlocks confidence for Power BI
   *and* Excel. Any planning failure lands in `compat.rs`, whose shim
   architecture (rewrite-only-pg_catalog-statements) is built for it.
2. **Live smoke: Superset + Metabase + Grafana + Redash** against the dev
   stack via docker-compose — cheapest conversion of by-construction to
   proven-live, and the resulting recipes go straight into `clients.md`.
3. **Tableau Desktop run** (Extract first, then Live): capture the actual
   query/settings traffic, pin down whether extract streaming trips the
   in-txn `0A000`, and publish the known-good properties
   (`preferQueryMode`, fetch settings) as a documented recipe. TDVT for
   systematic Live-mode dialect coverage if Live matters.
4. **Power BI Desktop run** (Import, then DirectQuery, then via gateway),
   with the ODBC fallback documented alongside (`UseDeclareFetch=0`).
5. **pgwire per-statement timeout** — the one server-side hardening item BI
   genuinely needs (Flight already has it); currently queued in
   `limitations.md` §Timeouts.
6. **Docs**: fold the verified recipes and per-tool settings into
   `clients.md` §BI as they are proven.

## Bottom line

icegres does not need a "BI connector" — it needs *validation runs*. The
architecture bet (be a convincing Postgres 16 on the wire, with engineered
`pg_catalog` shims and probe-enforced driver coverage) is exactly the right
one for BI, and the read path's streaming/freshness/replica story fits
dashboard workloads well. The JDBC- and psycopg2-based tools (Tableau,
Metabase, Superset, Redash, DBeaver, Looker) should connect today with two
documented caveats (no named cursors; keep reads autocommit or
simple-protocol). The material unknown is **Npgsql**, which gates Power
BI's native path — probe it first; ODBC is the verified interim answer.
Nothing found in this analysis requires engine or protocol redesign; the
gaps are a probe, a timeout, and per-tool settings documentation.
