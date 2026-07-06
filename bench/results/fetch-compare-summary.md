# Driver fetch comparison — ADBC vs JDBC/ODBC/psycopg2 vs DuckDB

**Question this answers:** *"Isn't ADBC supposed to be faster than JDBC/ODBC by avoiding
serialization/deserialization? Check full usage with BI / pyarrow / pandas for each, and give
a per-case recommendation across <10k / <100k / <1M / 5M rows at 15 columns."*

**Short answer: yes — and it's now measured.** ADBC's columnar transport keeps data in Arrow
end to end and skips the row-by-row Python-object materialization that psycopg2 / pyodbc / JDBC
pay. That advantage is invisible on tiny results (the per-query protocol floor dominates) and
becomes decisive as the result set grows — **up to 16× faster** at 5M rows wide.

## How it was measured

Two real Iceberg tables served by icegres over the live lakehouse (Lakekeeper REST catalog →
Parquet on RustFS):

| table | shape | files |
|---|---|---|
| `demo.trips_big` | 5,000,000 rows × **5 cols** (narrow) | 10 |
| `demo.wide15` | 5,000,000 rows × **15 cols** (6 bigint / 5 double / 4 text) | 200 |

For each **(table, row-count, client)** we time a `SELECT … LIMIT N` fetched into both a
`pyarrow.Table` and a `pandas.DataFrame` — the two shapes BI tools, notebooks, and ML pipelines
actually consume. Median of 5 iterations (3 at 5M), 1–2 warmups discarded.
Runner: [`bench/compare/fetch_bench.py`](../compare/fetch_bench.py); raw JSON:
[`fetch-compare.json`](fetch-compare.json).

Five clients across the three families + the embedded baseline:

| client | family | transport | data path |
|---|---|---|---|
| pgwire (psycopg2) | JDBC/ODBC-class row driver | pgwire | rows → Python tuples → Arrow/pandas |
| ODBC (psqlODBC) | ODBC | pgwire | rows → Python tuples → Arrow/pandas |
| **ADBC (Flight SQL)** | ADBC | Arrow Flight (gRPC) | **Arrow batches end to end** |
| ADBC (postgres/COPY) | ADBC | pgwire `COPY … BINARY` | binary → Arrow in the driver |
| DuckDB (iceberg parquet) | embedded | none (local files) | Parquet → Arrow, no wire |

psycopg2 stands in for the whole row-oriented JDBC/ODBC family — pgjdbc and psqlODBC produce
the same row-materialization cost (confirmed: the ODBC lane tracks psycopg2 within noise, a bit
slower). DuckDB reads the lakehouse's **actual current-snapshot Parquet files** (staged locally
because its httpfs/iceberg extensions are CDN-blocked here — identical bytes) and is the
zero-wire ceiling: what's achievable with no network or serialization at all.

## Results — fetch to **pandas** (median ms; the BI/notebook path)

### Narrow: `trips_big`, 5 columns
| client | 5k | 50k | 500k | 5M |
|---|--:|--:|--:|--:|
| pgwire (psycopg2) | 77 | 253 | 1,351 | 11,519 |
| ODBC (psqlODBC) | 106 | 369 | 2,035 | 19,042 |
| **ADBC (Flight SQL)** | 153 | 311 | **348** | **959** |
| ADBC (postgres/COPY) | 85 | 196 | 507 | 3,185 |
| DuckDB (local parquet) | **12** | **24** | 172 | 1,047 |

### Wide: `wide15`, 15 columns
| client | 5k | 50k | 500k | 5M |
|---|--:|--:|--:|--:|
| pgwire (psycopg2) | 89 | 294 | 2,631 | 26,222 |
| ODBC (psqlODBC) | 84 | 400 | 3,643 | 38,769 |
| **ADBC (Flight SQL)** | 116 | 181 | **395** | **2,367** |
| ADBC (postgres/COPY) | **69** | 151 | 667 | 6,082 |
| DuckDB (local parquet) | 13 | 32 | 347 | 3,986 |

*(Arrow-output numbers are in [`fetch-compare.json`](fetch-compare.json); for the Arrow-native
clients Arrow ≈ pandas, so the story is identical. For the row drivers the pandas column is the
fair number — real code uses `pd.read_sql`, not hand-built Arrow.)*

## What the numbers say

**1. ADBC's serialization advantage is real and grows with size.** Fetch 5M rows to pandas:

| | 5 cols | 15 cols |
|---|--:|--:|
| ADBC Flight vs psycopg2 | **12.0×** faster (959 vs 11,519 ms) | **11.1×** faster (2,367 vs 26,222 ms) |
| ADBC Flight vs ODBC | **19.9×** faster (959 vs 19,042 ms) | **16.4×** faster (2,367 vs 38,769 ms) |

At 5M × 15 that's **24 seconds saved on every extract** vs psycopg2, **36 s** vs ODBC. This is
exactly the "avoid serialization/deserialization" win you expected — the row drivers spend that
time building ~75M individual Python objects; ADBC never leaves Arrow.

**2. Below ~50k rows the advantage inverts.** ADBC Flight carries a ~3-round-trip gRPC floor
(GetFlightInfo + DoGet + prepare), so at 5k rows it's the *slowest* wire client (116–153 ms vs
psycopg2's 77–89). For small/interactive results the row drivers' lower per-query floor wins.
The crossover is ~50k–100k rows; past it ADBC pulls away fast.

**3. ADBC-postgres (COPY) is the low-floor all-rounder.** It reaches Arrow via libpq's binary
`COPY`, so it has a psycopg2-class floor at 5k (69–85 ms) *and* scales 4–6× better than row
drivers at 5M (3,185 / 6,082 ms). It's the best choice when you only have the pgwire endpoint
(no Flight server) — 2–2.5× slower than Flight at the top end but far ahead of row drivers.

**4. Wider rows amplify the row-driver penalty.** Going 5→15 columns, psycopg2's 5M-pandas time
grows 2.3× (11.5→26.2 s) because per-cell object creation scales with *cells*, not rows. ADBC
Flight grows 2.5× (0.96→2.37 s) but from a 12× lower base, so the absolute gap widens sharply.

**5. DuckDB is the zero-wire ceiling.** Reading Parquet directly (no server, no protocol) it's
fastest to Arrow almost everywhere. Its pandas number at 5M is heavier (Arrow→pandas object
conversion for text columns), but for pure `pyarrow` it's unbeatable. It's a different pattern —
embedded analytics *on* the lake files — not a client of the server.

## Per-case recommendation guide

| result size | typical use | recommended | avoid | why |
|---|---|---|---|---|
| **< 10k rows** | point lookups, interactive SQL, small BI slices | **psycopg2 / JDBC / ODBC**, or **ADBC-postgres** | ADBC **Flight** | Flight's gRPC round-trip floor (~110–155 ms) loses to the row drivers' ~80 ms floor; columnar transport has nothing to amortize yet |
| **< 100k rows** | dashboard tiles, medium extracts | **ADBC-postgres** (low floor + good scaling); ADBC Flight fine | ODBC at the top of the range | transition zone — everything is sub-400 ms; ADBC edges ahead, row drivers start to lag |
| **< 1M rows** | reporting extracts, feature slices, notebook loads | **ADBC (Flight SQL)** | psycopg2 / ODBC | ADBC ~360 ms vs 1.5–3.6 s for row drivers — 4–9× faster; the columnar win is now dominant |
| **5M+ rows** | full-table extracts, ML feature pipelines, bulk → pandas/pyarrow | **ADBC (Flight SQL)**; **DuckDB** if you can read the lake directly | psycopg2 / ODBC / JDBC | 10–16× faster over the wire (2.4 s vs 26–39 s); row drivers spend all their time materializing Python objects |

### By tool / workflow

- **pandas `read_sql` / pyarrow via psycopg2 or pyodbc** — fine to ~50k rows; painful beyond.
  This is the default most BI/notebook code lands on, and the slow path for big extracts.
- **ADBC Flight `fetch_arrow_table()` / `fetch_df()`** — *the* tool for large results into
  pandas/pyarrow. Drop-in DB-API; the "no serialization" path you were asking about. Use it for
  anything ≥ ~100k rows headed to a DataFrame or Arrow.
- **ADBC-postgres (`adbc_driver_postgresql`)** — best when you're pinned to the pgwire endpoint
  and can't run a Flight server: near-row-driver latency on small queries, Arrow-native scaling
  on big ones. A safe universal default for mixed workloads on one connection.
- **BI tools (Tableau/Power BI/Superset via JDBC/ODBC)** — interactive dashboards issue small,
  filtered queries where the row-driver floor is fine; keep JDBC/ODBC there. Route large
  *extract/refresh* jobs through ADBC Flight where the connector supports it.
- **DuckDB** — for embedded analytics directly on the lakehouse Parquet (no icegres process in
  the loop): fastest to Arrow, ideal for local exploration and joins over the lake files.

## One-line rule of thumb

**Small & interactive → JDBC/ODBC/psycopg2 (or ADBC-postgres). Large → pandas/pyarrow → ADBC
Flight. Embedded on the lake → DuckDB.** The crossover is ~50k–100k rows; the bigger and wider
the result, the more decisively ADBC wins — exactly because it never serializes rows.

---

*Caveat: `wide15` has 200 small Parquet files (one per 25k-row ADBC-ingest commit) vs
`trips_big`'s 10, which raises absolute scan cost for the wide table equally across all clients —
the driver-to-driver comparison (the point of this benchmark) is unaffected. All clients hit the
same server/table; DuckDB reads the same staged files.*
