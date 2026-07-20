# Per-BI-stack fetch — 2026-07-20 sandbox run

**Question.** For each BI connection stack this repo ships kits for, what
does one fetch cost into that stack's *natural client structure* — and
where does Npgsql (the Power BI driver, absent from the recorded
[`fetch-compare`](fetch-compare-summary.md) study) actually land?

**Runner.** [`bench/compare/bi_stack_fetch.py`](../compare/bi_stack_fetch.py)
(Python lanes) + [`bench/compare/npgsql-fetch/`](../compare/npgsql-fetch/)
(.NET lane). Table `demo.wide1m`: 1,000,000 rows × 5 cols (int64, string,
2× float64, bool), seeded via Flight `adbc_ingest`. Query
`SELECT * FROM demo.wide1m LIMIT n`; median of 5 (1 warmup) per point.

**Environment — read before quoting.** A CI sandbox, not the standard
bench box: catalog = Apache `RESTCatalogServer` test fixture
(sqlite-backed), S3 = moto in-memory, everything on localhost, icegres
release build. All lanes traverse the identical server/catalog/store, so
the *relative* numbers are meaningful; absolute floors are not
representative, and each client lands in a different structure (Arrow
table vs pandas rows vs .NET typed reads vs Python tuples) — this is a
transport + decode comparison that complements, not replaces, the
recorded fetch-to-pandas study.

## Results (median ms)

| client → structure | 10k | 100k | 1M |
|---|--:|--:|--:|
| ADBC Flight SQL → pyarrow Table | 74.1 | **96.2** | **316.2** |
| Npgsql (typed reads, .NET) | 82.4 | 135.9 | 552.2 |
| ADBC postgres (COPY) → pyarrow Table | 71.8 | 142.0 | 754.3 |
| flightsql-dbapi (Superset stack) → pandas | **65.4** | 183.7 | 1,543.9 |
| psycopg2 → Python tuples | 87.2 | 266.4 | 1,936.7 |

## What it adds to the recorded study

1. **Npgsql, measured for the first time, is much faster than the Python
   row-driver family**: 552 ms at 1M narrow rows vs psycopg2's 1,937 ms
   (~3.5×) — its binary extended protocol and typed .NET field reads skip
   the per-cell Python-object tax the recorded study's row-family
   stand-in pays. It still trails ADBC Flight (~1.7×). Consequence for
   the Power BI kit: the row-family numbers overstate the native
   connector's Import cost; the Arrow lane remains the fast path.
2. **flightsql-dbapi wins the wire but pays a pandas materialization tax**
   at large results (Arrow transport, rows materialized through pandas in
   its cursor) — irrelevant in practice for Superset, whose SQL Lab caps
   result rows.
3. At 10k rows all five lanes sit within ~25 ms — consistent with the
   recorded study's interactive-floor finding.
