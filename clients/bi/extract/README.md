# icegres-bi-extract — columnar BI extracts over ADBC / Flight SQL

The refresh half of the BI story
([`docs/bi-integration.md`](../../../docs/bi-integration.md) §6): packaged
BI tools refresh their extracts through row-oriented Postgres drivers,
which the recorded driver benchmark
([`bench/results/fetch-compare-summary.md`](../../../bench/results/fetch-compare-summary.md))
measures at **10–16× slower** than the Arrow lane on full extracts
(5M rows × 15 cols to a DataFrame: 2.4 s over ADBC Flight vs 26–39 s over
psycopg2/ODBC). This tool moves the bulk pull onto the fast lane and hands
each tool the file its own engine loads natively:

- **`.hyper` → Tableau.** Every Tableau extract is a `.hyper` file served
  by Tableau's embedded Hyper engine; Desktop/Server/Cloud never query
  icegres interactively in Extract mode. `icegres-extract` builds that file
  directly (Arrow → pantab → Hyper API) and can publish it to Tableau
  Server/Cloud, so Tableau consumes icegres data at columnar speed without
  speaking a word of ADBC itself.
- **`.parquet` → Power BI** (Parquet connector), DuckDB, Spark, or any
  columnar consumer.

Both lanes **stream**: batches flow from the server into the writer as they
arrive (client memory ≈ one batch, not the extract — the same bounded-read
contract the server keeps, `docs/limitations.md` §Memory).

## Install

```bash
pip install ./clients/bi/extract                # Parquet lane only
pip install './clients/bi/extract[hyper]'       # + Tableau .hyper output (pantab)
pip install './clients/bi/extract[hyper,publish]'  # + Tableau Server publish
```

## Usage

Tableau — nightly extract, published in place of the native refresh:

```bash
export ICEGRES_PASSWORD=… TABLEAU_TOKEN=…
icegres-extract --dsn grpc+tls://icegres:50051 --username bi \
    --table demo.trips trips.hyper \
    --publish --server https://tableau.example.com --site analytics \
    --project Lakehouse --token-name refresh-bot
```

Power BI — Parquet drop for the Parquet connector (or a lake shortcut):

```bash
icegres-extract --dsn grpc://localhost:50051 \
    --query "SELECT city, count(*) AS trips FROM demo.trips GROUP BY city" \
    trips.parquet
```

Reproducible point-in-time extract (month-end reporting): pin the snapshot —
the Flight lane has no `AS OF` sugar, so the tool spells the
`"table@snapshot"` form for you:

```bash
icegres-extract --dsn grpc://localhost:50051 \
    --table demo.trips --at-snapshot 1234567890123 trips-2026-06-30.hyper
```

Schedule it with cron / your orchestrator; the single stdout line
(`wrote … rows=… size=… elapsed=…`) is designed for run logs.

## Server-side pairing

- Point it at `icegres flight-serve` with `--tls-cert/--tls-key` and
  `--auth-file`; give the extract principal read-only scope
  (`--authz-file`, `CanReadData`) so the refresh credential cannot write.
- The pull is one streaming `DoGet`; the server's read path is
  memory-bounded regardless of extract size.
- `--flight-statement-timeout-ms` / `--flight-max-result-bytes` on the
  listener bound a runaway extract query.

## Honest labels

- **Both output lanes are smoke-verified against a live `flight-serve`**
  (REST-catalog fixture + moto stack): `demo.trips` extracted to
  `.parquet` and `.hyper` (pantab 5.3, Hyper engine), both read back with
  full row/column counts, plus the `--query` variant. The underlying ADBC
  Flight fetch path is probe-proven (`bench/clients/a11_adbc_probe.py`).
- The **publish leg is by-construction**: `tableauserverclient` is the
  standard library for exactly this job, but no Tableau Server run is
  recorded in this repo — run one refresh cycle against a dev site before
  trusting it in production.
