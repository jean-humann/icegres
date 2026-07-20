# clients/bi — BI tool connection kits (ADBC / Flight SQL first)

Per-tool kits for connecting packaged BI products to icegres the
state-of-the-art way: **columnar (ADBC / Arrow Flight SQL) wherever the
tool can reach it, pgwire where it cannot** — with the measured reasoning
in [`docs/bi-integration.md`](../../docs/bi-integration.md) (§6: 10–16×
on extracts, inverted below ~50k rows, so interactive traffic stays on
each tool's native connector).

| Kit | Tool | Fast lane | Fallback lane |
|---|---|---|---|
| [`extract/`](extract/) | Tableau (.hyper) · Power BI (Parquet) | ADBC Flight → native file | tool-native connector refresh |
| [`powerbi/`](powerbi/) | Power BI | Dremio-connector Flight SQL ADBC bridge · Parquet sideload | native PostgreSQL connector (Npgsql, probe A14) |
| [`superset/`](superset/) | Apache Superset | `datafusion://` SQLAlchemy dialect (flightsql-dbapi) | `postgresql+psycopg2://` (probe A8) |
| [`grafana/`](grafana/) | Grafana | FlightSQL datasource plugin (archived upstream) | built-in PostgreSQL datasource |
| [`tableau/`](tableau/) | Tableau live/extract | Flight SQL JDBC via "Other Databases (JDBC)" | native PostgreSQL connector + properties file |
| [`digdash/`](digdash/) | DigDash Enterprise | Flight SQL JDBC in the driver registry | pgjdbc in the driver registry |

Every claim in these kits carries the repo's verification labels:

- **proven-live** — a committed probe exercises the exact path
  (`bench/clients/`: A8 SQLAlchemy/psycopg2, A9 pgjdbc, A9F Flight SQL
  JDBC, A10 psqlODBC, A11 ADBC both lanes, A13 flightsql-dbapi,
  A14 Npgsql).
- **product-smoked** — the packaged product itself was run against a live
  icegres and exercised end to end (currently: Superset over the Flight
  lane; Grafana over both lanes).
- **by-construction** — the driver stack under the tool is probe-verified;
  the packaged product itself has not been run against icegres here.
- **unverified** — no probe coverage; treat as a recipe to validate.

Server-side posture for every kit: `flight-serve --tls-cert/--tls-key
--auth-file`, a read-only BI principal (`--authz-file` with `CanReadData`,
or `flight-serve --read-only`), and the resource guards
(`--flight-statement-timeout-ms`, `--flight-max-result-bytes`,
`--flight-max-concurrent-rpcs`). pgwire lanes: TLS + SCRAM via `serve
--tls-cert/--tls-key --auth-file`.
