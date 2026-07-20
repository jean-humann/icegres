# Tableau ⇄ icegres

Three lanes, ordered by what to try first. The measured background
(`docs/bi-integration.md` §2/§6): Tableau's native connector rides pgjdbc
(a row driver — fine interactively, 10–16× slower on extract-sized pulls),
and in Extract mode all viz queries run in Tableau's embedded **Hyper**
engine, so icegres only pays for the refresh.

## Lane A — Hyper extract sideload (recommended for extracts)

Build the `.hyper` yourself over ADBC Flight and publish it — Tableau
consumes icegres data at columnar speed without speaking ADBC. That is
[`../extract/`](../extract/): `icegres-extract --table demo.trips
trips.hyper --publish …` on a schedule, replacing the native refresh.
icegres side **proven** (probe A11 + the recorded bench); the
pantab → publish leg is **by-construction** — run one cycle against a dev
site first.

## Lane B — native PostgreSQL connector (interactive / Live)

Connect as PostgreSQL: host, port `5439`, database `icegres`, SSL
required (server: `serve --tls-cert/--tls-key --auth-file`). Driver stack
**by-construction** (pgjdbc = probe A9; Tableau itself not yet smoke-run).

Two settings guard the known sharp edges (`docs/bi-integration.md` §3):

- Drop [`postgresql.properties`](postgresql.properties) into
  `~/Documents/My Tableau Repository/Datasources/` (per machine running
  Desktop; on Tableau Server, the equivalent datasource properties
  directory). It sets `preferQueryMode=simple`, which keeps reads legal
  inside any transaction Tableau opens — the extended-protocol
  `SELECT`-in-transaction shape is refused by icegres with `0A000`, and
  pgjdbc's fetch-size streaming triggers exactly that shape.
- Prefer **Extract** over Live for heavy workbooks: extracts move all viz
  SQL into Hyper; Live mode compiles each viz into Postgres SQL and a tail
  of dialect gaps against DataFusion is expected until a TDVT run maps
  them.

Time travel works as custom SQL on this lane:
`SELECT * FROM demo.trips AS OF TIMESTAMP '2026-06-30 00:00:00'`.

## Lane C — Flight SQL JDBC bridge (columnar Live, rough edges)

The Arrow **Flight SQL JDBC driver** is probe-verified against icegres
(`bench/clients/A9FlightJdbcProbe.java`), and Tableau loads custom JDBC
drivers through **Other Databases (JDBC)**:

1. Download the `flight-sql-jdbc-driver` JAR (Maven Central:
   `org.apache.arrow:flight-sql-jdbc-driver`) into Tableau's driver
   directory (`~/Library/Tableau/Drivers` on macOS,
   `C:\Program Files\Tableau\Drivers` on Windows).
2. Connect → Other Databases (JDBC):
   - URL: `jdbc:arrow-flight-sql://<host>:50051?useEncryption=true`
     (dev listener without TLS: `useEncryption=false`)
   - Dialect: **PostgreSQL**; username/password = the `--auth-file`
     principal.

Expect rough edges: a generic JDBC connector skips Tableau's
Postgres-specific SQL generation and metadata niceties, and `AS OF` sugar
does not exist on Flight — use `"trips@<snapshot_id>"`. Status:
**by-construction** (driver proven, in-Tableau run pending). This lane
shines for extract refreshes defined inside Tableau when Lane A's
external job is not an option.
