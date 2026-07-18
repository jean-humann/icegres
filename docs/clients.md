# Connecting clients

Copy-paste connection recipes for every verified client. Defaults assume the
local dev stack: pgwire on `localhost:5439`, Flight SQL on `localhost:50051`,
database name `icegres`, no auth (the permissive default — any user/password;
see [`configuration.md`](configuration.md) for `--auth-file` and TLS).

Every recipe below is exercised continuously by the probes under
[`bench/clients/`](../bench/clients/) — if a snippet here drifts from reality,
the e2e suite catches it. Known per-driver caveats live in
[`limitations.md`](limitations.md); the big three: no `COPY … FROM STDIN`
(bulk ingest goes through Flight `adbc_ingest`), no server-side (named)
cursors, and extended-protocol `SELECT` inside an *explicit* transaction is
answered with `0A000` (use autocommit for reads).

## psql

```bash
psql "host=localhost port=5439 dbname=icegres user=postgres"
# TLS-enabled server: append sslmode=require (or verify-full with a CA)
```

## Python — psycopg2

```python
import psycopg2
conn = psycopg2.connect(host="localhost", port=5439,
                        dbname="icegres", user="postgres")
conn.autocommit = True          # reads via the simple protocol
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM demo.trips")
    print(cur.fetchone()[0])
```

Bulk **reads** via COPY (server encodes binary/text/csv):

```python
import io
sink = io.BytesIO()
with conn.cursor() as cur:
    cur.copy_expert(
        "COPY (SELECT * FROM demo.trips) TO STDOUT (FORMAT binary)", sink)
```

## Python — SQLAlchemy

```python
import sqlalchemy as sa
engine = sa.create_engine(
    "postgresql+psycopg2://postgres:ignored@localhost:5439/icegres")
insp = sa.inspect(engine)        # schema/table reflection works (pg_catalog shims)
print(insp.get_table_names(schema="demo"))
```

Over `pg8000` (pure-Python, extended protocol), set AUTOCOMMIT — an
extended-protocol SELECT inside an explicit transaction is refused with a
clean `0A000`:

```python
engine = sa.create_engine(
    "postgresql+pg8000://postgres:ignored@localhost:5439/icegres",
    isolation_level="AUTOCOMMIT")
```

pandas works through either engine: `pd.read_sql("SELECT …", engine)`.

## Python — ADBC / Arrow Flight SQL (the Arrow-native fast lane)

```python
import adbc_driver_flightsql.dbapi as flight
conn = flight.connect("grpc://localhost:50051")
cur = conn.cursor()
cur.execute("SELECT city, count(*) FROM demo.trips GROUP BY city")
print(cur.fetch_arrow_table())          # Arrow end to end, no row conversion
```

**Bulk ingest** — the whole Arrow stream lands as ONE Iceberg commit
(streaming server-side; memory stays flat regardless of volume):

```python
import pyarrow as pa
tbl = pa.table({"trip_id": pa.array(range(100_000), pa.int64()),
                "city": pa.array(["Paris"] * 100_000)})
cur.adbc_ingest("trips", tbl, mode="append", db_schema_name="demo")
```

Against a `--auth-file`-secured listener (basic-auth handshake; pair with
`flight-serve --tls-cert/--tls-key` so credentials are never cleartext):

```python
conn = flight.connect("grpc+tls://host:50051",
                      db_kwargs={"username": "u", "password": "pw"})
```

## Python — ADBC postgres driver (libpq lane)

```python
import adbc_driver_postgresql.dbapi as pg
conn = pg.connect("postgresql://postgres:postgres@localhost:5439/icegres",
                  autocommit=True)
# reads arrive via COPY (FORMAT binary) under the hood — already columnar
```

## JDBC (stock pgjdbc)

```java
String url = "jdbc:postgresql://localhost:5439/icegres";
Connection c = DriverManager.getConnection(url, "postgres", "ignored");
// DatabaseMetaData, PreparedStatement, executeUpdate, txn cycles verified
```

A Flight SQL JDBC driver also works against `:50051` (see
`bench/clients/A9FlightJdbcProbe.java`).

## ODBC (stock psqlODBC)

```bash
bash infra/scripts/odbc-setup.sh   # registers a DSN "icegres" -> 127.0.0.1:5439
isql icegres
# or DSN-less: "DRIVER={PostgreSQL Unicode};SERVER=localhost;PORT=5439;DATABASE=icegres"
```

## Browser JavaScript — `@icegres/flight-web` (gRPC-web, Arrow end-to-end)

Start the Flight listener with gRPC-web enabled (`flight-serve --grpc-web`),
then query straight from the page — results stay Arrow all the way into
`apache-arrow` (measured ~2.5–2.8× faster than JSON at 100k–1M rows,
[frontend-dashboards.md](frontend-dashboards.md)):

```js
import { FlightWebClient } from "@icegres/flight-web"; // clients/flight-web

const db = new FlightWebClient({
  baseUrl: "http://localhost:50051",
  credentials: { username: "u", password: "pw" },  // when --auth-file is set
});
const table = await db.query(
  "SELECT city, count(*) AS trips FROM demo.trips GROUP BY city",
);
```

Auth over gRPC-web is per-RPC Basic (there is no Handshake in that
protocol) — pair with `--tls-cert/--tls-key` and pin `--cors-origin`. Node
backends can use the same package (`@icegres/flight-web/zstd-node`), plain
`@grpc/grpc-js` (see `bench/clients/js/lib/flight.js`), or ADBC.

## BI tools / anything Postgres

Anything that speaks the Postgres wire protocol connects like a stock
Postgres 16: host, port `5439`, database `icegres`. The `pg_catalog` /
`information_schema` shims answer the introspection queries ORMs and BI tools
issue (`icegres/src/compat.rs`). Time travel is plain SQL:
`SELECT … FROM "demo"."trips@1234567890" ` or
`SELECT … FROM demo.trips AS OF TIMESTAMP '2026-07-01 00:00:00'`.
