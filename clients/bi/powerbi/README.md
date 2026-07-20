# Power BI ⇄ icegres

Three lanes, ordered by what to try first. Background: Power BI/Fabric is
migrating its embedded connectors from Simba ODBC drivers to **ADBC**
(tenant default planned August 2026; ODBC removed from the service late
2026 and from Desktop/gateway in spring 2027). The migration is
per-connector — there is still **no generic "any ADBC driver" data
source** — but one of the migrated connectors is protocol-generic enough
to matter here.

## Lane A — the Dremio connector's Flight SQL ADBC driver (the ADBC path)

Since May 2025, Power BI's **Dremio Software connector** offers an
embedded **Flight SQL ADBC driver** (the `arrow-adbc` C# FlightSql
driver; the ODBC→ADBC transition table maps Dremio to "FlightSQL ADBC").
Dremio's endpoint is a *standard* Arrow Flight SQL server — the same
protocol `icegres flight-serve` speaks — and the connector's Server field
takes a raw `adbc://host` target. That makes it the Power BI analogue of
the Tableau Flight-JDBC bridge:

1. Get Data → **Dremio Software** → select the **Flight SQL ADBC
   driver** option.
2. Server: `adbc://<icegres-host>:50051` (icegres's Flight port — do not
   omit it; the driver defaults to Dremio's 32010).
3. Authentication: username/password → the `--auth-file` principal
   (Dremio's "personal access token" kind also rides the password slot).
4. Run the listener with TLS (`flight-serve --tls-cert/--tls-key`) —
   the connector expects encrypted endpoints for service/DirectQuery use.

**Status: unverified against icegres** (Power BI Desktop is
Windows-only; nothing in this repo's CI can run it). By construction the
wire layer is exactly what probe A9F already proves (standard Flight SQL
RPCs: handshake auth, `GetTables`/`GetDbSchemas`, prepared statements,
`DoGet`), so the risk concentrates in the connector's *navigation and
generated SQL* (Dremio-flavored `INFORMATION_SCHEMA` probes or dialect
corners), not the transport. Early-adopter reports of connection quirks
exist on Dremio's community forum — treat this lane as a validation
target, and report findings back into `docs/bi-integration.md` §7.

## Lane B — native PostgreSQL connector (Npgsql) — driver probe-proven

The supported interactive lane. Npgsql is probed green against icegres
(A14, `bench/clients/a14-npgsql-probe/` — including the connect-time
type-loading step the coherent `pg_type` patch keeps honest), and
measured fast for a row driver — 1M narrow rows in 552 ms vs psycopg2's
1,937 ms, with the Arrow lane's 316 ms ~1.7× quicker (recorded run:
`bench/results/bi-stack-fetch-2026-07-20-sandbox.md`). Connect as
PostgreSQL: host, port `5439`, database `icegres`, TLS on. Known edges:
`docs/bi-integration.md` §2 (Power BI section).

## Lane C — Parquet sideload (ADBC speed without connector risk)

For scheduled Import refreshes at scale: pull over ADBC Flight with
[`../extract/`](../extract/) and hand Power BI a Parquet file its Parquet
connector reads locally —

```bash
icegres-extract --dsn grpc+tls://icegres:50051 --username bi \
    --table demo.trips trips.parquet
```

This is "ADBC for Power BI" available today, with zero dependence on
connector behavior. The generic **ODBC** connector (psqlODBC, probe A10,
`UseDeclareFetch=0`) remains the verified fallback — note Microsoft's
timeline retires only *embedded* ODBC drivers; separately-installed ODBC
drivers through the ODBC connector are explicitly out of that scope.

## Lane D — a branded icegres custom connector (scaffold shipped)

[`connector/`](connector/) scaffolds a Power Query custom connector on
the same principle as the Dremio one — an M wrapper over the Arrow
Flight SQL ODBC driver via `Odbc.DataSource` (there is no public
`Adbc.DataSource` extensibility yet), with DataFusion dialect
capabilities and DirectQuery enabled. Windows-only toolchain; build and
verification steps in its README.
