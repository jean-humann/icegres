# Icegres custom connector for Power BI (scaffold)

A branded Power Query connector for icegres, built on **the same principle
as Power BI's Dremio connector**: a thin M wrapper over a driver that
speaks Arrow Flight SQL, letting the M engine fold queries (filters,
projections, joins → SQL) against `icegres flight-serve`.

## The principle, stated precisely

- Power BI's in-box connectors are moving to **embedded ADBC drivers**
  (Dremio's uses the arrow-adbc FlightSql driver behind
  `Implementation="2.0"`), but that engine surface is **not exposed to
  custom connectors** — there is no public `Adbc.DataSource` M function
  today.
- The documented extensibility path for driver-based custom connectors is
  **`Odbc.DataSource`** — exactly how the Dremio connector worked before
  its ADBC switch. This scaffold wraps it over the free **Arrow Flight
  SQL ODBC driver** (Dremio-built, protocol-generic), so the wire is
  Flight SQL end to end with one in-process ODBC translation on the
  client.
- The day Microsoft exposes ADBC to custom connectors, the swap is
  confined to the `Odbc.DataSource` call in `Icegres.pq`.

## Files

- [`Icegres.pq`](Icegres.pq) — connector source (data source kind, auth,
  publish record, `Odbc.DataSource` wrapper with DataFusion dialect
  capabilities). Structure mirrors `microsoft/DataConnectors`
  `samples/ODBC/SqlODBC`.
- [`Icegres.query.pq`](Icegres.query.pq) — SDK evaluation query.

## Build & test (Windows required)

1. Install [VS Code](https://code.visualstudio.com/) + the
   **Power Query SDK** extension (marketplace:
   `PowerQuery.vscode-powerquery-sdk`).
2. Install the **Arrow Flight SQL ODBC driver** (free download from
   Dremio; registers as `Arrow Flight SQL ODBC Driver` — if your install
   registers another name, adjust `Config_DriverName`).
3. Open this folder, let the SDK create the workspace, then **Evaluate**
   `Icegres.query.pq` against a reachable `flight-serve` (set
   credentials via the SDK's *Set credential* command; the icegres
   `--auth-file` user/password).
4. Build → produces `Icegres.mez`.
5. Sideload: copy `Icegres.mez` to
   `Documents\Power BI Desktop\Custom Connectors`, and in Power BI
   Desktop set *Options → Security → Data extensions → Allow any
   extension* (or sign the connector and use the recommended setting).
   Restart Desktop; "icegres" appears under Get Data → Database.
6. Gateway: the on-premises data gateway loads custom connectors from a
   configured folder — put the `.mez` (and the ODBC driver) on the
   gateway machine for scheduled refresh.
7. Distribution beyond sideloading = Microsoft's
   [connector certification program](https://learn.microsoft.com/en-us/power-query/connector-certification-overview).

## Verification status

**Authored to spec, not yet built or run** — the Power Query SDK
toolchain is Windows-only, so this repo's Linux CI cannot compile or
evaluate M. Before first use: run step 3 against a live icegres, then
exercise Import and DirectQuery on the seeded demo tables and extend the
`SqlCapabilities` / `SQLGetInfo` overrides as folding gaps surface
(`docs/bi-integration.md` §7 collects findings). The two lanes that need
no Windows build remain in [`../README.md`](../README.md): the in-box
Dremio connector pointed at `adbc://host:50051`, and the native
PostgreSQL connector (Npgsql, probe A14).

## Documentation & code inventory (everything used to build this)

**Power Query connector development**
- SDK overview + samples: <https://github.com/microsoft/DataConnectors>
  (`samples/ODBC/SqlODBC` is the template this scaffold follows;
  `HiveSample`, `ImpalaODBC`, `RedshiftODBC`, `SnowflakeODBC` show
  per-dialect capability tuning)
- ODBC-based connectors + DirectQuery enablement (every
  `SqlCapabilities`/`SQLGetInfo`/`SQLGetTypeInfo` knob):
  <https://learn.microsoft.com/en-us/power-query/odbc>
- `Odbc.DataSource` parameter reference:
  <https://learn.microsoft.com/en-us/power-query/odbc-parameters>
- Connector certification:
  <https://learn.microsoft.com/en-us/power-query/connector-certification-overview>

**The ADBC context**
- Power BI/Fabric ODBC→ADBC transition (per-connector driver table,
  `Implementation="2.0"`, timeline):
  <https://learn.microsoft.com/en-us/power-query/transition-to-adbc>
- Dremio's ADBC-in-Power-BI announcement and docs (the in-box
  Flight SQL ADBC lane):
  <https://www.dremio.com/blog/announcing-arrow-database-connectivity-adbc-in-microsoft-power-bis-connector-for-dremio/>,
  <https://docs.dremio.com/current/client-applications/microsoft-power-bi-adbc/>
- arrow-adbc C# drivers (incl. the FlightSql driver Power BI embeds):
  <https://github.com/apache/arrow-adbc/tree/main/csharp/src/Drivers>

**The driver under this connector**
- Arrow Flight SQL ODBC driver (download, connection parameters
  `HOST`/`PORT`/`UID`/`PWD`/`UseEncryption`/
  `DisableCertificateVerification`):
  <https://docs.dremio.com/current/client-applications/drivers/arrow-flight-sql-odbc-driver/>
