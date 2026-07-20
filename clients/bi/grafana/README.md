# Grafana ⇄ icegres

Provisioning files for both lanes are under
[`provisioning/datasources/`](provisioning/datasources/) — drop them into
Grafana's `conf/provisioning/datasources/` (or mount them in the
container) and set `ICEGRES_BI_PASSWORD` in Grafana's environment.

Both lanes are **product-smoked**: a real Grafana 11.6 (OSS tarball)
provisioned from these files answered `/api/ds/query` aggregates over
live icegres data on each datasource.

## Lane 1 — FlightSQL datasource plugin (columnar; archived upstream)

InfluxData's `influxdata-flightsql-datasource` speaks Flight SQL directly
(query builder + raw SQL editor, basic-auth/token/TLS). It is signed and
installable from the Grafana catalog:

```bash
grafana-cli plugins install influxdata-flightsql-datasource
```

**One mandatory server-side setting: `--result-compression none`.** The
plugin's bundled Arrow build cannot decode icegres's default
ZSTD-compressed IPC batches — the failure mode is a silently EMPTY panel
(HTTP 200, zero frames, no error), verified against a live listener both
ways. Point this datasource at a listener started with
`flight-serve --result-compression none` (a second dedicated listener is
fine; keep zstd on the one ADBC/extract clients use).

**Honest status:** upstream archived the plugin at v1.1.1 (April 2024,
"not under active development") — the smoke run above proves it works
today (uncompressed batches), but expect no fixes. Treat this lane as
acceleration for heavy panels and keep the Postgres lane provisioned
alongside as the supported path.

## Lane 2 — built-in PostgreSQL datasource (the supported fallback)

Zero plugins: Grafana's native postgres datasource against `serve
:5439` (`icegres-postgres.yaml`). Grafana's generated time-series SQL is
plain SELECTs with `$__timeFilter`-expanded predicates — the thinnest
introspection footprint of any tool in this directory. Product-smoked
green (aggregate via `/api/ds/query` over live data).

## Notes for both lanes

- Use a read-only principal (`--authz-file`, `CanReadData`) — Grafana
  never needs writes.
- Dashboard refresh intervals multiply query load; pair high-refresh
  dashboards with `--freshness-ms` on the server (bounded staleness +
  result cache, `docs/bi-integration.md` §5) or point Grafana at a read
  replica / `db:ro` endpoint.
- Time travel: the Flight lane accepts the `"trips@<snapshot_id>"` form
  in raw SQL; `AS OF` sugar is pgwire-only.
