# Grafana ⇄ icegres

Provisioning files for both lanes are under
[`provisioning/datasources/`](provisioning/datasources/) — drop them into
Grafana's `conf/provisioning/datasources/` (or mount them in the
container) and set `ICEGRES_BI_PASSWORD` in Grafana's environment.

## Lane 1 — FlightSQL datasource plugin (columnar, honest status: archived)

InfluxData's `influxdata-flightsql-datasource` speaks Flight SQL directly
(query builder + raw SQL editor, basic-auth/token/TLS). It is signed and
installable from the Grafana catalog:

```bash
grafana-cli plugins install influxdata-flightsql-datasource
```

**Honest status:** upstream archived the plugin at v1.1.1 (April 2024,
"not under active development") — it works, but expect no fixes. Treat
this lane as best-effort acceleration for heavy panels and keep the
Postgres lane provisioned alongside as the supported path. Verification:
**unverified** against icegres (the plugin embeds its own Go Flight SQL
client; no probe covers it) — validate your dashboards on a dev Grafana
before rollout.

## Lane 2 — built-in PostgreSQL datasource (the supported fallback)

Zero plugins: Grafana's native postgres datasource against `serve
:5439` (`icegres-postgres.yaml`). Grafana's generated time-series SQL is
plain SELECTs with `$__timeFilter`-expanded predicates — the thinnest
introspection footprint of any tool in this directory
(**by-construction**; Go pgx has no dedicated probe yet).

## Notes for both lanes

- Use a read-only principal (`--authz-file`, `CanReadData`) — Grafana
  never needs writes.
- Dashboard refresh intervals multiply query load; pair high-refresh
  dashboards with `--freshness-ms` on the server (bounded staleness +
  result cache, `docs/bi-integration.md` §5) or point Grafana at a read
  replica / `db:ro` endpoint.
- Time travel: the Flight lane accepts the `"trips@<snapshot_id>"` form
  in raw SQL; `AS OF` sugar is pgwire-only.
