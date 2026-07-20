# Superset ⇄ icegres

Two lanes, both through standard Superset database URIs.

## Fast lane — Flight SQL (`datafusion+flightsql://`)

Superset connects through SQLAlchemy; InfluxData's
[`flightsql-dbapi`](https://github.com/influxdata/flightsql-dbapi)
provides a DB-API 2 driver **and** a SQLAlchemy dialect whose primary
target is the **DataFusion** SQL dialect — exactly the engine icegres
runs. The full stack under this lane (DB-API, engine connect, schema /
table / column reflection — i.e. Superset's schema browser and SQL Lab
paths) is exercised by
[`bench/clients/a12_flightsql_dbapi_probe.py`](../../../bench/clients/a12_flightsql_dbapi_probe.py);
the Superset product itself is **by-construction** until a live smoke run
is recorded. One stack note: `flightsql-dbapi`'s cursor materializes
results through **pandas** without declaring it as a dependency — Superset
ships pandas, so nothing extra is needed there, but a bare install needs
`pip install flightsql-dbapi pandas`.

1. Add the driver to your Superset image — with the standard
   docker-compose setup, copy [`requirements-local.txt`](requirements-local.txt)
   to `docker/requirements-local.txt` and rebuild
   (`docker compose up --build`); on a bare install, `pip install
   flightsql-dbapi` into Superset's venv.
2. **Settings → Database Connections → + Database → Other**, SQLAlchemy URI:

   ```
   datafusion+flightsql://<user>:<password>@<icegres-host>:50051
   ```

   TLS is the default; against a dev listener without TLS append
   `?insecure=True`. Extra gRPC metadata can ride query params; a
   pre-minted bearer instead of user/password rides `?token=…`.
3. Test Connection → the schema browser lists icegres namespaces as
   schemas; SQL Lab queries stream Arrow under the hood.

Caveats on this lane:
- `AS OF` time-travel sugar is pgwire-only — in SQL Lab use the
  `"trips@<snapshot_id>"` table form (`docs/limitations.md`).
- Keep the Superset database's *Allow DML* off; enforce read-only
  server-side regardless (`flight-serve --read-only`, or a `CanReadData`
  principal in `--authz-file` — both statement-form based).

## Fallback lane — stock Postgres (proven driver stack, probe A8)

```
postgresql+psycopg2://<user>:<password>@<icegres-host>:5439/icegres
```

Works today with zero extra dependencies — SQLAlchemy reflection over the
pg_catalog shims is probe-verified (A8). Prefer it for small interactive
charts (the row drivers win below ~50k rows); prefer the Flight lane for
SQL Lab exports and big result sets.
