# Superset ⇄ icegres

Two lanes, both through standard Superset database URIs.

## Fast lane — Flight SQL (`datafusion://`) — product-smoked GREEN

Superset connects through SQLAlchemy; InfluxData's
[`flightsql-dbapi`](https://github.com/influxdata/flightsql-dbapi)
provides a DB-API 2 driver **and** a SQLAlchemy dialect whose primary
target is the **DataFusion** SQL dialect — exactly the engine icegres
runs. Verified at two levels: the driver stack by
[`bench/clients/a13_flightsql_dbapi_probe.py`](../../../bench/clients/a13_flightsql_dbapi_probe.py)
(reflection, SQL-Lab shapes, auth — all green), and **the Superset
product itself smoke-run against a live `flight-serve`**: Test
Connection OK, database created, SQL Lab executed aggregates, and the
schema/table browsers listed icegres namespaces — all through Superset's
own REST API.

Two stack notes found during that run:

- **URI scheme is `datafusion://`**, not `datafusion+flightsql://`. The
  package's SQLAlchemy entry point registers the dialect under the name
  `datafusion`; the `+flightsql` form resolves only after a manual
  `import flightsql.sqlalchemy`, which Superset never performs.
- `flightsql-dbapi` has one **undeclared runtime dependency**: `pandas`
  (its cursor materializes through it — every Superset image ships
  pandas). The recorded smoke also had to add `cachetools` — imported by
  **Superset's own** engine-spec machinery during Test Connection and
  missing from that bare pip install — so
  [`requirements-local.txt`](requirements-local.txt) carries it too.
  (`flightsql-dbapi` also pins `sqlalchemy<2.0`, matching Superset's own
  pin.)

1. Add the driver to your Superset image — with the standard
   docker-compose setup, copy [`requirements-local.txt`](requirements-local.txt)
   to `docker/requirements-local.txt` and rebuild
   (`docker compose up --build`); on a bare install, `pip install
   flightsql-dbapi cachetools` into Superset's venv.
2. **Settings → Database Connections → + Database → Other**, SQLAlchemy URI:

   ```
   datafusion://<user>:<password>@<icegres-host>:50051
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
