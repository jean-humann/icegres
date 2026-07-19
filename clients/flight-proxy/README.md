# @icegres/flight-proxy

The **production-safe** way to put an icegres dashboard in the browser: a
backend-for-frontend (BFF) with a **named-query allowlist**. The browser
sends a query *name* and parameters — never SQL — and gets an **Arrow IPC
stream** back, so you keep the Arrow-end-to-end speed of the direct path
without exposing arbitrary SQL or database credentials to the page.

This is the recommended default. Use the direct
[`@icegres/flight-web`](../flight-web/) path only for fully trusted internal
dashboards; see [`docs/frontend-dashboards.md`](../../docs/frontend-dashboards.md)
for the tradeoff and the measured numbers (the proxy hop costs almost
nothing — the speed win is Arrow-vs-JSON, not topology).

## Why an allowlist and not raw SQL

The browser is an untrusted client. A raw-SQL endpoint is arbitrary query
execution against your warehouse. Here, every query the frontend can run is
declared server-side with typed parameters, and the surface is
**injection-proof by construction**:

- There is **no free-form string parameter**. Text filters are an `enum`
  whose allowed values are server-defined, so no untrusted string reaches
  SQL.
- `int` / `number` / `bool` are re-serialized from their coerced primitive
  (a metacharacter cannot survive).
- `date` is validated against a strict ISO regex before quoting.
- Undeclared parameters and unknown query names are rejected (fail closed).

## Define the queries

```js
// queries.js
export default {
  trips_by_city: {
    description: "Trip counts per city.",
    params: { limit: { type: "int", min: 1, max: 100, default: 10 } },
    sql: (p) =>
      `SELECT city, count(*) AS trips FROM demo.trips
       GROUP BY city ORDER BY trips DESC LIMIT ${p.limit}`,
  },
};
```

`sql(p)` receives only **validated literals** — the framework rejects
mismatched input before this runs.

## Run it

Standalone:

```bash
npx icegres-flight-proxy queries.js
# FLIGHT_ADDR=host:50051 FLIGHT_TLS=1 FLIGHT_USER=… FLIGHT_PASSWORD=… PORT=8090 CORS_ORIGIN=https://dash.example
```

Embedded in your app (any Node HTTP server; Express via a thin adapter):

```js
import { createHandler } from "@icegres/flight-proxy";
import queries from "./queries.js";

const handler = createHandler({
  queries,
  flight: { address: "lakehouse:50051", tls: true,
            credentials: { username: "svc", password: process.env.PW } },
  corsOrigin: "https://dash.example",
  authenticate: (req) => verifySession(req),          // → principal or null (401)
  authorize: (principal, name) => can(principal, name), // → false = 403
});
http.createServer((req, res) => handler(req, res)).listen(8090);
```

## Call it from the browser

```js
const res = await fetch("https://api.example/query", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ query: "trips_by_city", params: { limit: 5 } }),
});
const table = tableFromIPC(new Uint8Array(await res.arrayBuffer())); // apache-arrow
```

`GET /queries` returns the allowlist schema (names + parameter types, **no
SQL**) so a frontend can discover what it may run.

## Routes

| Method | Path | Body | Returns |
|---|---|---|---|
| `POST` | `/query` | `{ query, params? }` | Arrow IPC stream (`application/vnd.apache.arrow.stream`) |
| `GET` | `/queries` | — | allowlist schema (JSON, no SQL) |

Errors: `400` bad/unknown parameter, `401` unauthenticated, `403`
unauthorized for that query, `404` unknown query name.

## Notes

- The proxy is a **byte pass-through** for the Arrow stream — it never
  decodes results, so it has no `apache-arrow` dependency and adds no
  per-row cost. icegres's ZSTD batch compression rides through untouched;
  the browser registers the codec (importing `@icegres/flight-web` does it).
- Pair with icegres's resource limits (`--flight-statement-timeout-ms`,
  `--flight-max-result-bytes`, `--flight-max-concurrent-rpcs`) so a costly
  allowed query is still bounded.

## SQL explorer (arbitrary user queries)

A dashboard should never send raw SQL — but a **SQL explorer** is the opposite:
user-written queries are the feature. Use `createSqlGateway` (not the
allowlist). It is safe by *sandboxing the user*, not restricting the SQL:

```js
import { createSqlGateway } from "@icegres/flight-proxy";

const gateway = createSqlGateway({
  sessionSecret: process.env.SESSION_SECRET,
  // Your SSO: verify the app session -> the icegres principal to run as.
  authenticate: async (req) => {
    const user = await verifyAppSession(req);         // → null rejects (401)
    return user && { principal: user.dbRole, readOnly: true };
  },
  flight: { address: "lakehouse:50051", tls: true },
  // Optional: present each user's own icegres credential so authz scopes them.
  credentialFor: (principal) => lookupCred(principal),
  corsOrigin: "https://studio.example",
});
```

Flow: the browser `POST /session` (once) → a short-lived token; then
`POST /sql` `{ sql }` with `Authorization: Bearer <token>` → the Arrow result
streams back. The browser never holds a database credential.

**The safety model — four controls, none of which restrict the SQL text:**

1. **Per-user identity + authz.** Every query runs as the session's icegres
   principal; `--authz-file` scopes it to the tables that principal may read.
   **The authoritative read-only control** is the engine: run
   `flight-serve --read-only` (rejects all writes, no authz file needed), or
   grant the principal only `CanReadData` (a write fails with `42501`).
2. **Resource limits** — run icegres with `--flight-statement-timeout-ms`,
   `--flight-max-result-bytes`, and `--flight-max-concurrent-rpcs` so a
   runaway query is bounded, not fatal.
3. **Short-lived tokens** — the gateway brokers them; the browser holds no
   standing credential.
4. **Read-only guard** — the gateway also rejects obvious non-read statements
   (`enforceReadOnly`, default on) as defense in depth.

A runnable browser explorer (editor + Run/Stop with `AbortController` +
streamed Arrow results) is in `bench/clients/js/web/explorer.{html,js}`.
