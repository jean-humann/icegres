# @icegres/flight-web

Query icegres from the browser with results staying **Arrow end-to-end** —
no app backend, no JSON conversion. Speaks Arrow Flight SQL over gRPC-web
against `icegres flight-serve --grpc-web` (or the same service behind an
Envoy `grpc_web` filter). Measured against the JSON-over-proxy alternative:
~2.5–2.8× faster end-to-end at 100k–1M rows and 5× less wire traffic
([docs/frontend-dashboards.md](../../docs/frontend-dashboards.md)).

```js
import { FlightWebClient } from "@icegres/flight-web";

const db = new FlightWebClient({
  baseUrl: "https://lakehouse.example:50051",
  credentials: { username: "dash", password: "…" },   // server --auth-file
});

// Whole result as an apache-arrow Table
const table = await db.query(
  "SELECT city, count(*) trips FROM demo.trips GROUP BY city",
);

// Progressive rendering: batches surface as they arrive off the wire
await db.queryBatches("SELECT * FROM demo.trips", (batch, i) => {
  chart.append(batch);
});

// Cancellation
const ctl = new AbortController();
db.query("SELECT …", { signal: ctl.signal });
ctl.abort();
```

## Server prerequisites

```bash
icegres flight-serve --grpc-web \
  --tls-cert cert.pem --tls-key key.pem \
  --auth-file users.txt --cors-origin https://dash.example
```

- `--grpc-web` makes the Flight port answer browsers directly; native gRPC
  clients are unaffected.
- Auth is a per-RPC `authorization: Basic …` header (gRPC-web cannot carry
  the Handshake RPC). Always pair credentials with TLS.
- Pin `--cors-origin` to your dashboard origin in production.

## Notes

- **ZSTD**: importing the package registers a browser codec (fzstd) for
  icegres's compressed result batches. Node backends: import
  `@icegres/flight-web/zstd-node` for the native codec, or run the server
  with `--result-compression none`.
- **Types**: int64 arrives as BigInt, timestamps as epoch millis. Prefer
  DOUBLE over DECIMAL in dashboard queries — arrow-js renders decimal128
  poorly.
- **Retries** apply only to `GetFlightInfo` (idempotent, result-free);
  a `DoGet` failing mid-stream surfaces as an error, never a silent re-run.
- **Security**: this client sends raw SQL. Expose it to internal dashboards
  behind auth + `--authz-file` read scopes; for public apps put an
  allowlisting proxy in front instead
  (see `bench/clients/js/proxy/server.js` for the shape).
