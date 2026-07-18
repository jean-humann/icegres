# JS frontend data-path probes & benchmark

Answers one question: **what is the best way for a browser dashboard to query
icegres?** Four end-to-end paths are implemented behind one interface
(`web/paths.js`) and measured inside real Chromium.

## The candidate paths

| Path | Browser wire | Who speaks to icegres | Endpoint |
|---|---|---|---|
| `arrow-proxy` | Arrow IPC stream | Node proxy → Flight SQL (`@grpc/grpc-js`, pure JS) | Flight `:50051` |
| `grpcweb-direct` | gRPC-web (FlightData frames) | **the browser itself** through a protocol translator | Flight `:50051` |
| `flight-json` | JSON | Node proxy → Flight SQL, rows flattened server-side | Flight `:50051` |
| `pg-json` | JSON | Node proxy → node-postgres | pgwire `:5439` |

Browsers cannot speak native gRPC (no trailer access); the `grpcweb-direct`
lane talks to `flight-serve --grpc-web`, which translates gRPC-web
in-process. All protobuf and Arrow work happens in the page via the
[`@icegres/flight-web`](../../../clients/flight-web/) client package.

## Layout

- `lib/flight.js` — Node Flight SQL client (GetFlightInfo → DoGet) over
  `@grpc/grpc-js` and `proto/flight.proto` (protobuf codec comes from
  `@icegres/flight-web/pb`).
- `proxy/server.js` — the thin backend: `/api/arrow`, `/api/flight-json`,
  `/api/pg-json` + static files. Port 8090.
- `web/dashboard.html` — demo dashboard with a data-path selector.
- `web/bench.html` + `web/bench-page.js` — in-browser benchmark harness.
- `bench/run.mjs` — Playwright driver (real Chromium), writes results JSON.
- `bench/node-bench.mjs` — Node-side reference lanes (`@grpc/grpc-js` vs the
  native `@lakehouse-rs/flight-sql-client` vs `pg`).
- `bench/seed_dash_trips.py` — seeds `demo.dash_trips` (1M rows) through the
  Flight bulk-ingest lane.

## Running

```bash
npm install --ignore-scripts
node build.mjs                       # bundle web/ -> dist/
python3 bench/seed_dash_trips.py     # once, stack must be up
node proxy/server.js &               # :8090  (flight-serve --grpc-web on :50051)
node bench/run.mjs                   # browser bench -> bench-results.json
node bench/node-bench.mjs            # backend reference lanes
```

Open `http://127.0.0.1:8090/` for the live dashboard.

Results and the recommendation live in `../../results/` and
`docs/frontend-dashboards.md`.
