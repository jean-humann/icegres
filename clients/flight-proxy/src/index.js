// @icegres/flight-proxy — the backend-for-frontend for icegres dashboards.
//
// Browser dashboards should not hold raw SQL or database credentials. This
// package is the production-safe shape: a named-query allowlist that streams
// Arrow end-to-end, so the frontend keeps the Arrow speed of the direct path
// without the arbitrary-SQL exposure. Pair it with @icegres/flight-web on the
// browser (point the client at this proxy) or plain `fetch` +
// `tableFromIPC`.
export { createHandler, serve } from "./server.js";
export { resolveQuery, describeRegistry, ParamError } from "./allowlist.js";
