// The BFF request handler: browser -> named query -> Arrow IPC stream.
//
// Exposes exactly two routes and nothing else:
//   POST /query    { query: "<name>", params: {...} }  -> Arrow IPC stream
//   GET  /queries                                       -> allowlist schema (no SQL)
//
// There is deliberately no raw-SQL path. Auth and authorization are hooks the
// host app supplies; the handler is embeddable in any Node HTTP server
// (http.createServer, Express via a thin adapter, etc.) or run standalone.

import { connect, queryToIpc } from "./flight.js";
import { resolveQuery, describeRegistry, ParamError } from "./allowlist.js";
import {
  corsHeaders,
  sendJson,
  readJson,
  streamArrow,
  serveHandler,
} from "./http.js";

/**
 * Build an (req, res) handler.
 * @param {object} config
 * @param {Record<string, object>} config.queries  the named-query registry
 * @param {object} [config.flight]  { address, tls, credentials } for icegres
 * @param {string} [config.corsOrigin="*"]
 * @param {(req) => Promise<unknown|null>} [config.authenticate]  return a
 *   principal, or null to reject with 401. Omit to leave the endpoint open
 *   (only sensible on a trusted network).
 * @param {(principal, queryName) => boolean|Promise<boolean>} [config.authorize]
 *   per-query gate; return false to reject with 403.
 */
export function createHandler(config) {
  const {
    queries,
    flight = {},
    corsOrigin = "*",
    authenticate,
    authorize,
  } = config;
  if (!queries || typeof queries !== "object") {
    throw new Error("createHandler requires a { queries } registry");
  }
  const conn = connect(flight);
  const cors = corsHeaders("GET, POST, OPTIONS", corsOrigin);

  return async function handler(req, res) {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    if (req.method === "OPTIONS") {
      res.writeHead(204, cors);
      res.end();
      return;
    }

    // Auth (optional hook) runs BEFORE routing, so it gates the query
    // catalogue (GET /queries) as well as execution (POST /query) — a
    // configured authenticate should not leak the allowlist to anonymous
    // callers. A thrown/null result is a hard reject.
    let principal = null;
    if (authenticate) {
      try {
        principal = await authenticate(req);
      } catch {
        principal = null;
      }
      if (principal == null) {
        sendJson(res, 401, { error: "unauthorized" }, cors);
        return;
      }
    }

    // GET /queries — the allowlist schema, so a frontend can discover the
    // available queries and their parameters. Never exposes SQL.
    if (req.method === "GET" && url.pathname === "/queries") {
      sendJson(res, 200, describeRegistry(queries), cors);
      return;
    }

    if (req.method !== "POST" || url.pathname !== "/query") {
      sendJson(res, 404, { error: "not found" }, cors);
      return;
    }

    let body;
    try {
      body = await readJson(req);
    } catch (e) {
      sendJson(res, 400, { error: e.message }, cors);
      return;
    }
    const name = body?.query;
    if (typeof name !== "string") {
      sendJson(res, 400, { error: "body must be { query: string, params?: object }" }, cors);
      return;
    }

    if (authorize && !(await authorize(principal, name))) {
      sendJson(res, 403, { error: `not permitted to run "${name}"` }, cors);
      return;
    }

    let sql;
    try {
      ({ sql } = resolveQuery(queries, name, body.params ?? {}));
    } catch (e) {
      if (e.code === "UNKNOWN_QUERY") {
        sendJson(res, 404, { error: e.message }, cors);
      } else if (e instanceof ParamError) {
        sendJson(res, 400, { error: e.message }, cors);
      } else {
        sendJson(res, 500, { error: "query resolution failed" }, cors);
      }
      return;
    }

    // Stream the Arrow IPC result straight to the browser, untouched — the
    // whole point: Arrow end to end, no JSON re-encode.
    await streamArrow(
      res,
      cors,
      (write, signal) => queryToIpc(conn, sql, write, { signal }),
      // eslint-disable-next-line no-console
      (e) => console.error(`query "${name}" failed:`, e.message),
    );
  };
}

/** Convenience: start a standalone http server with the handler. */
export function serve(config, { port = 8090, host = "127.0.0.1" } = {}) {
  return serveHandler(createHandler(config), { port, host });
}
