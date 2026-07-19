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

const CORS_BASE = {
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers": "content-type, authorization",
  vary: "Origin",
};

function sendJson(res, status, body, cors) {
  res.writeHead(status, { "content-type": "application/json", ...cors });
  res.end(JSON.stringify(body));
}

async function readJson(req, limitBytes = 64 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > limitBytes) throw new Error("request body too large");
    chunks.push(c);
  }
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

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

  return async function handler(req, res) {
    const cors = { ...CORS_BASE, "access-control-allow-origin": corsOrigin };
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    if (req.method === "OPTIONS") {
      res.writeHead(204, cors);
      res.end();
      return;
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

    // Auth (optional hook). A thrown/false result is a hard reject.
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
    res.writeHead(200, {
      "content-type": "application/vnd.apache.arrow.stream",
      "cache-control": "no-store",
      ...cors,
    });
    try {
      await queryToIpc(conn, sql, (chunk) => res.write(chunk));
      res.end();
    } catch (e) {
      // Headers are already sent (streaming): the browser will see a
      // truncated Arrow stream; also log server-side. We cannot change the
      // status now, so destroy the socket to signal an incomplete response.
      // eslint-disable-next-line no-console
      console.error(`query "${name}" failed mid-stream:`, e.message);
      res.destroy(e);
    }
  };
}

/** Convenience: start a standalone http server with the handler. */
export async function serve(config, { port = 8090, host = "127.0.0.1" } = {}) {
  const http = await import("node:http");
  const handler = createHandler(config);
  const server = http.createServer((req, res) => {
    handler(req, res).catch((e) => {
      if (!res.headersSent) {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: String(e) }));
      } else {
        res.destroy(e);
      }
    });
  });
  await new Promise((resolve) => server.listen(port, host, resolve));
  return server;
}
