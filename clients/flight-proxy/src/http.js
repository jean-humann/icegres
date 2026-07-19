// Shared HTTP plumbing for the two BFF entry points (server.js, gateway.js):
// JSON responses, bounded body reads, the CORS header block, the Arrow-IPC
// streaming response, and the standalone-server wrapper. Both handlers speak
// the same wire contract (Arrow end-to-end, JSON errors), so keeping this in
// one place stops them from drifting apart.

const ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

/** The CORS header block for a handler: fixed allow-headers, per-handler
 *  allowed methods and origin. */
export function corsHeaders(methods, origin) {
  return {
    "access-control-allow-methods": methods,
    "access-control-allow-headers": "content-type, authorization",
    "access-control-allow-origin": origin,
    vary: "Origin",
  };
}

export function sendJson(res, status, body, cors) {
  res.writeHead(status, { "content-type": "application/json", ...cors });
  res.end(JSON.stringify(body));
}

/** Read a request body as JSON, rejecting anything larger than `limit` bytes. */
export async function readJson(req, limit = 64 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > limit) throw new Error("request body too large");
    chunks.push(c);
  }
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
}

/** Map a gRPC (Flight) error to an HTTP status; default 500. Used to turn a
 *  pre-stream query failure into a meaningful response instead of a 200. */
function grpcStatusToHttp(code) {
  switch (code) {
    case 3: // INVALID_ARGUMENT — bad SQL / planning error
      return 400;
    case 16: // UNAUTHENTICATED
      return 401;
    case 7: // PERMISSION_DENIED
      return 403;
    case 5: // NOT_FOUND
      return 404;
    case 8: // RESOURCE_EXHAUSTED — result cap / concurrency
      return 429;
    case 4: // DEADLINE_EXCEEDED — statement timeout
      return 504;
    case 14: // UNAVAILABLE
      return 503;
    default:
      return 500;
  }
}

/**
 * Stream a Flight SQL result to the browser as Arrow IPC, untouched. `run` is
 * invoked with a per-chunk writer.
 *
 * The 200 header is deferred until the FIRST chunk arrives, so a query that
 * fails before any bytes (the common case: a SQL syntax error surfaced at
 * GetFlightInfo) returns a real JSON error with a mapped status — not a
 * 200-then-truncated stream the browser reports as a network error. Once
 * streaming has begun the status cannot change, so a mid-stream failure is
 * reported via `onError` and the socket destroyed to signal truncation.
 */
export async function streamArrow(res, cors, run, onError) {
  let started = false;
  const begin = () => {
    if (started) return;
    started = true;
    res.writeHead(200, {
      "content-type": ARROW_CONTENT_TYPE,
      "cache-control": "no-store",
      ...cors,
    });
  };
  try {
    await run((chunk) => {
      begin();
      res.write(chunk);
    });
    begin(); // a zero-chunk result still completes as an (empty) 200 stream
    res.end();
  } catch (e) {
    onError(e);
    if (started) {
      res.destroy(e);
    } else {
      const status = grpcStatusToHttp(e && e.code);
      sendJson(res, status, { error: (e && e.message) || String(e) }, cors);
    }
  }
}

/** Start a standalone http server around `handler`, 500-ing uncaught errors. */
export async function serveHandler(handler, { port, host }) {
  const http = await import("node:http");
  const server = http.createServer((req, res) =>
    handler(req, res).catch((e) => {
      if (!res.headersSent) sendJson(res, 500, { error: String(e) }, {});
      else res.destroy(e);
    }),
  );
  await new Promise((resolve) => server.listen(port, host, resolve));
  return server;
}
