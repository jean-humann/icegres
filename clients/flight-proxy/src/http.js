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

/**
 * Stream a Flight SQL result to the browser as Arrow IPC, untouched. `run` is
 * invoked with a per-chunk writer. On a mid-stream failure the response
 * headers are already sent, so the status cannot change: report via `onError`
 * and destroy the socket to signal an incomplete stream.
 */
export async function streamArrow(res, cors, run, onError) {
  res.writeHead(200, {
    "content-type": ARROW_CONTENT_TYPE,
    "cache-control": "no-store",
    ...cors,
  });
  try {
    await run((chunk) => res.write(chunk));
    res.end();
  } catch (e) {
    onError(e);
    res.destroy(e);
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
