// The "thin backend" of the frontend data-path bench: one HTTP server that
// serves the static dashboard/bench pages plus three query endpoints, each a
// different frontend->icegres data path:
//
//   GET /api/arrow?sql=...        Flight SQL -> Arrow IPC stream, streamed to
//                                 the browser as-is (Arrow end to end).
//   GET /api/flight-json?sql=...  Flight SQL -> rows decoded in the proxy ->
//                                 JSON (isolates transport vs format cost).
//   GET /api/pg-json?sql=...      pgwire (node-postgres) -> JSON (the
//                                 classic REST-over-Postgres shape).
//
// Env: ICEGRES_FLIGHT_ADDR (default 127.0.0.1:50051),
//      ICEGRES_PG (default postgres://bench:bench@127.0.0.1:5439/icegres),
//      PORT (default 8090).

import http from "node:http";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "../lib/zstd-node.js";
import { tableFromIPC } from "apache-arrow";
import pgpkg from "pg";
import { connect, queryToIpc, queryToIpcBuffer } from "../lib/flight.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const PORT = Number(process.env.PORT || 8090);
const FLIGHT_ADDR = process.env.ICEGRES_FLIGHT_ADDR || "127.0.0.1:50051";
const PG_URL =
  process.env.ICEGRES_PG || "postgres://bench:bench@127.0.0.1:5439/icegres";

const flight = connect(FLIGHT_ADDR);
const pgPool = new pgpkg.Pool({ connectionString: PG_URL, max: 8 });

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
};

/** Arrow Table -> array of plain-object rows (for the flight-json lane). */
function tableToRows(table) {
  const rows = new Array(table.numRows);
  const names = table.schema.fields.map((f) => f.name);
  let i = 0;
  for (const row of table) {
    const o = {};
    for (const n of names) {
      const v = row[n];
      o[n] = typeof v === "bigint" ? Number(v) : v;
    }
    rows[i++] = o;
  }
  return rows;
}

async function handleApi(req, res, url) {
  const sql = url.searchParams.get("sql");
  if (!sql) {
    res.writeHead(400).end("missing ?sql=");
    return;
  }
  const route = url.pathname;
  if (route === "/api/arrow") {
    res.writeHead(200, {
      "content-type": "application/vnd.apache.arrow.stream",
      "cache-control": "no-store",
    });
    await queryToIpc(flight, sql, (chunk) => res.write(chunk));
    res.end();
  } else if (route === "/api/flight-json") {
    const buf = await queryToIpcBuffer(flight, sql);
    const rows = tableToRows(tableFromIPC(buf));
    res.writeHead(200, {
      "content-type": "application/json",
      "cache-control": "no-store",
    });
    res.end(JSON.stringify(rows));
  } else if (route === "/api/pg-json") {
    const out = await pgPool.query(sql);
    res.writeHead(200, {
      "content-type": "application/json",
      "cache-control": "no-store",
    });
    res.end(JSON.stringify(out.rows));
  } else {
    res.writeHead(404).end("unknown api route");
  }
}

async function handleStatic(req, res, url) {
  let rel = url.pathname === "/" ? "/web/dashboard.html" : url.pathname;
  const file = path.normalize(path.join(ROOT, rel));
  if (!file.startsWith(ROOT)) {
    res.writeHead(403).end();
    return;
  }
  try {
    const body = await readFile(file);
    res.writeHead(200, {
      "content-type": MIME[path.extname(file)] || "application/octet-stream",
      "cache-control": "no-store",
    });
    res.end(body);
  } catch {
    res.writeHead(404).end("not found");
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  try {
    if (url.pathname.startsWith("/api/")) await handleApi(req, res, url);
    else await handleStatic(req, res, url);
  } catch (e) {
    // Surface backend errors to the bench page rather than hanging it.
    if (!res.headersSent) res.writeHead(500, { "content-type": "text/plain" });
    res.end(`error: ${e.message}`);
  }
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`proxy listening on http://127.0.0.1:${PORT}`);
  console.log(`  flight: ${FLIGHT_ADDR}   pg: ${PG_URL}`);
});
