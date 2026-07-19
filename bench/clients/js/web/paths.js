// The four candidate frontend->icegres data paths, behind one uniform
// browser-side interface. Each returns { rows, cols, bytes, rowCount } where
// `bytes` is what actually crossed the wire to the browser and `rows` is the
// decoded, renderable result (array of row objects for charts/tables).

import "./zstd-web.js";
import { tableFromIPC } from "apache-arrow";
import { FlightWebClient } from "@icegres/flight-web";

// Native gRPC-web on the Flight port itself (flight-serve --grpc-web).
// The port is overridable via ?grpcwebPort= so the CI smoke gate can point
// the browser at its own test listener (defaults to the dev-stack 50051).
// Validate the port is purely numeric so a crafted ?grpcwebPort=80@evil.com
// cannot smuggle a userinfo host and repoint the Flight client off-origin.
const rawPort = new URLSearchParams(location.search).get("grpcwebPort");
const GRPCWEB_PORT = /^\d{1,5}$/.test(rawPort ?? "") ? rawPort : "50051";
const GRPCWEB_BASE = `http://${location.hostname}:${GRPCWEB_PORT}`;
const flightWeb = new FlightWebClient({ baseUrl: GRPCWEB_BASE });

function tableToRows(table) {
  const names = table.schema.fields.map((f) => f.name);
  const rows = new Array(table.numRows);
  let i = 0;
  for (const r of table) {
    const o = {};
    for (const n of names) {
      const v = r[n];
      o[n] = typeof v === "bigint" ? Number(v) : v;
    }
    rows[i++] = o;
  }
  return { rows, cols: names };
}

/** Flight SQL at the proxy, Arrow IPC on the browser wire, arrow-js decode. */
async function arrowProxy(sql) {
  const resp = await fetch(`/api/arrow?sql=${encodeURIComponent(sql)}`);
  if (!resp.ok) throw new Error(await resp.text());
  const buf = await resp.arrayBuffer();
  const table = tableFromIPC(new Uint8Array(buf));
  const { rows, cols } = tableToRows(table);
  return { rows, cols, bytes: buf.byteLength, rowCount: table.numRows };
}

/**
 * A JSON-over-fetch path: the proxy flattens the result (Flight or pgwire) to
 * JSON before the browser. Both such paths differ only by their endpoint.
 */
function jsonPath(endpoint) {
  return async function (sql) {
    const resp = await fetch(`${endpoint}?sql=${encodeURIComponent(sql)}`);
    if (!resp.ok) throw new Error(await resp.text());
    const buf = await resp.arrayBuffer();
    const rows = JSON.parse(new TextDecoder().decode(buf));
    // Measure the bytes that actually crossed the wire: with a gzip'd JSON
    // response fetch decompresses transparently, so buf.byteLength is the
    // UNCOMPRESSED size — use the proxy's x-wire-bytes (the encoded length)
    // so this is comparable to the Arrow lanes' compressed IPC byteLength.
    const wire = Number(resp.headers.get("x-wire-bytes")) || buf.byteLength;
    return {
      rows,
      cols: rows.length ? Object.keys(rows[0]) : [],
      bytes: wire,
      rowCount: rows.length,
    };
  };
}

/** Browser speaks Flight SQL itself over gRPC-web — no app backend. */
async function grpcWebDirect(sql) {
  const ipc = await flightWeb.queryIpc(sql);
  const table = tableFromIPC(ipc);
  const { rows, cols } = tableToRows(table);
  return { rows, cols, bytes: ipc.length, rowCount: table.numRows };
}

export const PATHS = {
  "arrow-proxy": arrowProxy,
  "flight-json": jsonPath("/api/flight-json"),
  "pg-json": jsonPath("/api/pg-json"),
  "grpcweb-direct": grpcWebDirect,
};
