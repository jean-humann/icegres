// The four candidate frontend->icegres data paths, behind one uniform
// browser-side interface. Each returns { rows, cols, bytes, rowCount } where
// `bytes` is what actually crossed the wire to the browser and `rows` is the
// decoded, renderable result (array of row objects for charts/tables).

import "./zstd-web.js";
import { tableFromIPC } from "apache-arrow";
import { flightQuery } from "./flight-web.js";

const GRPCWEB_BASE = `http://${location.hostname}:8091`;

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

/** Flight SQL at the proxy, but flattened to JSON before the browser. */
async function flightJson(sql) {
  const resp = await fetch(`/api/flight-json?sql=${encodeURIComponent(sql)}`);
  if (!resp.ok) throw new Error(await resp.text());
  const text = await resp.text();
  const rows = JSON.parse(text);
  return {
    rows,
    cols: rows.length ? Object.keys(rows[0]) : [],
    bytes: text.length,
    rowCount: rows.length,
  };
}

/** pgwire (node-postgres) at the proxy, JSON on the browser wire. */
async function pgJson(sql) {
  const resp = await fetch(`/api/pg-json?sql=${encodeURIComponent(sql)}`);
  if (!resp.ok) throw new Error(await resp.text());
  const text = await resp.text();
  const rows = JSON.parse(text);
  return {
    rows,
    cols: rows.length ? Object.keys(rows[0]) : [],
    bytes: text.length,
    rowCount: rows.length,
  };
}

/** Browser speaks Flight SQL itself over gRPC-web — no app backend. */
async function grpcWebDirect(sql) {
  const { table, ipcBytes } = await flightQuery(GRPCWEB_BASE, sql);
  const { rows, cols } = tableToRows(table);
  return { rows, cols, bytes: ipcBytes, rowCount: table.numRows };
}

export const PATHS = {
  "arrow-proxy": arrowProxy,
  "flight-json": flightJson,
  "pg-json": pgJson,
  "grpcweb-direct": grpcWebDirect,
};
