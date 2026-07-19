// Dashboard demo: three dashboard-shaped queries rendered as stat tiles, a
// bar chart (magnitude), a line chart (change over time) and a table — all
// fetched through whichever data path is selected. Single-series charts, so
// slot-1 blue carries both; text stays in ink tokens per the dataviz rules.

import { PATHS } from "./paths.js";

const $ = (id) => document.getElementById(id);
const NS = "http://www.w3.org/2000/svg";
const fmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });

const QUERIES = {
  tiles:
    "SELECT count(*) AS trips, sum(fare) AS revenue, avg(distance_km) AS avg_km FROM demo.dash_trips",
  byCity:
    "SELECT city, count(*) AS trips FROM demo.dash_trips GROUP BY city ORDER BY trips DESC LIMIT 8",
  overTime:
    "SELECT day, avg(fare) AS avg_fare FROM (SELECT date_trunc('day', ts) AS day, fare FROM demo.dash_trips) GROUP BY day ORDER BY day",
  latest:
    "SELECT trip_id, city, fare, distance_km, ts FROM demo.dash_trips ORDER BY ts DESC LIMIT 8",
};

function el(tag, attrs, parent) {
  const node = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  parent.append(node);
  return node;
}

function accent() {
  return getComputedStyle(document.documentElement)
    .getPropertyValue("--accent")
    .trim();
}

function renderTiles(rows) {
  const r = rows[0] ?? {};
  $("tiles").innerHTML = ["trips", "revenue", "avg_km"]
    .map(
      (k) =>
        `<div class="tile"><div class="v">${fmt.format(Number(r[k] ?? 0))}</div><div class="l">${k}</div></div>`,
    )
    .join("");
}

function renderBars(rows) {
  const W = 360;
  const H = 24 * rows.length + 8;
  $("bars").innerHTML = "";
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: "100%" }, $("bars"));
  const max = Math.max(...rows.map((r) => Number(r.trips)), 1);
  rows.forEach((r, i) => {
    const w = Math.max((Number(r.trips) / max) * (W - 160), 2);
    const y = i * 24 + 4;
    const bar = el(
      "rect",
      { x: 90, y, width: w, height: 14, rx: 4, fill: accent() },
      svg,
    );
    const title = document.createElementNS(NS, "title");
    title.textContent = `${r.city}: ${fmt.format(Number(r.trips))} trips`;
    bar.append(title);
    el("text", { x: 86, y: y + 11, "text-anchor": "end" }, svg).textContent =
      r.city;
    el("text", { x: 94 + w, y: y + 11 }, svg).textContent = fmt.format(
      Number(r.trips),
    );
  });
}

function renderLine(rows) {
  const W = 320;
  const H = 140;
  const PAD = { l: 40, r: 8, t: 8, b: 20 };
  $("line").innerHTML = "";
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: "100%" }, $("line"));
  if (!rows.length) return;
  const ys = rows.map((r) => Number(r.avg_fare));
  const lo = Math.min(...ys);
  const hi = Math.max(...ys);
  const x = (i) => PAD.l + (i / Math.max(rows.length - 1, 1)) * (W - PAD.l - PAD.r);
  const y = (v) =>
    H - PAD.b - ((v - lo) / Math.max(hi - lo, 1e-9)) * (H - PAD.t - PAD.b);
  for (const v of [lo, hi]) {
    el("line", {
      x1: PAD.l, x2: W - PAD.r, y1: y(v), y2: y(v),
      stroke: "var(--grid)", "stroke-width": 1,
    }, svg);
    el("text", { x: PAD.l - 4, y: y(v) + 4, "text-anchor": "end" }, svg).textContent =
      fmt.format(v);
  }
  const d = rows.map((r, i) => `${i ? "L" : "M"}${x(i)},${y(Number(r.avg_fare))}`).join("");
  el("path", { d, fill: "none", stroke: accent(), "stroke-width": 2 }, svg);
  rows.forEach((r, i) => {
    const dot = el("circle", {
      cx: x(i), cy: y(Number(r.avg_fare)), r: 4, fill: accent(),
    }, svg);
    const title = document.createElementNS(NS, "title");
    const day = String(r.day).slice(0, 10);
    title.textContent = `${day}: ${fmt.format(Number(r.avg_fare))}`;
    dot.append(title);
  });
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
}

function fmtCell(col, v) {
  if (col === "ts") {
    // Arrow lanes yield epoch millis; the pg-json lane an ISO string.
    const d = typeof v === "number" ? new Date(v) : new Date(String(v));
    if (!Number.isNaN(d.getTime()))
      return d.toISOString().slice(0, 19).replace("T", " ");
  }
  return typeof v === "number" ? fmt.format(v) : String(v ?? "");
}

function renderTable(rows, cols) {
  const head = cols.map((c) => `<th>${esc(c)}</th>`).join("");
  const body = rows
    .map(
      (r) =>
        `<tr>${cols.map((c) => `<td>${esc(fmtCell(c, r[c]))}</td>`).join("")}</tr>`,
    )
    .join("");
  $("tbl").innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

let refreshSeq = 0;
async function refresh() {
  // Guard against overlapping refreshes (rapid clicks / path switches): only
  // the most recent call may write the DOM, so a slower stale response cannot
  // land after — and mislabel — a newer one.
  const seq = ++refreshSeq;
  const pathName = $("path").value;
  const fn = PATHS[pathName];
  $("status").textContent = "loading…";
  const t0 = performance.now();
  try {
    const [tiles, byCity, overTime, latest] = await Promise.all(
      [QUERIES.tiles, QUERIES.byCity, QUERIES.overTime, QUERIES.latest].map(fn),
    );
    if (seq !== refreshSeq) return; // superseded by a newer refresh
    renderTiles(tiles.rows);
    renderBars(byCity.rows);
    renderLine(overTime.rows);
    renderTable(latest.rows, latest.cols);
    const bytes = tiles.bytes + byCity.bytes + overTime.bytes + latest.bytes;
    $("status").textContent = `${pathName}: 4 queries in ${fmt.format(
      performance.now() - t0,
    )} ms, ${fmt.format(bytes / 1024)} KiB over the wire`;
  } catch (e) {
    if (seq !== refreshSeq) return;
    $("status").innerHTML = `<span class="err">${esc(e)}</span>`;
  }
}

$("refresh").addEventListener("click", refresh);
$("path").addEventListener("change", refresh);
refresh();
