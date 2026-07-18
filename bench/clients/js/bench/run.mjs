// Drives the in-browser benchmark with real Chromium via playwright-core.
// Assumes: icegres pgwire :5439 + flight :50051 up, proxy :8090 and grpc-web
// bridge :8091 running, dist/ bundled. Writes bench/results JSON + a markdown
// table to stdout.

import { chromium } from "playwright-core";
import { writeFile } from "node:fs/promises";

const CHROMIUM = process.env.CHROMIUM_PATH || "/opt/pw-browsers/chromium";
const BASE = process.env.BENCH_BASE || "http://127.0.0.1:8090";
const OUT = process.env.BENCH_OUT || "bench-results.json";
const REPS = Number(process.env.BENCH_REPS || 7);

const ALL_PATHS = ["arrow-proxy", "grpcweb-direct", "flight-json", "pg-json"];

const CASES = [
  {
    name: "agg-8",
    sql: "SELECT city, count(*) AS trips, avg(fare) AS avg_fare FROM demo.dash_trips GROUP BY city ORDER BY trips DESC LIMIT 8",
    paths: ALL_PATHS,
  },
  {
    name: "rows-1k",
    sql: "SELECT trip_id, city, fare, distance_km, ts FROM demo.dash_trips ORDER BY trip_id LIMIT 1000",
    paths: ALL_PATHS,
  },
  {
    name: "rows-10k",
    sql: "SELECT trip_id, city, fare, distance_km, ts FROM demo.dash_trips ORDER BY trip_id LIMIT 10000",
    paths: ALL_PATHS,
  },
  {
    name: "rows-100k",
    sql: "SELECT trip_id, city, fare, distance_km, ts FROM demo.dash_trips ORDER BY trip_id LIMIT 100000",
    paths: ALL_PATHS,
  },
  {
    name: "rows-1m",
    sql: "SELECT trip_id, city, fare, distance_km, ts FROM demo.dash_trips ORDER BY trip_id LIMIT 1000000",
    paths: ALL_PATHS,
  },
];

const browser = await chromium.launch({ executablePath: CHROMIUM });
const page = await browser.newPage();

// Optional network throttling via CDP: BENCH_NET="<mbps>,<latency_ms>".
// Localhost hides the payload-size difference between Arrow and JSON lanes;
// a 50 Mbit / 20 ms profile approximates a good office connection.
if (process.env.BENCH_NET) {
  const [mbps, latency] = process.env.BENCH_NET.split(",").map(Number);
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Network.enable");
  await cdp.send("Network.emulateNetworkConditions", {
    offline: false,
    latency: latency || 0,
    downloadThroughput: (mbps * 1024 * 1024) / 8,
    uploadThroughput: (mbps * 1024 * 1024) / 8,
  });
  console.log(`network throttled to ${mbps} Mbit/s, ${latency} ms latency`);
}
page.on("console", (m) => {
  if (m.type() === "error") console.error("page error:", m.text());
});
await page.goto(`${BASE}/web/bench.html`);
await page.waitForFunction(() => window.benchReady === true, { timeout: 15000 });

const results = await page.evaluate(
  ({ cases, reps }) => window.runBench({ cases, reps, warmup: 2 }),
  { cases: CASES, reps: REPS },
);

await browser.close();
await writeFile(OUT, JSON.stringify({ when: new Date().toISOString(), reps: REPS, results }, null, 2));

// Markdown summary.
const byCase = new Map();
for (const r of results) {
  if (!byCase.has(r.case)) byCase.set(r.case, []);
  byCase.get(r.case).push(r);
}
for (const [name, rows] of byCase) {
  console.log(`\n### ${name}`);
  console.log("| path | rows | wire KiB | median ms | min ms | max ms |");
  console.log("|---|---|---|---|---|---|");
  for (const r of rows) {
    if (r.error) {
      console.log(`| ${r.path} | — | — | ERROR: ${r.error.slice(0, 80)} | | |`);
    } else {
      console.log(
        `| ${r.path} | ${r.rowCount} | ${(r.bytes / 1024).toFixed(1)} | ${r.median_ms.toFixed(1)} | ${r.min_ms.toFixed(1)} | ${r.max_ms.toFixed(1)} |`,
      );
    }
  }
}
console.log(`\nresults written to ${OUT}`);
