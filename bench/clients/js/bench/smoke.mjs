// Browser smoke test (CI gate, not a benchmark): drive every frontend data
// path once in real Chromium against a live icegres, and ASSERT each returns
// the right shape — so a regression in the gRPC-web client, the Arrow decode,
// the zstd codec, or the proxy fails the build. Exits non-zero on any failure.
//
// Env: BENCH_BASE (proxy, default http://127.0.0.1:8090), CHROMIUM_PATH.
import { chromium } from "playwright-core";

const CHROMIUM = process.env.CHROMIUM_PATH || "/opt/pw-browsers/chromium";
const BASE = process.env.BENCH_BASE || "http://127.0.0.1:8090";

// Small, deterministic cases against the demo seed (present after `icegres
// seed`). Every lane must return the same row count for the same query.
const CASES = [
  { name: "agg", sql: "SELECT city, count(*) AS n FROM demo.trips GROUP BY city", minRows: 1 },
  { name: "rows", sql: "SELECT trip_id, city FROM demo.trips ORDER BY trip_id LIMIT 50", rows: 50 },
];
// The Arrow + Flight lanes are the feature under test; pg-json is a JSON
// baseline that needs a pgwire listener, which may not be part of every
// browser-gate stack — treat its connection failure as a skip, not a gate
// failure (a WRONG answer from it would still fail the cross-lane check).
const CORE_PATHS = ["arrow-proxy", "grpcweb-direct", "flight-json"];
const PATHS = [...CORE_PATHS, "pg-json"];

const browser = await chromium.launch({ executablePath: CHROMIUM });
const page = await browser.newPage();
const pageErrors = [];
page.on("pageerror", (e) => pageErrors.push(String(e)));
const grpcwebPort = process.env.GRPCWEB_PORT || "50051";
await page.goto(`${BASE}/web/bench.html?grpcwebPort=${grpcwebPort}`);
await page.waitForFunction(() => window.benchReady === true, { timeout: 20000 });

const results = await page.evaluate(
  ({ cases, paths }) =>
    window.runBench({ cases: cases.map((c) => ({ ...c, paths })), reps: 1, warmup: 0 }),
  { cases: CASES, paths: PATHS },
);
await browser.close();

let failed = 0;
const fail = (m) => {
  console.error(`FAIL ${m}`);
  failed++;
};
const ok = (m) => console.log(`ok  ${m}`);

if (pageErrors.length) fail(`page errors: ${pageErrors.join("; ")}`);

// Group results by case so lanes can be cross-checked against each other.
const byCase = new Map();
for (const r of results) {
  if (!byCase.has(r.case)) byCase.set(r.case, []);
  byCase.get(r.case).push(r);
}

for (const c of CASES) {
  const rows = byCase.get(c.name) || [];
  for (const path of PATHS) {
    const r = rows.find((x) => x.path === path);
    if (!r) { fail(`${c.name}/${path}: no result`); continue; }
    if (r.error) {
      if (path === "pg-json" && /ECONNREFUSED|5439/.test(r.error)) {
        console.log(`skip ${c.name}/pg-json: no pgwire listener`);
        continue;
      }
      fail(`${c.name}/${path}: ${r.error}`);
      continue;
    }
    if (c.rows != null && r.rowCount !== c.rows) {
      fail(`${c.name}/${path}: expected ${c.rows} rows, got ${r.rowCount}`);
      continue;
    }
    if (c.minRows != null && r.rowCount < c.minRows) {
      fail(`${c.name}/${path}: expected >= ${c.minRows} rows, got ${r.rowCount}`);
      continue;
    }
    if (!(r.bytes > 0)) { fail(`${c.name}/${path}: zero wire bytes`); continue; }
    ok(`${c.name}/${path}: ${r.rowCount} rows, ${r.bytes} B`);
  }
  // The Arrow and JSON lanes must AGREE on row count for the same query —
  // catches a decode that silently drops or duplicates rows.
  const counts = new Set(rows.filter((r) => !r.error).map((r) => r.rowCount));
  if (counts.size > 1) fail(`${c.name}: lanes disagree on row count: ${[...counts]}`);
  else ok(`${c.name}: all lanes agree on row count`);
}

console.log(failed ? `\nsmoke: ${failed} failure(s)` : "\nsmoke: all lanes correct");
process.exit(failed ? 1 : 0);
