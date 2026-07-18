// Benchmark driver that runs INSIDE Chromium. Playwright (bench/run.mjs)
// calls window.runBench with a matrix of {path, sql, reps}; timings use
// performance.now() so they measure exactly what a dashboard user would see:
// request + transfer + decode into renderable rows.

import { PATHS } from "./paths.js";

async function runCase(pathName, sql, reps, warmup) {
  const fn = PATHS[pathName];
  const samples = [];
  let meta = null;
  for (let i = 0; i < warmup + reps; i++) {
    const t0 = performance.now();
    const out = await fn(sql);
    const ms = performance.now() - t0;
    if (i >= warmup) samples.push(ms);
    meta = { bytes: out.bytes, rowCount: out.rowCount, cols: out.cols.length };
  }
  samples.sort((a, b) => a - b);
  const sum = samples.reduce((a, b) => a + b, 0);
  return {
    path: pathName,
    ...meta,
    reps: samples.length,
    min_ms: samples[0],
    median_ms: samples[Math.floor(samples.length / 2)],
    mean_ms: sum / samples.length,
    max_ms: samples[samples.length - 1],
  };
}

window.runBench = async function runBench({ cases, reps = 5, warmup = 1 }) {
  const results = [];
  for (const c of cases) {
    for (const pathName of c.paths) {
      try {
        const r = await runCase(pathName, c.sql, reps, warmup);
        results.push({ case: c.name, ...r });
      } catch (e) {
        results.push({ case: c.name, path: pathName, error: String(e) });
      }
      document.getElementById("log").textContent = JSON.stringify(
        results[results.length - 1],
      );
    }
  }
  return results;
};

document.getElementById("log").textContent = "bench harness ready";
window.benchReady = true;
