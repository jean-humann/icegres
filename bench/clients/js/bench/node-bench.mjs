// Node-side reference bench (no browser): how fast can a JS *backend* pull
// the same results from icegres? Compares the pure-JS grpc client, the native
// (Rust napi) Flight SQL client, and node-postgres over pgwire. This bounds
// what any proxy adds on top of the browser numbers.

import { performance } from "node:perf_hooks";
import "../lib/zstd-node.js";
import { tableFromIPC } from "apache-arrow";
import pgpkg from "pg";
import { connect, queryToIpcBuffer } from "../lib/flight.js";
import { createFlightSqlClient } from "@lakehouse-rs/flight-sql-client";

const FLIGHT_ADDR = process.env.ICEGRES_FLIGHT_ADDR || "127.0.0.1:50051";
const PG_URL =
  process.env.ICEGRES_PG || "postgres://bench:bench@127.0.0.1:5439/icegres";
const REPS = Number(process.env.BENCH_REPS || 7);

const SQLS = {
  "agg-8":
    "SELECT city, count(*) AS trips, avg(fare) AS avg_fare FROM demo.dash_trips GROUP BY city ORDER BY trips DESC LIMIT 8",
  "rows-10k":
    "SELECT trip_id, city, fare, distance_km, ts FROM demo.dash_trips ORDER BY trip_id LIMIT 10000",
  "rows-100k":
    "SELECT trip_id, city, fare, distance_km, ts FROM demo.dash_trips ORDER BY trip_id LIMIT 100000",
  "rows-1m":
    "SELECT trip_id, city, fare, distance_km, ts FROM demo.dash_trips ORDER BY trip_id LIMIT 1000000",
};

const grpcClient = connect(FLIGHT_ADDR);
const nativeClient = await createFlightSqlClient({
  host: FLIGHT_ADDR.split(":")[0],
  port: Number(FLIGHT_ADDR.split(":")[1]),
  tls: false,
  headers: [],
});
const pgPool = new pgpkg.Pool({ connectionString: PG_URL, max: 4 });

const LANES = {
  "flight-grpc-js": async (sql) => {
    const buf = await queryToIpcBuffer(grpcClient, sql);
    return tableFromIPC(buf).numRows;
  },
  "flight-native": async (sql) => {
    const buf = await nativeClient.query(sql);
    return tableFromIPC(buf).numRows;
  },
  "pg-wire": async (sql) => (await pgPool.query(sql)).rows.length,
};

async function bench(fn, sql) {
  const samples = [];
  let rows = 0;
  for (let i = 0; i < REPS + 2; i++) {
    const t0 = performance.now();
    rows = await fn(sql);
    const ms = performance.now() - t0;
    if (i >= 2) samples.push(ms);
  }
  samples.sort((a, b) => a - b);
  return { rows, median: samples[Math.floor(samples.length / 2)], min: samples[0] };
}

console.log("| case | lane | rows | median ms | min ms |");
console.log("|---|---|---|---|---|");
for (const [caseName, sql] of Object.entries(SQLS)) {
  for (const [lane, fn] of Object.entries(LANES)) {
    try {
      const r = await bench(fn, sql);
      console.log(
        `| ${caseName} | ${lane} | ${r.rows} | ${r.median.toFixed(1)} | ${r.min.toFixed(1)} |`,
      );
    } catch (e) {
      console.log(`| ${caseName} | ${lane} | ERROR ${String(e).slice(0, 60)} | | |`);
    }
  }
}
process.exit(0);
