// Integration tests against a live `icegres flight-serve --grpc-web`
// endpoint (ICEGRES_GRPCWEB_URL, default http://127.0.0.1:50051). Skipped
// when the endpoint is unreachable so `npm test` stays green offline; the
// bench/e2e harness runs them against the real stack.
import { test } from "node:test";
import assert from "node:assert/strict";
import "../src/zstd-node.js";
import { FlightWebClient, FlightError } from "../src/client.js";

const BASE = process.env.ICEGRES_GRPCWEB_URL || "http://127.0.0.1:50051";

async function reachable() {
  try {
    const client = new FlightWebClient({ baseUrl: BASE, retries: 0 });
    await client.query("SELECT 1 AS probe");
    return true;
  } catch {
    return false;
  }
}
const up = await reachable();

test("query returns a typed Arrow table", { skip: !up }, async () => {
  const client = new FlightWebClient({ baseUrl: BASE });
  const table = await client.query("SELECT 1 AS a, 'x' AS b, 2.5 AS c");
  assert.equal(table.numRows, 1);
  assert.deepEqual(
    table.schema.fields.map((f) => f.name),
    ["a", "b", "c"],
  );
  const row = table.get(0);
  assert.equal(Number(row.a), 1);
  assert.equal(row.b, "x");
  assert.equal(row.c, 2.5);
});

test("queryBatches surfaces batches progressively", { skip: !up }, async () => {
  const client = new FlightWebClient({ baseUrl: BASE });
  let rows = 0;
  const batches = await client.queryBatches(
    "SELECT trip_id, city FROM demo.trips ORDER BY trip_id",
    (batch) => {
      rows += batch.numRows;
    },
  );
  assert.ok(batches >= 1);
  assert.ok(rows >= 200, `expected the seeded demo.trips rows, got ${rows}`);
});

test("server errors surface as FlightError with grpc code", { skip: !up }, async () => {
  const client = new FlightWebClient({ baseUrl: BASE, retries: 0 });
  await assert.rejects(
    () => client.query("SELECT * FROM demo.definitely_missing"),
    (e) => e instanceof FlightError && e.code > 0,
  );
});

test("abort cancels an in-flight query", { skip: !up }, async () => {
  const client = new FlightWebClient({ baseUrl: BASE });
  const ctl = new AbortController();
  const pending = client.query("SELECT * FROM demo.trips", {
    signal: ctl.signal,
  });
  setTimeout(() => ctl.abort(), 30);
  await assert.rejects(pending, (e) => e.name === "AbortError");
});
