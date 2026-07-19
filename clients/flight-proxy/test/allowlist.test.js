// The security core: prove the allowlist rejects everything a browser must
// not be able to do, and that no untrusted input reaches SQL as raw text.
import { test } from "node:test";
import assert from "node:assert/strict";
import { resolveQuery, describeRegistry, ParamError } from "../src/allowlist.js";

const REGISTRY = {
  trips_by_city: {
    description: "trip counts per city since a date",
    params: {
      since: { type: "date" },
      limit: { type: "int", min: 1, max: 100, default: 10 },
    },
    sql: (p) =>
      `SELECT city, count(*) AS trips FROM demo.trips WHERE ts >= ${p.since} ` +
      `GROUP BY city ORDER BY trips DESC LIMIT ${p.limit}`,
  },
  by_city_name: {
    params: { city: { type: "enum", values: ["paris", "lyon", "nice"] } },
    sql: (p) => `SELECT * FROM demo.trips WHERE city = ${p.city}`,
  },
  flag: {
    params: { active: { type: "bool", default: true } },
    sql: (p) => `SELECT * FROM demo.trips WHERE active = ${p.active}`,
  },
};

test("valid query resolves with validated literals", () => {
  const { sql } = resolveQuery(REGISTRY, "trips_by_city", { since: "2026-06-01", limit: 5 });
  assert.match(sql, /ts >= '2026-06-01'/);
  assert.match(sql, /LIMIT 5$/);
});

test("defaults apply when a param is omitted", () => {
  const { sql } = resolveQuery(REGISTRY, "trips_by_city", { since: "2026-06-01" });
  assert.match(sql, /LIMIT 10$/);
});

test("unknown query name is rejected", () => {
  assert.throws(() => resolveQuery(REGISTRY, "drop_everything", {}), /unknown query/);
});

test("undeclared parameter is rejected (fail closed)", () => {
  assert.throws(
    () => resolveQuery(REGISTRY, "flag", { active: true, evil: 1 }),
    (e) => e instanceof ParamError && /unexpected parameter/.test(e.message),
  );
});

test("SQL injection via a date param cannot pass the validator", () => {
  for (const bad of [
    "2026-06-01'; DROP TABLE demo.trips; --",
    "2026-06-01 OR 1=1",
    "'; DELETE FROM demo.trips; --",
    "2026-06-01`",
  ]) {
    assert.throws(
      () => resolveQuery(REGISTRY, "trips_by_city", { since: bad }),
      (e) => e instanceof ParamError,
      `date "${bad}" must be rejected`,
    );
  }
});

test("int param coerces and range-checks; non-integers rejected", () => {
  assert.throws(() => resolveQuery(REGISTRY, "trips_by_city", { since: "2026-06-01", limit: 0 }), ParamError);
  assert.throws(() => resolveQuery(REGISTRY, "trips_by_city", { since: "2026-06-01", limit: 101 }), ParamError);
  assert.throws(
    () => resolveQuery(REGISTRY, "trips_by_city", { since: "2026-06-01", limit: "5); DROP--" }),
    ParamError,
  );
  const { sql } = resolveQuery(REGISTRY, "trips_by_city", { since: "2026-06-01", limit: "42" });
  assert.match(sql, /LIMIT 42$/); // numeric string coerces to the number literal
});

test("enum only admits configured values; arbitrary strings rejected", () => {
  const { sql } = resolveQuery(REGISTRY, "by_city_name", { city: "lyon" });
  assert.match(sql, /city = 'lyon'/);
  assert.throws(
    () => resolveQuery(REGISTRY, "by_city_name", { city: "paris' OR '1'='1" }),
    ParamError,
  );
});

test("bool param emits only TRUE/FALSE, never raw input", () => {
  assert.match(resolveQuery(REGISTRY, "flag", { active: false }).sql, /active = FALSE/);
  assert.throws(() => resolveQuery(REGISTRY, "flag", { active: "TRUE; DROP" }), ParamError);
});

test("describeRegistry exposes names + params but never SQL", () => {
  const desc = describeRegistry(REGISTRY);
  assert.deepEqual(Object.keys(desc).sort(), ["by_city_name", "flag", "trips_by_city"]);
  assert.ok(desc.trips_by_city.params.since);
  assert.equal(JSON.stringify(desc).includes("SELECT"), false, "SQL must not leak");
});
