// Token-broker + read-only guard: the security core of the SQL-explorer path.
import { test } from "node:test";
import assert from "node:assert/strict";
import { issueToken, verifyToken } from "../src/gateway.js";

const SECRET = "test-secret-please-rotate";

test("token round-trips principal + read-only flag", () => {
  const tok = issueToken(SECRET, { principal: "analyst", readOnly: true }, 1000);
  const s = verifyToken(SECRET, tok, 1001);
  assert.equal(s.principal, "analyst");
  assert.equal(s.readOnly, true);
});

test("expired token is rejected", () => {
  const tok = issueToken(SECRET, { principal: "a", ttlSec: 60 }, 1000);
  assert.throws(() => verifyToken(SECRET, tok, 1000 + 61), /expired/);
});

test("tampered payload or signature is rejected", () => {
  const tok = issueToken(SECRET, { principal: "a" }, 1000);
  const [p] = tok.split(".");
  assert.throws(() => verifyToken(SECRET, p + ".deadbeef", 1000), /signature|malformed/);
  // a token signed with a different secret must not verify
  const other = issueToken("different-secret", { principal: "a" }, 1000);
  assert.throws(() => verifyToken(SECRET, other, 1000), /signature/);
});

import { createSqlGateway, serveGateway, isReadOnlySql } from "../src/gateway.js";
test("createSqlGateway validates its required config", () => {
  assert.throws(() => createSqlGateway({}), /sessionSecret/);
  assert.throws(() => createSqlGateway({ sessionSecret: "x" }), /authenticate/);
  // valid config builds a handler
  const h = createSqlGateway({ sessionSecret: "x", authenticate: () => ({ principal: "a" }) });
  assert.equal(typeof h, "function");
});

test("read-only guard admits reads and refuses writes (incl. bypasses)", () => {
  // Reads pass.
  assert.equal(isReadOnlySql("SELECT 1"), true);
  assert.equal(isReadOnlySql("WITH t AS (SELECT 1) SELECT * FROM t"), true);
  assert.equal(isReadOnlySql("EXPLAIN SELECT * FROM demo.trips"), true);
  assert.equal(isReadOnlySql("EXPLAIN ANALYZE SELECT * FROM demo.trips"), true);
  // Plain writes and multi-statement injection.
  assert.equal(isReadOnlySql("DELETE FROM t"), false);
  assert.equal(isReadOnlySql("INSERT INTO t VALUES (1)"), false);
  assert.equal(isReadOnlySql("SELECT 1; DROP TABLE t"), false);
  // The two textbook bypasses of a leading-verb check.
  assert.equal(
    isReadOnlySql("WITH d AS (DELETE FROM t RETURNING 1) SELECT * FROM d"),
    false,
  );
  assert.equal(isReadOnlySql("EXPLAIN ANALYZE INSERT INTO t VALUES (1)"), false);
  // A comment must not smuggle a write past the leading-verb check.
  assert.equal(isReadOnlySql("/* SELECT */ DELETE FROM t"), false);
});

test("gateway HTTP: session mint, token gating, read-only 403", async () => {
  const server = await serveGateway(
    {
      sessionSecret: SECRET,
      authenticate: async () => ({ principal: "analyst", readOnly: true }),
      corsOrigin: "https://dash.example",
    },
    { port: 0, host: "127.0.0.1" },
  );
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    // Mint a session token.
    const s = await fetch(`${base}/session`, { method: "POST" });
    assert.equal(s.status, 200);
    const { token } = await s.json();
    assert.ok(token);

    // Missing / bad token is 401.
    assert.equal((await fetch(`${base}/sql`, { method: "POST" })).status, 401);
    assert.equal(
      (
        await fetch(`${base}/sql`, {
          method: "POST",
          headers: { authorization: "Bearer nope" },
        })
      ).status,
      401,
    );

    // A valid token but a write statement is refused at the gateway (403),
    // before any Flight connection is attempted.
    const w = await fetch(`${base}/sql`, {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify({ sql: "DELETE FROM demo.trips" }),
    });
    assert.equal(w.status, 403);
  } finally {
    server.close();
  }
});
