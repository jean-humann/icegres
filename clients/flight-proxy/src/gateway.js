// SQL-explorer gateway: the token-broker pattern for a browser query editor.
//
// Unlike the named-query allowlist (server.js), this path ACCEPTS ARBITRARY
// SQL — because for a SQL explorer, user-written queries are the feature. It
// is made safe by sandboxing the user, not by restricting the SQL text:
//
//   1. The browser never holds a long-lived database credential. It exchanges
//      an app session for a short-lived, principal-scoped token here
//      (POST /session), and sends that token with each query (POST /sql).
//   2. Every query runs as the session's icegres principal, so icegres
//      authorization (--authz-file) scopes it to the tables that principal
//      may read. THE REAL read-only control is granting that principal only
//      CanReadData — a write then fails with SQLSTATE 42501 at the engine.
//   3. This gateway adds a defense-in-depth read-only guard (reject obvious
//      non-read statements) and relies on icegres's resource limits
//      (--flight-statement-timeout-ms / --flight-max-result-bytes /
//      --flight-max-concurrent-rpcs) to bound a runaway query.
//   4. Results stream back as Arrow IPC, untouched (byte pass-through).

import crypto from "node:crypto";
import { connect, queryToIpc } from "./flight.js";
import {
  corsHeaders,
  sendJson,
  readJson,
  streamArrow,
  serveHandler,
} from "./http.js";

const b64url = (buf) =>
  Buffer.from(buf).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const fromB64url = (s) => Buffer.from(s.replace(/-/g, "+").replace(/_/g, "/"), "base64");

/** Mint a short-lived signed session token bound to a principal + read-only flag. */
export function issueToken(secret, { principal, readOnly = true, ttlSec = 900 }, nowSec) {
  const now = nowSec ?? Math.floor(Date.now() / 1000);
  const payload = b64url(JSON.stringify({ p: principal, ro: !!readOnly, exp: now + ttlSec }));
  const sig = b64url(crypto.createHmac("sha256", secret).update(payload).digest());
  return `${payload}.${sig}`;
}

/** Verify a token; returns { principal, readOnly } or throws. */
export function verifyToken(secret, token, nowSec) {
  const [payload, sig] = String(token).split(".");
  if (!payload || !sig) throw new Error("malformed token");
  const expected = b64url(crypto.createHmac("sha256", secret).update(payload).digest());
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) throw new Error("bad signature");
  const claims = JSON.parse(fromB64url(payload).toString("utf8"));
  const now = nowSec ?? Math.floor(Date.now() / 1000);
  if (typeof claims.exp !== "number" || claims.exp < now) throw new Error("token expired");
  return { principal: claims.p, readOnly: !!claims.ro };
}

// Conservative read-only guard: after stripping leading line/block comments and
// whitespace, allow only statements that begin with a read verb. This is
// DEFENSE IN DEPTH — the authoritative read-only control is icegres authz
// (grant the principal only CanReadData). A single statement only.
const READ_VERBS = /^(select|with|explain|show|describe|desc|table|values)\b/i;
// A data-modifying keyword anywhere is refused, which closes the two textbook
// bypasses of a leading-verb check: a data-modifying CTE
// (`WITH d AS (DELETE … RETURNING 1) SELECT * FROM d`) and
// `EXPLAIN ANALYZE INSERT …`. Coarse by design (word-boundary text scan, so a
// bare identifier equal to a keyword is over-rejected, never under) — the
// authoritative control is icegres authz + --read-only.
const MUTATING =
  /\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke|copy)\b/i;
export function isReadOnlySql(sql) {
  const stripped = sql
    .replace(/\/\*[\s\S]*?\*\//g, " ") // block comments
    .replace(/--[^\n]*/g, " ") // line comments
    .trim();
  if (!READ_VERBS.test(stripped)) return false;
  // Reject a second statement (a trailing `; DELETE …`). A lone trailing ';'
  // is fine.
  const semi = stripped.replace(/;\s*$/, "");
  if (semi.includes(";")) return false;
  if (MUTATING.test(semi)) return false;
  return true;
}

/**
 * Build a SQL-explorer gateway handler.
 * @param {object} config
 * @param {string} config.sessionSecret            HMAC secret for session tokens
 * @param {(req) => Promise<{principal: string, readOnly?: boolean}|null>} config.authenticate
 *        verify the browser's app session (your SSO) → the icegres principal to
 *        run as, or null to reject. Called only on POST /session.
 * @param {object} [config.flight]                 { address, tls, credentials }
 * @param {(principal) => {username,password}|undefined} [config.credentialFor]
 *        map a session principal to the icegres credential to present, so each
 *        query runs authorized as that user. Defaults to config.flight.credentials
 *        (a single service identity — fine only if authz is enforced elsewhere).
 * @param {number} [config.sessionTtlSec=900]
 * @param {string} [config.corsOrigin="*"]
 * @param {boolean} [config.enforceReadOnly=true]  reject non-read SQL at the gateway
 *        (defense in depth; the real control is authz).
 */
export function createSqlGateway(config) {
  const {
    sessionSecret,
    authenticate,
    flight = {},
    credentialFor,
    sessionTtlSec = 900,
    corsOrigin = "*",
    enforceReadOnly = true,
  } = config;
  if (!sessionSecret) throw new Error("createSqlGateway requires a sessionSecret");
  if (typeof authenticate !== "function") throw new Error("createSqlGateway requires an authenticate hook");

  // One channel per distinct credential; per-request we swap the principal's.
  const baseAddr = flight.address ?? "127.0.0.1:50051";
  const baseTls = !!flight.tls;
  const CONN_CACHE_MAX = 256;
  const connCache = new Map(); // credKey -> connection; bounded LRU
  function connFor(principal) {
    const cred = credentialFor ? credentialFor(principal) : flight.credentials;
    // Key on BOTH username and password so a credential rotation opens a
    // fresh channel instead of reusing one bound to the old (possibly
    // revoked) secret. In-memory only; never logged.
    const key = cred ? JSON.stringify([cred.username, cred.password]) : "__anon__";
    const existing = connCache.get(key);
    if (existing) {
      connCache.delete(key); // refresh LRU recency
      connCache.set(key, existing);
      return existing;
    }
    const conn = connect({ address: baseAddr, tls: baseTls, credentials: cred });
    connCache.set(key, conn);
    // Bound growth: evict the least-recently-used channel so a large principal
    // population cannot grow the map without bound. We do NOT close() it —
    // close cancels any in-flight call, which would truncate the Arrow stream
    // of a slow request that still holds this channel; a dereferenced idle
    // channel is reaped by grpc-js's own idle timeout instead.
    if (connCache.size > CONN_CACHE_MAX) {
      connCache.delete(connCache.keys().next().value);
    }
    return conn;
  }

  const cors = corsHeaders("POST, OPTIONS", corsOrigin);

  return async function handler(req, res) {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    if (req.method === "OPTIONS") return void res.writeHead(204, cors).end();

    // Exchange an app session for a short-lived query token.
    if (req.method === "POST" && url.pathname === "/session") {
      let ident;
      try {
        ident = await authenticate(req);
      } catch {
        ident = null;
      }
      if (!ident || !ident.principal) return sendJson(res, 401, { error: "unauthorized" }, cors);
      const token = issueToken(sessionSecret, {
        principal: ident.principal,
        readOnly: ident.readOnly ?? true,
        ttlSec: sessionTtlSec,
      });
      return sendJson(res, 200, { token, expiresInSec: sessionTtlSec }, cors);
    }

    if (req.method !== "POST" || url.pathname !== "/sql") {
      return sendJson(res, 404, { error: "not found" }, cors);
    }

    // Validate the session token.
    const auth = req.headers.authorization || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
    let session;
    try {
      session = verifyToken(sessionSecret, token);
    } catch (e) {
      return sendJson(res, 401, { error: `invalid session: ${e.message}` }, cors);
    }

    let body;
    try {
      body = await readJson(req, 256 * 1024);
    } catch (e) {
      return sendJson(res, 400, { error: e.message }, cors);
    }
    const sql = body?.sql;
    if (typeof sql !== "string" || !sql.trim()) {
      return sendJson(res, 400, { error: "body must be { sql: string }" }, cors);
    }

    // Read-only guard (defense in depth). authoritative control is icegres authz.
    if (enforceReadOnly && (session.readOnly ?? true) && !isReadOnlySql(sql)) {
      return sendJson(res, 403, { error: "read-only session: only SELECT/WITH/EXPLAIN/SHOW are allowed" }, cors);
    }

    await streamArrow(
      res,
      cors,
      (write, signal) => queryToIpc(connFor(session.principal), sql, write, { signal }),
      // A failure before the first chunk becomes a JSON error; a mid-stream
      // failure destroys the socket. Either way the explorer surfaces it.
      // eslint-disable-next-line no-console
      (e) => console.error(`explorer query failed (${session.principal}):`, e.message),
    );
  };
}

/** Convenience standalone server, mirroring server.js#serve. */
export function serveGateway(config, { port = 8091, host = "127.0.0.1" } = {}) {
  return serveHandler(createSqlGateway(config), { port, host });
}
