// Type definitions for @icegres/flight-proxy
import type { IncomingMessage, ServerResponse, Server } from "node:http";

/** A parameter declaration in a named query. No free-form string type exists:
 *  text filters must be an `enum` with server-defined values. */
export type ParamSpec =
  | { type: "int"; min?: number; max?: number; default?: number }
  | { type: "number"; min?: number; max?: number; default?: number }
  | { type: "bool"; default?: boolean }
  | { type: "date"; default?: string }
  | { type: "enum"; values: Array<string | number>; default?: string | number };

/** One entry in the named-query allowlist. `sql` receives ONLY validated,
 *  SQL-safe literal strings (never raw client input). */
export interface NamedQuery {
  description?: string;
  params?: Record<string, ParamSpec>;
  sql: (literals: Record<string, string>) => string;
}

export type QueryRegistry = Record<string, NamedQuery>;

export interface FlightConnectionOptions {
  /** icegres Flight address, "host:port". */
  address?: string;
  /** Use TLS (system roots) to reach icegres. */
  tls?: boolean;
  /** Basic credentials for an --auth-file icegres. */
  credentials?: { username: string; password: string };
}

export interface HandlerConfig {
  queries: QueryRegistry;
  flight?: FlightConnectionOptions;
  corsOrigin?: string;
  /** Return a principal, or null to reject with 401. Omit to leave open. */
  authenticate?: (req: IncomingMessage) => Promise<unknown | null> | unknown | null;
  /** Per-query gate; return false to reject with 403. */
  authorize?: (principal: unknown, queryName: string) => boolean | Promise<boolean>;
}

/** Build a Node (req,res) handler embeddable in any HTTP server / Express. */
export declare function createHandler(
  config: HandlerConfig,
): (req: IncomingMessage, res: ServerResponse) => Promise<void>;

/** Start a standalone http server with the handler. */
export declare function serve(
  config: HandlerConfig,
  opts?: { port?: number; host?: string },
): Promise<Server>;

/** Resolve a request to SQL (throws ParamError / unknown-query Error). */
export declare function resolveQuery(
  registry: QueryRegistry,
  name: string,
  rawParams?: Record<string, unknown>,
): { sql: string };

/** Public schema of the registry (names + params, never SQL). */
export declare function describeRegistry(
  registry: QueryRegistry,
): Record<string, { description: string | null; params: Record<string, ParamSpec> }>;

export declare class ParamError extends Error {}

// --- SQL-explorer gateway (arbitrary user SQL, sandboxed per user) ----------

export interface SqlGatewayConfig {
  /** HMAC secret for the short-lived session tokens. */
  sessionSecret: string;
  /** Verify the browser's app session (your SSO) -> the icegres principal to
   *  run queries as, or null to reject. Called only on POST /session. */
  authenticate: (req: import("node:http").IncomingMessage) =>
    Promise<{ principal: string; readOnly?: boolean } | null> |
    { principal: string; readOnly?: boolean } | null;
  flight?: FlightConnectionOptions;
  /** Map a session principal to the icegres credential to present, so each
   *  query runs authorized as that user. Defaults to flight.credentials. */
  credentialFor?: (principal: string) => { username: string; password: string } | undefined;
  sessionTtlSec?: number;
  corsOrigin?: string;
  /** Reject non-read SQL at the gateway (defense in depth; the authoritative
   *  read-only control is icegres authz granting the principal only
   *  CanReadData). Default true. */
  enforceReadOnly?: boolean;
}

/** Build a SQL-explorer gateway handler: POST /session (token broker) and
 *  POST /sql (arbitrary SQL under the token, streamed as Arrow). */
export declare function createSqlGateway(
  config: SqlGatewayConfig,
): (req: import("node:http").IncomingMessage, res: import("node:http").ServerResponse) => Promise<void>;

export declare function serveGateway(
  config: SqlGatewayConfig,
  opts?: { port?: number; host?: string },
): Promise<import("node:http").Server>;

/** Mint / verify a session token (exported for tests and custom brokers). */
export declare function issueToken(
  secret: string,
  claims: { principal: string; readOnly?: boolean; ttlSec?: number },
  nowSec?: number,
): string;
export declare function verifyToken(
  secret: string,
  token: string,
  nowSec?: number,
): { principal: string; readOnly: boolean };
