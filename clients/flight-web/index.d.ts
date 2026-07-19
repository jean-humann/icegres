// Type definitions for @icegres/flight-web
import type { Table, RecordBatch } from "apache-arrow";

export interface FlightWebCredentials {
  username: string;
  password: string;
}

export interface FlightTimingSample {
  sql: string;
  /** End-to-end latency in ms (request + transfer + decode). */
  ms: number;
  /** Bytes received on the wire (0 on error). */
  bytes: number;
  /** Decoded row count, or null on error. */
  rows: number | null;
  ok: boolean;
  error?: string;
}

export interface FlightWebClientOptions {
  /** gRPC-web endpoint, e.g. "https://db.example:50051". */
  baseUrl: string;
  /** Sent as a per-RPC `authorization: Basic ...` header (server --auth-file). */
  credentials?: FlightWebCredentials;
  /** Transport-error retries for GetFlightInfo only (default 1). */
  retries?: number;
  /** fetch override for tests/polyfills. */
  fetch?: typeof fetch;
  /** Real-user-monitoring hook: one sample per query() (never throws into the query). */
  onTiming?: (sample: FlightTimingSample) => void;
}

export interface CallOptions {
  signal?: AbortSignal;
}

export declare class FlightError extends Error {
  readonly code: number;
  readonly grpcMessage: string;
}

export declare class FlightWebClient {
  constructor(opts: FlightWebClientOptions);
  /** Arrow IPC stream chunks as they arrive (schema, batches, EOS). */
  ipcChunks(sql: string, opts?: CallOptions): AsyncGenerator<Uint8Array>;
  /** Whole result as one Arrow IPC stream buffer. */
  queryIpc(sql: string, opts?: CallOptions): Promise<Uint8Array>;
  /** Whole result as an apache-arrow Table. */
  query(sql: string, opts?: CallOptions): Promise<Table>;
  /** Progressive decode: onBatch fires per record batch; resolves to count. */
  queryBatches(
    sql: string,
    onBatch: (batch: RecordBatch, index: number) => void,
    opts?: CallOptions,
  ): Promise<number>;
}

/** Register the ZSTD IPC codec with apache-arrow (import side effect). */
export declare function registerZstd(): void;
