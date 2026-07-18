// Arrow Flight SQL over gRPC-web, for browsers (and any fetch() runtime).
//
// Speaks the spec-faithful two-RPC flow — GetFlightInfo(CommandStatementQuery)
// then DoGet(ticket) — against `icegres flight-serve --grpc-web` (or the same
// service behind an Envoy grpc_web filter). FlightData messages are
// reassembled into an Arrow IPC stream; batches surface incrementally, so a
// dashboard can paint before the last byte arrives.
//
// Auth: gRPC-web cannot carry the bidirectional Handshake RPC, so credentials
// ride every call as an `authorization: Basic ...` header — the server
// verifies per-RPC (with a server-side cache; see icegres/src/flight.rs).

import {
  encodeAny,
  encodeCommandStatementQuery,
  encodeCmdDescriptor,
  encodeTicket,
  decodeFlightInfoTicket,
  decodeFlightData,
  flightDataToIpc,
  ipcEos,
  concatBytes,
} from "./pb.js";

const SVC = "/arrow.flight.protocol.FlightService";

/** Wrap one protobuf message in a gRPC-web request body (5-byte frame). */
function grpcWebBody(message) {
  const head = new Uint8Array(5);
  new DataView(head.buffer).setUint32(1, message.length, false);
  return concatBytes([head, message]);
}

export class FlightError extends Error {
  constructor(code, message) {
    super(`grpc error ${code}: ${message}`);
    this.name = "FlightError";
    this.code = code;
    this.grpcMessage = message;
  }
}

export class FlightWebClient {
  /**
   * @param {object} opts
   * @param {string} opts.baseUrl   e.g. "https://db.example:50051"
   * @param {{username: string, password: string}} [opts.credentials]
   * @param {number} [opts.retries] retry count for transport errors on the
   *   (idempotent, result-free) GetFlightInfo call. DoGet is never retried:
   *   a mid-stream failure surfaces rather than silently re-running a query.
   * @param {typeof fetch} [opts.fetch] fetch override (tests, polyfills)
   */
  constructor({ baseUrl, credentials, retries = 1, fetch: fetchImpl } = {}) {
    if (!baseUrl) throw new Error("FlightWebClient requires baseUrl");
    this.base = baseUrl.replace(/\/$/, "");
    this.retries = retries;
    this.fetch = fetchImpl ?? fetch.bind(globalThis);
    this.authHeader = credentials
      ? "Basic " +
        btoa(
          `${credentials.username}:${credentials.password}`
            .split("")
            .map((c) => (c.charCodeAt(0) > 255 ? "?" : c))
            .join(""),
        )
      : null;
  }

  headers() {
    const h = { "content-type": "application/grpc-web+proto" };
    if (this.authHeader) h.authorization = this.authHeader;
    return h;
  }

  /**
   * One gRPC-web call; yields each response message (Uint8Array).
   * Throws FlightError on a non-zero grpc-status trailer.
   */
  async *#call(path, message, signal) {
    const resp = await this.fetch(this.base + path, {
      method: "POST",
      headers: this.headers(),
      body: grpcWebBody(message),
      signal,
    });
    if (!resp.ok) throw new Error(`gRPC-web HTTP ${resp.status} on ${path}`);
    // Some runtimes surface trailer-only responses via headers.
    const headerStatus = resp.headers.get("grpc-status");
    if (headerStatus && headerStatus !== "0") {
      throw new FlightError(
        Number(headerStatus),
        decodeURIComponent(resp.headers.get("grpc-message") || ""),
      );
    }
    const reader = resp.body.getReader();
    let buf = new Uint8Array(0);
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (value) buf = buf.length ? concatBytes([buf, value]) : value;
        while (buf.length >= 5) {
          const flags = buf[0];
          const len = new DataView(buf.buffer, buf.byteOffset).getUint32(1, false);
          if (buf.length < 5 + len) break;
          const payload = buf.subarray(5, 5 + len);
          buf = buf.subarray(5 + len);
          if (flags & 0x80) {
            const trailers = new TextDecoder().decode(payload);
            const status = Number(trailers.match(/grpc-status:\s*(\d+)/)?.[1] ?? 0);
            if (status !== 0) {
              const msg = trailers.match(/grpc-message:\s*([^\r\n]*)/)?.[1] || "";
              throw new FlightError(status, decodeURIComponent(msg));
            }
            return;
          }
          yield payload;
        }
        if (done) return;
      }
    } finally {
      reader.releaseLock();
    }
  }

  /** GetFlightInfo for `sql`; returns the DoGet ticket bytes. */
  async #ticketFor(sql, signal) {
    const descriptor = encodeCmdDescriptor(
      encodeAny(
        "arrow.flight.protocol.sql.CommandStatementQuery",
        encodeCommandStatementQuery(sql),
      ),
    );
    let lastErr;
    for (let attempt = 0; attempt <= this.retries; attempt++) {
      try {
        let ticket = null;
        for await (const msg of this.#call(`${SVC}/GetFlightInfo`, descriptor, signal)) {
          ticket = decodeFlightInfoTicket(msg);
        }
        if (!ticket) throw new Error("FlightInfo carried no endpoint ticket");
        return ticket;
      } catch (e) {
        // Server-reported errors (bad SQL, auth) and aborts are final;
        // only transport-level failures are retried.
        if (e instanceof FlightError || e.name === "AbortError") throw e;
        lastErr = e;
      }
    }
    throw lastErr;
  }

  /**
   * Run `sql`, yielding each Arrow IPC chunk (schema message first, then one
   * chunk per record batch, then end-of-stream) as it arrives off the wire.
   * Feed to `RecordBatchReader.from` for incremental decoding, or
   * concatenate for `tableFromIPC`.
   * @param {string} sql
   * @param {{signal?: AbortSignal}} [opts]
   */
  async *ipcChunks(sql, { signal } = {}) {
    const ticket = await this.#ticketFor(sql, signal);
    for await (const msg of this.#call(`${SVC}/DoGet`, encodeTicket(ticket), signal)) {
      const { dataHeader, dataBody } = decodeFlightData(msg);
      yield flightDataToIpc(dataHeader, dataBody);
    }
    yield ipcEos();
  }

  /** Run `sql` and return the complete Arrow IPC stream as one Uint8Array. */
  async queryIpc(sql, opts) {
    const chunks = [];
    for await (const c of this.ipcChunks(sql, opts)) chunks.push(c);
    return concatBytes(chunks);
  }

  /**
   * Run `sql` and return an apache-arrow Table. Requires `apache-arrow` (a
   * peer dependency) and a registered ZSTD codec (see ./zstd.js) unless the
   * server runs `--result-compression none`.
   */
  async query(sql, opts) {
    const { tableFromIPC } = await import("apache-arrow");
    return tableFromIPC(await this.queryIpc(sql, opts));
  }

  /**
   * Run `sql`, invoking `onBatch(recordBatch, i)` as each batch decodes —
   * charts can render progressively. Resolves to the batch count.
   */
  async queryBatches(sql, onBatch, opts) {
    const { RecordBatchReader } = await import("apache-arrow");
    const reader = await RecordBatchReader.from(this.ipcChunks(sql, opts));
    let i = 0;
    for await (const batch of reader) onBatch(batch, i++);
    return i;
  }
}
