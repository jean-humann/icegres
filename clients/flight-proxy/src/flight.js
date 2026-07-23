// Node Arrow Flight SQL client (native gRPC via @grpc/grpc-js) for the proxy
// to reach icegres. Speaks the spec-faithful GetFlightInfo -> DoGet flow and
// reassembles the FlightData stream into Arrow IPC chunks. Native gRPC here
// (not gRPC-web): the proxy is a server, only the browser needs gRPC-web.

import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  encodeAny,
  encodeCommandStatementQuery,
  flightDataToIpc,
  ipcEos,
} from "@icegres/flight-web/pb";

const require = createRequire(import.meta.url);
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");

const PROTO = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "proto",
  "flight.proto",
);
const packageDefinition = protoLoader.loadSync(PROTO, {
  keepCase: true,
  longs: Number,
  bytes: Buffer,
  defaults: true,
});
const proto = grpc.loadPackageDefinition(packageDefinition);
const FlightService = proto.arrow.flight.protocol.FlightService;

const GRPC_OPTS = { "grpc.max_receive_message_length": 256 * 1024 * 1024 };

/** Open a Flight client. `tls` uses the system roots; `credentials` sets a
 *  Basic auth header on every call (for an --auth-file icegres). */
export function connect({ address = "127.0.0.1:50051", tls = false, credentials } = {}) {
  const creds = tls
    ? grpc.credentials.createSsl()
    : grpc.credentials.createInsecure();
  const client = new FlightService(address, creds, GRPC_OPTS);
  const meta = new grpc.Metadata();
  if (credentials) {
    const b64 = Buffer.from(
      `${credentials.username}:${credentials.password}`,
      "utf8",
    ).toString("base64");
    meta.set("authorization", `Basic ${b64}`);
  }
  return { client, meta };
}

function sqlDescriptor(sql) {
  const any = encodeAny(
    "arrow.flight.protocol.sql.CommandStatementQuery",
    encodeCommandStatementQuery(sql),
  );
  return { type: 2, cmd: Buffer.from(any) };
}

/**
 * Run `sql` and await `onChunk(Buffer)` for each Arrow IPC chunk as it
 * streams (schema, batches, EOS). The gRPC source is paused while the sink is
 * backpressured, and an optional AbortSignal cancels the upstream DoGet.
 */
export function queryToIpc({ client, meta }, sql, onChunk, { signal } = {}) {
  return new Promise((resolve, reject) => {
    let infoCall;
    let call;
    let settled = false;
    let ended = false;
    let writes = Promise.resolve();
    const finish = (error) => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", onAbort);
      if (error) reject(error);
      else resolve();
    };
    const onAbort = () => {
      infoCall?.cancel();
      call?.cancel();
      finish(new Error("query cancelled because the browser connection closed"));
    };
    if (signal?.aborted) return onAbort();
    signal?.addEventListener("abort", onAbort, { once: true });

    infoCall = client.GetFlightInfo(sqlDescriptor(sql), meta, (err, info) => {
      if (settled) return;
      if (err) return finish(err);
      const ticket = info.endpoint?.[0]?.ticket;
      if (!ticket) return finish(new Error("FlightInfo carried no endpoint ticket"));
      call = client.DoGet(ticket, meta);
      if (signal?.aborted) return onAbort();
      call.on("data", (fd) => {
        if (settled) return;
        call.pause();
        const chunk = Buffer.from(flightDataToIpc(fd.data_header, fd.data_body));
        writes = writes
          .then(() => onChunk(chunk))
          .then(() => {
            if (!ended && !settled) call.resume();
          });
        writes.catch((error) => {
          call.cancel();
          finish(error);
        });
      });
      call.on("end", () => {
        if (settled) return;
        ended = true;
        writes = writes.then(() => onChunk(Buffer.from(ipcEos())));
        writes.then(() => finish(), finish);
      });
      call.on("error", finish);
    });
  });
}
