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
 * Run `sql` and invoke `onChunk(Buffer)` for each Arrow IPC chunk as it
 * streams (schema, batches, EOS). Resolves when the DoGet stream ends.
 */
export function queryToIpc({ client, meta }, sql, onChunk) {
  return new Promise((resolve, reject) => {
    client.GetFlightInfo(sqlDescriptor(sql), meta, (err, info) => {
      if (err) return reject(err);
      const ticket = info.endpoint?.[0]?.ticket;
      if (!ticket) return reject(new Error("FlightInfo carried no endpoint ticket"));
      const call = client.DoGet(ticket, meta);
      call.on("data", (fd) =>
        onChunk(Buffer.from(flightDataToIpc(fd.data_header, fd.data_body))),
      );
      call.on("end", () => {
        onChunk(Buffer.from(ipcEos()));
        resolve();
      });
      call.on("error", reject);
    });
  });
}
