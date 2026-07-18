// Node Arrow Flight SQL client over @grpc/grpc-js (pure JS, no native addon).
// Speaks the spec-faithful two-RPC flow: GetFlightInfo(CommandStatementQuery)
// -> DoGet(ticket), then reassembles the FlightData stream into an Arrow IPC
// stream (see lib/pb.js for the framing).

import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  encodeAny,
  encodeCommandStatementQuery,
  flightDataToIpc,
  ipcEos,
} from "./pb.js";

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

const GRPC_OPTS = {
  "grpc.max_receive_message_length": 256 * 1024 * 1024,
};

export function connect(addr = "127.0.0.1:50051") {
  return new FlightService(addr, grpc.credentials.createInsecure(), GRPC_OPTS);
}

export function sqlDescriptor(sql) {
  const any = encodeAny(
    "arrow.flight.protocol.sql.CommandStatementQuery",
    encodeCommandStatementQuery(sql),
  );
  return { type: 2, cmd: Buffer.from(any) };
}

export function getFlightInfo(client, sql) {
  return new Promise((resolve, reject) => {
    client.GetFlightInfo(sqlDescriptor(sql), (err, info) =>
      err ? reject(err) : resolve(info),
    );
  });
}

/**
 * Run `sql` and invoke `onIpcChunk(Buffer)` for every Arrow IPC chunk as it
 * arrives off the wire (schema message first, then record batches, then EOS).
 * Resolves once the DoGet stream completes.
 */
export async function queryToIpc(client, sql, onIpcChunk) {
  const info = await getFlightInfo(client, sql);
  const ticket = info.endpoint?.[0]?.ticket;
  if (!ticket) throw new Error("FlightInfo carried no endpoint ticket");
  await new Promise((resolve, reject) => {
    const call = client.DoGet(ticket);
    call.on("data", (fd) => {
      onIpcChunk(Buffer.from(flightDataToIpc(fd.data_header, fd.data_body)));
    });
    call.on("end", () => {
      onIpcChunk(Buffer.from(ipcEos()));
      resolve();
    });
    call.on("error", reject);
  });
}

/** Run `sql` and return the whole result as one Arrow IPC stream Buffer. */
export async function queryToIpcBuffer(client, sql) {
  const chunks = [];
  await queryToIpc(client, sql, (c) => chunks.push(c));
  return Buffer.concat(chunks);
}
