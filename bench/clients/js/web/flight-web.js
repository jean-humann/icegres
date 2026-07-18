// Browser Arrow Flight SQL client over gRPC-web (via the translator bridge
// or any Envoy grpc_web listener). Two-RPC flow like the Node client:
// GetFlightInfo(CommandStatementQuery) -> DoGet(ticket); FlightData messages
// are reassembled into an Arrow IPC stream and decoded with apache-arrow.
//
// Uses fetch() streaming, so record batches are parsed incrementally as they
// arrive rather than after the last byte.

import { tableFromIPC } from "apache-arrow";
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
} from "../lib/pb.js";

function grpcWebBody(message) {
  const head = new Uint8Array(5);
  new DataView(head.buffer).setUint32(1, message.length, false);
  return concatBytes([head, message]);
}

/**
 * POST one grpc-web call and yield each response message (Uint8Array).
 * Throws on non-zero grpc-status in the trailers frame.
 */
async function* grpcWebCall(base, path, message) {
  const resp = await fetch(base + path, {
    method: "POST",
    headers: { "content-type": "application/grpc-web+proto" },
    body: grpcWebBody(message),
  });
  if (!resp.ok) throw new Error(`grpc-web HTTP ${resp.status}`);
  const reader = resp.body.getReader();
  let buf = new Uint8Array(0);
  for (;;) {
    const { done, value } = await reader.read();
    if (value) buf = buf.length ? concatBytes([buf, value]) : value;
    // Drain complete frames.
    while (buf.length >= 5) {
      const flags = buf[0];
      const len = new DataView(buf.buffer, buf.byteOffset).getUint32(1, false);
      if (buf.length < 5 + len) break;
      const payload = buf.subarray(5, 5 + len);
      buf = buf.subarray(5 + len);
      if (flags & 0x80) {
        const trailers = new TextDecoder().decode(payload);
        const m = trailers.match(/grpc-status:\s*(\d+)/);
        const status = m ? Number(m[1]) : 0;
        if (status !== 0) {
          const msg = trailers.match(/grpc-message:\s*([^\r\n]*)/);
          throw new Error(
            `grpc error ${status}: ${decodeURIComponent(msg?.[1] || "")}`,
          );
        }
        return;
      }
      yield payload;
    }
    if (done) return;
  }
}

/**
 * Run `sql` against a Flight SQL server behind a grpc-web endpoint.
 * Returns { table, ipcBytes } — an apache-arrow Table plus wire-size info.
 */
export async function flightQuery(grpcWebBase, sql) {
  const svc = "/arrow.flight.protocol.FlightService";
  const descriptor = encodeCmdDescriptor(
    encodeAny(
      "arrow.flight.protocol.sql.CommandStatementQuery",
      encodeCommandStatementQuery(sql),
    ),
  );
  let ticket = null;
  for await (const msg of grpcWebCall(grpcWebBase, `${svc}/GetFlightInfo`, descriptor)) {
    ticket = decodeFlightInfoTicket(msg);
  }
  if (!ticket) throw new Error("no ticket in FlightInfo");

  const chunks = [];
  let ipcBytes = 0;
  for await (const msg of grpcWebCall(grpcWebBase, `${svc}/DoGet`, encodeTicket(ticket))) {
    const { dataHeader, dataBody } = decodeFlightData(msg);
    const chunk = flightDataToIpc(dataHeader, dataBody);
    ipcBytes += chunk.length;
    chunks.push(chunk);
  }
  chunks.push(ipcEos());
  const table = tableFromIPC(concatBytes(chunks));
  return { table, ipcBytes };
}
