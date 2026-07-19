// Unit tests for the minimal protobuf codec — byte-exact against the Arrow
// Flight protocol wire format.
import { test } from "node:test";
import assert from "node:assert/strict";
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
} from "../src/pb.js";

const hex = (u8) => Buffer.from(u8).toString("hex");

test("CommandStatementQuery descriptor is byte-exact", () => {
  const desc = encodeCmdDescriptor(
    encodeAny(
      "arrow.flight.protocol.sql.CommandStatementQuery",
      encodeCommandStatementQuery("SELECT 1"),
    ),
  );
  // field1 (type) = CMD(2); field2 = Any{type_url, value{query="SELECT 1"}}
  assert.equal(
    hex(desc),
    "080212510a43747970652e676f6f676c65617069732e636f6d2f6172726f772e666c69" +
      "6768742e70726f746f636f6c2e73716c2e436f6d6d616e6453746174656d656e745175" +
      "657279120a0a0853454c45435420 31".replaceAll(" ", ""),
  );
});

test("FlightInfo ticket extraction skips unknown fields", () => {
  // FlightInfo { schema=1: bytes, endpoint=3: { ticket=1: { ticket=1: bytes } },
  //              total_records=4: varint } — hand-built with extras present.
  const ticketBytes = encodeTicket(new TextEncoder().encode("T"));
  const endpoint = concatBytes([Uint8Array.from([0x0a, ticketBytes.length]), ticketBytes]);
  const info = concatBytes([
    Uint8Array.from([0x0a, 2, 0xff, 0xfe]), // schema (opaque, skipped)
    Uint8Array.from([0x1a, endpoint.length]),
    endpoint,
    Uint8Array.from([0x20, 0x2a]), // total_records = 42 (skipped)
  ]);
  const got = decodeFlightInfoTicket(info);
  assert.equal(new TextDecoder().decode(got), "T");
});

test("FlightData decode reads header and 1000-numbered body", () => {
  const header = Uint8Array.from([1, 2, 3]);
  const body = Uint8Array.from([9, 9, 9, 9]);
  // data_header (field 2) + data_body (field 1000: key = 1000<<3|2 = 8002 -> varint c2 3e)
  const msg = concatBytes([
    Uint8Array.from([0x12, header.length]),
    header,
    Uint8Array.from([0xc2, 0x3e, body.length]),
    body,
  ]);
  const fd = decodeFlightData(msg);
  assert.deepEqual([...fd.dataHeader], [1, 2, 3]);
  assert.deepEqual([...fd.dataBody], [9, 9, 9, 9]);
});

test("IPC chunk framing is 8-byte aligned with continuation marker", () => {
  const header = new Uint8Array(10); // deliberately unaligned length
  const body = new Uint8Array(16);
  const chunk = flightDataToIpc(header, body);
  const dv = new DataView(chunk.buffer, chunk.byteOffset);
  assert.equal(dv.getUint32(0, true), 0xffffffff, "continuation marker");
  const declared = dv.getUint32(4, true);
  assert.equal((8 + declared) % 8, 0, "prefix+header padded to 8");
  assert.equal(chunk.length, 8 + declared + body.length);
  assert.deepEqual([...ipcEos()], [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
});

// --- credential encoding ----------------------------------------------------
import { FlightWebClient } from "../src/client.js";

test("Basic credentials are UTF-8 encoded, not Latin-1", () => {
  // A non-ASCII password must survive as its UTF-8 bytes: the server decodes
  // the Basic payload with String::from_utf8 and would reject Latin-1 btoa.
  const client = new FlightWebClient({
    baseUrl: "http://x",
    credentials: { username: "café", password: "pä€ss" },
  });
  const b64 = client.authHeader.replace("Basic ", "");
  const decoded = Buffer.from(b64, "base64");
  assert.equal(decoded.toString("utf8"), "café:pä€ss");
  // And the bytes are genuinely multi-byte UTF-8, not one-byte Latin-1.
  assert.ok(decoded.length > "café:pä€ss".length);
});
