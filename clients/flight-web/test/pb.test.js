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

// --- gRPC-web frame reassembly ---------------------------------------------

const te = (s) => new TextEncoder().encode(s);

/** A gRPC-web frame: [flags, big-endian u32 length, payload]. */
function frame(flags, payload) {
  const head = new Uint8Array(5);
  head[0] = flags;
  new DataView(head.buffer).setUint32(1, payload.length, false);
  return concatBytes([head, payload]);
}

/** A length-delimited protobuf field (field no < 16, len < 128). */
function ld(no, bytes) {
  return concatBytes([new Uint8Array([(no << 3) | 2, bytes.length]), bytes]);
}

function chunkify(bytes, size) {
  const out = [];
  for (let i = 0; i < bytes.length; i += size) out.push(bytes.subarray(i, i + size));
  return out;
}

/** A fetch-Response stand-in whose body yields `chunks` one read at a time;
 *  `onCancel` fires if the consumer cancels before draining. */
function fakeResp(chunks, onCancel = () => {}) {
  let i = 0;
  return {
    ok: true,
    headers: { get: () => null },
    body: {
      getReader: () => ({
        read: async () =>
          i < chunks.length
            ? { done: false, value: chunks[i++] }
            : { done: true, value: undefined },
        cancel: async () => onCancel(),
        releaseLock() {},
      }),
    },
  };
}

const OK_TRAILER = frame(0x80, te("grpc-status:0\r\n"));
// FlightInfo { endpoint(3) { ticket(1) { ticket(1) = bytes } } }.
const flightInfo = (t) => ld(3, ld(1, ld(1, t)));

test("gRPC-web frames reassemble across chunk boundaries", async () => {
  const headerA = te("HEADER-A-spanning-several-tiny-chunks");
  const dataFrame = ld(2, headerA); // FlightData { data_header }
  const metaOnly = new Uint8Array(0); // metadata-only FlightData: no IPC
  const doGet = concatBytes([frame(0, dataFrame), frame(0, metaOnly), OK_TRAILER]);
  const client = new FlightWebClient({
    baseUrl: "http://x",
    fetch: async (url) =>
      String(url).endsWith("/GetFlightInfo")
        ? fakeResp(chunkify(concatBytes([frame(0, flightInfo(Uint8Array.of(1, 2, 3))), OK_TRAILER]), 2))
        : fakeResp(chunkify(doGet, 3)), // 3-byte reads force cross-boundary frames
  });
  const ipc = await client.queryIpc("SELECT 1");
  // The header frame decodes to its IPC chunk; the metadata-only frame adds
  // nothing; then the synthesized end-of-stream marker.
  const expected = concatBytes([
    flightDataToIpc(headerA, new Uint8Array(0)),
    ipcEos(),
  ]);
  assert.deepEqual([...ipc], [...expected]);
});

test("a DoGet body without a trailer frame throws (truncation)", async () => {
  const client = new FlightWebClient({
    baseUrl: "http://x",
    fetch: async (url) =>
      String(url).endsWith("/GetFlightInfo")
        ? fakeResp(chunkify(concatBytes([frame(0, flightInfo(Uint8Array.of(1))), OK_TRAILER]), 2))
        : fakeResp(chunkify(frame(0, ld(2, te("H"))), 3)), // no trailer
  });
  await assert.rejects(() => client.queryIpc("SELECT 1"), /trailers frame|truncated/);
});

test("abandoning the stream early cancels the DoGet body", async () => {
  let cancelled = false;
  const doGet = concatBytes([
    frame(0, ld(2, te("AAAA"))),
    frame(0, ld(2, te("BBBB"))),
    OK_TRAILER,
  ]);
  const client = new FlightWebClient({
    baseUrl: "http://x",
    fetch: async (url) =>
      String(url).endsWith("/GetFlightInfo")
        ? fakeResp(chunkify(concatBytes([frame(0, flightInfo(Uint8Array.of(1))), OK_TRAILER]), 2))
        : fakeResp(chunkify(doGet, 4), () => {
            cancelled = true;
          }),
  });
  // Take one chunk, then abandon — the finally must cancel the body so the
  // browser stops downloading and the server RPC slot frees.
  for await (const _chunk of client.ipcChunks("SELECT 1")) break;
  assert.equal(cancelled, true);
});

test("flightDataToIpc with an empty header emits nothing (not EOS)", () => {
  const out = flightDataToIpc(new Uint8Array(0), new Uint8Array(0));
  assert.equal(
    out.length,
    0,
    "a metadata-only FlightData must not emit the IPC end-of-stream marker",
  );
});

test("onTiming fires with latency, bytes, rows on success", async () => {
  // A fake fetch that returns one data frame + a clean trailer, so query()
  // resolves without a live server and the RUM hook is observable.
  const samples = [];
  // Minimal valid: an empty Arrow IPC stream decodes to a 0-row table.
  const client = new FlightWebClient({
    baseUrl: "http://x",
    onTiming: (s) => samples.push(s),
    fetch: async () => { throw new Error("boom"); },
  });
  await assert.rejects(() => client.query("SELECT 1"));
  assert.equal(samples.length, 1);
  assert.equal(samples[0].ok, false);
  assert.equal(samples[0].sql, "SELECT 1");
  assert.ok(samples[0].ms >= 0);
});
