// Minimal protobuf wire-format helpers shared by the Node probes and the
// browser gRPC-web client. Only what the Flight query path needs: varint,
// length-delimited fields, and the handful of Flight / Flight SQL messages.
// Field numbers match the upstream Arrow Flight protocol; unknown fields on
// decode are skipped, so a protocol superset on the server is safe.

const te = new TextEncoder();
const td = new TextDecoder();

// --- writer ----------------------------------------------------------------

function varint(n) {
  const out = [];
  let v = n >>> 0;
  if (n > 0xffffffff) {
    // Not needed for our message sizes; guard anyway.
    let big = BigInt(n);
    while (big > 0x7fn) {
      out.push(Number(big & 0x7fn) | 0x80);
      big >>= 7n;
    }
    out.push(Number(big));
    return out;
  }
  while (v > 0x7f) {
    out.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  out.push(v);
  return out;
}

function concatBytes(parts) {
  let len = 0;
  for (const p of parts) len += p.length;
  const out = new Uint8Array(len);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

/** Length-delimited field (wire type 2): strings, bytes, sub-messages. */
function ldField(fieldNo, payload) {
  const bytes = typeof payload === "string" ? te.encode(payload) : payload;
  return concatBytes([
    Uint8Array.from([...varint((fieldNo << 3) | 2)]),
    Uint8Array.from(varint(bytes.length)),
    bytes,
  ]);
}

/** Varint field (wire type 0). */
function vField(fieldNo, value) {
  return concatBytes([
    Uint8Array.from(varint(fieldNo << 3)),
    Uint8Array.from(varint(value)),
  ]);
}

// --- reader ----------------------------------------------------------------

/** Iterate (fieldNo, wireType, value) triples of a message buffer. */
function* fields(buf) {
  let i = 0;
  while (i < buf.length) {
    let shift = 0;
    let key = 0;
    for (;;) {
      const b = buf[i++];
      key |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) break;
      shift += 7;
    }
    const fieldNo = key >>> 3;
    const wire = key & 7;
    if (wire === 0) {
      let shift2 = 0;
      let val = 0n;
      for (;;) {
        const b = buf[i++];
        val |= BigInt(b & 0x7f) << BigInt(shift2);
        if ((b & 0x80) === 0) break;
        shift2 += 7;
      }
      yield [fieldNo, wire, val];
    } else if (wire === 2) {
      let shift2 = 0;
      let len = 0;
      for (;;) {
        const b = buf[i++];
        len |= (b & 0x7f) << shift2;
        if ((b & 0x80) === 0) break;
        shift2 += 7;
      }
      yield [fieldNo, wire, buf.subarray(i, i + len)];
      i += len;
    } else if (wire === 5) {
      yield [fieldNo, wire, buf.subarray(i, i + 4)];
      i += 4;
    } else if (wire === 1) {
      yield [fieldNo, wire, buf.subarray(i, i + 8)];
      i += 8;
    } else {
      throw new Error(`unsupported protobuf wire type ${wire}`);
    }
  }
}

// --- Flight / Flight SQL messages ------------------------------------------

const ANY_PREFIX = "type.googleapis.com/";

/** google.protobuf.Any wrapping `inner` under the given fully-qualified name. */
export function encodeAny(typeName, inner) {
  return concatBytes([ldField(1, ANY_PREFIX + typeName), ldField(2, inner)]);
}

/** arrow.flight.protocol.sql.CommandStatementQuery { string query = 1 } */
export function encodeCommandStatementQuery(sql) {
  return ldField(1, sql);
}

/** FlightDescriptor { type = 1 (CMD = 2), cmd = 2 } */
export function encodeCmdDescriptor(anyBytes) {
  return concatBytes([vField(1, 2), ldField(2, anyBytes)]);
}

/** Ticket { bytes ticket = 1 } */
export function encodeTicket(ticketBytes) {
  return ldField(1, ticketBytes);
}

/** FlightInfo → { ticket: Uint8Array | null } (first endpoint's ticket). */
export function decodeFlightInfoTicket(buf) {
  for (const [no, wire, val] of fields(buf)) {
    if (no === 3 && wire === 2) {
      // FlightEndpoint { Ticket ticket = 1 } ; Ticket { bytes ticket = 1 }
      for (const [eno, ewire, eval_] of fields(val)) {
        if (eno === 1 && ewire === 2) {
          for (const [tno, twire, tval] of fields(eval_)) {
            if (tno === 1 && twire === 2) return tval;
          }
        }
      }
    }
  }
  return null;
}

/** FlightData → { dataHeader, dataBody } (fields 2 and 1000). */
export function decodeFlightData(buf) {
  let dataHeader = new Uint8Array(0);
  let dataBody = new Uint8Array(0);
  for (const [no, wire, val] of fields(buf)) {
    if (no === 2 && wire === 2) dataHeader = val;
    else if (no === 1000 && wire === 2) dataBody = val;
  }
  return { dataHeader, dataBody };
}

// --- Arrow IPC stream re-assembly ------------------------------------------

const EOS = Uint8Array.from([0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);

/**
 * One FlightData message → the equivalent Arrow IPC stream chunk:
 * continuation marker + padded metadata length + flatbuffer header + body.
 */
export function flightDataToIpc(dataHeader, dataBody) {
  const headLen = dataHeader.length;
  const pad = (8 - ((headLen + 8) % 8)) % 8;
  const prefix = new Uint8Array(8 + headLen + pad);
  const dv = new DataView(prefix.buffer);
  dv.setUint32(0, 0xffffffff, true);
  dv.setUint32(4, headLen + pad, true);
  prefix.set(dataHeader, 8);
  return dataBody.length ? concatBytes([prefix, dataBody]) : prefix;
}

/** Terminate an IPC stream assembled with flightDataToIpc. */
export function ipcEos() {
  return EOS;
}

export { concatBytes };
