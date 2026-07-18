// gRPC-web -> gRPC bridge for the browser-direct Flight path.
//
// Browsers cannot speak native gRPC (no HTTP/2 trailer access), so "frontend
// queries icegres directly" requires a protocol translator in front of the
// Flight port — in production that is Envoy's grpc_web filter; here it is a
// dependency-free Node bridge speaking the same application/grpc-web+proto
// wire: 5-byte-framed protobuf messages in a POST body, response DATA frames
// (0x00) followed by one trailers frame (0x80). The bridge is transparent:
// message bytes pass through unmodified in both directions (identity
// serializers), so the browser client does all protobuf work itself.
//
// Env: ICEGRES_FLIGHT_ADDR (default 127.0.0.1:50051), PORT (default 8091).

import http from "node:http";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const grpc = require("@grpc/grpc-js");

const PORT = Number(process.env.PORT || 8091);
const FLIGHT_ADDR = process.env.ICEGRES_FLIGHT_ADDR || "127.0.0.1:50051";

const identity = (b) => b;
const client = new grpc.Client(FLIGHT_ADDR, grpc.credentials.createInsecure(), {
  "grpc.max_receive_message_length": 256 * 1024 * 1024,
});

// Methods the bridge exposes, with their streaming shape.
const METHODS = {
  "/arrow.flight.protocol.FlightService/GetFlightInfo": { serverStream: false },
  "/arrow.flight.protocol.FlightService/DoGet": { serverStream: true },
};

const CORS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "POST, OPTIONS",
  "access-control-allow-headers": "content-type, x-grpc-web, x-user-agent",
  "access-control-expose-headers": "grpc-status, grpc-message",
};

function frame(flags, payload) {
  const head = Buffer.alloc(5);
  head.writeUInt8(flags, 0);
  head.writeUInt32BE(payload.length, 1);
  return Buffer.concat([head, payload]);
}

function trailersFrame(status, message) {
  const text = `grpc-status: ${status}\r\ngrpc-message: ${encodeURIComponent(
    message || "",
  )}\r\n`;
  return frame(0x80, Buffer.from(text, "utf8"));
}

/** Extract the first message from the grpc-web framed request body. */
function unframeRequest(body) {
  if (body.length < 5) throw new Error("short grpc-web request");
  const len = body.readUInt32BE(1);
  return body.subarray(5, 5 + len);
}

const server = http.createServer((req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, CORS).end();
    return;
  }
  const method = METHODS[req.url];
  if (req.method !== "POST" || !method) {
    res.writeHead(404, CORS).end();
    return;
  }
  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    let msg;
    try {
      msg = unframeRequest(Buffer.concat(chunks));
    } catch (e) {
      res.writeHead(400, CORS).end(e.message);
      return;
    }
    res.writeHead(200, {
      ...CORS,
      "content-type": "application/grpc-web+proto",
    });
    if (method.serverStream) {
      const call = client.makeServerStreamRequest(
        req.url,
        identity,
        identity,
        msg,
      );
      call.on("data", (payload) => res.write(frame(0x00, payload)));
      call.on("end", () => {
        res.write(trailersFrame(0, ""));
        res.end();
      });
      call.on("error", (err) => {
        res.write(trailersFrame(err.code ?? 2, err.details || err.message));
        res.end();
      });
    } else {
      client.makeUnaryRequest(req.url, identity, identity, msg, (err, out) => {
        if (err) {
          res.write(trailersFrame(err.code ?? 2, err.details || err.message));
        } else {
          res.write(frame(0x00, out));
          res.write(trailersFrame(0, ""));
        }
        res.end();
      });
    }
  });
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(
    `grpc-web bridge on http://127.0.0.1:${PORT} -> grpc ${FLIGHT_ADDR}`,
  );
});
