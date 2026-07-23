import { EventEmitter } from "node:events";
import { test } from "node:test";
import assert from "node:assert/strict";

import { queryToIpc } from "../src/flight.js";
import { streamArrow } from "../src/http.js";

class FakeResponse extends EventEmitter {
  constructor() {
    super();
    this.headersSent = false;
    this.writableEnded = false;
    this.destroyed = false;
    this.chunks = [];
    this.firstWrite = true;
  }

  writeHead() {
    this.headersSent = true;
  }

  write(chunk) {
    this.chunks.push(chunk);
    if (this.firstWrite) {
      this.firstWrite = false;
      return false;
    }
    return true;
  }

  end() {
    this.writableEnded = true;
  }

  destroy() {
    this.destroyed = true;
  }
}

test("streamArrow waits for drain before accepting another chunk", async () => {
  const res = new FakeResponse();
  let firstResolved = false;
  const streaming = streamArrow(
    res,
    {},
    async (write) => {
      const pending = write(Buffer.from("first")).then(() => {
        firstResolved = true;
      });
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(firstResolved, false);
      res.emit("drain");
      await pending;
      await write(Buffer.from("second"));
    },
    () => {},
  );
  await streaming;
  assert.equal(firstResolved, true);
  assert.equal(res.writableEnded, true);
  assert.deepEqual(res.chunks.map(String), ["first", "second"]);
});

function flightMocks() {
  const call = new EventEmitter();
  call.pauses = 0;
  call.resumes = 0;
  call.cancelled = false;
  call.pause = () => call.pauses++;
  call.resume = () => call.resumes++;
  call.cancel = () => {
    call.cancelled = true;
  };
  const client = {
    GetFlightInfo(_descriptor, _meta, callback) {
      callback(null, { endpoint: [{ ticket: Buffer.from("ticket") }] });
    },
    DoGet() {
      return call;
    },
  };
  return { call, connection: { client, meta: {} } };
}

test("queryToIpc pauses gRPC until an async sink drains", async () => {
  const { call, connection } = flightMocks();
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  let chunks = 0;
  const query = queryToIpc(connection, "select 1", async () => {
    chunks++;
    if (chunks === 1) await gate;
  });
  call.emit("data", { data_header: Buffer.alloc(0), data_body: Buffer.alloc(0) });
  assert.equal(call.pauses, 1);
  assert.equal(call.resumes, 0);
  release();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(call.resumes, 1);
  call.emit("end");
  await query;
  assert.equal(chunks, 2); // data + IPC EOS
});

test("queryToIpc cancels DoGet when the browser aborts", async () => {
  const { call, connection } = flightMocks();
  const abort = new AbortController();
  const query = queryToIpc(connection, "select 1", async () => {}, {
    signal: abort.signal,
  });
  abort.abort();
  await assert.rejects(query, /browser connection closed/);
  assert.equal(call.cancelled, true);
});

test("queryToIpc cancels GetFlightInfo when the browser aborts during planning", async () => {
  const infoCall = {
    cancelled: false,
    cancel() {
      this.cancelled = true;
    },
  };
  const client = {
    GetFlightInfo() {
      return infoCall; // deliberately never invokes the planning callback
    },
    DoGet() {
      throw new Error("DoGet must not start after an early abort");
    },
  };
  const abort = new AbortController();
  const query = queryToIpc({ client, meta: {} }, "select 1", async () => {}, {
    signal: abort.signal,
  });
  abort.abort();
  await assert.rejects(query, /browser connection closed/);
  assert.equal(infoCall.cancelled, true);
});
