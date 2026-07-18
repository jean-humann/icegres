// Register a ZSTD decoder with arrow-js's compression registry (Node side).
// icegres compresses Flight DoGet record batches with ZSTD
// (flight_ipc_options in icegres/src/flight.rs), and apache-arrow only
// throws "codec not found" until a codec is registered. Node 22 ships a
// native zstd in node:zlib.
import { zstdDecompressSync } from "node:zlib";
import { compressionRegistry, CompressionType } from "apache-arrow";

compressionRegistry.set(CompressionType.ZSTD, {
  decode(data) {
    return new Uint8Array(zstdDecompressSync(data));
  },
});
