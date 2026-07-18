// Node-side ZSTD registration for apache-arrow's codec registry, backed by
// node:zlib's native zstd (Node >= 22). Import once before decoding icegres
// Flight results in a Node backend; browsers use "./zstd.js" (fzstd).
import { zstdDecompressSync } from "node:zlib";
import { compressionRegistry, CompressionType } from "apache-arrow";

export function registerZstd() {
  compressionRegistry.set(CompressionType.ZSTD, {
    decode(data) {
      return new Uint8Array(zstdDecompressSync(data));
    },
  });
}

registerZstd();
