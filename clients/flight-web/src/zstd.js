// Browser-side ZSTD registration for apache-arrow's codec registry.
//
// icegres compresses Flight result batches with ZSTD by default
// (`flight-serve --result-compression zstd`); apache-arrow JS ships only the
// registry, so import this module once before decoding. Node backends should
// import "./zstd-node.js" instead (native codec, no wasm/JS decode cost).
import { decompress } from "fzstd";
import { compressionRegistry, CompressionType } from "apache-arrow";

export function registerZstd() {
  compressionRegistry.set(CompressionType.ZSTD, {
    decode(data) {
      const out = decompress(data);
      // arrow-js builds typed arrays (BigInt64Array etc.) directly over this
      // buffer at computed offsets; a subarray view with non-zero byteOffset
      // breaks their 8-byte alignment. Re-base when needed.
      return out.byteOffset === 0 ? out : out.slice();
    },
  });
}

registerZstd();
