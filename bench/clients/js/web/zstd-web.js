// Register a ZSTD decoder with arrow-js's compression registry (browser
// side). Same role as lib/zstd-node.js but backed by fzstd, a small pure-JS
// zstd decoder, since browsers have no native zstd API.
import { decompress } from "fzstd";
import { compressionRegistry, CompressionType } from "apache-arrow";

compressionRegistry.set(CompressionType.ZSTD, {
  decode(data) {
    const out = decompress(data);
    // arrow-js builds typed arrays (e.g. BigInt64Array) directly over this
    // buffer at computed byte offsets; a decoder-returned subarray view with
    // a non-zero byteOffset breaks the 8-byte alignment those constructors
    // require. Re-base to an offset-0 buffer when needed.
    return out.byteOffset === 0 ? out : out.slice();
  },
});
