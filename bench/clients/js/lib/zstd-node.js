// Register a ZSTD decoder with arrow-js's compression registry (Node side).
//
// Deliberately NOT `import "@icegres/flight-web/zstd-node"`: the file:-linked
// package resolves its own apache-arrow copy (clients/flight-web/
// node_modules), so its registration lands on a different registry instance
// than the one this harness decodes with — the browser bundles solve the
// same split with an esbuild alias (build.mjs), but Node module resolution
// has no alias, so the codec must register here against THIS tree's arrow.
import { zstdDecompressSync } from "node:zlib";
import { compressionRegistry, CompressionType } from "apache-arrow";

compressionRegistry.set(CompressionType.ZSTD, {
  decode(data) {
    return new Uint8Array(zstdDecompressSync(data));
  },
});
