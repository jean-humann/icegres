// Bundle the browser entry points (apache-arrow + the probe code) into
// self-contained ESM files under dist/, served by proxy/server.js.
import { build } from "esbuild";
import path from "node:path";

await build({
  entryPoints: ["web/dashboard.js", "web/bench-page.js"],
  bundle: true,
  format: "esm",
  outdir: "dist",
  minify: true,
  sourcemap: false,
  logLevel: "info",
  // The @icegres/flight-web file: dependency carries its own dev copy of
  // apache-arrow; without deduping, its zstd codec registers on that copy
  // while the pages decode with this one — "codec not found".
  alias: { "apache-arrow": path.resolve("node_modules/apache-arrow") },
});
