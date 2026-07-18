// Bundle the browser entry points (apache-arrow + the probe code) into
// self-contained ESM files under dist/, served by proxy/server.js.
import { build } from "esbuild";

await build({
  entryPoints: ["web/dashboard.js", "web/bench-page.js"],
  bundle: true,
  format: "esm",
  outdir: "dist",
  minify: true,
  sourcemap: false,
  logLevel: "info",
});
