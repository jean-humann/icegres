#!/usr/bin/env node
// Standalone runner: load a query-registry module and serve the BFF.
//
//   icegres-flight-proxy ./queries.js
//
// The registry module default-exports the named-query map (see README).
// Env: FLIGHT_ADDR (default 127.0.0.1:50051), FLIGHT_TLS=1, PORT (8090),
//      HOST (127.0.0.1), CORS_ORIGIN (*), FLIGHT_USER / FLIGHT_PASSWORD.
import path from "node:path";
import { pathToFileURL } from "node:url";
import { serve } from "../src/index.js";

const arg = process.argv[2];
if (!arg) {
  console.error("usage: icegres-flight-proxy <registry.js>");
  process.exit(2);
}
const registryUrl = pathToFileURL(path.resolve(arg)).href;
const mod = await import(registryUrl);
const queries = mod.default ?? mod.queries;
if (!queries) {
  console.error(`registry module ${arg} must default-export the query map`);
  process.exit(2);
}

const credentials =
  process.env.FLIGHT_USER && process.env.FLIGHT_PASSWORD
    ? { username: process.env.FLIGHT_USER, password: process.env.FLIGHT_PASSWORD }
    : undefined;

const port = Number(process.env.PORT || 8090);
const host = process.env.HOST || "127.0.0.1";
await serve(
  {
    queries,
    flight: {
      address: process.env.FLIGHT_ADDR || "127.0.0.1:50051",
      tls: process.env.FLIGHT_TLS === "1",
      credentials,
    },
    corsOrigin: process.env.CORS_ORIGIN || "*",
  },
  { port, host },
);
console.log(
  `icegres-flight-proxy on http://${host}:${port} -> flight ${
    process.env.FLIGHT_ADDR || "127.0.0.1:50051"
  } (${Object.keys(queries).length} named queries)`,
);
