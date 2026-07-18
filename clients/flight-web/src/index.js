// @icegres/flight-web — browser Arrow Flight SQL client for icegres.
//
// Importing the package root registers the browser ZSTD codec (fzstd) and
// exports the client. Node backends that prefer the native codec can import
// "@icegres/flight-web/zstd-node" before (or instead of) this side effect —
// last registration wins.
import "./zstd.js";

export { FlightWebClient, FlightError } from "./client.js";
export { registerZstd } from "./zstd.js";
