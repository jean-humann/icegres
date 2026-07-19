// A minimal, hardened SQL explorer — the browser-writes-queries case.
//
// Talks to the @icegres/flight-proxy SQL GATEWAY (not the allowlist): it
// exchanges an app session for a short-lived token (POST /session), then
// sends arbitrary user SQL under that token (POST /sql) and streams the Arrow
// result. Safety is per-user (authz-scoped principal + the gateway's
// read-only guard + icegres resource limits), NOT SQL restriction. A Stop
// button cancels an in-flight query via AbortController.
import "./zstd-web.js";
import { tableFromIPC } from "apache-arrow";

// The ?gateway= override may only change the scheme/port of the SAME host (or
// localhost): a shared link must not repoint the explorer — and the SQL and
// results that flow through it — at an arbitrary attacker origin.
function resolveGateway() {
  const fallback = `http://${location.hostname}:8093`;
  const raw = new URLSearchParams(location.search).get("gateway");
  if (!raw) return fallback;
  try {
    const u = new URL(raw);
    const sameHost =
      u.hostname === location.hostname ||
      u.hostname === "127.0.0.1" ||
      u.hostname === "localhost";
    if ((u.protocol === "http:" || u.protocol === "https:") && sameHost) {
      return u.origin;
    }
  } catch {
    /* malformed — fall back */
  }
  return fallback;
}
const GATEWAY = resolveGateway();

const $ = (id) => document.getElementById(id);
let token = null;
let inflight = null;

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
}

async function getSession() {
  const r = await fetch(`${GATEWAY}/session`, { method: "POST" });
  if (!r.ok) throw new Error(`session ${r.status}`);
  token = (await r.json()).token;
}

function renderTable(table) {
  const cols = table.schema.fields.map((f) => f.name);
  const rows = table.toArray().slice(0, 500); // cap the DOM, not the query
  const head = cols.map((c) => `<th>${esc(c)}</th>`).join("");
  const body = rows
    .map(
      (r) =>
        `<tr>${cols
          .map((c) => {
            const v = r[c];
            return `<td>${esc(typeof v === "bigint" ? v.toString() : v)}</td>`;
          })
          .join("")}</tr>`,
    )
    .join("");
  $("out").innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  $("status").textContent = `${table.numRows} rows${
    table.numRows > 500 ? " (showing first 500)" : ""
  }`;
}

async function run() {
  if (!token) {
    try {
      await getSession();
    } catch (e) {
      $("status").innerHTML = `<span class="err">no session: ${esc(e)}</span>`;
      return;
    }
  }
  const sql = $("sql").value;
  $("status").textContent = "running…";
  $("run").disabled = true;
  $("stop").disabled = false;
  const t0 = performance.now();
  inflight = new AbortController();
  try {
    const r = await fetch(`${GATEWAY}/sql`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
      body: JSON.stringify({ sql }),
      signal: inflight.signal,
    });
    if (!r.ok) {
      const msg = (await r.json().catch(() => ({}))).error || `HTTP ${r.status}`;
      $("status").innerHTML = `<span class="err">${esc(msg)}</span>`;
      return;
    }
    const buf = new Uint8Array(await r.arrayBuffer());
    const table = tableFromIPC(buf);
    renderTable(table);
    $("timing").textContent = `${(performance.now() - t0).toFixed(0)} ms · ${(
      buf.length / 1024
    ).toFixed(1)} KiB`;
  } catch (e) {
    if (e.name === "AbortError") $("status").innerHTML = `<span class="err">stopped</span>`;
    else $("status").innerHTML = `<span class="err">${esc(e)}</span>`;
  } finally {
    inflight = null;
    $("run").disabled = false;
    $("stop").disabled = true;
  }
}

$("run").addEventListener("click", run);
$("stop").addEventListener("click", () => inflight?.abort());
$("sql").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") run();
});
window.__explorerReady = true;
$("status").textContent = "ready — write SQL, ⌘/Ctrl+Enter to run";
