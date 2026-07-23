# Catalog support: serving any Iceberg REST catalog

icegres talks to its lakehouse through a **stock Iceberg REST catalog client**
(`iceberg-catalog-rest 0.10.0`, driven by `RestCatalogBuilder` in
`icegres/src/context.rs`). Nothing in the read path is Lakekeeper-specific: the
`.load("lakekeeper", …)` string is a cosmetic name label, not a coupling. This
document is the honest, endpoint-by-endpoint account of **which REST surface
icegres relies on**, **which auth flows it now supports** (P6/Half B) vs which
are **blocked at our pin**, and a **per-catalog status** where every claim is
labeled `proven-live` or `by-construction`.

Companion docs: `docs/limitations.md` (multi-table-transaction degradation,
freshness/tail edges), `docs/roadmap-v2-beyond-lakebase.md` §P6.

---

## 1. The REST surface icegres uses

All endpoints below are **Iceberg REST spec-standard** (the OpenAPI in
`apache/iceberg` `open-api/rest-catalog-open-api.yaml`). icegres uses **no
Lakekeeper-proprietary endpoint**. The one optional-in-spec endpoint it *can*
use — `transactions/commit` — is capability-probed and degrades cleanly when
absent (see §4).

| Endpoint | Spec status | Used by icegres for | Client |
|---|---|---|---|
| `GET /v1/config?warehouse=` | standard (ConfigApi) | prefix + `endpoints` capability discovery | read + write |
| `GET /v1/{prefix}/namespaces` | standard | namespace enumeration (session build) | read |
| `POST /v1/{prefix}/namespaces` | standard | `CREATE SCHEMA` / seed | read |
| `GET /v1/{prefix}/namespaces/{ns}` | standard | namespace load | read |
| `HEAD/DELETE /v1/{prefix}/namespaces/{ns}` | standard | existence / drop | read |
| `GET /v1/{prefix}/namespaces/{ns}/tables` | standard | table enumeration | read |
| `POST /v1/{prefix}/namespaces/{ns}/tables` | standard | `CREATE TABLE` / seed | read |
| `GET /v1/{prefix}/namespaces/{ns}/tables/{t}` | standard | table metadata load (every scan/commit anchors here) | read + write |
| `POST /v1/{prefix}/namespaces/{ns}/tables/{t}` | standard | single-table copy-on-write commit (`updateTable`) | **write** |
| `DELETE …/tables/{t}?purgeRequested=` | standard | `DROP TABLE` / maintenance | read |
| `POST /v1/{prefix}/tables/rename` | standard | rename | read |
| `POST /v1/{prefix}/namespaces/{ns}/register` | standard | register-table | read |
| `POST /v1/{prefix}/transactions/commit` | standard but **OPTIONAL** | atomic multi-table COMMIT / whole-lakehouse branches | **write** |
| `POST /v1/oauth/tokens` (or `oauth2-server-uri`) | standard (OAuth2) | client-credentials token minting | read |

**Two clients, one catalog.** The read/metadata plane is the vendored
iceberg-rust REST client. The copy-on-write **DML commit** plane
(`icegres/src/overwrite.rs`) issues its **own** REST calls (config discovery,
per-table `updateTable`, `transactions/commit`) because it builds a custom
`CommitTableRequest` the high-level API does not expose. Both speak the same
spec endpoints; the auth consequences of the split are in §3.

---

## 2. Auth flows — supported

Auth was the one real coupling to an **open** (unauthenticated) Lakekeeper. P6
threaded the Iceberg REST client's existing auth props through `CatalogOpts`
(`icegres/src/main.rs`) into the catalog props map (`context.rs
apply_catalog_auth`). **Zero new dependencies** — OAuth2 is already vendored in
`iceberg-catalog-rest 0.10.0`; the props are plain strings. When **no** auth flag
is set the props map is **byte-identical** to before, so the default open
Lakekeeper path is untouched (invariant I3).

| Flag / env | Iceberg REST prop | Flow | Status |
|---|---|---|---|
| `--catalog-token` / `ICEGRES_CATALOG_TOKEN` | `token` | pre-minted static bearer, sent verbatim on every call | **proven-live** (read + write) |
| `--catalog-credential` / `ICEGRES_CATALOG_CREDENTIAL` | `credential` (`client_id:client_secret`) | OAuth2 **client-credentials** grant; client mints + refreshes the bearer | **proven-live** (read/metadata plane) |
| `--catalog-oauth2-uri` / `ICEGRES_CATALOG_OAUTH2_URI` | `oauth2-server-uri` | override the token endpoint (else `{uri}/v1/oauth/tokens`) | proven-live |
| `--catalog-scope` / `ICEGRES_CATALOG_SCOPE` | `scope` | OAuth2 scope in the grant (else the client default `catalog`) | proven-live |

Also available **in the pinned crate** (not yet surfaced as an icegres flag):
arbitrary static header injection via `header.<name>` props (`catalog.rs`
`extra_headers`) — enough for API-key / custom-bearer schemes that ride a fixed
header.

### Secrets are redacted
`--catalog-token` and `--catalog-credential` carry secrets. `CatalogOpts` has a
**hand-written `Debug`** that renders those two fields as `<redacted>` (a stray
`info!(?opts)` / `{:?}` cannot spill them), and the static bearer threaded into
the write client's HTTP headers is marked `sensitive`. The pinned crate already
redacts `Authorization` (its `SENSITIVE_HEADERS`). An e2e assertion greps the
server logs to confirm neither the bearer nor the client secret ever appears
(`icegres/tests/e2e.sh`, section `(cat)`).

### The write-plane caveat under *pure* client-credentials
The DML commit client (`overwrite.rs`) authenticates via the static **`token`**
prop only. Under `--catalog-token` it carries the bearer on config discovery
and every commit, so **reads and writes are both authenticated** — this is the
path the end-to-end CRUD proof uses. The OAuth2 **client-credentials** grant is
**not** duplicated into the commit client (house rule: no hand-rolled auth; and
iceberg-rust 0.10.0 does not expose its internal token provider to reuse). So
under a *pure* `--catalog-credential` deployment the read/metadata/DDL/
time-travel plane is fully authenticated, but the copy-on-write **data commit**
plane is not. Two honest options today:

- serve **read-mostly** workloads on the credential flow (fully authenticated), or
- pass a longer-lived **`--catalog-token`** alongside for the write plane.

**Re-check trigger:** when iceberg-rust exposes its OAuth2 token provider (or a
bumped pin lets the commit client share the read client's session), thread it
into `overwrite.rs` so client-credentials authenticates writes too. Record the
version that first exposes it.

### `icegres verify` under an auth-guarded catalog
`icegres verify` (`src/verify.rs`) is wired for the same auth surface as `serve`:

- **Scratch-server spawning.** Each scratch `icegres serve` child verify
  launches is given the four `--catalog-token` / `--catalog-credential` /
  `--catalog-oauth2-uri` / `--catalog-scope` flags for every auth opt the
  operator set (only-when-set), so the children authenticate against a guarded
  catalog. This holds whether auth was supplied as a **flag** or as an
  `ICEGRES_CATALOG_*` **env var** (env vars are inherited by the children;
  before this fix only the env-var form worked).
- **Cleanup.** The scratch namespace + tables are dropped through the
  **authenticated catalog client**, not a raw REST DELETE, so cleanup succeeds
  under both `--catalog-token` and `--catalog-credential` — the run keeps its
  create-test-**drop** contract against a guarded catalog (a raw unauthenticated
  DELETE would 401 and strand the scratch namespace). The catalog client's
  `drop_table` does not request an object-store purge, so the dropped tables'
  data files are left to the object store's own lifecycle (out of verify's reach
  — `docs/limitations.md`).
- **Write-based suites under *pure* credential auth.** verify's suites INSERT
  rows, so they hit the **same** write-plane caveat above: with
  `--catalog-credential` and **no** `--catalog-token`, every write-based suite
  **SKIPs loudly** (naming the fix) rather than failing confusingly. Supply
  `--catalog-token` to re-prove the durability/exactly-once/fencing/freshness/
  failover claims against such a catalog. The config-open discovery handshake
  (`GET /v1/config`) stays reachable per Iceberg-REST convention, so this is not
  treated as a startup failure.

---

## 3. Auth flows — blocked at the pin

### AWS SigV4 / AWS Glue REST — blocked
AWS Glue's Iceberg REST endpoint is IAM-signed (AWS SigV4), not bearer-token.
The pinned `iceberg-catalog-rest 0.10.0` has **no SigV4 support**: a full-crate
grep for `sigv4 | signing-region | signing-name | signing_region | aws` over
`catalog.rs`, `client.rs`, `lib.rs`, `types.rs` returns **nothing** — there are
no `rest.sigv4-enabled` / `signing-region` / `signing-name` prop constants and
no request-signing code path. The crate's only auth mechanisms are OAuth2 bearer
(§2) plus static `header.<name>` injection, none of which produce a valid SigV4
signature. **We do not hand-roll SigV4** (house rule).

**Verdict:** Glue-via-REST is **unreachable at `=0.10.0`** (re-checked on the
0.9.1 → 0.10.0 bump: the grep still returns nothing).

**Re-check trigger** (same shape as the P2 DV finding): whenever the pinned
`iceberg-catalog-rest` is bumped past 0.10.0, re-run the grep for
`sigv4|signing-region|signing-name`. If those constants / code appear upstream,
Glue becomes reachable — B1 can then add the SigV4 props (`rest.sigv4-enabled`,
`rest.signing-region`, `rest.signing-name`) exactly as it added the OAuth2 props.
Record the exact version that introduces them.

---

## 4. Multi-table transactions: capability, not coupling

Atomic multi-table COMMIT and whole-lakehouse branch ops need
`POST /v1/{prefix}/transactions/commit`, which is **spec-standard but optional**.
icegres seeds support from the config `endpoints` list and, when absent, runs a
**data-free probe** once (a spec-shaped identifier-less change: a supporting
catalog answers `400 TableIdentifierRequiredForCommitTransaction`; a
non-supporting one answers `404/405/501`). Without it, single-table commits
still work and multi-table COMMITs degrade per `ICEGRES_TXN_STRICT` (see
`docs/limitations.md`). This path is **already catalog-agnostic** and needs no
per-catalog code.

---

## 5. Per-catalog status

Every row is labeled. `proven-live` = exercised by the running test suite on
this box. `by-construction` = uses only the spec surface + an auth flow icegres
supports, but not stood up here.

| Catalog | REST-spec | Auth it needs | Status |
|---|---|---|---|
| **Lakekeeper** | yes (incl. `transactions/commit`) | open, or OAuth2 / bearer | **proven-live** — the entire e2e suite, tail-durability, HA, and the scale bench run against it |
| **OAuth2 gateway harness** (`bench/clients/catalog-gateway`) | yes (proxies Lakekeeper) | OAuth2 client-credentials + bearer, **enforced** | **proven-live (by-construction)** — a spec-conformant OAuth2 front door that genuinely 401s unauthenticated calls; proves both B1 auth props end to end (§6). NOT a second Iceberg engine. |
| **Apache Polaris** | yes | OAuth2 client-credentials | **by-construction, UNTESTED here** — Polaris cannot be built on this box: its wrapper pins **Gradle 9.6.1**, whose distribution download is **denied by the agent proxy** (`downloads.gradle.org:443` CONNECT rejected); system Gradle 8.14.3 fails Polaris's Kotlin build-logic, and there is no docker daemon for its integration path. Its OAuth2 client-credentials flow is exactly the `credential` path proven against the gateway. |
| **AWS Glue REST** | yes | **AWS SigV4** | **BLOCKED-AT-PIN** — no SigV4 in `iceberg-catalog-rest 0.10.0` (§3). Re-check on any pin bump. |
| **Nessie (Iceberg REST)** | yes | bearer / OAuth2 | **by-construction, untested** — supported via `--catalog-token` / `--catalog-credential`. |
| **Databricks Unity Catalog (Iceberg REST)** | yes (read-centric) | bearer (PAT / OAuth) | **by-construction, untested** — bearer via `--catalog-token`. Write surface varies by Unity tier. |
| **Tabular / R2 Data Catalog / Gravitino / other REST** | yes | bearer / OAuth2 | **by-construction, untested** — any REST-spec catalog with bearer or client-credentials auth. |

**Honesty note.** The gateway is a **fallback**: an auth harness fronting the
*real* Lakekeeper, chosen because Apache Polaris (the preferred genuine second
implementation) is proven-infeasible on this box. It is labeled
`by-construction` / `spec-conformant-auth-harness`; we do **not** claim "proven
against Polaris." What it *does* prove is real: a server that rejects
unauthenticated catalog calls, spoken to over the exact OAuth2 client-
credentials wire flow the pinned client uses.

---

## 6. The proof (reproducible)

`bench/clients/catalog-gateway/` is a ~200-line Go program (stdlib only:
`net/http` + `httputil.ReverseProxy`, no dependencies). It fronts Lakekeeper on
`:8182` and:

- **`POST /v1/oauth/tokens`** — open; validates a form
  `grant_type=client_credentials` + `client_id` + `client_secret` (+ `scope`),
  mints an opaque bearer, returns `{access_token, token_type:"Bearer",
  expires_in:3600}`; bad creds → `401`.
- **`GET …/v1/config`** — open (the discovery handshake that advertises the
  token endpoint).
- **everything else** — requires `Authorization: Bearer <minted>` or `401`;
  valid tokens reverse-proxy to `http://127.0.0.1:8181`.

The e2e leg (`icegres/tests/e2e.sh` section `(cat)`, skips cleanly if `go` is
absent) asserts:

1. unauthenticated / bad-bearer / bad-client-secret calls → `401`; config open → `200`.
2. **`token` prop, full CRUD through the front door**: create namespace
   (authenticated `curl`), `CREATE TABLE`, `INSERT` (write client bearer),
   `SELECT`, and `AS OF <snapshot>` time-travel — reads **and** copy-on-write
   writes authenticate.
3. **`credential` prop, OAuth2 client-credentials**: a second server mints its
   **own** bearer from the token endpoint and serves reads + time-travel (the
   gateway log confirms the grant fired).
4. no secret (bearer / client secret) appears in any server log.

Run it standalone:

```sh
# stack up (Lakekeeper :8181, RustFS :9000, PG :5433)
bash infra/scripts/up.sh
# then run the full e2e; the (cat) section builds + drives the gateway
bash icegres/tests/e2e.sh
```

Manual smoke (mint + serve through the gateway):

```sh
go build -o /tmp/catalog-gateway ./bench/clients/catalog-gateway
/tmp/catalog-gateway -listen 127.0.0.1:8182 -backend http://127.0.0.1:8181 &
TOKEN=$(curl -s -X POST http://127.0.0.1:8182/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=icegres -d client_secret=supersecret \
  -d scope=catalog | jq -r .access_token)
# credential flow (OAuth2, read/metadata plane):
icegres serve --port 5501 --catalog-uri http://127.0.0.1:8182/catalog \
  --catalog-credential icegres:supersecret \
  --catalog-oauth2-uri http://127.0.0.1:8182/v1/oauth/tokens --catalog-scope catalog
# token flow (full CRUD incl. writes):
icegres serve --port 5500 --catalog-uri http://127.0.0.1:8182/catalog --catalog-token "$TOKEN"
```
