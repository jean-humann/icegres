# Rust quality posture — enforced safe-Rust invariants

> This document records icegres's safe-Rust posture and, more importantly,
> the mechanisms that keep it from regressing. A Rust-quality audit (graded
> against the "Rewriting Bun in Rust" SOTA thesis) found icegres already
> proper, reviewed Rust: **one** justified `unsafe` in ~44k LOC, **zero**
> serving-path `.unwrap()`, RAII/`Drop` cleanup, `?`/`anyhow` propagation,
> and SIGKILL-tested exactly-once durability. This increment does not change
> that behavior — **it turns those audited facts into compiler- and
> test-enforced invariants that cannot silently regress**, and adds the
> supply-chain governance and parser fuzzing the blog holds up as the bar.
>
> **Zero runtime behavior change.** Everything here is lints, tests,
> governance, and docs. The default serving path is byte-identical; the
> bench A/B is flat. No new runtime dependency is added.

Each section names the Bun-blog SOTA criterion it satisfies.

## 1. `unsafe` is compiler-forbidden except one audited FFI site

**SOTA criterion: minimize `unsafe`; every remaining use is audited and local.**

`icegres/Cargo.toml` carries a package-wide `[lints]` table, so all three bin
targets (`icegres`, `icegresd`, `icekeeperd`) and every module inherit it:

```toml
[lints.rust]
rust_2018_idioms = { level = "deny", priority = -1 }
unsafe_code = "deny"

[lints.clippy]
unwrap_used = "deny"
```

`unsafe_code = "deny"` makes every FUTURE `unsafe` block a compile error. The
crate has exactly one legitimate `unsafe`: the advisory `flock` FFI at
`src/segment.rs` (`libc::flock` on an owned fd, guarding the data directory
against a second process). It carries a local, commented
`#[allow(unsafe_code)]` — the single audited exception. The headline claim
"icegres has almost no unsafe" is now "unsafe is compiler-forbidden except one
reviewed FFI site."

## 2. No serving-path `.unwrap()` — enforced

**SOTA criterion: no panics on the hot path; fallible operations return errors.**

`clippy::unwrap_used = "deny"` permanently forbids `.unwrap()` outside tests.
`icegres/clippy.toml` scopes the deny to non-test code:

```toml
allow-unwrap-in-tests = true
allow-expect-in-tests = true
```

The audit's "zero non-test `.unwrap()`" holds with **no** rewrites and **no**
`#[allow]` in serving code. The one subtlety: clippy's `allow-unwrap-in-tests`
recognizes a bare `#[cfg(test)]`, not the compound `#[cfg(all(test, feature =
"managed"))]` form. The crate's single such module (`src/authz.rs`) is
therefore written as stacked `#[cfg(test)]` + `#[cfg(feature = "managed")]`
attributes — a byte-identical change (same cfg predicate) that lets clippy
exempt its test code. Verified clean under `-D clippy::unwrap_used` across all
three bins, in both default and `--no-default-features` builds.

`expect_used` is deliberately **not** denied: the crate has ~330 intentional
non-test `.expect(...)` calls, almost all `"…lock poisoned"` guards that are a
fail-fast durability choice (see §5) or documented byte-layout invariants
(`try_into().expect("4 bytes")` on a slice already length-checked one line
above). Denying `expect_used` would demand hundreds of `#[allow]`s and mask
nothing — that is churn, not SOTA.

## 3. `rust_2018_idioms` — elided-lifetime hygiene

**SOTA criterion: idiomatic, warning-clean under the edition's lint group.**

The only member that fired is `elided_lifetimes_in_paths`, at five sites. Four
are trivial `<'_>` additions (`&QualScope<'_>`, `Formatter<'_>`). The fifth,
`FileAuthSource::get_password` in `src/pgauth.rs`, is an async-trait method
(`pgwire`'s `AuthSource`) whose lifetime is late-bound; an explicit `<'_>` is a
hard `E0195` error, so that one site carries a commented
`#[allow(elided_lifetimes_in_paths)]`. All other group members
(`bare_trait_objects`, `explicit_outlives_requirements`, `keyword_idents`, …)
were already clean.

### Lints considered and dropped

- **`unreachable_pub`** — dropped. The crate is bins-only (no `lib.rs`), so
  nearly every `pub` item is technically crate-internal; the lint flags ~300
  sites. The fix (`pub` → `pub(crate)`) is behavior-neutral and auto-fixable
  but is a large mechanical diff far over the "handful of justified allows"
  bar — visibility hygiene, not a safety invariant.
- **`clippy::pedantic` / `clippy::nursery`** — not enabled. Noisy; would need
  dozens of allows. Not SOTA, just churn.

The bar for the enabled set: every lint compiles clean under `-D warnings`
with only justified, commented allows.

## 4. Malformed-input-never-panics fuzz harness

**SOTA criterion: parsers of untrusted bytes are fuzzed continuously.**

`src/fuzz.rs` is a deterministic, std-only, in-tree fuzz harness (fixed-seed
SplitMix64 PRNG — **no** `rand`, `arbitrary`, or `libfuzzer`; zero new
dependencies). It drives every untrusted-byte decode entry point with
thousands of random, truncated, bit-flipped, byte-splatted, inserted/deleted,
and oversized inputs (16k iterations each) and asserts each returns `Err` —
never panics, never reads out of bounds, never hangs. A caught panic reports
the seed and iteration for exact reproduction.

The targets, and the untrusted source each guards:

| Decoder | Untrusted source | Coverage |
|---|---|---|
| `segment::scan_frame_bytes` | durable-tail/quorum WAL frames replayed at boot | full |
| `quorum::proto::decode_record_payload` | one log record from a peer | full |
| `quorum::proto::decode_records` | a concatenated record stream from a peer | full |
| `quorum::proto::read_message` (→ `decode_rest`) | framed acceptor-protocol network bytes | full |
| `flight::decode_plan_ticket` | client-supplied Flight-SQL ticket bytes | full |
| `tailapi::TailTicket::from_any` | client-supplied Flight-SQL `Any` ticket (JSON) | full |
| `tail::decode_op_payload` / `tail::decode_payload` | tail op/frame payload bytes | icegres layers only (see boundary below) |

The private decoders are reached through their real boundary, not a wrapper,
so the tests are not vacuous:

- `decode_rest` (private) is reached via `read_message` fed a crc-valid frame
  wrapping arbitrary/mutated `rest`, so the JSON header parse, type dispatch,
  and field extraction all run on attacker bytes.
- `decode_payload` and `decode_plan_ticket` (private to their modules) are
  fuzzed from those modules' own `#[cfg(test)]` blocks, calling the shared
  generators in `crate::fuzz`.

`read_message` is async; the harness polls it to completion against an
in-memory `Cursor` with a std no-op waker (`Waker::noop`) — no runtime, still
std-only.

### The Arrow IPC boundary — a real finding

The tail op/frame decoders (`decode_op_payload`, `decode_payload`) are fuzzed
only over the icegres-owned layers — the format-version byte, the op-kind
discriminant, the `seq` header, and the empty-body path. The **Arrow IPC body
decode** they wrap (`decode_ipc` → Arrow's `StreamReader`) is deliberately
**not** fed adversarial bytes, and the harness documents why rather than
papering over it:

> Arrow's IPC reader is built for TRUSTED internal data and is **not
> adversarial-input-safe**. `arrow_ipc::reader`'s `read_meta_len` accepts any
> positive `i32` with no plausibility cap, and `maybe_next` then `resize`s a
> buffer to that length *before* checking it against the available bytes; a
> record-batch message's `bodyLength` (`i64`) drives an even larger
> allocation. A crafted stream therefore forces an unbounded allocation →
> allocation-failure `abort()`, which **no in-process guard (`catch_unwind`
> included) can recover** — the fuzz harness proved this by aborting on a
> `~2.3 EB` allocation attempt.

This is reachable **only from outside icegres's documented trust model**: a
crc-valid but crafted local WAL frame (needs write access to the flock-guarded
data directory) or a malicious quorum peer record body (semi-trusted network).
The same Arrow reader decodes quorum record bodies during boot replay (the
"validation-only record walk" backlog item). Hardening it needs either a
hardened Arrow (the dependency matrix is pinned — out of scope) or an
out-of-process / bounded-allocator decode (a serving-path change out of scope
for this zero-runtime-change increment). It is recorded as a hardening-backlog
item in `docs/limitations.md`. **This is exactly the kind of finding the
harness exists to surface.**

## 5. Lock-poisoning policy — deliberate fail-fast

**SOTA criterion: state a considered policy for poisoning; don't paper over it.**

icegres uses `std::sync::Mutex`/`RwLock` with `.expect("…poisoned")` at most
lock sites. A `PoisonError` means a thread panicked while holding the lock, so
the protected state may be torn. The policy is a **two-tier, deliberate**
choice, and it is a convention across the crate:

- **Fail-fast (keep `.expect("…poisoned")`)** at every lock guarding a
  durability, tail, quorum, txn, slot, or compiled-plan invariant: the
  write-buffer and table state (`buffer.rs`), the durable-tail seq/watermark
  (`tail.rs`, `tail_pg.rs`, `tail_quorum.rs`), quorum term/election/mirror
  state (`quorum/proposer.rs`, `peer.rs`), the 2PC txn registry (`txn.rs`),
  control-plane slot/lease state (`icegresd.rs`), and the compiled-plan caches
  (`plancache.rs`, `cache.rs` pinned map). A panic under any of these is
  already a bug; **continuing on maybe-torn durability or serving state would
  be worse than crashing.** Propagating the poison is the correct, honest
  behavior — the process restarts and replays from the durable log.

- **Recover (`freshness::recover`)** only where a torn value is provably
  harmless. The crate already implements this tier: `freshness::recover<G>`
  (`src/freshness.rs`) does `unwrap_or_else(|e| { note_poisoned(what);
  e.into_inner() })` with rate-limited telemetry, and is used on the
  generation-guarded read cache (`cache.rs`), the freshness `last_ok`/registry
  gauges, and the peer-age metric gauge (`metrics.rs`). Those are non-invariant
  observability/gauge values; recovering the guard cannot serve corrupt data.

This increment adds **no** conversions: the provably-safe sites are already on
`recover()`, and every remaining `.expect("…poisoned")` is fail-fast-by-design.
`parking_lot` (which has no poisoning) stays rejected — it is a dependency and
a behavioral change, already benchmarked out (`Cargo.toml`).

When adding a lock: default to `.expect("<what> lock poisoned")` (fail-fast).
Use `freshness::recover` only for a gauge/counter/cache whose torn value a
reviewer confirms cannot propagate corrupt invariant state.

## 6. Supply-chain governance — `cargo-deny`

**SOTA criterion: audited dependencies, licenses, and advisories in CI.**

`icegres/deny.toml` mirrors the ASF-grade governance the audit credited
iceberg-rust for. `cargo deny check` passes (advisories, licenses, bans,
sources) and is a gate.

- **Licenses**: an explicit allowlist covering the actual 529-package graph
  (Apache-2.0, MIT, the BSD family, ISC, Zlib, Unicode-3.0, MPL-2.0,
  CDLA-Permissive-2.0, and the handful of single-package permissive licenses
  it pulls in). Any new dependency under a license not on the list fails the
  gate.
- **Bans**: `multiple-versions = "allow"` with a comment naming the ~26
  duplicated crates — all from the datafusion/arrow/opendal/tonic transitive
  stack (`hashbrown`, `rand`, `getrandom`, …). They are documented as expected,
  not pretended absent.
- **Advisories**: `yanked = "deny"`; the five advisories currently on the
  graph are **known-accepted, pending a pinned-matrix bump**, each `ignore`d
  with a one-line reason (two `quick-xml` DoS via `opendal` S3 XML, one
  `crossbeam` Debug-fmt deref, and two unmaintained proc-macro/wrapper crates).
  None is fixable without moving the pinned matrix, which is out of scope here.
  See `docs/limitations.md` for the operator-facing caveat (the `quick-xml`
  pair is the most real: attacker-influenced S3 XML).

If the pinned/offline CI cannot install `cargo-deny`, the gate is skipped with
a documented posture (as `tests/helm.sh` does for `helm`) — the `deny.toml`
itself is the durable artifact.

## Regeneration / gates

The invariants above are enforced by the standard gate run:

- `cargo clippy --release --all-targets -- -D warnings` — now includes
  `deny(unsafe_code)`, `deny(unwrap_used)`, and `deny(rust_2018_idioms)`.
- `cargo test --release` — includes the fuzz harness (`fuzz_*` tests).
- `cargo deny check` — advisories/licenses/bans/sources.

Because every applied edit is an attribute, a lifetime-elision token, or
test-only code, the release binary is byte-identical and the bench A/B stays
flat.
