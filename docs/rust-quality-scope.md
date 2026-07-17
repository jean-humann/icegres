# Scope: SOTA-grade safe-Rust hardening

A Rust-quality audit (against Bun's "Rewriting Bun in Rust" SOTA thesis)
graded icegres proper, reviewed Rust already: **1** justified `unsafe` in
~44k LOC (segment.rs:166 `flock`), **0** serving-path `.unwrap()`, RAII/
`Drop` cleanup, `?`/`anyhow` propagation, SIGKILL-tested exactly-once. This
increment does not "fix" icegres — it **turns those audited facts into
compiler- and test-enforced invariants that cannot regress**, and adds the
governance + fuzz-resistance the blog holds up as the bar. **Zero runtime
behavior change**: lints, tests, governance, and docs only (plus, at most,
provably-safe poison-recovery at non-invariant lock sites).

## Deliverables

### 1. Compiler-enforced safety invariants (`[lints]` table, package-wide)
`icegres/Cargo.toml` gains a `[lints]` table (edition 2021, toolchain
1.96.1 supports it) so all three bin targets + every module inherit:
- `[lints.rust] unsafe_code = "deny"` — the ONE legitimate `unsafe`
  (segment.rs:166 `libc::flock`) gets a local `#[allow(unsafe_code)]`
  with a comment. Every FUTURE `unsafe` becomes a compile error. This is
  the headline: "we have almost no unsafe" becomes "unsafe is
  compiler-forbidden except one audited FFI site."
- `[lints.clippy] unwrap_used = "deny"` — the audit found zero non-test
  `.unwrap()`; recon must CONFIRM that (fix or `#[allow]`-with-reason any
  stray) so this compiles clean, permanently enforcing "no serving-path
  unwrap."
- `clippy.toml`: `allow-unwrap-in-tests = true`, `allow-expect-in-tests =
  true` so the denies scope to non-test code only.
- A curated, CLEAN-COMPILING set of additional lints (recon picks the
  ones that pass or need only a handful of justified allows):
  `unreachable_pub`, `rust_2018_idioms` (rust); a conservative clippy
  slice. Do NOT blanket-enable `clippy::pedantic`/`nursery` (noisy, would
  need dozens of allows — not SOTA, just churn). `expect_used` is NOT
  denied (330 deliberate poison/invariant expects — see §4).
- Any lint that would require touching >~5 sites or masking a real
  concern is dropped from the set with a one-line reason. The bar: every
  enabled lint compiles clean under `-D warnings` with only justified,
  commented allows.

### 2. Supply-chain governance (`deny.toml` + cargo-deny)
Mirror the ASF-grade governance the audit credited iceberg-rust for:
- `icegres/deny.toml` (or repo root): RUSTSEC advisories (deny
  vulnerabilities/unmaintained), license allowlist (Apache-2.0/MIT/BSD/…
  covering the actual graph — recon enumerates), and a bans policy that
  documents the KNOWN transitive duplicates (hashbrown, rand, getrandom,
  syn, indexmap — all from the datafusion/arrow stack) as allowed rather
  than pretending they're absent.
- Install `cargo-deny` (dev tool, NOT a runtime dep) and run `cargo deny
  check` as a gate; if the pinned/offline env can't install it, vendor
  the invocation + document the skip-if-unavailable posture (like
  tests/helm.sh does for helm).

### 3. Malformed-input-never-panics harness (std-only, zero new deps)
The blog fuzzes parsers 24/7. icegres decodes UNTRUSTED bytes on several
paths — the durable-tail frame decoder (crc32-framed, version/op
discriminants, IPC payload), the Flight `protobuf-Any` ticket parser
(tailapi.rs / flight.rs), and segment record decode. Add a deterministic,
std-only in-tree harness (a `#[test]` with a fixed-seed PRNG, thousands of
iterations of random + adversarial/truncated/bit-flipped/oversized inputs)
asserting each decoder returns `Err` (never panics, never hangs, never
reads OOB). Recon enumerates the exact decode entry points; the harness
must genuinely reach them (a reviewer checks it isn't vacuous). This is
the highest-value safety addition: it hardens the actual attack surface.

### 4. Poison-cascade policy (document; harden only provably-safe sites)
The one smell the audit named: `std::sync::Mutex` + `.expect("poisoned")`
means a panic while holding a lock cascades a panic to other lockers.
This is a DELIBERATE fail-fast choice for durability/tail/quorum state (a
panic under those locks is itself a bug; continuing on maybe-corrupt
state would be worse) — DOCUMENT that policy crate-wide (a comment
convention + the quality doc). For clearly NON-invariant, recoverable
sites (e.g. metrics gauges, pure counters where a torn value is harmless),
recon may convert `.expect("poisoned")` → `.unwrap_or_else(|e|
e.into_inner())` (poison-recovery) — but ONLY where a reviewer confirms
recovering the guard cannot propagate corrupt invariant state. Default to
NOT changing a site if unsure. `parking_lot` stays rejected (dep +
behavioral change, already benchmarked out — Cargo.toml:70-74).

### 5. Docs
`docs/rust-quality.md`: the safe-Rust posture — the audit's findings, the
now-enforced invariants (deny unsafe/unwrap), the fuzz harness and what it
covers, the governance (deny.toml), and the poison-fail-fast policy — each
mapped to the Bun-blog SOTA criteria it satisfies. README "Architecture
notes" gains a one-line safe-Rust posture reference. limitations.md notes
the deliberate poison-fail-fast tradeoff.

## Constraints
Invariants I1–I4. **Zero runtime behavior change** — this is the whole
point; the bench A/B MUST be flat and the default path byte-identical
(lints/tests/governance don't affect codegen; any poison-recovery site
must be provably behavior-neutral on the non-poison path, which it is —
`.expect()` and `.unwrap_or_else(into_inner)` are identical when not
poisoned). ZERO new RUNTIME dependencies (std-only fuzz harness;
cargo-deny is a dev tool). Pinned matrix untouched.

## Gates
fmt/clippy `-D warnings` (now WITH the new denies — the real test: does
the tree compile clean under deny(unsafe_code)+deny(unwrap_used)?) →
cargo test --release (395 + new fuzz-lite tests) → tail_durability (71) →
FULL e2e (295) → tests/helm.sh (103) → `cargo deny check` (or documented
skip) → bench A/B vs `scratchpad/icegres-pre-sota` (drift-controlled,
MUST be flat) → a11 + parity. Adversarial review: does any enabled lint
over-`allow` and thus mask a real issue? is the fuzz harness vacuous or
does it reach the real decoders? is any poison-recovery site actually
unsafe (could serve corrupt invariant state)? Fix-or-revert per house
rule.
