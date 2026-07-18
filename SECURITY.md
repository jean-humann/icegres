# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via **GitHub's private
vulnerability reporting** on this repository (Security tab → "Report a
vulnerability"). Do not open a public issue for anything you believe is
exploitable. You should receive an acknowledgement within a few days; please
include reproduction steps and the commit/version you tested.

## Security model (summary)

The full posture lives in [`docs/deployment.md`](docs/deployment.md) §7 and
[`docs/rust-quality.md`](docs/rust-quality.md); the operator knobs are in
[`docs/configuration.md`](docs/configuration.md). The short version:

- **Secure by default at the edge.** Binding a non-loopback address without
  `--auth-file` is refused at startup unless `--insecure` explicitly
  acknowledges it. With `--auth-file`, pgwire authenticates via SCRAM-SHA-256
  and Flight SQL via a basic-auth handshake (per-source-IP failed-auth
  backoff); `--authz-file` adds per-table ReBAC (denials are SQLSTATE 42501).
- **TLS** is in-process on both listeners (`--tls-cert`/`--tls-key` on `serve`
  and `flight-serve`); catalog credentials/tokens are redacted from logs.
- **Trusted-network components.** `icekeeperd` (quorum acceptor) and the
  icegresd→compute path are plain TCP with no auth/TLS by design — deploy them
  on a trusted network segment only, as their docs state.
- **Code posture.** Package-wide `deny(unsafe_code)` (one audited `flock` FFI
  exception), `deny(clippy::unwrap_used)`, a malformed-input-never-panics fuzz
  harness over every untrusted-byte decoder, and `cargo-deny` supply-chain
  governance in the pre-merge gates.

## Scope notes for researchers

Findings we consider in scope include: authentication/authorization bypasses,
SQLSTATE-42501 policy escapes, panics reachable from untrusted network bytes
(pgwire, Flight, tail-api, icekeeperd framing), and durability-claim breaks
(acked-then-lost writes under the documented failure model). The documented
trust assumptions above (plain-TCP quorum/control-plane links on a trusted
network) are not vulnerabilities by themselves.
