# Contributing to icegres

The operating contract for changes — branch discipline, commit format, and the
pre-merge gates — lives in [`CLAUDE.md`](CLAUDE.md). It is written for both
human and AI contributors and is enforced by `scripts/verify-history.sh`;
everything below is a human-friendly summary, and `CLAUDE.md` wins on detail.

## The short version

1. **Branch**: develop on a feature branch, never on `main`. Keep history
   linear (rebase, don't merge); one concern per commit.
2. **Commits**: Conventional Commits with a body —
   `type(scope): summary` (≤ 72 chars, imperative), then a short paragraph on
   *what* and *why*. Run `bash scripts/verify-history.sh` before pushing; it
   enforces the full rules (including required trailers) and must exit 0.
3. **Gates** (all must be green before merge):
   - `cargo fmt --check`
   - `cargo clippy --release --all-targets -- -D warnings`
   - `cargo test --release` against the live local stack
     (`bash infra/scripts/up.sh` brings it up)
   - `icegres/tests/e2e.sh`, `icegres/tests/tail_durability.sh`, and
     `tests/helm.sh` where the change touches those areas
   - `cargo deny check`
4. **Review**: a fresh-eyes review (independent of the implementer) must be
   clean before merge. On any regression: fix or revert — never merge over a
   known break.
5. **Do not bump the pinned dependency matrix** (`iceberg-rust` / DataFusion /
   arrow / `rust-toolchain.toml`) as a side effect — it is deliberately pinned
   and moves only as a coordinated change.

## Getting a dev environment

```bash
bash infra/scripts/up.sh        # Lakekeeper + RustFS + Postgres, local
cargo build --release --bins    # in icegres/
icegres seed && icegres serve   # demo data + pgwire on :5439
```

See [`icegres/README.md`](icegres/README.md) for the full tour,
[`docs/configuration.md`](docs/configuration.md) for every knob, and
[`docs/limitations.md`](docs/limitations.md) before filing behavior issues —
many deliberate non-goals are documented there with their rationale.

## Security issues

Never report suspected vulnerabilities in public issues — see
[`SECURITY.md`](SECURITY.md).
