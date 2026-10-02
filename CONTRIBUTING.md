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

### Reusing a live test stack

`ICEGRES_E2E_EXTERNAL_STACK=1 bash icegres/tests/e2e.sh` uses an already running
local stack without starting or stopping its containers. Use an isolated test
warehouse and database. The suite creates fixtures and exercises crashes.

By default, the suite builds all three release binaries. To test a fixed build,
set `ICEGRES_E2E_BIN_DIR` to a directory containing executable `icegres`,
`icegresd` and `icekeeperd` binaries. The suite checks all three before running.

The SQLAlchemy 2 ORM probe and the Flight SQL DB-API probe can require different
Python dependencies. In particular, `flightsql-dbapi` can constrain SQLAlchemy
to version 1.4. Keep the ORM probe's SQLAlchemy 2 and pandas environment on
`PATH`, and set `ICEGRES_FLIGHT_DBAPI_PYTHON` to the Python executable in a
separate environment containing `flightsql-dbapi`, its SQLAlchemy dependency
and pandas. Both probes run when their dependencies are installed.

## Security issues

Never report suspected vulnerabilities in public issues — see
[`SECURITY.md`](SECURITY.md).
