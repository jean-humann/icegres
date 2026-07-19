# icegres — contributor & agent guide

Operating contract for anyone (human or agent) changing this repository. The
rules under **Pre-merge contract** MUST be satisfied before any branch is
merged, so the git history stays clean and every prior convention is upheld.

## Pre-merge contract (respect before every merge)

Run `bash scripts/verify-history.sh` on the branch first — it enforces the
history rules below and must exit 0. Then confirm the review and gates.

### 1. Branch & push discipline
- Develop on the designated feature branch; never commit straight to `main`.
- **Never push or force-push `main` (or any branch you were not assigned)
  without explicit human approval.** `--force-with-lease` is allowed only on
  your own feature branch.
- If the branch's PR is already merged, restart the branch from the latest
  `main` for follow-up work; never stack new commits on merged history.

### 2. Commit messages (Conventional Commits, title + body)
- Subject: `type(scope): summary` — imperative mood, **≤ 72 characters**, no
  trailing period. Allowed `type`: `feat`, `fix`, `docs`, `chore`, `test`,
  `refactor`, `perf`, `build`, `ci`, `style`. Scope is a short lowercase area
  (e.g. `engine`, `tail`, `quorum`, `flightsql`, `ha`, `deploy`, `bench`).
- **Every commit has a body** — one short paragraph on *what* changed and
  *why*, wrapped at ~80 columns. No subject-only commits.
- **No roadmap/phase tokens** in commit messages — never write `phase`, `P1`…
  `P7`, `P5+P7`, `Round N`, `RNN`, or similar internal milestone labels. Titles
  describe the change itself.
- **No model identifiers** anywhere in commits, PR titles/bodies, code, or
  docs (e.g. no `claude-opus-*`, `Fable`, etc.). Keep those to chat only.
- Every commit ends with the trailers:
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: <session url>
  ```

### 3. Atomic commits
- One concern per commit. Split multi-concern work (separate engine change from
  its tests, docs, and bench artifacts) rather than one omnibus commit.
- History should read as a clean, linear build-up. Prefer rebase/recompose over
  merge commits; keep the branch linear.

### 4. Verification gates (must be green before merge)
- `cargo fmt --check` and `cargo clippy --release --all-targets -- -D warnings`.
- `cargo test --release` against the live stack; `tests/e2e.sh`,
  `icegres/tests/tail_durability.sh`, and `tests/helm.sh` where relevant.
- `cargo deny check` for supply-chain governance.
- Do not bump the pinned toolchain / dependency matrix
  (`rust-toolchain.toml`) as an incidental change — it is deliberately pinned.

### 5. Review & regressions
- A fresh-eyes review (independent of the implementer) must be clean before
  merge. On any regression: fix or revert — never merge over a known break.

## Layout
- `icegres/` — the Rust crate (Postgres-wire + Arrow Flight SQL over Iceberg).
- `clients/` — official client packages (JS: `flight-web`, browser gRPC-web).
- `deploy/helm/` — Helm chart (single-node, quorum, read-replica topologies).
- `infra/` — local development stack scripts.
- `bench/` — benchmark + SQL-parity harness and recorded result artifacts.
- `docs/` — architecture, deployment, limitations, and design notes.
