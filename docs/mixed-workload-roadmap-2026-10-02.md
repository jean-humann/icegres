# Mixed workload implementation and acceptance roadmap

Reviewed on 2026-10-02. The priority is concurrent transactional updates and
analytical reads. This document describes the five draft PRs built from the
review, then the work still required for broader production and PostgreSQL
compatibility claims. It supersedes earlier gap summaries for this stack;
[limitations.md](limitations.md) remains the detailed operating contract.
Implementation in a draft PR does not establish merge readiness, a latency SLO,
or equivalence to Lakebase.

## Implemented in the stack

| PR | Result | Boundary that remains |
| --- | --- | --- |
| [22: write and authorization safety](https://github.com/jean-humann/icegres/pull/22) | Authorize SQL wrappers and resolved object identities; enforce read-only replicas and opt-in primary keys across SQL listeners; reject unsafe recursive field-ID/schema changes; retain table identity and schema requirements; report uncertain publication as `40003`; quarantine uncertain buffered commits; reserve buffer bytes before durable staging; make required integration prerequisites fail. | Schema evolution is safely rejected where projection is unsupported. Foreign writers do not inherit Icegres constraints. A timeout does not prove rollback. |
| [23: request and transaction budgets](https://github.com/jean-humann/icegres/pull/23) | Bound Flight handler admission, read deadlines and encoded results; limit retained transaction batches with ownership-based reservations, including casts and replacement state. | External client cancellation can interrupt mutation handlers. Decoder temporaries, commit assembly and all process RSS are not fully budgeted. Shared session-setting isolation remains work. |
| [24: streaming rewrites](https://github.com/jean-humann/icegres/pull/24) | Use ranged Parquet reads and deterministic batch evaluation; stage changed files without retaining a manifest's replacement rows; prune supported single-operation candidates; preserve schema guards and all-file PK checks; expose read-byte and batch counters. | Still copy-on-write. Unsupported expressions use a per-file fallback. Prefix rereads, compressed row groups, writer buffers and retained keys have separate costs. No persistent general-purpose index was added. |
| [25: quorum resource and placement controls](https://github.com/jean-humann/icegres/pull/25) | Bound acceptor connections, requests and bytes; read bounded WAL ranges; move serialized durable disk work to blocking workers whose ownership survives caller cancellation; add strict three-zone placement for data and lease trios. | Quorum transport remains plaintext on a trusted network. A placement profile and local fault tests do not prove availability across real zones. Automated idle parking remains disabled in the strict profile. |
| [26: mixed workload and tail gates](https://github.com/jean-humann/icegres/pull/26) | Reject invalid measurements and p95 regressions beyond configured limits; run concurrent transactions, point reads and aggregates with independent replica visibility and PyIceberg verification; record raw samples, p50/p95/p99 and compute RSS; bound the whole run and invalidate failed artifacts. | The local synchronous fixture does not test durable backlog, remote storage capacity, PostgreSQL isolation parity or a Lakebase deployment. |

Independent reviewers challenged the source findings and the implementations.
Corrections included distinguishing unknown commits from failure, preserving
schema/table identity across retries, checking keys in pruned files, and
retaining reservations through asynchronous work. This was substantive source
review, supplemented by the regression suites below, rather than a substitute
for execution.

## Disposition of the original findings

These identifiers preserve the review's thirteen findings. "Fixed" refers to
the identified paths, not universal feature parity; the operating bounds above
still apply.

| Finding | Disposition after this stack |
| --- | --- |
| 1. Unchecked authorization paths | Fixed for reviewed wrappers and identifier resolution; unsupported forms fail closed. |
| 2. Stale physical field identity | Fixed by recursive validation and commit requirements; general evolved-schema writes remain unsupported. |
| 3. False rollback after response loss | Fixed in custom commit paths with typed uncertainty and positive reconciliation; client-independent completion remains deferred. |
| 4. Unbounded allocations | Partial: buffer, retained transaction and handler budgets added; total RSS, decode and full PK-set bounds remain open. |
| 5. Per-table first-touch isolation | Deferred: no database-wide read snapshot added. Atomic multi-table publication does not close this gap. |
| 6. Inconsistent constraint paths | Fixed for opt-in SQL listener policy; constrained bulk ingestion rejects unsupported writes. Foreign-writer uniqueness remains a coordination problem. |
| 7. Incomplete deadlines/admission | Partial: Flight reads and custom catalog requests bounded; supervised mutation completion and remaining pgwire hook coverage remain open. |
| 8. DML scan/rewrite amplification | Partial: ranged streaming and conservative pruning added; indexed writes and lower-amplification updates remain open. |
| 9. Catalog polling/discovery scale | Deferred: no event-driven invalidation, lazy foreign-table discovery or maximum stale-age contract added. |
| 10. Median-only benchmark gate | Fixed for the specified metrics and artifact validation; production capacity and competitor measurements remain outstanding. |
| 11. Writable read replicas | Fixed by server-side read-only policy on reviewed pgwire paths. |
| 12. HA/security deployment gaps | Partial: bounded acceptor work and strict zone placement added; peer TLS, actual multi-zone chaos and safe idle parking remain open. |
| 13. Green-looking skipped integrations | Partial: required live tests and browser prerequisites now fail; the full ecosystem matrix still needs an explicit required lane. |

## Current comparison, verified against official sources

Lakebase provides the main transactional reference. Its documented architecture
runs PostgreSQL compute over quorum-replicated WAL, pageservers and object
storage. Icegres has useful durable-tail machinery, but that alone does not give
it PostgreSQL's transaction engine or persistent indexes.
[Lakebase architecture](https://docs.databricks.com/aws/en/oltp/projects/architecture)

| Reference | Relevant documented capability | Implication for Icegres |
| --- | --- | --- |
| Lakebase LTAP on AWS | PostgreSQL indexes remain in row storage. Analytical reads use an LSN and merge recent pageserver changes with columnar data; each table has one writing engine. Registration and synced tables are GA, Lakehouse//RT is Beta, and Change Data Feed is Public Preview. [Architecture and availability](https://docs.databricks.com/aws/en/oltp/projects/ltap-overview) | Define a consistent analytical read barrier and writer ownership. Best-effort peer overlays are not an equivalent cross-table visibility contract. |
| Lakebase operations | Autoscaling observes CPU, memory and working set. HA computes cannot scale to zero. [Autoscaling](https://docs.databricks.com/aws/en/oltp/projects/autoscaling) Search became GA September 18; telemetry system tables and backup-schedule APIs are Beta; Direct Writes became GA October 1 for initial/full synced-table loads. [Release notes](https://docs.databricks.com/aws/en/release-notes/lakebase) | Finish observability, safe lifecycle control and tested recovery before claiming comparable operations. Direct Writes does not imply unrestricted multi-engine writes. Cloud and staged account availability matter. |
| pg_lake | PostgreSQL integration, Iceberg partition evolution and VACUUM; current documentation also supports owned writable tables in an external REST catalog. Existing tables created by other engines attach read-only, as do external metadata-path tables. [Iceberg tables, pinned documentation](https://github.com/Snowflake-Labs/pg_lake/blob/6eb4c101bac12ac0af7fc3c9fcc8a3af5517baad/docs/iceberg-tables.md) | Compare catalog ownership and supported types explicitly. The earlier description that writable tables require only pg_lake's internal catalog was too narrow. These are current project-doc capabilities, not a tested release compatibility matrix. |
| DuckLake | Snapshot-isolated transactions include DDL. Small inserts and deletions can live in its SQL metadata catalog before flushing to Parquet. [Transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions), [data inlining](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining) | Useful references for atomic metadata and small-change costs. Adopting DuckLake's storage contract would change Icegres's Iceberg compatibility contract. |
| Moonlink | Preview CDC ingestion with row-position indexes, caching and deletion vectors. [Project documentation](https://github.com/Mooncake-Labs/moonlink) | Study its update representation. It is not evidence of a mature general-purpose PostgreSQL replacement or a measured winner against Icegres. |

No vendor comparison was executed for this stack. Include current PostgreSQL
and Lakebase in application tests, and DuckDB, pg_lake and Trino/Spark in
analytical tests. Pin each version and distinguish single-node efficiency from
distributed capacity. Existing historical benchmark artifacts cannot rank the
current systems.

## Next implementation gates

The order below is a recommendation for mixed workloads. Each item needs a
reviewable contract and its acceptance evidence before being called complete.

1. **Database-wide transaction semantics.** Establish one read snapshot across
   tables, including tables first accessed later and one-statement joins.
   Retain atomic publication only on catalogs that support it; reject unsupported
   strict multi-table commits before any write. Test interleaved account
   transfers, read skew, retries, schema replacement and restart with controlled
   barriers and checked transaction histories. Returned rows must satisfy the
   chosen isolation contract; a COMMIT-time check cannot retract an inconsistent
   earlier result. Specify write-skew expectations separately. PostgreSQL
   Repeatable Read and Serializable are different targets.
   [PostgreSQL isolation](https://www.postgresql.org/docs/18/transaction-iso.html)

2. **Persistent indexed reads and writes.** Add a snapshot-consistent key-to-row
   or key-to-file index with recovery and compaction integration. Decide how
   uniqueness is coordinated across supported writers. Test skewed hot keys,
   duplicate races, restart and index rebuild against full-scan reference
   results. Measure object requests, touched bytes, write amplification and
   throughput as total table size grows with a fixed hot set. Sustain the
   advertised rate for at least an hour without growing backlog, then drain an
   injected outage within a declared recovery target. Buffer acknowledgment
   latency alone is insufficient.

3. **Supervised mutation completion.** After publication can begin, a task
   independent of the client must retain admission and mutation ownership until
   the outcome is durable or explicitly unknown. Provide operation identity and
   reconciliation without blind replay. Cancel clients before submission,
   during upload, during catalog publication and after publication but before
   reply; inject lost responses and server restarts. Require no duplicate
   effect, no false rollback assertion and no premature resource release.
   Definite conflicts remain `40001`; unresolved outcomes remain `40003`.

4. **Authenticated quorum and actual multi-zone recovery.** Implement peer TLS
   with authenticated identities, certificate rotation and explicit trust
   configuration for both data and lease traffic. Test on a real three-zone
   cluster with zone loss, asymmetric partitions, slow/full disks, replacement
   nodes and rolling upgrades. Check acknowledged records against an external
   history after every recovery, fence stale leaders, and publish measured
   RPO/RTO under the stated failure model. Reject unknown peers and fail closed
   during invalid certificate rotation. Helm rendering is a prerequisite only.

5. **Safe idle parking and elastic compute.** Unify activity from proxied and
   direct TLS sessions, Flight streams, active transactions, pending commits
   and tail flushes. Fence every scale action by the current leader and require
   an atomic drain/park decision. Race long-running reads, client reconnects,
   leadership changes and delayed Kubernetes responses against parking. No
   admitted operation may be cut off by an idle decision; unfinished writes
   must prevent parking. Retain the disabled default until these tests pass.

6. **Observable overload and freshness.** Export admission occupancy and wait
   time, rejection causes, retained bytes, flush age, durable and publication
   watermarks, per-table visibility age, catalog/object requests, rewrite
   amplification and compaction debt. Set workload-specific latency and stale
   age budgets before testing. Apply bounded admission through prolonged catalog
   outages and slow consumers; demonstrate explicit overload errors without
   OOM, preserved acknowledged WAL, and recovery without sustained backlog.
   Exercise large catalogs and foreign table creation; polling intervals are
   targets, not maximum stale-age guarantees. Use these signals before adding
   load-based autoscaling or a remote byte-range cache.

7. **Broader Iceberg writes and recovery.** Replace conservative schema refusal
   with recursive field-ID projection where semantics are known; add partition
   evolution, delete-file handling and bounded compaction. Verify drop/re-add,
   rename, nested defaults, nullability and type evolution with an independent
   Iceberg reader. Validate deletes and historical snapshots across engines.
   Upstream deletion-vector reads merged September 3, but the latest release
   observed here is 0.10.1 from August 1; this does not establish a compatible
   released reader/writer stack. Audit the complete pinned dependency matrix
   before an isolated upgrade. [Merged read support](https://github.com/apache/iceberg-rust/pull/3035),
   [releases](https://github.com/apache/iceberg-rust/releases),
   [field projection rules](https://iceberg.apache.org/spec/#column-projection)
   Also test backup/restore and expiration with active readers. Distinguish
   Iceberg data branches from branches that isolate schema, roles and properties.

8. **Ecosystem compatibility and session isolation.** Maintain a versioned
   acceptance matrix for psycopg, SQLAlchemy 2, JDBC, pg8000, ADBC, Flight and BI
   clients. Cover connection initialization, two-client settings isolation,
   reflection, prepared binds, binary/text results, transaction state after
   errors, cancellation, COPY, RETURNING, cursors and supported migrations.
   Unsupported operations must return explicit errors without side effects.
   Required jobs must fail missing dependencies. A SQLAlchemy connection or
   reflection fix does not establish full ORM transaction compatibility.
   Run foreign-writer, read-only and authorization cases through both SQL
   protocols; add search/vector extensions only against a named application
   requirement and its own compatibility tests.

## Verification and performance claims

The combined stack completed 502 release Rust test invocations with live
prerequisites required and no ignored tests; release clippy passed. The initial
safety set also passed nine live safety regressions, 71 tail-durability
assertions and four real-browser lanes. The mixed runner completed a
100-transaction live fixture with exact final-state verification and no errors;
a forced whole-run timeout produced a failing artifact. Broad ecosystem testing
found a SQLAlchemy 2 reflection failure that is under repair. Those repairs and
subsequent rebases need validation against their resulting heads.

These are test results, not a capacity or comparative performance claim. PR
descriptions track integration results and revisions tested; all five PRs are
drafts at this writing.

Before merging each rebased head, run formatting, warning-free release clippy,
release tests with required live services, the safety and ecosystem suites,
tail durability, browser/Helm checks, supply-chain policy and the history gate.
Retain commit and binary hashes with logs. Record skipped optional tests
separately; never count them as exercised integrations.

For performance, use [the mixed runner](../bench/MIXED.md) on otherwise idle,
identical environments. Repeat interleaved baseline/candidate runs with fixed
versions, data layout, memory, network distance, durability, constraints and
transaction semantics. Keep raw samples and report errors, p50/p95/p99,
throughput, visibility delay, resource use and object-storage cost. Extend the
fixture to remote storage, larger data and hot-key skew; test buffered backlog
separately. Its relative regression thresholds are not application SLOs, and
100 observations give a coarse p99 estimate. More samples and repeated runs are
needed before attributing differences or making a faster/cheaper-than-Lakebase
claim.
