# Mixed workload regression gate

`mixed_workload.py` runs one transactional writer while two readers continuously
issue point queries and analytical aggregates. Every transaction reads a row,
updates its balance and revision, and commits. A separate read-only process must
observe each acknowledged revision. After the run, PyIceberg verifies every row,
balance, revision and bucket against the expected state.

The default fixture has 20,000 rows in ten data files. Each run creates its own
namespace and two server processes, then removes its catalog entries. Use an
isolated test warehouse. Historical data objects follow the catalog's cleanup
policy. The runner never uses a user-supplied table.

The whole-run deadline includes setup, readiness, verification and cleanup.
When it expires, the parent kills only the run's process group and writes an
incomplete result that fails the gate. That result names the run-owned namespace
if catalog cleanup needs to be completed afterward. REST and S3 requests also
have finite connection and response timeouts.

Install `psycopg2-binary`, `pyarrow` and `pyiceberg` in a Python environment.
Start the local Lakekeeper and S3-compatible stack. Build both binaries before
timing and run them sequentially on the same otherwise idle host:

```sh
python bench/mixed_workload.py --binary /path/to/baseline/icegres \
  --label baseline --output /tmp/mixed-baseline.json
python bench/mixed_workload.py --binary /path/to/candidate/icegres \
  --label candidate --output /tmp/mixed-candidate.json
python bench/check_metrics.py --mixed \
  /tmp/mixed-baseline.json /tmp/mixed-candidate.json
```

The runner records all measured samples, p50/p95/p99, transaction and aggregate
throughput, independent-replica visibility delay, and sampled combined writer
and replica RSS. Visibility delay includes the replica query round trip.
RSS excludes the object store and catalog processes. No buffer or durable tail
is enabled in this synchronous workload, so it makes no backlog claim.

The gate requires at least 100 samples for each distribution, zero errors,
successful final data verification, and matching workload and host descriptors.
It rejects a median, p95 or p99 increase above 20%, either throughput decrease
above 10%, or peak RSS increase above 25%. Missing, nonnumeric, negative,
nonfinite and incomplete values fail. These thresholds detect regressions;
they are not application latency SLOs. Repeat runs and inspect raw samples
before attributing a small change to the implementation. Keep background build
and test processes out of a performance comparison.

The existing `gate.sh` also checks p95 and validates input values. Its latency
distributions require at least 20 observations, including cold starts. Older
artifacts with five cold starts must be remeasured. When its browser test is
enabled, missing prerequisites fail that release gate. `--skip-e2e` remains an
explicit way to compare metrics after the live gates ran separately.

This local fixture exercises contention between reads and copy-on-write updates.
It does not measure a Lakebase deployment, remote object-store latency, production
data sizes, quorum failure recovery, buffered outage backlog, or PostgreSQL
isolation parity. Run the separate durability and schema/auth regression suites
as well. Larger file/row configurations and repeated remote runs are necessary
before making capacity or vendor-performance claims.
