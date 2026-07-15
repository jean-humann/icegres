# Scope: P3 — icegresd-ha + Kubernetes/Helm distribution with HA options

Roadmap-v2 P3, extended per user direction with first-class Kubernetes
integration: a Helm chart whose HA values light up the same machinery.
Claim when done: the only self-hostable lakehouse-Postgres with
automated, consensus-fenced failover — installable with one `helm
install`.

## Positioning (the pgpool question, answered in design)

icegresd sits where a pgbouncer/pgpool sits — the single endpoint in
front of a fleet — but it is a router + compute-lifecycle manager, NOT a
connection pooler:
- No transaction/statement multiplexing, ever (explicit non-goal). An
  icegres session is a cheap in-process DataFusion session; the scarcity
  poolers multiplex does not exist. icegresd pools WARM COMPUTE SESSIONS
  (skip cold start), preserving full session semantics.
- It starts databases (wake-on-connect for scale-to-zero — the Knative
  activator role); poolers never do. This is why it stays on the DATA
  PATH in Kubernetes: a Service cannot wake a scaled-to-zero pod on TCP
  connect. The proxy hop is microseconds against 3-8 ms queries; if the
  bench says otherwise a direct-routing values toggle is the escape
  hatch (documented, not built, unless the bench forces it).
- Failover is FENCING, not promotion: data truth lives in the lake +
  quorum tail, so a replacement compute's tail open() fences the old
  writer by consensus term (shipped machinery) and replays the window.
  A fenced zombie cannot ack. No WAL promotion, no data copy.
- Read replicas are stateless computes over the same single copy
  (peer-tail + freshness bounds), not replication streams. Routing is by
  endpoint identity (dbname/branch), never SQL parsing.

## Deliverables

### 1. Automated tail-writer failover (icegresd-ha core)
- icegresd health-checks serving computes (existing /health + a
  liveness probe on the tail lease); on writer failure: route new
  connections to a replacement compute (spawn in process mode; see §4
  for k8s mode) whose quorum-tail open() FENCES the old term and replays
  un-flushed frames before accepting writes.
- Measured: failover_ms (kill -9 the buffering writer under load →
  first successful write on the replacement), zero acked-row loss
  (the durability suite already proves the data half; the e2e leg
  proves it THROUGH a failover).
- Honest scope: quorum tail mode only (dir is single-node by nature;
  pg-tail failover documented as manual — its HA is the tail DB's).

### 2. icegresd redundancy (leader election over icekeeperd)
- N icegresd instances; a small lease (term + holder + TTL) written
  through the EXISTING icekeeperd quorum machinery — it IS a consensus
  service; no new system, no new dependency. Leader serves; standbys
  health-check the lease and take over on expiry (fenced by lease term
  so two leaders cannot both spawn/route writers).
- e2e: kill the icegresd leader; standby holds the lease within TTL;
  clients reconnect and keep working; no double-writer (fencing assert).

### 3. Autoscaling-lite (process mode)
- Session/qps thresholds (from existing /metrics counters) spawn
  additional read computes (branch/replica endpoints, peer-tail wired)
  and reap them when idle. Single-digit-node scope, honestly not
  Kubernetes — in k8s mode this maps to HPA guidance instead (§4).

### 4. Kubernetes integration + Helm chart (deploy/helm/icegres)
- Chart (apiVersion v2) components:
  - icegresd: Deployment, `ha.enabled` => replicas>=2 with the §2 lease;
    Service (LoadBalancer/ClusterIP) exposing pgwire + Flight + tail-API
    ports; PodDisruptionBudget.
  - Writer compute: StatefulSet (stable identity for the tail lease),
    quorum tail wiring when `tail.mode=quorum`.
  - Read replicas: Deployment with `computes.readReplicas`, `--peer-tail`
    toward the writer's tail API, `--freshness-ms` from values.
  - icekeeperd trio: StatefulSet (3 replicas, PVCs, podAntiAffinity,
    PDB minAvailable 2) when `tail.mode=quorum`.
  - ConfigMap/Secret wiring: catalog URI, S3 endpoint/creds, auth-file,
    TLS certs (existingSecret patterns); probes wired to /health and
    catalog-aware /ready; resources; ServiceMonitor (optional, gated);
    NetworkPolicy (optional, gated).
- k8s-mode lifecycle: icegresd does NOT fork processes in k8s. Compute
  wake/scale = patching the replica count of the compute
  Deployment/StatefulSet via the Kubernetes API (serviceaccount token,
  in-cluster CA, plain HTTPS through the HTTP client machinery already
  in-tree — recon verifies; if that machinery cannot do it cleanly with
  ZERO new dependencies, k8s-mode scaling ships as HPA guidance +
  values-provided minReplicas instead, and wake-on-connect applies only
  to warm pools — documented honestly either way). RBAC in the chart is
  scoped to exactly that one Deployment patch.
- Validation ladder (no cluster in this box — docker daemon absent):
  build helm from source (Go 1.24 present, repo clonable; pin the rev);
  gate `tests/helm.sh`: helm lint; helm template golden fixtures
  (committed, diffed) across the values matrix (defaults; ha.enabled;
  tail.mode=dir|quorum; readReplicas>0; TLS+auth on); schema-validate
  every rendered manifest against vendored Kubernetes JSON schemas
  (kubeconform built from source, or an equivalent offline validator —
  recon picks); assert invariants in rendered output (anti-affinity
  present, PDBs, probe paths, no privileged/root, RBAC scoped).
  A real-cluster smoke procedure (kind/minikube commands + expected
  outputs) documented in deployment.md for operators; honestly labeled
  as not CI-run here.

### 5. Tests / bench / docs
- Unit: lease state machine (acquire/renew/expire/fence), failover
  routing decision, autoscale thresholds.
- e2e additions: §1 failover leg (kill writer under load, measure
  failover_ms, all acked rows present, zombie fenced), §2 leader-kill
  leg, autoscale spawn/reap leg. All process-mode (the same code paths
  the chart wires).
- Bench: failover_ms recorded in SCORECARD (ungated extra); existing
  ladder A/B vs pre-P3 baseline (scratchpad icegres-pre-p3 +
  bench-20260714T134211Z.json as the fresh same-box baseline),
  drift-controlled.
- Docs: deployment.md gains the k8s section (install matrix, HA
  runbook: what fails how, what heals itself, what pages a human);
  README HA row; limitations.md honest edges (pg-tail manual failover,
  autoscaling-lite scope, chart smoke not CI-run).

## Constraints
Invariants I1-I4. ZERO new Rust dependencies (lease over existing quorum
client; k8s API over existing HTTP machinery or not at all). Default
behavior byte-identical: no lease/failover/k8s code runs unless flags
are set. Chart is additive files only. Pinned matrix untouched.

## Gates
fmt/clippy -D warnings → cargo test --release (live) → tail_durability
(71) → FULL e2e (217 + new legs) → tests/helm.sh (lint + golden +
schema + invariants) → bench A/B vs pre-P3 baseline (drift-controlled)
+ failover_ms recorded → a11 + parity green. Fix-or-revert per house
rule; adversarial review ×2 + refutation before the PR.
