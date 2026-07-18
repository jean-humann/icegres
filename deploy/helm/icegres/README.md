# icegres Helm chart

Postgres wire endpoint over an Iceberg lakehouse, with optional
consensus-fenced HA: the `icegresd` control plane (wake-on-connect,
leader-lease redundancy), a single buffered writer compute, stateless read
replicas over the same single copy of the data, and the `icekeeperd`
quorum-tail acceptor trio.

- **Chart / app version:** 0.1.0 (the image tag defaults to the appVersion)
- **Operator runbook** (what fails how, what heals itself, what pages a
  human): [`docs/deployment.md`](../../../docs/deployment.md) §11
- **Every icegres knob** behind these values:
  [`docs/configuration.md`](../../../docs/configuration.md)

## Prerequisites

- An existing **Iceberg REST catalog** (e.g. Lakekeeper) and an
  **S3-compatible object store** — the chart wires them, it never provisions
  them.
- **No public image registry is published**: build the repository `Dockerfile`
  (it ships all three binaries) and push to your own registry.
- TLS requires a pre-existing `kubernetes.io/tls` Secret (the chart never
  mints certificates). Auth requires an auth file (inline or in a Secret).

## Quickstart

```bash
docker build -t registry.example.com/icegres:0.1.0 .
docker push registry.example.com/icegres:0.1.0

helm install icegres deploy/helm/icegres \
  --namespace icegres --create-namespace \
  --set image.repository=registry.example.com/icegres \
  --set catalog.uri=https://catalog.example.com/catalog \
  --set s3.endpoint=https://s3.example.com \
  --set s3.existingSecret=my-s3-creds
```

Clients connect to the `<release>-icegres` Service (pgwire, port 5432).

## Topologies

The defaults deploy the smallest honest topology — one `icegresd` + one
always-on writer; no tail, no lease trio, no read replicas, no RBAC. Modes
compose:

| Values | Topology |
|---|---|
| defaults | Scale-to-zero-ready proxy + 1 always-on writer; no HA promise |
| `tail.mode=dir` + `writer.writeBufferMs>0` | Buffered writes with a WAL on a writer PVC — survives a crash on the same volume; no automated node failover |
| `tail.mode=quorum` + `writer.writeBufferMs>0` | `-keeper` icekeeperd trio, 2-of-3 fsync before every ack; automated pod-level writer failover (liveness-driven, fence-and-replay) |
| `ha.enabled=true` | ≥2 `icegresd` + a **dedicated** `-lease` trio; the client endpoint survives icegresd pod/node loss within ~1–2× `ha.leaseTtlMs` |
| `k8sScaling.enabled=true` | Writer scale-to-zero: parks after `icegresd.idleShutdownSecs` idle, wakes on connect (scoped Role on exactly the writer's scale subresource) |
| `computes.readReplicas=N` | `-read` Deployment + Service of stateless read computes, peer-tailed to the writer when a durable tail exists |

## Values

### Image & global

| Key | Default | Meaning |
|---|---|---|
| `image.repository` | `icegres` | Image built from the repo Dockerfile (icegres + icegresd + icekeeperd) |
| `image.tag` | `""` (appVersion) | — |
| `image.pullPolicy` | `IfNotPresent` | — |
| `imagePullSecrets` | `[]` | — |
| `nameOverride` / `fullnameOverride` | `""` | Standard name overrides |
| `commonLabels` | `{}` | Extra labels on every rendered object |

### Lakehouse wiring

| Key | Default | Meaning |
|---|---|---|
| `catalog.uri` | `http://lakekeeper:8181/catalog` | Iceberg REST catalog |
| `catalog.warehouse` | `lakehouse` | Warehouse name |
| `s3.endpoint` | `http://rustfs:9000` | Object store endpoint |
| `s3.region` | `us-east-1` | — |
| `s3.accessKey` / `s3.secretKey` | `""` | Inline creds land in the chart-managed Secret — prefer `existingSecret` beyond a demo |
| `s3.existingSecret` | `""` | Secret with keys `s3-access-key` / `s3-secret-key`; overrides inline |

### Security

| Key | Default | Meaning |
|---|---|---|
| `auth.enabled` | `false` | SCRAM on every compute. **Disabled ⇒ computes run `ICEGRES_INSECURE=true`** — anything in-cluster can connect. Enabled ⇒ icegresd session pooling is disabled (SCRAM cannot be pre-authenticated) |
| `auth.users` | `""` | Inline auth-file content for the chart-managed Secret |
| `auth.existingSecret` | `""` | Secret with key `users` (+ `peer-tail-password` when replicas ride a durable tail) |
| `auth.peerTailUser` / `auth.peerTailPassword` | `""` | Credentials replicas present to the writer's tail API |
| `tls.enabled` | `false` | TLS terminates **at the computes**; icegresd answers SSLRequest with `N` — `sslmode=require` clients must connect to a compute Service directly |
| `tls.existingSecret` | `""` | `kubernetes.io/tls` Secret; **required** when `tls.enabled` |

### icegresd (control plane)

| Key | Default | Meaning |
|---|---|---|
| `icegresd.replicas` | `1` | Forced ≥2 when `ha.enabled` |
| `icegresd.port` / `icegresd.service.port` | `5432` | Client endpoint |
| `icegresd.service.type` | `ClusterIP` | `LoadBalancer` to expose externally |
| `icegresd.wakeTimeoutMs` | `120000` | Cold-connect budget (pod scheduling + image pull) |
| `icegresd.idleShutdownSecs` | `300` | With `k8sScaling`: idle window before the writer parks to 0 |
| `icegresd.poolSize` / `icegresd.poolUser` | `8` / `postgres` | Warm session pool (forced 0 under auth) |
| `icegresd.resources` | 50m/64Mi req, 256Mi lim | — |
| `icegresd.extraEnv` | `[]` | — |

### HA & scaling

| Key | Default | Meaning |
|---|---|---|
| `ha.enabled` | `false` | icegresd redundancy behind a dedicated `-lease` trio (never shared with the data trio); standbys answer retryable `57P03` and are pulled from the Service by a leadership readiness probe |
| `ha.leaseTtlMs` | `6000` | Failover ≈ 1–2× TTL |
| `k8sScaling.enabled` | `false` | Wake-on-connect + park-when-idle on the writer StatefulSet via the scale subresource (single namespaced Role, `get`+`patch`, that one object) |

### Tail & writer

| Key | Default | Meaning |
|---|---|---|
| `tail.mode` | `none` | `none` = synchronous commits (`writeBufferMs` must stay 0) · `dir` = single-node WAL PVC · `quorum` = keeper trio, 2-of-3 fsync per ack |
| `tail.quorumTimeoutMs` | `10000` | Quorum-ack timeout (min 1000) |
| `tail.dir.size` / `tail.dir.storageClass` | `1Gi` / `""` | WAL PVC |
| `writer.writeBufferMs` | `0` | Buffered writes; **required > 0 when `tail.mode != none`** |
| `writer.port` / `writer.healthPort` | `5439` / `8080` | pgwire · /health,/ready,/metrics |
| `writer.tailApiPort` | `5499` | Open-tail API; served only with replicas + a durable tail |
| `writer.startupProbe.*` | 2s × 60 | Load-bearing: /health binds only after catalog build (+ election/replay on failover) — shrinking it turns failover into CrashLoopBackOff |
| `writer.resources` | 500m/1Gi req, 4Gi lim | — |
| `writer.extraEnv` | `[]` | — |

### Read replicas

| Key | Default | Meaning |
|---|---|---|
| `computes.readReplicas` | `0` | Fixed `-read` Deployment size; `0` renders nothing. No chart-managed HPA — bring your own, keep `minReplicas ≥ 1` |
| `computes.freshnessMs` | `0` | Bounded-staleness reads on the replicas (`0` = exact) |
| `computes.startupProbe.*` | 2s × 60 | Same rationale as the writer probe |
| `computes.service.type` / `.port` | `ClusterIP` / `5439` | Client read endpoint |
| `computes.resources` | 250m/512Mi req, 2Gi lim | — |
| `computes.extraEnv` | `[]` | — |

### Acceptor trios (`keeper` = data, `lease` = icegresd HA)

Tiny fsync-only processes; **must** land on different nodes.

| Key | keeper | lease | Meaning |
|---|---|---|---|
| `.port` | `5471` | `5471` | Acceptor port |
| `.antiAffinity` | `required` | `required` | `required` refuses co-scheduling (pending pods are the honest signal); `soft` for single-node dev/kind |
| `.storage.size` | `1Gi` | `256Mi` | Per-acceptor PVC |
| `.resources` | 50m/64Mi | 25m/32Mi | — |

### Integrations & hardening

| Key | Default | Meaning |
|---|---|---|
| `metrics.serviceMonitor.enabled` | `false` | Prometheus Operator ServiceMonitor on the compute health ports (needs the CRD); `interval` 30s, `labels` `{}` |
| `networkPolicy.enabled` | `false` | Ingress-only policies: client ports admit `clientFrom` (empty = any in-cluster), internals admit only this release's pods |
| `networkPolicy.clientFrom` | `[]` | NetworkPolicyPeer list for the client-facing ports |
| `serviceAccount.create` / `.name` | `true` / `""` | `automountServiceAccountToken: false` everywhere except icegresd under `k8sScaling` |
| `podSecurityContext` | non-root 10001, seccomp RuntimeDefault | Every pod |
| `containerSecurityContext` | no privilege escalation, read-only rootfs, drop ALL | Every container |
| `nodeSelector` / `tolerations` | `{}` / `[]` | — |

## Sharp edges (read before operating)

- **Failover IS the liveness probe.** A fenced or quorum-lost writer wedges
  (accepts TCP, can never ack); its `/health` turns 503, the kubelet restarts
  it, and the replacement fences the old term and replays the acked window
  *before* binding pgwire. Liveness deliberately never touches the catalog.
- **Writer NODE loss is not auto-healed** on clusters without node-lifecycle
  GC — the pod sits Terminating until a manual
  `kubectl delete pod --grace-period=0 --force` (safe in quorum mode: the old
  writer is fenced). `tail.mode=dir` remains single-node durability.
- **`helm upgrade` resets a parked writer to 1 replica**; the idle loop parks
  it again. Only traffic through icegresd holds the idle clock — TLS-direct
  clients don't.
- **The `-read` Service is a convention, not enforcement** — a write sent
  there executes. Replicas deliberately carry no tail/buffer env (a replica
  opening the writer's tail would fence it).
- **Helm-time validation** (`icegres.validate`) rejects inconsistent values
  loudly: bad `tail.mode`, a tail without `writeBufferMs>0`, TLS without the
  Secret, auth without users, replicas+tail+auth without peer-tail creds.

## Chart validation

`tests/helm.sh` lints 5 values profiles, diffs `helm template` output against
committed golden fixtures, runs strict `kubeconform` against two Kubernetes
versions, and asserts the security/topology invariants (non-root, read-only
rootfs, PDBs, RBAC scope, probe presence, …). Honest label: **it renders and
validates manifests; it does not run a cluster** — the real-cluster kind smoke
procedure is documented in [`docs/deployment.md`](../../../docs/deployment.md)
§11 and not CI-run.
