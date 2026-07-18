#!/usr/bin/env bash
# tests/helm.sh — the P3 chart gate (docs/p3-ha-scope.md §4): validate
# deploy/helm/icegres without a cluster.
#
#   1. deterministic toolchain (pinned helm + kubeconform built from
#      source, vendored Kubernetes JSON schemas fetched by commit sha)
#   2. helm lint across the values matrix
#   3. helm template vs COMMITTED golden fixtures
#      (deploy/helm/tests/golden/; regenerate with UPDATE_GOLDEN=1)
#   4. offline schema validation of every rendered manifest against
#      Kubernetes v1.31.0 AND v1.34.0 (strict: unknown fields rejected),
#      plus a self-check that the validator actually rejects garbage
#   5. invariant asserts over the rendered output (anti-affinity, PDBs,
#      probe paths, runAsNonRoot/no-privileged, no default-namespace
#      hardcode, RBAC scoped to exactly one scale subresource)
#
# Toolchain resolution (in order):
#   - env overrides ICEGRES_HELM_BIN / ICEGRES_KUBECONFORM_BIN /
#     ICEGRES_K8S_SCHEMA_DIR (wrong pinned version = hard FAIL)
#   - binaries already on PATH at exactly the pinned versions
#   - bootstrap into $HOME/.cache/icegres-helm-tools/<pin> (needs git,
#     go >= 1.24, and network; go may auto-download its 1.26 toolchain)
# If no path yields tools, every leg SKIPs with a loud reason and exit 0
# — the P3 PR gate itself must run on a box where bootstrap succeeds.
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/deploy/helm/icegres"
VALUES_DIR="$ROOT/deploy/helm/tests/values"
GOLDEN_DIR="$ROOT/deploy/helm/tests/golden"
PROFILES=(defaults ha tail-dir tail-quorum readreplicas-tls-auth flight-grpcweb)
RELEASE=icegres
NAMESPACE=icegres-system
KUBE_VERSIONS=(1.31.0 1.34.0)

# --- pins (change together with a recon of the new revs) --------------------
HELM_PIN=v3.21.3
HELM_SHA=1ad6e68924fdf6fb0c7dcef8e9e1dfc0f36eaed6
KUBECONFORM_PIN=v0.8.0
KUBECONFORM_SHA=02374e583d700721f57300fae78e11acd27ee539
SCHEMA_SHA=5a65d88146aaabf1648f5a21fca28b6abf196f83
SCHEMA_DIRS=(v1.31.0-standalone-strict v1.34.0-standalone-strict)
CACHE="${ICEGRES_HELM_TOOLS_CACHE:-$HOME/.cache/icegres-helm-tools}"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "ok  $1"; }
bad()  { FAIL=$((FAIL+1)); echo "FAIL $1"; }
die()  { echo "FATAL: $1" >&2; exit 1; }
skip_all() { echo "SKIP tests/helm.sh entirely: $1 (the P3 PR gate must run where bootstrap succeeds)"; exit 0; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --- 1. toolchain -------------------------------------------------------------
helm_version()        { "$1" version --template '{{.Version}}' 2>/dev/null; }
kubeconform_version() { "$1" -v 2>/dev/null; }
schema_dir_ok()       { local d; for d in "${SCHEMA_DIRS[@]}"; do [ -d "$1/$d" ] || return 1; done; }

resolve_helm() {
    if [ -n "${ICEGRES_HELM_BIN:-}" ]; then
        [ "$(helm_version "$ICEGRES_HELM_BIN")" = "$HELM_PIN" ] \
            || die "ICEGRES_HELM_BIN=$ICEGRES_HELM_BIN is not helm $HELM_PIN"
        HELM="$ICEGRES_HELM_BIN"; return 0
    fi
    if command -v helm >/dev/null 2>&1 && [ "$(helm_version helm)" = "$HELM_PIN" ]; then
        HELM="$(command -v helm)"; return 0
    fi
    return 1
}

resolve_kubeconform() {
    if [ -n "${ICEGRES_KUBECONFORM_BIN:-}" ]; then
        [ "$(kubeconform_version "$ICEGRES_KUBECONFORM_BIN")" = "$KUBECONFORM_PIN" ] \
            || die "ICEGRES_KUBECONFORM_BIN=$ICEGRES_KUBECONFORM_BIN is not kubeconform $KUBECONFORM_PIN"
        KUBECONFORM="$ICEGRES_KUBECONFORM_BIN"; return 0
    fi
    if command -v kubeconform >/dev/null 2>&1 \
        && [ "$(kubeconform_version kubeconform)" = "$KUBECONFORM_PIN" ]; then
        KUBECONFORM="$(command -v kubeconform)"; return 0
    fi
    return 1
}

resolve_schemas() {
    if [ -n "${ICEGRES_K8S_SCHEMA_DIR:-}" ]; then
        schema_dir_ok "$ICEGRES_K8S_SCHEMA_DIR" \
            || die "ICEGRES_K8S_SCHEMA_DIR=$ICEGRES_K8S_SCHEMA_DIR lacks ${SCHEMA_DIRS[*]}"
        SCHEMAS="$ICEGRES_K8S_SCHEMA_DIR"; return 0
    fi
    return 1
}

bootstrap_ready() { command -v git >/dev/null 2>&1 && command -v go >/dev/null 2>&1; }

go_new_enough() {
    # go >= 1.24 (the sources declare go 1.26; GOTOOLCHAIN auto-downloads it)
    local v
    v="$(go env GOVERSION 2>/dev/null | sed 's/^go//')" || return 1
    [ "$(printf '%s\n' "1.24" "$v" | sort -V | head -1)" = "1.24" ]
}

bootstrap_helm() {
    local dir="$CACHE/helm-$HELM_PIN-$HELM_SHA"
    HELM="$dir/helm"
    [ "$(helm_version "$HELM")" = "$HELM_PIN" ] && return 0
    echo "bootstrap: building helm $HELM_PIN@${HELM_SHA:0:9} into $dir ..."
    rm -rf "$dir" && mkdir -p "$dir" || return 1
    git clone -q --depth 1 --branch "$HELM_PIN" https://github.com/helm/helm "$dir/src" \
        || { echo "bootstrap: helm clone failed"; return 1; }
    [ "$(git -C "$dir/src" rev-parse HEAD)" = "$HELM_SHA" ] \
        || { echo "bootstrap: helm tag $HELM_PIN is not $HELM_SHA — refusing moved pin"; return 1; }
    (cd "$dir/src" && go build -ldflags "-X helm.sh/helm/v3/internal/version.version=$HELM_PIN \
        -X helm.sh/helm/v3/internal/version.gitCommit=$HELM_SHA \
        -X helm.sh/helm/v3/internal/version.gitTreeState=clean" \
        -o "$HELM" ./cmd/helm) || { echo "bootstrap: helm build failed"; return 1; }
    rm -rf "$dir/src"
    [ "$(helm_version "$HELM")" = "$HELM_PIN" ]
}

bootstrap_kubeconform() {
    local dir="$CACHE/kubeconform-$KUBECONFORM_PIN-$KUBECONFORM_SHA"
    KUBECONFORM="$dir/kubeconform"
    [ "$(kubeconform_version "$KUBECONFORM")" = "$KUBECONFORM_PIN" ] && return 0
    echo "bootstrap: building kubeconform $KUBECONFORM_PIN@${KUBECONFORM_SHA:0:9} into $dir ..."
    rm -rf "$dir" && mkdir -p "$dir" || return 1
    git clone -q --depth 1 --branch "$KUBECONFORM_PIN" https://github.com/yannh/kubeconform "$dir/src" \
        || { echo "bootstrap: kubeconform clone failed"; return 1; }
    [ "$(git -C "$dir/src" rev-parse HEAD)" = "$KUBECONFORM_SHA" ] \
        || { echo "bootstrap: kubeconform tag $KUBECONFORM_PIN is not $KUBECONFORM_SHA — refusing moved pin"; return 1; }
    (cd "$dir/src" && go build -ldflags "-X main.version=$KUBECONFORM_PIN" \
        -o "$KUBECONFORM" ./cmd/kubeconform) || { echo "bootstrap: kubeconform build failed"; return 1; }
    rm -rf "$dir/src"
    [ "$(kubeconform_version "$KUBECONFORM")" = "$KUBECONFORM_PIN" ]
}

bootstrap_schemas() {
    local dir="$CACHE/kubernetes-json-schema-$SCHEMA_SHA"
    SCHEMAS="$dir"
    schema_dir_ok "$dir" && [ -f "$dir/.icegres-pin-ok" ] && return 0
    echo "bootstrap: fetching Kubernetes JSON schemas @${SCHEMA_SHA:0:9} (blob-filtered sparse checkout: ${SCHEMA_DIRS[*]}) ..."
    rm -rf "$dir" && mkdir -p "$dir" || return 1
    # Pinned by sha, never a branch: fetch the commit directly (GitHub
    # allows fetching arbitrary shas), sparse-checkout only the two
    # version dirs (the full repo is many GB).
    git clone -q --filter=blob:none --no-checkout \
        https://github.com/yannh/kubernetes-json-schema "$dir" \
        || { echo "bootstrap: schema clone failed"; return 1; }
    git -C "$dir" fetch -q --depth 1 --filter=blob:none origin "$SCHEMA_SHA" \
        || { echo "bootstrap: schema fetch by sha failed"; return 1; }
    git -C "$dir" sparse-checkout set "${SCHEMA_DIRS[@]}" \
        || { echo "bootstrap: sparse-checkout failed"; return 1; }
    git -C "$dir" checkout -q "$SCHEMA_SHA" \
        || { echo "bootstrap: schema checkout failed"; return 1; }
    schema_dir_ok "$dir" || { echo "bootstrap: schema dirs missing after checkout"; return 1; }
    touch "$dir/.icegres-pin-ok"
}

HELM=""; KUBECONFORM=""; SCHEMAS=""
NEED_BOOTSTRAP=0
resolve_helm        || NEED_BOOTSTRAP=1
resolve_kubeconform || NEED_BOOTSTRAP=1
resolve_schemas     || NEED_BOOTSTRAP=1
if [ "$NEED_BOOTSTRAP" = 1 ]; then
    bootstrap_ready || skip_all "no pinned tools resolved and git/go are unavailable for bootstrap"
    go_new_enough   || skip_all "go >= 1.24 required to bootstrap the pinned tools (found $(go env GOVERSION 2>/dev/null || echo none))"
    [ -n "$HELM" ]        || bootstrap_helm        || skip_all "helm bootstrap failed (network to github.com/helm/helm + go toolchain download needed)"
    [ -n "$KUBECONFORM" ] || bootstrap_kubeconform || skip_all "kubeconform bootstrap failed (network to github.com/yannh/kubeconform needed)"
    [ -n "$SCHEMAS" ]     || bootstrap_schemas     || skip_all "schema bootstrap failed (network to github.com/yannh/kubernetes-json-schema needed)"
fi
echo "tools: helm=$HELM ($(helm_version "$HELM")) kubeconform=$KUBECONFORM ($(kubeconform_version "$KUBECONFORM"))"
echo "       schemas=$SCHEMAS"

# --- helpers over rendered output ---------------------------------------------
render() { # $1 profile -> file path on stdout
    local out="$TMP/render-$1.yaml"
    "$HELM" template "$RELEASE" "$CHART" --namespace "$NAMESPACE" \
        -f "$VALUES_DIR/$1.yaml" > "$out" 2> "$TMP/render-$1.err" \
        || { bad "helm template $1: $(head -2 "$TMP/render-$1.err" | tr '\n' ' ')"; return 1; }
    echo "$out"
}

# Extract one rendered document by kind+name (for scoped asserts).
# python3 rather than an awk state machine: it is already a dependency of
# this repo's test suites (e2e uses it) and keeps the split exact.
doc() { # $1 file, $2 kind, $3 name
    python3 - "$1" "$2" "$3" <<'EOF'
import sys
path, kind, name = sys.argv[1:4]
docs = open(path).read().split("\n---\n")
for d in docs:
    lines = d.splitlines()
    k = any(l.strip() == f"kind: {kind}" for l in lines)
    n = any(l.strip() == f"name: {name}" for l in lines[:12])
    if k and n:
        print(d)
        sys.exit(0)
sys.exit(1)
EOF
}

count() { grep -c -- "$2" "$1" 2>/dev/null || true; }

# --- 2. helm lint ---------------------------------------------------------------
for p in "${PROFILES[@]}"; do
    if "$HELM" lint "$CHART" -f "$VALUES_DIR/$p.yaml" > "$TMP/lint-$p.log" 2>&1; then
        ok "helm lint ($p)"
    else
        bad "helm lint ($p): $(grep -E 'ERROR|Error' "$TMP/lint-$p.log" | head -2 | tr '\n' ' ')"
    fi
done

# --- 3. golden fixtures -----------------------------------------------------------
mkdir -p "$GOLDEN_DIR"
for p in "${PROFILES[@]}"; do
    r="$(render "$p")" || continue
    if [ "${UPDATE_GOLDEN:-0}" = 1 ]; then
        cp "$r" "$GOLDEN_DIR/$p.yaml"
        ok "golden updated ($p)"
    elif [ ! -f "$GOLDEN_DIR/$p.yaml" ]; then
        bad "golden missing ($p) — run UPDATE_GOLDEN=1 tests/helm.sh and commit"
    elif diff -u "$GOLDEN_DIR/$p.yaml" "$r" > "$TMP/diff-$p"; then
        ok "golden matches ($p)"
    else
        bad "golden drift ($p) — inspect and re-commit if intended:"
        head -40 "$TMP/diff-$p"
    fi
done

# --- 4. offline schema validation ---------------------------------------------------
SCHEMA_LOC="$SCHEMAS/{{ .NormalizedKubernetesVersion }}-standalone-strict/{{ .ResourceKind }}{{ .KindSuffix }}.json"
for p in "${PROFILES[@]}"; do
    r="$TMP/render-$p.yaml"
    [ -f "$r" ] || continue
    for v in "${KUBE_VERSIONS[@]}"; do
        # ServiceMonitor is a Prometheus Operator CRD — not in the
        # Kubernetes schemas by definition; everything else must validate
        # STRICTLY (unknown fields rejected).
        if out=$("$KUBECONFORM" -strict -kubernetes-version "$v" -skip ServiceMonitor \
                -summary -schema-location "$SCHEMA_LOC" "$r" 2>&1); then
            ok "kubeconform $p @ $v ($(echo "$out" | tail -1 | sed 's/Summary: //'))"
        else
            bad "kubeconform $p @ $v:"; echo "$out" | head -6
        fi
    done
done
# Validator self-check: a corrupted manifest MUST be rejected (guards
# against a silently-permissive schema location).
sed 's/^  replicas: 1$/  replicas: "one"\n  bogusField: true/' "$TMP/render-defaults.yaml" > "$TMP/render-corrupt.yaml"
if "$KUBECONFORM" -strict -kubernetes-version "${KUBE_VERSIONS[0]}" -skip ServiceMonitor \
        -schema-location "$SCHEMA_LOC" "$TMP/render-corrupt.yaml" >/dev/null 2>&1; then
    bad "kubeconform self-check: corrupted manifest was NOT rejected"
else
    ok "kubeconform self-check (corrupted manifest rejected)"
fi

# --- 5. invariant asserts --------------------------------------------------------------
# (a) every profile: pods hardened, probes wired, namespace honest
for p in "${PROFILES[@]}"; do
    r="$TMP/render-$p.yaml"
    [ -f "$r" ] || continue
    pods=$(( $(count "$r" '^kind: Deployment$') + $(count "$r" '^kind: StatefulSet$') ))
    [ "$(count "$r" 'runAsNonRoot: true')" = "$pods" ] \
        && ok "runAsNonRoot on all $pods pod specs ($p)" \
        || bad "runAsNonRoot count != pod specs ($p)"
    containers=$(count "$r" 'image: "')
    [ "$(count "$r" 'readOnlyRootFilesystem: true')" = "$containers" ] \
        && ok "readOnlyRootFilesystem on all $containers containers ($p)" \
        || bad "readOnlyRootFilesystem count != containers ($p)"
    [ "$(count "$r" 'privileged: true')" = 0 ] \
        && ok "nothing privileged ($p)" || bad "privileged container ($p)"
    [ "$(count "$r" 'namespace: default')" = 0 ] \
        && ok "no default-namespace hardcode ($p)" || bad "default namespace hardcoded ($p)"
    docs=$(count "$r" '^kind: ')
    [ "$(count "$r" "namespace: $NAMESPACE")" -ge "$docs" ] \
        && ok "every object namespaced ($p)" || bad "un-namespaced object ($p)"
    # compute probes: liveness /health (the failover trigger) + /ready
    [ "$(count "$r" 'path: /health')" -ge 1 ] && [ "$(count "$r" 'path: /ready')" -ge 1 ] \
        && ok "compute probes wired to /health + /ready ($p)" \
        || bad "compute probe paths missing ($p)"
    # startup headroom: /health binds only AFTER catalog build (+ quorum
    # election + tail replay) — without a startupProbe the liveness probe
    # kills a slow boot ~20-30s in, forever (CrashLoopBackOff).
    [ "$(count "$r" 'startupProbe:')" -ge 1 ] \
        && ok "compute startupProbe present ($p)" \
        || bad "no startupProbe — slow boots get liveness-killed forever ($p)"
    # the writer's headless Service must publish not-ready addresses: a
    # catalog blip fails the catalog-aware /ready, and an unpublished
    # unready-but-alive writer black-holes icegresd's dial/wake path for
    # wakeTimeoutMs per new connection.
    doc "$r" Service "$RELEASE-writer" | grep -q 'publishNotReadyAddresses: true' \
        && ok "writer Service publishes not-ready addresses ($p)" \
        || bad "writer Service hides unready pods (catalog-blip black-hole) ($p)"
done

# (b) defaults: the minimal posture — no HA objects leak in
r="$TMP/render-defaults.yaml"
for kind in PodDisruptionBudget Role RoleBinding NetworkPolicy; do
    [ "$(count "$r" "^kind: $kind\$")" = 0 ] \
        && ok "defaults render no $kind" || bad "defaults leaked a $kind"
done
grep -q 'ICEGRESD_K8S_SCALE' "$r" && bad "defaults leaked ICEGRESD_K8S_SCALE" \
    || ok "defaults do not wire the scale hook"

# (c) ha profile: lease trio + PDBs + minimal RBAC + leadership readiness
r="$TMP/render-ha.yaml"
doc "$r" StatefulSet "$RELEASE-lease" >/dev/null \
    && ok "ha renders the dedicated lease trio" || bad "ha lease trio missing"
[ "$(count "$r" '^kind: PodDisruptionBudget$')" = 2 ] \
    && ok "ha renders both PDBs (icegresd + lease trio)" || bad "ha PDB count wrong"
# Leadership readiness pins the Ready count at 1 (only the leader), so ANY
# availability-demanding icegresd budget computes disruptionsAllowed = 0
# forever and deadlocks every drain of the leader's node. The icegresd PDB
# must always allow eviction (SIGTERM demote + lease takeover is the
# recovery); the lease trio keeps its real budget.
doc "$r" PodDisruptionBudget "$RELEASE" | grep -q 'maxUnavailable: 100%' \
    && ok "icegresd PDB never blocks a leader eviction (maxUnavailable 100%)" \
    || bad "icegresd PDB demands availability — leader drains deadlock (Ready count is pinned at 1)"
doc "$r" PodDisruptionBudget "$RELEASE-lease" | grep -q 'minAvailable: 2' \
    && ok "lease trio PDB keeps minAvailable 2" || bad "lease trio PDB wrong"
doc "$r" Deployment "$RELEASE" | grep -q 'preStop:' \
    && ok "icegresd preStop covers endpoint-removal propagation on eviction" \
    || bad "icegresd preStop missing (evicted leader vanishes before endpoints update)"
grep -q 'requiredDuringSchedulingIgnoredDuringExecution' "$r" \
    && ok "ha lease trio anti-affinity is required" || bad "ha anti-affinity missing"
grep -qF '\"leader\": true' "$r" \
    && ok "ha leadership readiness probe present" || bad "ha leadership readiness probe missing"
role="$(doc "$r" Role "$RELEASE-scale")" || { bad "ha Role missing"; role=""; }
if [ -n "$role" ]; then
    echo "$role" | grep -q 'resources: \["statefulsets/scale"\]' \
        && echo "$role" | grep -q "resourceNames: \[\"$RELEASE-writer\"\]" \
        && echo "$role" | grep -q 'verbs: \["get", "patch"\]' \
        && ok "RBAC scoped to exactly the writer scale subresource, get+patch" \
        || bad "RBAC broader than the one scale PATCH: $(echo "$role" | grep -A4 rules:)"
    [ "$(echo "$role" | grep -c 'apiGroups:')" = 1 ] \
        && ok "RBAC has a single rule" || bad "RBAC has extra rules"
fi
[ "$(count "$r" '^kind: ClusterRole$')" = 0 ] \
    && ok "no ClusterRole anywhere" || bad "ClusterRole rendered"
grep -q 'automountServiceAccountToken: true' "$r" \
    && ok "ha+scaling mounts the token on icegresd only ($(count "$r" 'automountServiceAccountToken: true')x true)" \
    || bad "icegresd token mount missing under k8sScaling"
[ "$(count "$r" 'automountServiceAccountToken: true')" = 1 ] \
    || bad "more than one pod mounts a serviceaccount token"

# (d) quorum profiles: keeper trio invariants
for p in tail-quorum readreplicas-tls-auth; do
    r="$TMP/render-$p.yaml"
    keeper="$(doc "$r" StatefulSet "$RELEASE-keeper")" \
        && ok "keeper trio rendered ($p)" || { bad "keeper trio missing ($p)"; continue; }
    echo "$keeper" | grep -q 'replicas: 3' \
        && ok "keeper trio is exactly 3 ($p)" || bad "keeper trio replicas != 3 ($p)"
    echo "$keeper" | grep -q 'requiredDuringSchedulingIgnoredDuringExecution' \
        && ok "keeper anti-affinity required ($p)" || bad "keeper anti-affinity missing ($p)"
    doc "$r" PodDisruptionBudget "$RELEASE-keeper" | grep -q 'minAvailable: 2' \
        && ok "keeper PDB minAvailable 2 ($p)" || bad "keeper PDB wrong ($p)"
    doc "$r" StatefulSet "$RELEASE-writer" | grep -q 'ICEGRES_TAIL_QUORUM' \
        && ok "writer wired to the keeper quorum ($p)" || bad "writer tail-quorum env missing ($p)"
    # The failover path is exactly where boot is slowest (election +
    # replay stack on top of catalog build): the WRITER pod itself must
    # carry the startupProbe, not just some pod in the render.
    doc "$r" StatefulSet "$RELEASE-writer" | grep -q 'startupProbe:' \
        && ok "quorum writer has startup headroom ($p)" \
        || bad "quorum writer missing startupProbe ($p)"
done
doc "$TMP/render-ha.yaml" StatefulSet "$RELEASE-writer" | grep -q 'startupProbe:' \
    && ok "ha writer has startup headroom" || bad "ha writer missing startupProbe"

# (e) tail-dir: WAL PVC on the writer
doc "$TMP/render-tail-dir.yaml" StatefulSet "$RELEASE-writer" | grep -q 'volumeClaimTemplates' \
    && ok "tail-dir writer has the WAL PVC" || bad "tail-dir writer PVC missing"

# (f) read replicas: peer-tail wired, tail/buffer env NEVER leaks in
r="$TMP/render-readreplicas-tls-auth.yaml"
read_doc="$(doc "$r" Deployment "$RELEASE-read")" \
    && ok "read Deployment rendered" || bad "read Deployment missing"
if [ -n "${read_doc:-}" ]; then
    echo "$read_doc" | grep -q "ICEGRES_PEER_TAILS" \
        && echo "$read_doc" | grep -q "value: \"$RELEASE-writer:" \
        && ok "read replicas peer-tail the writer's tail API" \
        || bad "read replica peer-tail wiring missing"
    echo "$read_doc" | grep -qE 'ICEGRES_TAIL_(QUORUM|DIR|URL)|ICEGRES_WRITE_BUFFER' \
        && bad "read replicas leaked tail/buffer env (would fence the writer)" \
        || ok "read replicas carry no tail/buffer env"
    echo "$read_doc" | grep -q 'ICEGRES_PEER_TAIL_PASSWORD' \
        && ok "peer-tail auth wired (SCRAM computes)" || bad "peer-tail auth missing"
    echo "$read_doc" | grep -q 'ICEGRES_FRESHNESS_MS' \
        && ok "read replicas run bounded-staleness freshness" || bad "freshness env missing"
    # freshnessMs > 0 replicas exist to keep serving bounded-stale reads
    # THROUGH a catalog outage — a catalog-gated /ready readiness would
    # empty the -read Service in exactly that outage.
    echo "$read_doc" | grep -A3 'readinessProbe:' | grep -q 'path: /health' \
        && ok "stale-serve replicas' readiness is not catalog-gated" \
        || bad "freshnessMs>0 replicas readiness-gated on /ready (Service empties during catalog outages)"
fi
grep -q 'ICEGRES_TLS_CERT' "$r" && ok "TLS terminates at the computes" || bad "TLS env missing"
grep -q 'ICEGRESD_POOL_SIZE' "$r" && doc "$r" Deployment "$RELEASE" | grep -A1 'ICEGRESD_POOL_SIZE' | grep -q '"0"' \
    && ok "session pooling disabled under SCRAM" || bad "pooling not disabled under auth"
[ "$(count "$r" '^kind: NetworkPolicy$')" = 4 ] \
    && ok "NetworkPolicies rendered for all 4 components" || bad "NetworkPolicy count wrong"
# clientFrom is empty in this profile and TLS terminates at the computes:
# the writer pgwire ingress must be open to ANY peer (NOTES.txt sends
# sslmode=require clients here — pinning it to release pods black-holes
# every TLS write), while the tail API stays release-pinned.
wnp="$(doc "$r" NetworkPolicy "$RELEASE-writer")" || bad "writer NetworkPolicy missing"
if [ -n "${wnp:-}" ]; then
    echo "$wnp" | grep -A1 -- "- port: 5439" | grep -q 'from:' \
        && bad "writer pgwire ingress pinned despite empty clientFrom (TLS-direct writes black-holed)" \
        || ok "writer pgwire ingress open to any peer when clientFrom is empty"
    echo "$wnp" | grep -A1 -- "- port: 5499" | grep -q 'from:' \
        && ok "writer tail API ingress stays release-pinned" \
        || bad "writer tail API ingress lost its release pinning"
fi
grep -q '^kind: ServiceMonitor$' "$r" \
    && ok "ServiceMonitor rendered when gated on" || bad "ServiceMonitor missing"

# (h) flight profile: the Arrow Flight SQL listener + gRPC-web surface
fr="$TMP/render-flight-grpcweb.yaml"
flight_doc="$(doc "$fr" Deployment "$RELEASE-flight")" \
    && ok "flight Deployment rendered" || bad "flight Deployment missing"
if [ -n "${flight_doc:-}" ]; then
    echo "$flight_doc" | grep -q 'ICEGRES_GRPC_WEB' \
        && ok "flight gRPC-web enabled" || bad "ICEGRES_GRPC_WEB missing"
    echo "$flight_doc" | grep -q 'value: "https://dash.example"' \
        && ok "flight CORS origin pinned" || bad "CORS origin not pinned"
    echo "$flight_doc" | grep -q 'ICEGRES_FLIGHT_TLS_CERT' \
        && ok "flight TLS terminates in-process" || bad "flight TLS env missing"
    echo "$flight_doc" | grep -q 'ICEGRES_AUTH_FILE' \
        && ok "flight basic auth wired" || bad "flight auth env missing"
    # Same fencing rule as read replicas: a Flight process opening the
    # writer's tail would fence it.
    echo "$flight_doc" | grep -qE 'ICEGRES_TAIL_(QUORUM|DIR|URL)|ICEGRES_WRITE_BUFFER' \
        && bad "flight leaked tail/buffer env (would fence the writer)" \
        || ok "flight carries no tail/buffer env"
fi
doc "$fr" NetworkPolicy "$RELEASE-flight" | grep -q "port: 50051" \
    && ok "flight NetworkPolicy segments the client port" \
    || bad "flight NetworkPolicy missing"
grep -q "^# Source: icegres/templates/flight-deployment.yaml" "$TMP/render-defaults.yaml" \
    && bad "defaults leaked the flight Deployment" \
    || ok "defaults render no flight objects"

echo "---- tests/helm.sh: $PASS passed, $FAIL failed"
exit $((FAIL > 0))
