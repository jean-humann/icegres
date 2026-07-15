//! Kubernetes scale hook for icegresd (P3 §4, `--k8s-scale`): wake and
//! idle-park the compute WORKLOAD by patching its apps/v1 `scale`
//! subresource, instead of forking child processes. In Kubernetes the
//! compute is a pod behind a Service, and a Service cannot wake a
//! scaled-to-zero workload on TCP connect — this module is the Knative
//! activator role, in ~200 lines, over machinery that is already in the
//! tree (reqwest is a direct dependency of this crate; its rustls TLS
//! backend arrives through the existing opendal feature unification, so
//! this adds ZERO new dependencies).
//!
//! The entire Kubernetes API surface used is two verbs on ONE
//! subresource of ONE named object:
//!
//! * `GET  /apis/apps/v1/namespaces/{ns}/{deployments|statefulsets}/{name}/scale`
//! * `PATCH` (same URL, `application/merge-patch+json`, body
//!   `{"spec":{"replicas":N}}`)
//!
//! which is exactly what the Helm chart's Role grants
//! (`resources: [statefulsets/scale]`, `resourceNames: [<writer>]`,
//! `verbs: [get, patch]` — see deploy/helm/icegres/templates/rbac.yaml).
//!
//! In-cluster plumbing follows the standard serviceaccount contract:
//! `KUBERNETES_SERVICE_HOST`/`KUBERNETES_SERVICE_PORT` name the API
//! server, `/var/run/secrets/kubernetes.io/serviceaccount/` provides
//! `ca.crt` (pinned as the ONLY trusted root), `namespace`, and `token`.
//! The token is re-read on every request — bound serviceaccount tokens
//! rotate and the kubelet refreshes the projected file.
//!
//! Test escape hatches (never needed in a cluster): `ICEGRESD_K8S_API_URL`
//! overrides the API base URL (a plain `http://` URL skips the CA
//! entirely) and `ICEGRESD_K8S_SA_DIR` overrides the serviceaccount
//! directory. Compiled into `icegresd` only (`#[path]` include, like the
//! quorum tree); nothing here touches the arrow/iceberg/datafusion stack.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context as _, Result};

/// Default in-cluster serviceaccount directory (`ca.crt`, `namespace`,
/// `token`).
const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Per-request budget. The scale subresource is a tiny object on the
/// local API server; anything slower than this is an outage and the
/// caller's TCP-readiness poll (or the next idle tick) retries anyway.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Parse `--k8s-scale`: exactly `deployments/<name>` or
/// `statefulsets/<name>` (the two apps/v1 kinds with a scale
/// subresource that can host a compute), name a valid DNS-1123
/// subdomain. Returns `(resource, name)`.
pub(crate) fn parse_scale_target(spec: &str) -> Result<(String, String)> {
    let Some((resource, name)) = spec.split_once('/') else {
        bail!(
            "--k8s-scale must be \"deployments/<name>\" or \"statefulsets/<name>\", got {spec:?}"
        );
    };
    if !matches!(resource, "deployments" | "statefulsets") {
        bail!(
            "--k8s-scale resource must be \"deployments\" or \"statefulsets\" (apps/v1), \
             got {resource:?}"
        );
    }
    let dns1123 = !name.is_empty()
        && name.len() <= 253
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        && !name.starts_with(['-', '.'])
        && !name.ends_with(['-', '.']);
    if !dns1123 {
        bail!("--k8s-scale object name {name:?} is not a DNS-1123 subdomain");
    }
    Ok((resource.to_string(), name.to_string()))
}

/// One workload's scale subresource, reachable with the pod's
/// serviceaccount. Built once at boot (loud failure if the pod has no
/// serviceaccount plumbing); the token file is re-read per request.
pub(crate) struct K8sScaler {
    client: reqwest::Client,
    scale_url: String,
    token_path: PathBuf,
    /// The validated `--k8s-scale` value, for logs.
    target: String,
}

impl K8sScaler {
    /// In-cluster construction from the standard environment/serviceaccount
    /// contract (with the test-only `ICEGRESD_K8S_API_URL` /
    /// `ICEGRESD_K8S_SA_DIR` overrides documented in the module docs).
    pub(crate) fn from_env(target: &str) -> Result<K8sScaler> {
        // Validate the flag's shape before anything environmental: a typo'd
        // target must be named as such even outside a cluster.
        parse_scale_target(target)?;
        let sa_dir = std::env::var("ICEGRESD_K8S_SA_DIR").unwrap_or_else(|_| SA_DIR.to_string());
        let api_url = match std::env::var("ICEGRESD_K8S_API_URL") {
            Ok(url) => url,
            Err(_) => {
                let host = std::env::var("KUBERNETES_SERVICE_HOST").context(
                    "--k8s-scale needs the in-cluster API environment \
                     (KUBERNETES_SERVICE_HOST is unset — not running in a Kubernetes pod?)",
                )?;
                let port =
                    std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".to_string());
                format!("https://{host}:{port}")
            }
        };
        Self::from_parts(&api_url, Path::new(&sa_dir), target)
    }

    /// Construction from explicit parts (unit tests point this at a mock
    /// server over plain HTTP; `from_env` is the thin in-cluster wrapper).
    pub(crate) fn from_parts(api_url: &str, sa_dir: &Path, target: &str) -> Result<K8sScaler> {
        let (resource, name) = parse_scale_target(target)?;
        let namespace_path = sa_dir.join("namespace");
        let namespace = std::fs::read_to_string(&namespace_path)
            .with_context(|| {
                format!(
                    "could not read the serviceaccount namespace {}",
                    namespace_path.display()
                )
            })?
            .trim()
            .to_string();
        let mut builder = reqwest::Client::builder().timeout(REQUEST_TIMEOUT);
        if api_url.starts_with("https://") {
            // Trust EXACTLY the cluster CA, nothing else: the API server's
            // serving cert chains to it and a container image's system
            // roots have no business vouching for it.
            let ca_path = sa_dir.join("ca.crt");
            let ca = std::fs::read(&ca_path)
                .with_context(|| format!("could not read the cluster CA {}", ca_path.display()))?;
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(&ca)
                    .with_context(|| format!("{} is not a PEM certificate", ca_path.display()))?,
            );
        }
        let client = builder
            .build()
            .context("could not build the Kubernetes API HTTP client")?;
        Ok(K8sScaler {
            client,
            scale_url: format!(
                "{}/apis/apps/v1/namespaces/{namespace}/{resource}/{name}/scale",
                api_url.trim_end_matches('/')
            ),
            token_path: sa_dir.join("token"),
            target: target.to_string(),
        })
    }

    /// The validated `--k8s-scale` value (for logs).
    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    /// Re-read per request: bound serviceaccount tokens rotate and the
    /// kubelet refreshes the projected file in place.
    async fn token(&self) -> Result<String> {
        let raw = tokio::fs::read_to_string(&self.token_path)
            .await
            .with_context(|| {
                format!(
                    "could not read the serviceaccount token {}",
                    self.token_path.display()
                )
            })?;
        Ok(raw.trim().to_string())
    }

    /// `GET .../scale` → `.spec.replicas`.
    pub(crate) async fn replicas(&self) -> Result<i64> {
        let resp = self
            .client
            .get(&self.scale_url)
            .bearer_auth(self.token().await?)
            .send()
            .await
            .with_context(|| format!("scale GET {} failed", self.scale_url))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("scale GET {} -> {status}: {}", self.scale_url, brief(&body));
        }
        let v: serde_json::Value = serde_json::from_str(&body)
            .with_context(|| format!("scale GET returned non-JSON: {}", brief(&body)))?;
        v["spec"]["replicas"]
            .as_i64()
            .with_context(|| format!("scale GET response has no .spec.replicas: {}", brief(&body)))
    }

    /// `PATCH .../scale` with `{"spec":{"replicas":N}}` (merge patch —
    /// idempotent, no resourceVersion dance for a single integer).
    pub(crate) async fn set_replicas(&self, n: i64) -> Result<()> {
        let resp = self
            .client
            .patch(&self.scale_url)
            .bearer_auth(self.token().await?)
            .header("content-type", "application/merge-patch+json")
            .body(format!("{{\"spec\":{{\"replicas\":{n}}}}}"))
            .send()
            .await
            .with_context(|| format!("scale PATCH {} failed", self.scale_url))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "scale PATCH {} -> {status}: {}",
                self.scale_url,
                brief(&body)
            );
        }
        Ok(())
    }

    /// Wake-on-connect: scale 0 -> 1 (and only 0 -> 1: replicas already
    /// managed up by an operator or HPA are left alone). Returns whether a
    /// PATCH was issued.
    pub(crate) async fn wake(&self) -> Result<bool> {
        if self.replicas().await? > 0 {
            return Ok(false);
        }
        self.set_replicas(1).await?;
        Ok(true)
    }
}

/// First line of an API error body, bounded — enough to name the cause
/// (Kubernetes Status messages are single-line JSON) without dumping a
/// whole object into the log.
fn brief(body: &str) -> String {
    let line = body.lines().next().unwrap_or("").trim();
    let mut s = line.chars().take(300).collect::<String>();
    if s.len() < line.len() {
        s.push('…');
    }
    if s.is_empty() {
        s.push_str("(empty body)");
    }
    s
}

// ---------------------------------------------------------------------------
// Unit tests: the target parse (pure) and the exact HTTP shapes against a
// raw-TCP mock API server (plain-http `from_parts`, so no cluster and no
// TLS fixture is needed — the CA-pinning branch is exercised in a real
// cluster by the documented smoke procedure).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};

    #[test]
    fn scale_target_parses_and_rejects() {
        assert_eq!(
            parse_scale_target("statefulsets/demo-writer").unwrap(),
            ("statefulsets".to_string(), "demo-writer".to_string())
        );
        assert_eq!(
            parse_scale_target("deployments/r.0").unwrap(),
            ("deployments".to_string(), "r.0".to_string())
        );
        for bad in [
            "demo-writer",           // no resource
            "daemonsets/x",          // no scale subresource for computes
            "statefulsets/",         // empty name
            "statefulsets/Bad_Name", // not DNS-1123
            "statefulsets/-leading", // bad edge char
            "deployments/trailing-", // bad edge char
            "statefulsets/a/b",      // stray slash lands in the name
        ] {
            assert!(parse_scale_target(bad).is_err(), "accepted {bad:?}");
        }
    }

    /// A one-thread mock API server: for each scripted response, accept one
    /// connection, read one full HTTP/1.1 request (headers + content-length
    /// body), answer with `connection: close`, and record the raw request.
    fn mock_api(
        responses: Vec<(u16, &'static str)>,
    ) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock api");
        let addr = listener.local_addr().expect("mock api addr");
        let handle = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for (status, body) in responses {
                let (mut conn, _) = listener.accept().expect("mock api accept");
                let mut raw = Vec::new();
                let mut chunk = [0u8; 1024];
                let request = loop {
                    let n = conn.read(&mut chunk).expect("mock api read");
                    assert!(n > 0, "client closed mid-request");
                    raw.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&raw).into_owned();
                    if let Some(head_end) = text.find("\r\n\r\n") {
                        let content_length = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(str::trim)
                                    .map(str::to_string)
                            })
                            .map(|v| v.parse::<usize>().expect("content-length"))
                            .unwrap_or(0);
                        if raw.len() >= head_end + 4 + content_length {
                            break text;
                        }
                    }
                };
                seen.push(request);
                let reason = if status == 200 { "OK" } else { "Error" };
                let resp = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                conn.write_all(resp.as_bytes()).expect("mock api write");
            }
            seen
        });
        (format!("http://{addr}"), handle)
    }

    /// A serviceaccount directory with `namespace` + `token` (no `ca.crt`:
    /// the mock is plain http, which skips the CA branch by design).
    fn mock_sa_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("icegres-k8s-sa-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("mock sa dir");
        std::fs::write(dir.join("namespace"), "testns\n").expect("namespace");
        std::fs::write(dir.join("token"), "tok-123\n").expect("token");
        dir
    }

    #[tokio::test]
    async fn wake_scales_zero_to_one_with_the_exact_api_shapes() {
        let (url, server) = mock_api(vec![
            (
                200,
                r#"{"kind":"Scale","spec":{"replicas":0},"status":{"replicas":0}}"#,
            ),
            (
                200,
                r#"{"kind":"Scale","spec":{"replicas":1},"status":{"replicas":0}}"#,
            ),
        ]);
        let sa = mock_sa_dir("wake");
        let scaler = K8sScaler::from_parts(&url, &sa, "statefulsets/demo-writer").expect("scaler");
        assert!(scaler.wake().await.expect("wake"), "expected a PATCH");
        let seen = server.join().expect("mock api thread");
        assert_eq!(seen.len(), 2);
        let get = seen[0].to_ascii_lowercase();
        assert!(
            seen[0].starts_with(
                "GET /apis/apps/v1/namespaces/testns/statefulsets/demo-writer/scale HTTP/1.1"
            ),
            "GET line: {}",
            seen[0].lines().next().unwrap_or("")
        );
        assert!(
            get.contains("authorization: bearer tok-123"),
            "token sent: {get}"
        );
        let patch = seen[1].to_ascii_lowercase();
        assert!(
            seen[1].starts_with(
                "PATCH /apis/apps/v1/namespaces/testns/statefulsets/demo-writer/scale HTTP/1.1"
            ),
            "PATCH line: {}",
            seen[1].lines().next().unwrap_or("")
        );
        assert!(
            patch.contains("content-type: application/merge-patch+json"),
            "merge-patch content type: {patch}"
        );
        assert!(
            patch.ends_with(r#"{"spec":{"replicas":1}}"#),
            "merge-patch body: {patch}"
        );
        let _ = std::fs::remove_dir_all(&sa);
    }

    #[tokio::test]
    async fn wake_leaves_a_running_workload_alone() {
        let (url, server) = mock_api(vec![(
            200,
            r#"{"kind":"Scale","spec":{"replicas":2},"status":{"replicas":2}}"#,
        )]);
        let sa = mock_sa_dir("noop");
        let scaler = K8sScaler::from_parts(&url, &sa, "deployments/demo").expect("scaler");
        assert!(!scaler.wake().await.expect("wake"), "no PATCH expected");
        assert_eq!(server.join().expect("mock api thread").len(), 1);
        let _ = std::fs::remove_dir_all(&sa);
    }

    #[tokio::test]
    async fn api_errors_carry_status_and_cause() {
        let (url, server) = mock_api(vec![(
            403,
            r#"{"kind":"Status","status":"Failure","message":"scale is forbidden"}"#,
        )]);
        let sa = mock_sa_dir("err");
        let scaler = K8sScaler::from_parts(&url, &sa, "statefulsets/demo").expect("scaler");
        let err = format!("{:#}", scaler.replicas().await.expect_err("403 must error"));
        assert!(err.contains("403"), "status named: {err}");
        assert!(err.contains("forbidden"), "cause named: {err}");
        server.join().expect("mock api thread");
        let _ = std::fs::remove_dir_all(&sa);
    }

    #[test]
    fn boot_fails_loudly_without_the_serviceaccount_contract() {
        let missing = std::env::temp_dir().join("icegres-k8s-sa-missing-nonexistent");
        let err = match K8sScaler::from_parts("http://127.0.0.1:1", &missing, "statefulsets/x") {
            Ok(_) => panic!("missing namespace must fail"),
            Err(e) => format!("{e:#}"),
        };
        assert!(err.contains("namespace"), "names the missing piece: {err}");
    }
}
