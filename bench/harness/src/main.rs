//! icegres benchmark harness (bench/SPEC.md §2).
//!
//! Connects to a running icegres server over the Postgres wire protocol and
//! measures the 11 metrics from the spec. Emits a single machine-readable
//! JSON document on stdout; human-readable progress goes to stderr.
//!
//! Usage:
//!   icegres-bench --host 127.0.0.1 --port 5439 \
//!       --server-bin /path/to/release/icegres \
//!       --server-pid <pid-of-running-serve> \
//!       --cold-port 5442
//!
//! Method: every latency metric discards 3 warmup iterations and reports
//! p50/p95 over >= 20 measured iterations. qps_8conn is the MEDIAN of 3
//! consecutive 10 s windows after a warmup window (single-window qps showed
//! up to +/-34% run-to-run noise). cold_start_ms is >= 5 spawn->ready runs of
//! the release binary.
//!
//! Resource metrics (first-class, gated — see bench/gate.sh):
//!   binary_size_mb    size of --server-bin;
//!   rss_idle_mb       VmRSS of --server-pid after a light warmup, idle;
//!   rss_peak_mb       max VmRSS of --server-pid sampled every 100 ms across
//!                     the whole benchmark (plus the qps-window-only peak);
//!   rss_after_load_mb VmRSS after all load has finished (1 s settle).

use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};
use tokio_postgres::{Client, NoTls};

const WARMUP: usize = 3;
const ITERS: usize = 20;
const COLD_RUNS: usize = 5;
const QPS_CONNS: usize = 8;
const QPS_WINDOW_S: u64 = 10;
const QPS_WINDOWS: usize = 3;
const RSS_SAMPLE_MS: u64 = 100;

/// Write metrics target a bench-owned scratch table (created fresh and
/// dropped by bench.sh via the REST catalog each run) so demo.trips never
/// grows during a benchmark: append-only Iceberg means every insert adds a
/// Parquet file + snapshot, and a growing demo.trips makes read metrics
/// drift monotonically between runs (baseline runs could never agree).
const SCRATCH: &str = "demo.bench_scratch";

const Q_POINT: &str = "select trip_id, city, distance_km, fare, ts from demo.trips where trip_id = $1";
const Q_FILTER: &str =
    "select count(*) from demo.trips where city = 'Paris' and distance_km > 20";
const Q_AGG: &str =
    "select city, count(*) as trips from demo.trips group by city order by trips desc, city asc limit 5";
const Q_JOIN: &str = "select c.country, count(*) as trips from demo.trips t join demo.cities c on t.city = c.city group by c.country order by trips desc, c.country asc";

struct Args {
    host: String,
    port: u16,
    server_bin: String,
    server_pid: Option<u32>,
    cold_port: u16,
}

fn parse_args() -> Args {
    let mut host = "127.0.0.1".to_string();
    let mut port = 5439u16;
    let mut server_bin = String::new();
    let mut server_pid = None;
    let mut cold_port = 5442u16;
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        let need = |i: usize| -> &str {
            argv.get(i + 1)
                .unwrap_or_else(|| panic!("missing value for {}", argv[i]))
        };
        match argv[i].as_str() {
            "--host" => host = need(i).to_string(),
            "--port" => port = need(i).parse().expect("bad --port"),
            "--server-bin" => server_bin = need(i).to_string(),
            "--server-pid" => server_pid = Some(need(i).parse().expect("bad --server-pid")),
            "--cold-port" => cold_port = need(i).parse().expect("bad --cold-port"),
            other => panic!("unknown argument: {other}"),
        }
        i += 2;
    }
    if server_bin.is_empty() {
        panic!("--server-bin is required (release icegres binary path)");
    }
    Args {
        host,
        port,
        server_bin,
        server_pid,
        cold_port,
    }
}

fn conn_str(host: &str, port: u16) -> String {
    format!("host={host} port={port} user=postgres dbname=icegres connect_timeout=5")
}

async fn connect(host: &str, port: u16) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(&conn_str(host, port), NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = (p / 100.0) * (sorted.len() as f64 - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        sorted[lo] + (sorted[hi] - sorted[lo]) * (rank - lo as f64)
    }
}

fn summarize(mut samples: Vec<f64>) -> Value {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    json!({
        "p50": round2(percentile(&samples, 50.0)),
        "p95": round2(percentile(&samples, 95.0)),
        "min": round2(samples.first().copied().unwrap_or(f64::NAN)),
        "max": round2(samples.last().copied().unwrap_or(f64::NAN)),
        "n": samples.len(),
    })
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn eprint_metric(name: &str, v: &Value) {
    eprintln!("[bench] {name}: {v}");
}

/// Time an async closure over WARMUP+ITERS iterations, discard warmups.
async fn timed_loop<F, Fut>(mut f: F) -> Vec<f64>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut samples = Vec::with_capacity(ITERS);
    for i in 0..(WARMUP + ITERS) {
        let t0 = Instant::now();
        f(i).await;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if i >= WARMUP {
            samples.push(ms);
        }
    }
    samples
}

async fn measure_connect(host: &str, port: u16) -> Vec<f64> {
    let mut samples = Vec::with_capacity(ITERS);
    for i in 0..(WARMUP + ITERS) {
        let t0 = Instant::now();
        let client = connect(host, port).await.expect("connect failed");
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        drop(client);
        if i >= WARMUP {
            samples.push(ms);
        }
    }
    samples
}

async fn measure_query(client: &Client, sql: &str, param: Option<i64>) -> Vec<f64> {
    timed_loop(|_| async move {
        let rows = match param {
            Some(p) => client.query(sql, &[&p]).await.expect("query failed"),
            None => client.query(sql, &[]).await.expect("query failed"),
        };
        assert!(!rows.is_empty(), "query returned no rows: {sql}");
    })
    .await
}

async fn max_trip_id(client: &Client) -> i64 {
    let rows = client
        .query(
            &format!("select max(trip_id) from {SCRATCH}"),
            &[],
        )
        .await
        .expect("max(trip_id) on scratch table failed");
    rows[0].get::<_, Option<i64>>(0).unwrap_or(0)
}

fn insert_sql(base_id: i64, n: i64) -> String {
    let mut vals = Vec::with_capacity(n as usize);
    for k in 0..n {
        let id = base_id + k;
        vals.push(format!(
            "({id}, 'Bench City', 3.14, 9.99, TIMESTAMP '2026-01-01 00:00:00')"
        ));
    }
    format!(
        "insert into {SCRATCH} (trip_id, city, distance_km, fare, ts) values {}",
        vals.join(", ")
    )
}

/// Single-row inserts; returns (samples, next_free_id).
async fn measure_insert_single(client: &Client, mut next_id: i64) -> (Vec<f64>, i64) {
    let mut samples = Vec::with_capacity(ITERS);
    for i in 0..(WARMUP + ITERS) {
        let sql = insert_sql(next_id, 1);
        next_id += 1;
        let t0 = Instant::now();
        client.execute(&sql, &[]).await.expect("insert failed");
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if i >= WARMUP {
            samples.push(ms);
        }
    }
    (samples, next_id)
}

async fn measure_insert_batch(client: &Client, mut next_id: i64) -> (Vec<f64>, i64) {
    let mut samples = Vec::with_capacity(ITERS);
    for i in 0..(WARMUP + ITERS) {
        let sql = insert_sql(next_id, 100);
        next_id += 100;
        let t0 = Instant::now();
        let n = client.execute(&sql, &[]).await.expect("batch insert failed");
        assert_eq!(n, 100, "batch insert affected {n} rows, expected 100");
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if i >= WARMUP {
            samples.push(ms);
        }
    }
    (samples, next_id)
}

/// Freshness: commit on conn A -> first successful readback on conn B,
/// polling every 10 ms. Clock starts when the INSERT completes.
async fn measure_freshness(
    writer: &Client,
    reader: &Client,
    mut next_id: i64,
) -> (Vec<f64>, i64) {
    let mut samples = Vec::with_capacity(ITERS);
    for i in 0..(WARMUP + ITERS) {
        let id = next_id;
        next_id += 1;
        writer
            .execute(&insert_sql(id, 1), &[])
            .await
            .expect("freshness insert failed");
        let t0 = Instant::now();
        loop {
            let rows = reader
                .query(
                    &format!("select trip_id from {SCRATCH} where trip_id = $1"),
                    &[&id],
                )
                .await
                .expect("freshness readback failed");
            if !rows.is_empty() {
                break;
            }
            if t0.elapsed() > Duration::from_secs(30) {
                panic!("freshness: row {id} not visible on reader after 30s");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if i >= WARMUP {
            samples.push(ms);
        }
    }
    (samples, next_id)
}

/// Mixed read workload over 8 connections. Runs a discarded 2 s warmup
/// window, then QPS_WINDOWS consecutive QPS_WINDOW_S-second windows; the
/// reported value is the MEDIAN window (single-window qps was observed to be
/// noisy up to +/-34% run-to-run). Returns (median, per-window values).
async fn measure_qps(host: &str, port: u16) -> (f64, Vec<f64>) {
    async fn window(host: &str, port: u16, secs: u64) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut handles = Vec::new();
        for c in 0..QPS_CONNS {
            let host = host.to_string();
            handles.push(tokio::spawn(async move {
                let client = connect(&host, port).await.expect("qps connect failed");
                let mut count: u64 = 0;
                let mut k = c; // stagger the mix across connections
                while Instant::now() < deadline {
                    match k % 4 {
                        0 => {
                            let id = 1 + (k as i64 % 280);
                            let _ = client.query(Q_POINT, &[&id]).await.expect("qps point");
                        }
                        1 => {
                            let _ = client.query(Q_FILTER, &[]).await.expect("qps filter");
                        }
                        2 => {
                            let _ = client.query(Q_AGG, &[]).await.expect("qps agg");
                        }
                        _ => {
                            let _ = client.query(Q_JOIN, &[]).await.expect("qps join");
                        }
                    }
                    count += 1;
                    k += 1;
                }
                count
            }));
        }
        let mut total = 0u64;
        for h in handles {
            total += h.await.expect("qps task panicked");
        }
        total
    }
    let _ = window(host, port, 2).await; // warmup, discarded
    let mut windows = Vec::with_capacity(QPS_WINDOWS);
    for _ in 0..QPS_WINDOWS {
        let total = window(host, port, QPS_WINDOW_S).await;
        windows.push(round2(total as f64 / QPS_WINDOW_S as f64));
    }
    let mut sorted = windows.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (percentile(&sorted, 50.0), windows)
}

fn spawn_server(bin: &str, host: &str, port: u16) -> Child {
    Command::new(bin)
        .args(["serve", "--host", host, "--port", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn icegres serve for cold start")
}

async fn measure_cold_start(bin: &str, host: &str, port: u16) -> Vec<f64> {
    let mut samples = Vec::with_capacity(COLD_RUNS);
    for run in 0..COLD_RUNS {
        let t0 = Instant::now();
        let mut child = spawn_server(bin, host, port);
        let ms = loop {
            if let Ok(client) = connect(host, port).await {
                if client.simple_query("select 1").await.is_ok() {
                    break t0.elapsed().as_secs_f64() * 1000.0;
                }
            }
            if t0.elapsed() > Duration::from_secs(60) {
                let _ = child.kill();
                panic!("cold start run {run}: server not ready in 60s");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        samples.push(ms);
        child.kill().expect("failed to kill cold-start server");
        let _ = child.wait();
        // Wait for the port to actually be free again before the next run.
        let free_t0 = Instant::now();
        while connect(host, port).await.is_ok() {
            if free_t0.elapsed() > Duration::from_secs(10) {
                panic!("cold start: port {port} still occupied after kill");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    samples
}

fn binary_size_mb(bin: &str) -> f64 {
    let meta = std::fs::metadata(bin).expect("stat --server-bin failed");
    round2(meta.len() as f64 / (1024.0 * 1024.0))
}

fn rss_kb(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().trim_end_matches("kB").trim().parse().ok();
        }
    }
    None
}

fn rss_mb(pid: u32) -> Option<f64> {
    rss_kb(pid).map(|kb| round2(kb as f64 / 1024.0))
}

/// Background RSS sampler: reads VmRSS of the server pid every RSS_SAMPLE_MS
/// on a dedicated OS thread (independent of the tokio runtime, so sampling
/// keeps its cadence while the async load loops saturate the executor).
/// Tracks the peak over the whole run and, separately, the peak inside the
/// qps window (flagged on/off by the driver).
struct RssSampler {
    stop: Arc<AtomicBool>,
    qps_active: Arc<AtomicBool>,
    peak_kb: Arc<AtomicU64>,
    qps_peak_kb: Arc<AtomicU64>,
    samples: Arc<AtomicU64>,
    handle: std::thread::JoinHandle<()>,
}

impl RssSampler {
    fn start(pid: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let qps_active = Arc::new(AtomicBool::new(false));
        let peak_kb = Arc::new(AtomicU64::new(0));
        let qps_peak_kb = Arc::new(AtomicU64::new(0));
        let samples = Arc::new(AtomicU64::new(0));
        let handle = {
            let (stop, qps_active, peak_kb, qps_peak_kb, samples) = (
                stop.clone(),
                qps_active.clone(),
                peak_kb.clone(),
                qps_peak_kb.clone(),
                samples.clone(),
            );
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Some(kb) = rss_kb(pid) {
                        peak_kb.fetch_max(kb, Ordering::Relaxed);
                        if qps_active.load(Ordering::Relaxed) {
                            qps_peak_kb.fetch_max(kb, Ordering::Relaxed);
                        }
                        samples.fetch_add(1, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(RSS_SAMPLE_MS));
                }
            })
        };
        Self {
            stop,
            qps_active,
            peak_kb,
            qps_peak_kb,
            samples,
            handle,
        }
    }

    fn set_qps_window(&self, active: bool) {
        self.qps_active.store(active, Ordering::Relaxed);
    }

    /// Stop sampling; returns (peak_mb, qps_window_peak_mb, sample_count).
    fn finish(self) -> (f64, f64, u64) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.handle.join();
        let to_mb = |kb: u64| round2(kb as f64 / 1024.0);
        (
            to_mb(self.peak_kb.load(Ordering::Relaxed)),
            to_mb(self.qps_peak_kb.load(Ordering::Relaxed)),
            self.samples.load(Ordering::Relaxed),
        )
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let mut metrics = Map::new();

    // Footprint first (server is idle-ish before we hammer it).
    let bsz = binary_size_mb(&args.server_bin);
    metrics.insert("binary_size_mb".into(), json!({ "value": bsz }));
    eprint_metric("binary_size_mb", &json!(bsz));

    let client = connect(&args.host, args.port)
        .await
        .expect("cannot connect to icegres server");
    // Light warmup so RSS reflects a server that has actually served queries,
    // then let it settle idle for a moment.
    for _ in 0..WARMUP {
        let _ = client.query(Q_AGG, &[]).await.expect("warmup query failed");
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    if let Some(pid) = args.server_pid {
        let rss = rss_mb(pid).expect("could not read VmRSS for --server-pid");
        metrics.insert("rss_idle_mb".into(), json!({ "value": rss }));
        eprint_metric("rss_idle_mb", &json!(rss));
    } else {
        metrics.insert("rss_idle_mb".into(), json!({ "value": null, "note": "no --server-pid given" }));
    }

    // From here on, sample the server RSS every RSS_SAMPLE_MS for the whole
    // benchmark (rss_peak_mb) with a separate peak for the qps window.
    let sampler = args.server_pid.map(RssSampler::start);

    // Latency metrics: reads first so they run against the pre-insert table.
    let s = summarize(measure_connect(&args.host, args.port).await);
    eprint_metric("connect_ms", &s);
    metrics.insert("connect_ms".into(), s);

    let s = summarize(measure_query(&client, Q_POINT, Some(42)).await);
    eprint_metric("point_lookup_ms", &s);
    metrics.insert("point_lookup_ms".into(), s);

    let s = summarize(measure_query(&client, Q_FILTER, None).await);
    eprint_metric("filtered_scan_ms", &s);
    metrics.insert("filtered_scan_ms".into(), s);

    let s = summarize(measure_query(&client, Q_AGG, None).await);
    eprint_metric("aggregate_ms", &s);
    metrics.insert("aggregate_ms".into(), s);

    let s = summarize(measure_query(&client, Q_JOIN, None).await);
    eprint_metric("join_ms", &s);
    metrics.insert("join_ms".into(), s);

    // QPS before the write metrics so the read mix sees the same table state.
    if let Some(s) = sampler.as_ref() {
        s.set_qps_window(true);
    }
    let (qps, qps_windows) = measure_qps(&args.host, args.port).await;
    if let Some(s) = sampler.as_ref() {
        s.set_qps_window(false);
    }
    let v = json!({
        "value": round2(qps),
        "windows": qps_windows,
        "aggregation": format!("median of {QPS_WINDOWS} consecutive windows"),
        "connections": QPS_CONNS,
        "window_s": QPS_WINDOW_S,
    });
    eprint_metric("qps_8conn", &v);
    metrics.insert("qps_8conn".into(), v);

    // Writes go to the bench-owned scratch table (fresh each run, dropped by
    // bench.sh afterwards) so demo.trips stays byte-identical across runs.
    // Unique ids >= 2_000_000 guard against a leftover scratch table.
    let max_id = max_trip_id(&client).await;
    let next_id = std::cmp::max(max_id + 1, 2_000_000);

    let writer = connect(&args.host, args.port).await.expect("writer connect");
    let (samples, next_id) = measure_insert_single(&writer, next_id).await;
    let s = summarize(samples);
    eprint_metric("insert_single_ms", &s);
    metrics.insert("insert_single_ms".into(), s);

    let (samples, next_id) = measure_insert_batch(&writer, next_id).await;
    let s = summarize(samples);
    eprint_metric("insert_batch100_ms", &s);
    metrics.insert("insert_batch100_ms".into(), s);

    let reader = connect(&args.host, args.port).await.expect("reader connect");
    let (samples, _next_id) = measure_freshness(&writer, &reader, next_id).await;
    let s = summarize(samples);
    eprint_metric("freshness_ms", &s);
    metrics.insert("freshness_ms".into(), s);

    // Cold start against a dedicated port so the main server keeps running.
    let samples = measure_cold_start(&args.server_bin, &args.host, args.cold_port).await;
    let s = summarize(samples);
    eprint_metric("cold_start_ms", &s);
    metrics.insert("cold_start_ms".into(), s);

    // Resource wrap-up: stop the sampler, then read the settled post-load RSS.
    match (sampler, args.server_pid) {
        (Some(sampler), Some(pid)) => {
            let (peak_mb, qps_peak_mb, n) = sampler.finish();
            let v = json!({
                "value": peak_mb,
                "qps_window_peak_mb": qps_peak_mb,
                "samples": n,
                "interval_ms": RSS_SAMPLE_MS,
            });
            eprint_metric("rss_peak_mb", &v);
            metrics.insert("rss_peak_mb".into(), v);

            tokio::time::sleep(Duration::from_secs(1)).await; // settle
            let after = rss_mb(pid).expect("could not read VmRSS for --server-pid");
            let v = json!({ "value": after });
            eprint_metric("rss_after_load_mb", &v);
            metrics.insert("rss_after_load_mb".into(), v);
        }
        _ => {
            let note = json!({ "value": null, "note": "no --server-pid given" });
            metrics.insert("rss_peak_mb".into(), note.clone());
            metrics.insert("rss_after_load_mb".into(), note);
        }
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let out = json!({
        "schema": "icegres-bench-v1",
        "unix_ts": ts,
        "host": args.host,
        "port": args.port,
        "server_bin": args.server_bin,
        "warmup_discarded": WARMUP,
        "iterations": ITERS,
        "cold_start_runs": COLD_RUNS,
        "metrics": Value::Object(metrics),
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}
