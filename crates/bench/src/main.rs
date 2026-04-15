use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use spooky_config::config::{Backend, HealthCheck, LoadBalancing, RouteMatch, Upstream};
use spooky_edge::benchmark::{ConnectionLookupBench, RouteLookupBench};
use spooky_lb::UpstreamPool;

struct CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BenchSuite {
    Micro,
    Macro,
    All,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum FailOn {
    Severe,
    Any,
}

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Spooky benchmark suite (micro + macro + regression gates)"
)]
struct Args {
    #[arg(long, default_value = "bench/latest.json")]
    output: PathBuf,

    #[arg(long)]
    markdown_out: Option<PathBuf>,

    #[arg(long)]
    baseline: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    check_baseline: bool,

    #[arg(long, value_enum, default_value_t = BenchSuite::Micro)]
    suite: BenchSuite,

    #[arg(long, default_value = "full")]
    profile: String,

    #[arg(long, default_value = "bench/manifest.yaml")]
    manifest: PathBuf,

    #[arg(long, default_value = "bench/baselines/releases.json")]
    baseline_index: PathBuf,

    #[arg(long)]
    baseline_release: Option<String>,

    #[arg(long, value_enum, default_value_t = FailOn::Severe)]
    fail_on: FailOn,

    #[arg(long)]
    cpu_threshold: Option<f64>,

    #[arg(long)]
    mem_threshold: Option<f64>,

    #[arg(long)]
    promote_release: Option<String>,

    #[arg(long, default_value = "bench/latest.json")]
    promote_micro_report: PathBuf,

    #[arg(long, default_value = "bench/macro/latest.json")]
    promote_macro_report: PathBuf,

    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    set_current_release: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchCase {
    #[serde(default = "default_case_kind")]
    kind: String,
    name: String,
    scale: usize,
    iterations: u64,
    duration_ns: u128,
    latency_ns_per_op: f64,
    #[serde(default)]
    throughput_ops_per_sec: f64,
    alloc_calls: u64,
    alloc_bytes: u64,
    rss_delta_kb: u64,
    #[serde(default)]
    cpu_pct: f64,
    #[serde(default)]
    latency_p50_ns: f64,
    #[serde(default)]
    latency_p95_ns: f64,
    #[serde(default)]
    latency_p99_ns: f64,
    #[serde(default)]
    latency_max_ns: f64,
    #[serde(default)]
    latency_sampled: bool,
}

fn default_case_kind() -> String {
    "micro".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct BenchReport {
    #[serde(default)]
    suite: String,
    #[serde(default)]
    report_kind: String,
    #[serde(default)]
    profile: String,
    #[serde(default)]
    generated_unix_secs: u64,
    #[serde(default)]
    cpu_threshold: f64,
    #[serde(default)]
    mem_threshold: f64,
    #[serde(default)]
    cases: Vec<BenchCase>,
}

#[derive(Debug, Deserialize)]
struct BenchManifest {
    version: u32,
    profiles: HashMap<String, BenchProfile>,
    #[serde(default)]
    micro: MicroSuiteConfig,
    #[serde(rename = "macro", default)]
    macro_suite: MacroSuiteConfig,
    gates: GateConfig,
}

#[derive(Debug, Deserialize)]
struct BenchProfile {
    scales: Vec<usize>,
    #[serde(default)]
    macro_scales: Vec<usize>,
    #[serde(default = "default_macro_traffic_mix_iterations")]
    macro_traffic_mix_iterations: u64,
    #[serde(default = "default_macro_stream_iterations")]
    macro_long_lived_stream_iterations: u64,
    #[serde(default = "default_macro_stream_chunks")]
    macro_long_lived_stream_chunks: usize,
    #[serde(default = "default_macro_stream_chunk_bytes")]
    macro_long_lived_stream_chunk_bytes: usize,
}

#[derive(Debug, Deserialize)]
struct MicroSuiteConfig {
    #[serde(default = "default_true")]
    include_h3_header_collection: bool,
}

#[derive(Debug, Deserialize)]
struct MacroSuiteConfig {
    #[serde(default = "default_true")]
    include_traffic_mix: bool,
    #[serde(default = "default_true")]
    include_long_lived_stream: bool,
}

impl Default for MicroSuiteConfig {
    fn default() -> Self {
        Self {
            include_h3_header_collection: true,
        }
    }
}

impl Default for MacroSuiteConfig {
    fn default() -> Self {
        Self {
            include_traffic_mix: true,
            include_long_lived_stream: true,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_macro_traffic_mix_iterations() -> u64 {
    20_000
}

fn default_macro_stream_iterations() -> u64 {
    8_000
}

fn default_macro_stream_chunks() -> usize {
    64
}

fn default_macro_stream_chunk_bytes() -> usize {
    8 * 1024
}

#[derive(Debug, Clone, Deserialize)]
struct GateMetric {
    warn_pct: f64,
    severe_pct: f64,
    #[serde(default)]
    zero_baseline_limit: f64,
    #[serde(default)]
    min_delta_abs: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct GateConfig {
    cpu: GateMetric,
    memory: GateMetric,
    alloc_calls: GateMetric,
    alloc_bytes: GateMetric,
    tail_p99: GateMetric,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ReleaseBaselineIndex {
    #[serde(default)]
    current_release: String,
    #[serde(default)]
    releases: HashMap<String, ReleaseBaselineEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReleaseBaselineEntry {
    micro: String,
    #[serde(rename = "macro")]
    macro_report: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegressionSeverity {
    Warn,
    Severe,
}

#[derive(Debug, Clone)]
struct RegressionIssue {
    severity: RegressionSeverity,
    metric: &'static str,
    case: String,
    scale: usize,
    kind: String,
    current: f64,
    baseline: f64,
    warn_limit: f64,
    severe_limit: f64,
    unit: &'static str,
}

fn reset_alloc_counters() {
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
}

fn alloc_snapshot() -> (u64, u64) {
    (
        ALLOC_CALLS.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

fn current_rss_kb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return None;
        }
        Some(resident_pages * (page_size as u64) / 1024)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
        if rc != 0 {
            return None;
        }
        #[cfg(target_os = "macos")]
        {
            Some((usage.ru_maxrss as u64) / 1024)
        }
        #[cfg(not(target_os = "macos"))]
        {
            Some(usage.ru_maxrss as u64)
        }
    }
}

fn current_cpu_micros() -> Option<u64> {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return None;
    }

    let user = (usage.ru_utime.tv_sec as i128)
        .saturating_mul(1_000_000)
        .saturating_add(usage.ru_utime.tv_usec as i128);
    let sys = (usage.ru_stime.tv_sec as i128)
        .saturating_mul(1_000_000)
        .saturating_add(usage.ru_stime.tv_usec as i128);

    if user < 0 || sys < 0 {
        return None;
    }
    Some((user + sys) as u64)
}

fn cpu_pct(cpu_before: Option<u64>, cpu_after: Option<u64>, wall_ns: u128) -> f64 {
    let Some(before) = cpu_before else {
        return 0.0;
    };
    let Some(after) = cpu_after else {
        return 0.0;
    };
    if after <= before || wall_ns == 0 {
        return 0.0;
    }
    let cpu_used_us = after.saturating_sub(before);
    let wall_us = (wall_ns / 1_000).max(1) as f64;
    ((cpu_used_us as f64) / wall_us) * 100.0
}

fn percentile_from_sorted(values: &[u128], q: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let q = q.clamp(0.0, 1.0);
    let index = ((values.len().saturating_sub(1) as f64) * q).round() as usize;
    values[index] as f64
}

const BENCH_SAMPLES: usize = 3;

#[derive(Clone, Copy)]
struct CaseMeasurement {
    duration_ns: u128,
    mean_ns: f64,
    throughput_ops_per_sec: f64,
    alloc_calls: u64,
    alloc_bytes: u64,
    rss_delta_kb: u64,
    cpu_pct: f64,
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
    max_ns: f64,
    latency_sampled: bool,
}

fn run_case_aggregate(
    kind: &str,
    name: &str,
    scale: usize,
    iterations: u64,
    mut op: impl FnMut() -> usize,
) -> BenchCase {
    let warmup_iters = (iterations / 10).clamp(100, 10_000);
    for _ in 0..warmup_iters {
        black_box(op());
    }

    let mut measurements = Vec::with_capacity(BENCH_SAMPLES);
    for _ in 0..BENCH_SAMPLES {
        reset_alloc_counters();
        let rss_before = current_rss_kb().unwrap_or(0);
        let cpu_before = current_cpu_micros();
        let start = Instant::now();
        let mut sink = 0usize;
        for _ in 0..iterations {
            sink ^= op();
        }
        black_box(sink);
        let elapsed = start.elapsed();
        let cpu_after = current_cpu_micros();
        let rss_after = current_rss_kb().unwrap_or(rss_before);
        let (alloc_calls, alloc_bytes) = alloc_snapshot();
        let mean_ns = elapsed.as_secs_f64() * 1e9 / iterations as f64;
        let throughput = if elapsed.as_secs_f64() > 0.0 {
            iterations as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        measurements.push(CaseMeasurement {
            duration_ns: elapsed.as_nanos(),
            mean_ns,
            throughput_ops_per_sec: throughput,
            alloc_calls,
            alloc_bytes,
            rss_delta_kb: rss_after.saturating_sub(rss_before),
            cpu_pct: cpu_pct(cpu_before, cpu_after, elapsed.as_nanos()),
            p50_ns: mean_ns,
            p95_ns: mean_ns,
            p99_ns: mean_ns,
            max_ns: mean_ns,
            latency_sampled: false,
        });
    }
    measurements.sort_by(|left, right| {
        left.mean_ns
            .partial_cmp(&right.mean_ns)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let selected = measurements[measurements.len() / 2];

    BenchCase {
        kind: kind.to_string(),
        name: name.to_string(),
        scale,
        iterations,
        duration_ns: selected.duration_ns,
        latency_ns_per_op: selected.mean_ns,
        throughput_ops_per_sec: selected.throughput_ops_per_sec,
        alloc_calls: selected.alloc_calls,
        alloc_bytes: selected.alloc_bytes,
        rss_delta_kb: selected.rss_delta_kb,
        cpu_pct: selected.cpu_pct,
        latency_p50_ns: selected.p50_ns,
        latency_p95_ns: selected.p95_ns,
        latency_p99_ns: selected.p99_ns,
        latency_max_ns: selected.max_ns,
        latency_sampled: selected.latency_sampled,
    }
}

fn run_case_with_latencies(
    kind: &str,
    name: &str,
    scale: usize,
    iterations: u64,
    mut op: impl FnMut() -> usize,
) -> BenchCase {
    let warmup_iters = (iterations / 10).clamp(50, 5_000);
    for _ in 0..warmup_iters {
        black_box(op());
    }

    let mut measurements = Vec::with_capacity(BENCH_SAMPLES);
    for _ in 0..BENCH_SAMPLES {
        reset_alloc_counters();
        let rss_before = current_rss_kb().unwrap_or(0);
        let cpu_before = current_cpu_micros();
        let mut latencies = Vec::with_capacity(iterations.min(200_000) as usize);

        let start = Instant::now();
        let mut sink = 0usize;
        for _ in 0..iterations {
            let op_start = Instant::now();
            sink ^= op();
            latencies.push(op_start.elapsed().as_nanos());
        }
        black_box(sink);
        let elapsed = start.elapsed();
        latencies.sort_unstable();

        let cpu_after = current_cpu_micros();
        let rss_after = current_rss_kb().unwrap_or(rss_before);
        let (alloc_calls, alloc_bytes) = alloc_snapshot();
        let mean_ns = elapsed.as_secs_f64() * 1e9 / iterations as f64;
        let throughput = if elapsed.as_secs_f64() > 0.0 {
            iterations as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        measurements.push(CaseMeasurement {
            duration_ns: elapsed.as_nanos(),
            mean_ns,
            throughput_ops_per_sec: throughput,
            alloc_calls,
            alloc_bytes,
            rss_delta_kb: rss_after.saturating_sub(rss_before),
            cpu_pct: cpu_pct(cpu_before, cpu_after, elapsed.as_nanos()),
            p50_ns: percentile_from_sorted(&latencies, 0.50),
            p95_ns: percentile_from_sorted(&latencies, 0.95),
            p99_ns: percentile_from_sorted(&latencies, 0.99),
            max_ns: latencies.last().copied().unwrap_or(0) as f64,
            latency_sampled: true,
        });
    }
    measurements.sort_by(|left, right| {
        left.mean_ns
            .partial_cmp(&right.mean_ns)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let selected = measurements[measurements.len() / 2];

    BenchCase {
        kind: kind.to_string(),
        name: name.to_string(),
        scale,
        iterations,
        duration_ns: selected.duration_ns,
        latency_ns_per_op: selected.mean_ns,
        throughput_ops_per_sec: selected.throughput_ops_per_sec,
        alloc_calls: selected.alloc_calls,
        alloc_bytes: selected.alloc_bytes,
        rss_delta_kb: selected.rss_delta_kb,
        cpu_pct: selected.cpu_pct,
        latency_p50_ns: selected.p50_ns,
        latency_p95_ns: selected.p95_ns,
        latency_p99_ns: selected.p99_ns,
        latency_max_ns: selected.max_ns,
        latency_sampled: selected.latency_sampled,
    }
}

fn route_iterations(scale: usize, linear: bool) -> u64 {
    match (scale, linear) {
        (100, false) => 300_000,
        (1_000, false) => 200_000,
        (10_000, false) => 100_000,
        (100, true) => 200_000,
        (1_000, true) => 40_000,
        (10_000, true) => 4_000,
        _ => 20_000,
    }
}

fn fast_iterations(scale: usize) -> u64 {
    match scale {
        100 => 300_000,
        1_000 => 200_000,
        10_000 => 80_000,
        _ => 50_000,
    }
}

fn scan_iterations(scale: usize) -> u64 {
    match scale {
        100 => 150_000,
        1_000 => 30_000,
        10_000 => 3_000,
        _ => 10_000,
    }
}

fn lb_ch_iterations(scale: usize) -> u64 {
    match scale {
        100 => 300_000,
        1_000 => 200_000,
        10_000 => 80_000,
        _ => 50_000,
    }
}

fn header_collect_iterations(scale: usize, is_large_header_set: bool) -> u64 {
    match (scale, is_large_header_set) {
        (100, false) => 200_000,
        (1_000, false) => 120_000,
        (10_000, false) => 60_000,
        (100, true) => 120_000,
        (1_000, true) => 80_000,
        (10_000, true) => 40_000,
        _ => 50_000,
    }
}

fn synth_h3_headers(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let name = match i {
            0 => b":method".to_vec(),
            1 => b":path".to_vec(),
            2 => b":authority".to_vec(),
            3 => b"user-agent".to_vec(),
            _ => format!("x-bench-{i}").into_bytes(),
        };
        let value = match i {
            0 => b"GET".to_vec(),
            1 => b"/bench".to_vec(),
            2 => b"example.test".to_vec(),
            _ => format!("value-{i}").into_bytes(),
        };
        out.push((name, value));
    }
    out
}

fn benchmark_h3_header_collection(scale: usize) -> Vec<BenchCase> {
    const INLINE_HEADERS: usize = 16;
    let small_headers = synth_h3_headers(8);
    let large_headers = synth_h3_headers(24);

    vec![
        run_case_aggregate(
            "micro",
            "h3_headers_collect_vec_small",
            scale,
            header_collect_iterations(scale, false),
            || {
                let mut headers = Vec::with_capacity(small_headers.len());
                for (name, value) in &small_headers {
                    headers.push((name.as_slice().to_vec(), value.as_slice().to_vec()));
                }
                headers.len()
            },
        ),
        run_case_aggregate(
            "micro",
            "h3_headers_collect_smallvec_small",
            scale,
            header_collect_iterations(scale, false),
            || {
                let mut headers = SmallVec::<[(Vec<u8>, Vec<u8>); INLINE_HEADERS]>::with_capacity(
                    small_headers.len(),
                );
                for (name, value) in &small_headers {
                    headers.push((name.as_slice().to_vec(), value.as_slice().to_vec()));
                }
                headers.len()
            },
        ),
        run_case_aggregate(
            "micro",
            "h3_headers_collect_vec_large",
            scale,
            header_collect_iterations(scale, true),
            || {
                let mut headers = Vec::with_capacity(large_headers.len());
                for (name, value) in &large_headers {
                    headers.push((name.as_slice().to_vec(), value.as_slice().to_vec()));
                }
                headers.len()
            },
        ),
        run_case_aggregate(
            "micro",
            "h3_headers_collect_smallvec_large",
            scale,
            header_collect_iterations(scale, true),
            || {
                let mut headers = SmallVec::<[(Vec<u8>, Vec<u8>); INLINE_HEADERS]>::with_capacity(
                    large_headers.len(),
                );
                for (name, value) in &large_headers {
                    headers.push((name.as_slice().to_vec(), value.as_slice().to_vec()));
                }
                headers.len()
            },
        ),
    ]
}

fn benchmark_route_lookup(scale: usize) -> Vec<BenchCase> {
    let bench = RouteLookupBench::new(scale);
    assert_eq!(bench.indexed_hit() > 0, bench.linear_hit() > 0);

    vec![
        run_case_aggregate(
            "micro",
            "route_lookup_indexed_hit",
            scale,
            route_iterations(scale, false),
            || bench.indexed_hit(),
        ),
        run_case_aggregate(
            "micro",
            "route_lookup_linear_hit",
            scale,
            route_iterations(scale, true),
            || bench.linear_hit(),
        ),
        run_case_aggregate(
            "micro",
            "route_lookup_indexed_miss",
            scale,
            route_iterations(scale, false),
            || bench.indexed_miss(),
        ),
    ]
}

fn build_lb_upstream(scale: usize, lb_type: &str) -> Upstream {
    let backends = (0..scale.max(1))
        .map(|idx| Backend {
            id: format!("backend-{idx:05}"),
            address: format!("127.0.0.1:{}", 10_000 + (idx % 50_000)),
            weight: 1,
            health_check: HealthCheck {
                path: "/health".to_string(),
                interval: 5_000,
                timeout_ms: 1_000,
                failure_threshold: 3,
                success_threshold: 2,
                cooldown_ms: 5_000,
            },
        })
        .collect();

    Upstream {
        load_balancing: LoadBalancing {
            lb_type: lb_type.to_string(),
            key: None,
        },
        route: RouteMatch {
            host: None,
            path_prefix: Some("/".to_string()),
            method: None,
        },
        backends,
    }
}

fn build_lb_pool(scale: usize, lb_type: &str) -> Result<UpstreamPool, String> {
    UpstreamPool::from_upstream(&build_lb_upstream(scale, lb_type))
        .map_err(|err| format!("failed to build LB pool '{lb_type}' for scale {scale}: {err}"))
}

fn benchmark_lb(scale: usize) -> Result<Vec<BenchCase>, String> {
    let mut rr_pool = build_lb_pool(scale, "round-robin")?;
    let mut random_pool = build_lb_pool(scale, "random")?;
    let mut ch_pool = build_lb_pool(scale, "consistent-hash")?;

    let keys = [
        "user:1", "user:2", "user:3", "user:4", "user:5", "user:6", "user:7", "user:8",
    ];
    let mut ch_key_idx = 0usize;

    Ok(vec![
        run_case_aggregate(
            "micro",
            "lb_round_robin_pick",
            scale,
            fast_iterations(scale),
            || rr_pool.pick("ignored").unwrap_or(usize::MAX),
        ),
        run_case_aggregate(
            "micro",
            "lb_random_pick",
            scale,
            fast_iterations(scale),
            || random_pool.pick("ignored").unwrap_or(usize::MAX),
        ),
        run_case_aggregate(
            "micro",
            "lb_consistent_hash_pick",
            scale,
            lb_ch_iterations(scale),
            || {
                let key = keys[ch_key_idx & 7];
                ch_key_idx = ch_key_idx.wrapping_add(1);
                ch_pool.pick(key).unwrap_or(usize::MAX)
            },
        ),
    ])
}

fn benchmark_connection_lookup(scale: usize) -> Vec<BenchCase> {
    let bench = ConnectionLookupBench::new(scale);
    assert!(bench.peer_map_hit() > 0);

    vec![
        run_case_aggregate(
            "micro",
            "connection_exact_lookup",
            scale,
            fast_iterations(scale),
            || bench.exact_lookup(),
        ),
        run_case_aggregate(
            "micro",
            "connection_alias_lookup",
            scale,
            fast_iterations(scale),
            || bench.alias_lookup(),
        ),
        run_case_aggregate(
            "micro",
            "connection_prefix_scan_miss_lookup",
            scale,
            scan_iterations(scale),
            || bench.prefix_scan_miss_lookup(),
        ),
        run_case_aggregate(
            "micro",
            "connection_peer_scan_miss",
            scale,
            scan_iterations(scale),
            || bench.peer_scan_miss(),
        ),
        run_case_aggregate(
            "micro",
            "connection_peer_map_hit",
            scale,
            fast_iterations(scale),
            || bench.peer_map_hit(),
        ),
        run_case_aggregate(
            "micro",
            "connection_peer_map_miss",
            scale,
            fast_iterations(scale),
            || bench.peer_map_miss(),
        ),
    ]
}

fn benchmark_macro_traffic_mix(
    scale: usize,
    iterations: u64,
    mut rr_pool: UpstreamPool,
    mut random_pool: UpstreamPool,
    mut ch_pool: UpstreamPool,
) -> BenchCase {
    let route = RouteLookupBench::new(scale);
    let conn = ConnectionLookupBench::new(scale);
    let mut traffic_counter = 0usize;

    let mut headers_small = synth_h3_headers(8);
    let mut headers_large = synth_h3_headers(24);

    run_case_with_latencies("macro", "macro_traffic_mix", scale, iterations, move || {
        let bucket = traffic_counter % 100;
        traffic_counter = traffic_counter.wrapping_add(1);

        if bucket < 55 {
            let route_hit = route.indexed_hit();
            let lb = rr_pool.pick("mix-core").unwrap_or(0);
            let conn_hit = conn.peer_map_hit();
            route_hit ^ lb ^ conn_hit
        } else if bucket < 75 {
            let route_miss = route.indexed_miss();
            let lb = random_pool.pick("mix-random").unwrap_or(0);
            let conn_miss = conn.peer_map_miss();
            route_miss ^ lb ^ conn_miss
        } else if bucket < 90 {
            let route_hit = route.linear_hit();
            let key = if bucket & 1 == 0 {
                "tenant:a"
            } else {
                "tenant:b"
            };
            let lb = ch_pool.pick(key).unwrap_or(0);
            let alias = conn.alias_lookup();
            route_hit ^ lb ^ alias
        } else {
            let mut collected = 0usize;
            if bucket & 1 == 0 {
                headers_small.rotate_left(1);
                for (name, value) in &headers_small {
                    collected ^= name.len() ^ value.len();
                }
            } else {
                headers_large.rotate_left(1);
                for (name, value) in &headers_large {
                    collected ^= name.len() ^ value.len();
                }
            }
            collected ^ conn.prefix_scan_miss_lookup()
        }
    })
}

fn benchmark_macro_long_lived_stream(
    scale: usize,
    iterations: u64,
    chunks_per_stream: usize,
    chunk_bytes: usize,
    mut ch_pool: UpstreamPool,
) -> BenchCase {
    let route = RouteLookupBench::new(scale);
    let conn = ConnectionLookupBench::new(scale);

    let payload = vec![0xAB_u8; chunk_bytes.max(1)];
    let chunks = chunks_per_stream.max(1);
    let mut stream_counter = 0usize;

    run_case_with_latencies(
        "macro",
        "macro_long_lived_stream",
        scale,
        iterations,
        move || {
            let stream_id = stream_counter;
            stream_counter = stream_counter.wrapping_add(1);

            let mut checksum = route.indexed_hit() ^ conn.peer_map_hit();
            let lb = ch_pool
                .pick(if stream_id & 1 == 0 {
                    "stream-even"
                } else {
                    "stream-odd"
                })
                .unwrap_or(0);
            checksum ^= lb;

            for chunk_idx in 0..chunks {
                let offset = (chunk_idx * 31 + stream_id) % payload.len();
                checksum ^= payload[offset] as usize;
                if chunk_idx % 8 == 0 {
                    checksum ^= route.indexed_hit();
                }
                if chunk_idx % 11 == 0 {
                    checksum ^= conn.alias_lookup();
                }
            }

            checksum
        },
    )
}

fn run_micro_suite(
    profile: &BenchProfile,
    config: &MicroSuiteConfig,
) -> Result<Vec<BenchCase>, String> {
    let mut cases = Vec::new();
    for &scale in &profile.scales {
        cases.extend(benchmark_route_lookup(scale));
        cases.extend(benchmark_lb(scale)?);
        cases.extend(benchmark_connection_lookup(scale));
        if config.include_h3_header_collection {
            cases.extend(benchmark_h3_header_collection(scale));
        }
    }
    Ok(cases)
}

fn effective_macro_scales(profile: &BenchProfile) -> Vec<usize> {
    if !profile.macro_scales.is_empty() {
        return profile.macro_scales.clone();
    }
    if let Some(first) = profile.scales.first() {
        return vec![*first];
    }
    vec![100]
}

fn run_macro_suite(
    profile: &BenchProfile,
    config: &MacroSuiteConfig,
) -> Result<Vec<BenchCase>, String> {
    let mut cases = Vec::new();
    for scale in effective_macro_scales(profile) {
        if config.include_traffic_mix {
            let rr = build_lb_pool(scale, "round-robin")?;
            let random = build_lb_pool(scale, "random")?;
            let ch = build_lb_pool(scale, "consistent-hash")?;
            cases.push(benchmark_macro_traffic_mix(
                scale,
                profile.macro_traffic_mix_iterations,
                rr,
                random,
                ch,
            ));
        }

        if config.include_long_lived_stream {
            let ch = build_lb_pool(scale, "consistent-hash")?;
            cases.push(benchmark_macro_long_lived_stream(
                scale,
                profile.macro_long_lived_stream_iterations,
                profile.macro_long_lived_stream_chunks,
                profile.macro_long_lived_stream_chunk_bytes,
                ch,
            ));
        }
    }
    Ok(cases)
}

fn resolve_gate_config(mut gates: GateConfig, args: &Args) -> GateConfig {
    if let Some(cpu_override) = args.cpu_threshold {
        gates.cpu.warn_pct = cpu_override;
        gates.cpu.severe_pct = cpu_override;
    }
    if let Some(mem_override) = args.mem_threshold {
        gates.memory.warn_pct = mem_override;
        gates.memory.severe_pct = mem_override;
        gates.alloc_calls.warn_pct = mem_override;
        gates.alloc_calls.severe_pct = mem_override;
        gates.alloc_bytes.warn_pct = mem_override;
        gates.alloc_bytes.severe_pct = mem_override;
    }
    gates
}

fn classify_regression(
    current: f64,
    baseline: f64,
    gate: &GateMetric,
    zero_limit: f64,
) -> Option<(RegressionSeverity, f64, f64)> {
    let warn = gate.warn_pct.max(0.0);
    let severe = gate.severe_pct.max(warn);

    if baseline > 0.0 {
        // For tiny baselines (for example, allocation calls close to zero),
        // percent-only thresholds are too sensitive to allocator/runtime noise
        // across OS/toolchain environments. Apply a configurable floor so gates
        // remain stable while still catching meaningful growth.
        let effective_baseline = if gate.zero_baseline_limit > 0.0 {
            baseline.max(gate.zero_baseline_limit)
        } else {
            baseline
        };
        let warn_limit = effective_baseline * (1.0 + warn);
        let severe_limit = effective_baseline * (1.0 + severe);
        let delta = current - baseline;
        let min_delta_abs = gate.min_delta_abs.max(0.0);
        if delta < min_delta_abs {
            return None;
        }
        if current > severe_limit {
            return Some((RegressionSeverity::Severe, warn_limit, severe_limit));
        }
        if current > warn_limit {
            return Some((RegressionSeverity::Warn, warn_limit, severe_limit));
        }
        return None;
    }

    let base_limit = if gate.zero_baseline_limit > 0.0 {
        gate.zero_baseline_limit
    } else {
        zero_limit
    };
    let warn_limit = base_limit;
    let severe_limit = base_limit * (1.0 + severe);
    if current > severe_limit {
        return Some((RegressionSeverity::Severe, warn_limit, severe_limit));
    }
    if current > warn_limit {
        return Some((RegressionSeverity::Warn, warn_limit, severe_limit));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{GateMetric, RegressionSeverity, classify_regression};

    fn gate(
        warn_pct: f64,
        severe_pct: f64,
        zero_baseline_limit: f64,
        min_delta_abs: f64,
    ) -> GateMetric {
        GateMetric {
            warn_pct,
            severe_pct,
            zero_baseline_limit,
            min_delta_abs,
        }
    }

    #[test]
    fn min_delta_abs_suppresses_small_absolute_memory_drift() {
        let memory_gate = gate(0.20, 0.40, 128.0, 256.0);
        let regression = classify_regression(320.0, 200.0, &memory_gate, 128.0);
        assert!(regression.is_none());
    }

    #[test]
    fn min_delta_abs_still_allows_large_absolute_memory_regressions() {
        let memory_gate = gate(0.20, 0.40, 128.0, 256.0);
        let regression = classify_regression(520.0, 200.0, &memory_gate, 128.0);
        assert!(matches!(
            regression,
            Some((RegressionSeverity::Severe, _, _))
        ));
    }
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 1.0;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn compare_reports(
    current: &BenchReport,
    baseline: &BenchReport,
    gates: &GateConfig,
) -> Vec<RegressionIssue> {
    let baseline_map: HashMap<(String, String, usize), &BenchCase> = baseline
        .cases
        .iter()
        .map(|case| ((case.kind.clone(), case.name.clone(), case.scale), case))
        .collect();

    let mut cpu_ratios = Vec::new();
    let mut tail_ratios = Vec::new();
    for case in &current.cases {
        let key = (case.kind.clone(), case.name.clone(), case.scale);
        let Some(base) = baseline_map.get(&key) else {
            continue;
        };
        if base.latency_ns_per_op > 0.0 {
            cpu_ratios.push(case.latency_ns_per_op / base.latency_ns_per_op);
        }
        if base.latency_sampled && case.latency_sampled && base.latency_p99_ns > 0.0 {
            tail_ratios.push(case.latency_p99_ns / base.latency_p99_ns);
        }
    }
    let cpu_factor = if cpu_ratios.len() >= 5 {
        median(&mut cpu_ratios).max(0.01)
    } else {
        1.0
    };
    let tail_factor = if tail_ratios.len() >= 5 {
        median(&mut tail_ratios).max(0.01)
    } else {
        1.0
    };

    let mut issues = Vec::new();

    for case in &current.cases {
        let key = (case.kind.clone(), case.name.clone(), case.scale);
        let Some(base) = baseline_map.get(&key) else {
            continue;
        };

        let normalized_cpu_baseline = if base.latency_ns_per_op > 0.0 {
            base.latency_ns_per_op * cpu_factor
        } else {
            base.latency_ns_per_op
        };
        if let Some((severity, warn_limit, severe_limit)) = classify_regression(
            case.latency_ns_per_op,
            normalized_cpu_baseline,
            &gates.cpu,
            0.0,
        ) {
            issues.push(RegressionIssue {
                severity,
                metric: "cpu_ns_per_op",
                case: case.name.clone(),
                scale: case.scale,
                kind: case.kind.clone(),
                current: case.latency_ns_per_op,
                baseline: normalized_cpu_baseline,
                warn_limit,
                severe_limit,
                unit: "ns/op",
            });
        }

        if let Some((severity, warn_limit, severe_limit)) = classify_regression(
            case.alloc_calls as f64,
            base.alloc_calls as f64,
            &gates.alloc_calls,
            32.0,
        ) {
            issues.push(RegressionIssue {
                severity,
                metric: "alloc_calls",
                case: case.name.clone(),
                scale: case.scale,
                kind: case.kind.clone(),
                current: case.alloc_calls as f64,
                baseline: base.alloc_calls as f64,
                warn_limit,
                severe_limit,
                unit: "calls",
            });
        }

        if let Some((severity, warn_limit, severe_limit)) = classify_regression(
            case.alloc_bytes as f64,
            base.alloc_bytes as f64,
            &gates.alloc_bytes,
            (16 * 1024) as f64,
        ) {
            issues.push(RegressionIssue {
                severity,
                metric: "alloc_bytes",
                case: case.name.clone(),
                scale: case.scale,
                kind: case.kind.clone(),
                current: case.alloc_bytes as f64,
                baseline: base.alloc_bytes as f64,
                warn_limit,
                severe_limit,
                unit: "bytes",
            });
        }

        if let Some((severity, warn_limit, severe_limit)) = classify_regression(
            case.rss_delta_kb as f64,
            base.rss_delta_kb as f64,
            &gates.memory,
            128.0,
        ) {
            issues.push(RegressionIssue {
                severity,
                metric: "rss_delta_kb",
                case: case.name.clone(),
                scale: case.scale,
                kind: case.kind.clone(),
                current: case.rss_delta_kb as f64,
                baseline: base.rss_delta_kb as f64,
                warn_limit,
                severe_limit,
                unit: "KB",
            });
        }

        // Legacy baseline reports may not have tail latency fields populated.
        // Skip tail-p99 regression checks when baseline p99 is unavailable.
        if base.latency_sampled && case.latency_sampled && base.latency_p99_ns > 0.0 {
            let normalized_tail_baseline = base.latency_p99_ns * tail_factor;
            if let Some((severity, warn_limit, severe_limit)) = classify_regression(
                case.latency_p99_ns,
                normalized_tail_baseline,
                &gates.tail_p99,
                0.0,
            ) {
                issues.push(RegressionIssue {
                    severity,
                    metric: "tail_p99_ns",
                    case: case.name.clone(),
                    scale: case.scale,
                    kind: case.kind.clone(),
                    current: case.latency_p99_ns,
                    baseline: normalized_tail_baseline,
                    warn_limit,
                    severe_limit,
                    unit: "ns",
                });
            }
        }
    }

    issues
}

fn print_summary(report: &BenchReport) {
    println!(
        "{:<8} {:<30} {:>7} {:>12} {:>10} {:>12} {:>9}",
        "kind", "case", "scale", "ns/op", "cpu%", "ops/s", "p99(ns)"
    );
    for case in &report.cases {
        println!(
            "{:<8} {:<30} {:>7} {:>12.2} {:>10.2} {:>12.2} {:>9.0}",
            case.kind,
            case.name,
            case.scale,
            case.latency_ns_per_op,
            case.cpu_pct,
            case.throughput_ops_per_sec,
            case.latency_p99_ns
        );
    }
}

fn format_issue(issue: &RegressionIssue) -> String {
    let severity = match issue.severity {
        RegressionSeverity::Warn => "WARN",
        RegressionSeverity::Severe => "SEVERE",
    };
    let delta_pct = if issue.baseline > 0.0 {
        ((issue.current / issue.baseline) - 1.0) * 100.0
    } else {
        0.0
    };

    format!(
        "[{severity}] {} in {}:{} [{}] => {:.2}{} (baseline {:.2}{}, warn>{:.2}{}, severe>{:.2}{}; delta {:.1}%)",
        issue.metric,
        issue.case,
        issue.scale,
        issue.kind,
        issue.current,
        issue.unit,
        issue.baseline,
        issue.unit,
        issue.warn_limit,
        issue.unit,
        issue.severe_limit,
        issue.unit,
        delta_pct
    )
}

fn write_markdown(
    path: &Path,
    report: &BenchReport,
    issues: &[RegressionIssue],
    fail_on: FailOn,
) -> Result<(), String> {
    let mut lines = vec![
        "# Spooky Benchmark Report".to_string(),
        "".to_string(),
        format!("- Report kind: `{}`", report.report_kind),
        format!("- Profile: `{}`", report.profile),
        "".to_string(),
        "| kind | case | scale | ns/op | cpu% | ops/s | p50(ns) | p95(ns) | p99(ns) | alloc_calls | alloc_bytes | rss_delta_kb |".to_string(),
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ];

    for case in &report.cases {
        lines.push(format!(
            "| {} | {} | {} | {:.2} | {:.2} | {:.2} | {:.0} | {:.0} | {:.0} | {} | {} | {} |",
            case.kind,
            case.name,
            case.scale,
            case.latency_ns_per_op,
            case.cpu_pct,
            case.throughput_ops_per_sec,
            case.latency_p50_ns,
            case.latency_p95_ns,
            case.latency_p99_ns,
            case.alloc_calls,
            case.alloc_bytes,
            case.rss_delta_kb
        ));
    }

    lines.push("".to_string());
    if issues.is_empty() {
        lines.push("No regressions detected against baseline.".to_string());
    } else {
        lines.push("## Regression Findings".to_string());
        lines.push(format!("- Fail mode: `{:?}`", fail_on));
        lines.push("".to_string());

        let mut severe = issues
            .iter()
            .filter(|issue| issue.severity == RegressionSeverity::Severe)
            .collect::<Vec<_>>();
        let mut warn = issues
            .iter()
            .filter(|issue| issue.severity == RegressionSeverity::Warn)
            .collect::<Vec<_>>();

        severe.sort_by_key(|issue| (&issue.kind, &issue.case, issue.scale, issue.metric));
        warn.sort_by_key(|issue| (&issue.kind, &issue.case, issue.scale, issue.metric));

        if !severe.is_empty() {
            lines.push("### Severe".to_string());
            for issue in severe {
                lines.push(format!("- {}", format_issue(issue)));
            }
            lines.push("".to_string());
        }

        if !warn.is_empty() {
            lines.push("### Warn".to_string());
            for issue in warn {
                lines.push(format!("- {}", format_issue(issue)));
            }
        }
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create markdown dir '{}': {err}",
                parent.display()
            )
        })?;
    }
    fs::write(path, lines.join("\n"))
        .map_err(|err| format!("failed to write markdown '{}': {err}", path.display()))
}

fn load_report(path: &Path) -> Result<BenchReport, String> {
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read baseline '{}': {err}", path.display()))?;
    serde_json::from_str(&text)
        .map_err(|err| format!("failed to parse baseline '{}': {err}", path.display()))
}

fn load_manifest(path: &Path) -> Result<BenchManifest, String> {
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read manifest '{}': {err}", path.display()))?;
    let manifest: BenchManifest = serde_yaml::from_str(&text)
        .map_err(|err| format!("failed to parse manifest '{}': {err}", path.display()))?;

    if manifest.version != 1 {
        return Err(format!(
            "unsupported bench manifest version {} (expected 1)",
            manifest.version
        ));
    }
    Ok(manifest)
}

fn load_release_index(path: &Path) -> Result<ReleaseBaselineIndex, String> {
    if !path.exists() {
        return Ok(ReleaseBaselineIndex::default());
    }
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read baseline index '{}': {err}", path.display()))?;
    serde_json::from_str(&text)
        .map_err(|err| format!("failed to parse baseline index '{}': {err}", path.display()))
}

fn write_release_index(path: &Path, index: &ReleaseBaselineIndex) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create baseline index dir '{}': {err}",
                parent.display()
            )
        })?;
    }
    let text = serde_json::to_string_pretty(index)
        .map_err(|err| format!("failed to serialize baseline index: {err}"))?;
    fs::write(path, text)
        .map_err(|err| format!("failed to write baseline index '{}': {err}", path.display()))
}

fn write_report(path: &Path, report: &BenchReport) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create output dir '{}': {err}", parent.display()))?;
    }
    let json =
        serde_json::to_string_pretty(report).map_err(|err| format!("serialize report: {err}"))?;
    fs::write(path, json)
        .map_err(|err| format!("failed to write report '{}': {err}", path.display()))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn suite_label(suite: BenchSuite) -> &'static str {
    match suite {
        BenchSuite::Micro => "micro",
        BenchSuite::Macro => "macro",
        BenchSuite::All => "all",
    }
}

fn resolve_baseline_paths(
    args: &Args,
    release_index: &ReleaseBaselineIndex,
) -> Result<Vec<PathBuf>, String> {
    if let Some(path) = &args.baseline {
        return Ok(vec![path.clone()]);
    }

    let release = args
        .baseline_release
        .clone()
        .or_else(|| {
            (!release_index.current_release.is_empty()).then_some(release_index.current_release.clone())
        })
        .ok_or_else(|| {
            "baseline not specified; pass --baseline or configure --baseline-release / current_release in baseline index".to_string()
        })?;

    let entry = release_index
        .releases
        .get(&release)
        .ok_or_else(|| format!("release '{release}' missing from baseline index"))?;

    let paths = match args.suite {
        BenchSuite::Micro => vec![PathBuf::from(&entry.micro)],
        BenchSuite::Macro => vec![PathBuf::from(&entry.macro_report)],
        BenchSuite::All => vec![
            PathBuf::from(&entry.micro),
            PathBuf::from(&entry.macro_report),
        ],
    };

    Ok(paths)
}

fn merge_reports(reports: Vec<BenchReport>) -> BenchReport {
    let mut merged = BenchReport::default();
    merged.suite = "spooky-performance-baseline".to_string();
    merged.report_kind = "merged".to_string();
    merged.profile = "baseline".to_string();
    merged.generated_unix_secs = unix_now();

    for report in reports {
        merged.cases.extend(report.cases);
    }

    merged.cases.sort_by(|left, right| {
        (&left.kind, &left.name, left.scale).cmp(&(&right.kind, &right.name, right.scale))
    });
    merged
}

fn run_promotion(args: &Args) -> Result<(), String> {
    let release = args
        .promote_release
        .as_ref()
        .ok_or_else(|| "internal error: promote_release missing".to_string())?;

    let mut index = load_release_index(&args.baseline_index)?;

    let release_dir = PathBuf::from("bench").join("baselines").join(release);
    fs::create_dir_all(&release_dir).map_err(|err| {
        format!(
            "failed to create release baseline directory '{}': {err}",
            release_dir.display()
        )
    })?;

    if !args.promote_micro_report.exists() {
        return Err(format!(
            "micro report '{}' does not exist",
            args.promote_micro_report.display()
        ));
    }
    if !args.promote_macro_report.exists() {
        return Err(format!(
            "macro report '{}' does not exist",
            args.promote_macro_report.display()
        ));
    }

    let micro_dest = release_dir.join("micro.json");
    let macro_dest = release_dir.join("macro.json");

    fs::copy(&args.promote_micro_report, &micro_dest).map_err(|err| {
        format!(
            "failed to copy micro report '{}' -> '{}': {err}",
            args.promote_micro_report.display(),
            micro_dest.display()
        )
    })?;
    fs::copy(&args.promote_macro_report, &macro_dest).map_err(|err| {
        format!(
            "failed to copy macro report '{}' -> '{}': {err}",
            args.promote_macro_report.display(),
            macro_dest.display()
        )
    })?;

    let entry = ReleaseBaselineEntry {
        micro: micro_dest.to_string_lossy().to_string(),
        macro_report: macro_dest.to_string_lossy().to_string(),
    };
    index.releases.insert(release.clone(), entry);
    if args.set_current_release {
        index.current_release = release.clone();
    }

    write_release_index(&args.baseline_index, &index)?;

    println!(
        "Promoted release baseline '{}' (micro='{}', macro='{}')",
        release,
        micro_dest.display(),
        macro_dest.display()
    );

    Ok(())
}

fn main() -> Result<(), String> {
    let args = Args::parse();

    if args.promote_release.is_some() {
        return run_promotion(&args);
    }

    let manifest = load_manifest(&args.manifest)?;
    let profile = manifest
        .profiles
        .get(&args.profile)
        .ok_or_else(|| format!("profile '{}' missing in manifest", args.profile))?;

    let mut cases = Vec::new();
    match args.suite {
        BenchSuite::Micro => {
            cases.extend(run_micro_suite(profile, &manifest.micro)?);
        }
        BenchSuite::Macro => {
            cases.extend(run_macro_suite(profile, &manifest.macro_suite)?);
        }
        BenchSuite::All => {
            cases.extend(run_micro_suite(profile, &manifest.micro)?);
            cases.extend(run_macro_suite(profile, &manifest.macro_suite)?);
        }
    }

    cases.sort_by(|left, right| {
        (&left.kind, &left.name, left.scale).cmp(&(&right.kind, &right.name, right.scale))
    });

    let report = BenchReport {
        suite: "spooky-performance-regression".to_string(),
        report_kind: suite_label(args.suite).to_string(),
        profile: args.profile.clone(),
        generated_unix_secs: unix_now(),
        cpu_threshold: args.cpu_threshold.unwrap_or(manifest.gates.cpu.warn_pct),
        mem_threshold: args.mem_threshold.unwrap_or(manifest.gates.memory.warn_pct),
        cases,
    };

    print_summary(&report);
    write_report(&args.output, &report)?;

    let mut issues = Vec::new();
    if args.check_baseline {
        let release_index = load_release_index(&args.baseline_index)?;
        let baseline_paths = resolve_baseline_paths(&args, &release_index)?;
        let mut baseline_reports = Vec::with_capacity(baseline_paths.len());
        for path in &baseline_paths {
            baseline_reports.push(load_report(path)?);
        }
        let baseline = merge_reports(baseline_reports);

        let gates = resolve_gate_config(manifest.gates.clone(), &args);
        issues = compare_reports(&report, &baseline, &gates);
    }

    if let Some(markdown) = &args.markdown_out {
        write_markdown(markdown, &report, &issues, args.fail_on)?;
    }

    if !issues.is_empty() {
        let severe_count = issues
            .iter()
            .filter(|issue| issue.severity == RegressionSeverity::Severe)
            .count();
        let warn_count = issues.len().saturating_sub(severe_count);

        for issue in &issues {
            eprintln!("{}", format_issue(issue));
        }

        let fail = match args.fail_on {
            FailOn::Severe => severe_count > 0,
            FailOn::Any => !issues.is_empty(),
        };

        if fail {
            return Err(format!(
                "benchmark regression gate failed (severe={severe_count}, warn={warn_count}, mode={:?})",
                args.fail_on
            ));
        }
    }

    Ok(())
}
