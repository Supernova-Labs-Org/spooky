use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use clap::Parser;
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

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Spooky benchmark suite (CPU + memory regression checks)"
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

    #[arg(long, default_value_t = 0.40)]
    cpu_threshold: f64,

    #[arg(long, default_value_t = 0.20)]
    mem_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchCase {
    name: String,
    scale: usize,
    iterations: u64,
    duration_ns: u128,
    latency_ns_per_op: f64,
    alloc_calls: u64,
    alloc_bytes: u64,
    rss_delta_kb: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct BenchReport {
    suite: String,
    generated_unix_secs: u64,
    cpu_threshold: f64,
    mem_threshold: f64,
    cases: Vec<BenchCase>,
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
        return Some(resident_pages * (page_size as u64) / 1024);
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

fn run_case(name: &str, scale: usize, iterations: u64, mut op: impl FnMut() -> usize) -> BenchCase {
    let warmup_iters = (iterations / 10).clamp(100, 10_000);
    for _ in 0..warmup_iters {
        black_box(op());
    }

    reset_alloc_counters();
    let rss_before = current_rss_kb().unwrap_or(0);
    let start = Instant::now();
    let mut sink = 0usize;
    for _ in 0..iterations {
        sink ^= op();
    }
    black_box(sink);
    let elapsed = start.elapsed();
    let rss_after = current_rss_kb().unwrap_or(rss_before);
    let (alloc_calls, alloc_bytes) = alloc_snapshot();

    BenchCase {
        name: name.to_string(),
        scale,
        iterations,
        duration_ns: elapsed.as_nanos(),
        latency_ns_per_op: elapsed.as_secs_f64() * 1e9 / iterations as f64,
        alloc_calls,
        alloc_bytes,
        rss_delta_kb: rss_after.saturating_sub(rss_before),
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
        run_case(
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
        run_case(
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
        run_case(
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
        run_case(
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
        run_case(
            "route_lookup_indexed_hit",
            scale,
            route_iterations(scale, false),
            || bench.indexed_hit(),
        ),
        run_case(
            "route_lookup_linear_hit",
            scale,
            route_iterations(scale, true),
            || bench.linear_hit(),
        ),
        run_case(
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

fn benchmark_lb(scale: usize) -> Vec<BenchCase> {
    let mut rr_pool =
        UpstreamPool::from_upstream(&build_lb_upstream(scale, "round-robin")).expect("rr pool");
    let mut random_pool =
        UpstreamPool::from_upstream(&build_lb_upstream(scale, "random")).expect("random pool");
    let mut ch_pool =
        UpstreamPool::from_upstream(&build_lb_upstream(scale, "consistent-hash")).expect("ch pool");

    let keys = [
        "user:1", "user:2", "user:3", "user:4", "user:5", "user:6", "user:7", "user:8",
    ];
    let mut ch_key_idx = 0usize;

    vec![
        run_case("lb_round_robin_pick", scale, fast_iterations(scale), || {
            rr_pool.pick("ignored").unwrap_or(usize::MAX)
        }),
        run_case("lb_random_pick", scale, fast_iterations(scale), || {
            random_pool.pick("ignored").unwrap_or(usize::MAX)
        }),
        run_case(
            "lb_consistent_hash_pick",
            scale,
            lb_ch_iterations(scale),
            || {
                let key = keys[ch_key_idx & 7];
                ch_key_idx = ch_key_idx.wrapping_add(1);
                ch_pool.pick(key).unwrap_or(usize::MAX)
            },
        ),
    ]
}

fn benchmark_connection_lookup(scale: usize) -> Vec<BenchCase> {
    let bench = ConnectionLookupBench::new(scale);
    assert!(bench.peer_map_hit() > 0);

    vec![
        run_case(
            "connection_exact_lookup",
            scale,
            fast_iterations(scale),
            || bench.exact_lookup(),
        ),
        run_case(
            "connection_alias_lookup",
            scale,
            fast_iterations(scale),
            || bench.alias_lookup(),
        ),
        run_case(
            "connection_prefix_scan_miss_lookup",
            scale,
            scan_iterations(scale),
            || bench.prefix_scan_miss_lookup(),
        ),
        run_case(
            "connection_peer_scan_miss",
            scale,
            scan_iterations(scale),
            || bench.peer_scan_miss(),
        ),
        run_case(
            "connection_peer_map_hit",
            scale,
            fast_iterations(scale),
            || bench.peer_map_hit(),
        ),
        run_case(
            "connection_peer_map_miss",
            scale,
            fast_iterations(scale),
            || bench.peer_map_miss(),
        ),
    ]
}

fn compare_reports(
    current: &BenchReport,
    baseline: &BenchReport,
    cpu_threshold: f64,
    mem_threshold: f64,
) -> Vec<String> {
    let baseline_map: HashMap<(String, usize), &BenchCase> = baseline
        .cases
        .iter()
        .map(|case| ((case.name.clone(), case.scale), case))
        .collect();

    let mut regressions = Vec::new();
    for case in &current.cases {
        let Some(base) = baseline_map.get(&(case.name.clone(), case.scale)) else {
            continue;
        };

        let cpu_limit = base.latency_ns_per_op * (1.0 + cpu_threshold);
        if case.latency_ns_per_op > cpu_limit {
            regressions.push(format!(
                "CPU regression in {}[{}]: {:.2}ns/op > {:.2}ns/op (baseline {:.2}ns/op, threshold {:.0}%)",
                case.name,
                case.scale,
                case.latency_ns_per_op,
                cpu_limit,
                base.latency_ns_per_op,
                cpu_threshold * 100.0
            ));
        }

        let alloc_calls_limit = if base.alloc_calls == 0 {
            32
        } else {
            ((base.alloc_calls as f64) * (1.0 + mem_threshold)).ceil() as u64
        };
        if case.alloc_calls > alloc_calls_limit {
            regressions.push(format!(
                "alloc_calls regression in {}[{}]: {} > {} (baseline {}, threshold {:.0}%)",
                case.name,
                case.scale,
                case.alloc_calls,
                alloc_calls_limit,
                base.alloc_calls,
                mem_threshold * 100.0
            ));
        }

        let alloc_bytes_limit = if base.alloc_bytes == 0 {
            16 * 1024
        } else {
            ((base.alloc_bytes as f64) * (1.0 + mem_threshold)).ceil() as u64
        };
        if case.alloc_bytes > alloc_bytes_limit {
            regressions.push(format!(
                "alloc_bytes regression in {}[{}]: {} > {} (baseline {}, threshold {:.0}%)",
                case.name,
                case.scale,
                case.alloc_bytes,
                alloc_bytes_limit,
                base.alloc_bytes,
                mem_threshold * 100.0
            ));
        }

        // RSS can be noisy for heavy-allocation cases. Enforce strict RSS guardrails
        // only for low-allocation paths and rely on alloc counters elsewhere.
        if base.alloc_bytes == 0 && case.alloc_bytes == 0 {
            let rss_limit = if base.rss_delta_kb == 0 {
                128
            } else {
                ((base.rss_delta_kb as f64) * (1.0 + mem_threshold)).ceil() as u64
            };
            if case.rss_delta_kb > rss_limit {
                regressions.push(format!(
                    "rss_delta regression in {}[{}]: {}KB > {}KB (baseline {}KB, threshold {:.0}%)",
                    case.name,
                    case.scale,
                    case.rss_delta_kb,
                    rss_limit,
                    base.rss_delta_kb,
                    mem_threshold * 100.0
                ));
            }
        }
    }

    regressions
}

fn print_summary(report: &BenchReport) {
    println!(
        "{:<34} {:>7} {:>12} {:>12} {:>12}",
        "case", "scale", "ns/op", "alloc_calls", "rss_kb"
    );
    for case in &report.cases {
        println!(
            "{:<34} {:>7} {:>12.2} {:>12} {:>12}",
            case.name, case.scale, case.latency_ns_per_op, case.alloc_calls, case.rss_delta_kb
        );
    }
}

fn write_markdown(path: &Path, report: &BenchReport, regressions: &[String]) -> Result<(), String> {
    let mut lines = vec![
        "# Spooky Benchmark Report".to_string(),
        "".to_string(),
        format!("- CPU threshold: {:.0}%", report.cpu_threshold * 100.0),
        format!("- Memory threshold: {:.0}%", report.mem_threshold * 100.0),
        "".to_string(),
        "| case | scale | ns/op | alloc_calls | alloc_bytes | rss_delta_kb |".to_string(),
        "| --- | ---: | ---: | ---: | ---: | ---: |".to_string(),
    ];

    for case in &report.cases {
        lines.push(format!(
            "| {} | {} | {:.2} | {} | {} | {} |",
            case.name,
            case.scale,
            case.latency_ns_per_op,
            case.alloc_calls,
            case.alloc_bytes,
            case.rss_delta_kb
        ));
    }

    if regressions.is_empty() {
        lines.push("".to_string());
        lines.push("No regressions detected against baseline.".to_string());
    } else {
        lines.push("".to_string());
        lines.push("## Regressions".to_string());
        for regression in regressions {
            lines.push(format!("- {regression}"));
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

fn main() -> Result<(), String> {
    let args = Args::parse();

    let scales = [100usize, 1_000, 10_000];
    let mut cases = Vec::new();
    for scale in scales {
        cases.extend(benchmark_route_lookup(scale));
        cases.extend(benchmark_lb(scale));
        cases.extend(benchmark_connection_lookup(scale));
        cases.extend(benchmark_h3_header_collection(scale));
    }

    cases.sort_by(|a, b| (&a.name, a.scale).cmp(&(&b.name, b.scale)));

    let report = BenchReport {
        suite: "spooky-cpu-memory-regression".to_string(),
        generated_unix_secs: unix_now(),
        cpu_threshold: args.cpu_threshold,
        mem_threshold: args.mem_threshold,
        cases,
    };

    print_summary(&report);
    write_report(&args.output, &report)?;

    let mut regressions = Vec::new();
    if args.check_baseline {
        let baseline_path = args
            .baseline
            .as_ref()
            .ok_or_else(|| "--check-baseline requires --baseline <path>".to_string())?;
        let baseline = load_report(baseline_path)?;
        regressions = compare_reports(&report, &baseline, args.cpu_threshold, args.mem_threshold);
    }

    if let Some(markdown) = &args.markdown_out {
        write_markdown(markdown, &report, &regressions)?;
    }

    if !regressions.is_empty() {
        for regression in &regressions {
            eprintln!("{regression}");
        }
        return Err(format!(
            "benchmark regression check failed ({} cases)",
            regressions.len()
        ));
    }

    Ok(())
}
