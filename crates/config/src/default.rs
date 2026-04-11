use crate::config::{LoadBalancing, Log, LogFile};

// default values
pub fn get_default_version() -> u32 {
    1
}

pub fn get_default_protocol() -> String {
    String::from("http3")
}

pub fn get_default_port() -> u32 {
    9889
}

pub fn get_default_address() -> String {
    String::from("0.0.0.0")
}

pub fn get_default_weight() -> u32 {
    100
}

pub fn get_default_path() -> String {
    String::from("/health")
}

pub fn get_default_interval() -> u64 {
    5000
}

pub fn get_default_health_timeout() -> u64 {
    1000
}

pub fn get_default_failure_threshold() -> u32 {
    3
}

pub fn get_default_success_threshold() -> u32 {
    2
}

pub fn get_default_cooldown_ms() -> u64 {
    5_000
}

pub fn get_default_log_level() -> String {
    String::from("info")
}

pub fn get_default_log_file_path() -> String {
    String::from("/var/log/spooky/spooky.log")
}

pub fn get_default_load_balancing() -> LoadBalancing {
    LoadBalancing {
        lb_type: String::from("round_robin"),
        key: None,
    }
}

pub fn get_default_log() -> Log {
    Log {
        level: String::from("info"),
        file: LogFile {
            enabled: false,
            path: String::from(""),
        },
    }
}

pub fn perf_default_worker_threads() -> usize {
    1
}

pub fn perf_default_global_inflight_limit() -> usize {
    4096
}

pub fn perf_default_per_upstream_inflight_limit() -> usize {
    1024
}

pub fn perf_default_backend_timeout_ms() -> u64 {
    2_000
}

pub fn perf_default_backend_body_idle_timeout_ms() -> u64 {
    2_000
}

pub fn perf_default_backend_body_total_timeout_ms() -> u64 {
    30_000
}

pub fn observe_default_address() -> String {
    String::from("127.0.0.1")
}

pub fn observe_default_port() -> u16 {
    9901
}

pub fn observe_default_metrics_path() -> String {
    String::from("/metrics")
}
