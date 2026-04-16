use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::default::{
    get_default_address, get_default_cooldown_ms, get_default_failure_threshold,
    get_default_health_timeout, get_default_interval, get_default_load_balancing, get_default_log,
    get_default_log_file_path, get_default_log_level, get_default_path, get_default_port,
    get_default_protocol, get_default_success_threshold, get_default_version, get_default_weight,
    observe_default_address, observe_default_metrics_path, observe_default_port,
    perf_default_backend_body_idle_timeout_ms, perf_default_backend_body_total_timeout_ms,
    perf_default_backend_connect_timeout_ms, perf_default_backend_timeout_ms,
    perf_default_backend_total_request_timeout_ms, perf_default_control_plane_threads,
    perf_default_global_inflight_limit, perf_default_h2_pool_idle_timeout_ms,
    perf_default_h2_pool_max_idle_per_backend, perf_default_new_connections_burst,
    perf_default_new_connections_per_sec, perf_default_per_backend_inflight_limit,
    perf_default_per_upstream_inflight_limit, perf_default_pin_workers,
    perf_default_quic_initial_max_data, perf_default_quic_initial_max_stream_data,
    perf_default_max_response_body_bytes, perf_default_quic_initial_max_streams_bidi,
    perf_default_quic_initial_max_streams_uni, perf_default_quic_max_idle_timeout_ms,
    perf_default_reuseport,
    perf_default_udp_recv_buffer_bytes, perf_default_udp_send_buffer_bytes,
    perf_default_worker_threads, resilience_default_adaptive_decrease_step,
    resilience_default_adaptive_enabled, resilience_default_adaptive_high_latency_ms,
    resilience_default_adaptive_increase_step, resilience_default_adaptive_min_limit,
    resilience_default_brownout_enabled, resilience_default_brownout_recover_inflight_percent,
    resilience_default_brownout_trigger_inflight_percent, resilience_default_cb_enabled,
    resilience_default_cb_failure_threshold, resilience_default_cb_half_open_max_probes,
    resilience_default_cb_open_ms, resilience_default_hedging_delay_ms,
    resilience_default_hedging_enabled, resilience_default_retry_budget_enabled,
    resilience_default_retry_budget_ratio_percent, resilience_default_route_queue_default_cap,
    resilience_default_watchdog_check_interval_ms, resilience_default_watchdog_drain_grace_ms,
    resilience_default_watchdog_enabled, resilience_default_watchdog_min_requests_per_window,
    resilience_default_watchdog_overload_inflight_percent,
    resilience_default_watchdog_poll_stall_timeout_ms,
    resilience_default_watchdog_restart_cooldown_ms,
    resilience_default_watchdog_timeout_error_rate_percent,
    resilience_default_watchdog_unhealthy_consecutive_windows,
};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "get_default_version")] // Make version optional with default
    pub version: u32,

    pub listen: Listen,

    pub upstream: HashMap<String, Upstream>,

    #[serde(default)]
    pub load_balancing: Option<LoadBalancing>, // Global fallback load balancing

    #[serde(default = "get_default_log")]
    pub log: Log,

    #[serde(default)]
    pub performance: Performance,

    #[serde(default)]
    pub observability: Observability,

    #[serde(default)]
    pub resilience: Resilience,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Listen {
    #[serde(default = "get_default_protocol")]
    pub protocol: String, // "http3"

    #[serde(default = "get_default_port")]
    pub port: u32, // 9889

    #[serde(default = "get_default_address")]
    pub address: String, // "0.0.0.0"
    pub tls: Tls,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Tls {
    pub cert: String, // "/path/to/cert"
    pub key: String,  // "/path/to/key"
}

#[derive(Debug, Deserialize, Clone)]
pub struct Upstream {
    #[serde(default = "get_default_load_balancing")]
    pub load_balancing: LoadBalancing,

    pub route: RouteMatch, // Route matching criteria for this upstream

    pub backends: Vec<Backend>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Backend {
    pub id: String,      // "backend1"
    pub address: String, // "10.0.1.100:8080"

    #[serde(default = "get_default_weight")]
    pub weight: u32, // 100
    pub health_check: HealthCheck,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct RouteMatch {
    #[serde(default)]
    pub host: Option<String>, // host-based routing (e.g., "api.example.com")

    #[serde(default)]
    pub path_prefix: Option<String>, // path prefix matching (e.g., "/api")

    #[serde(default)]
    pub method: Option<String>, // 🔮 FUTURE: HTTP method filtering (GET, POST, etc.)
}

#[derive(Debug, Deserialize, Clone)]
pub struct HealthCheck {
    #[serde(default = "get_default_path")]
    pub path: String, // "/health"

    #[serde(default = "get_default_interval")]
    pub interval: u64, // "5000" (write in number of milli seconds)

    #[serde(default = "get_default_health_timeout")]
    pub timeout_ms: u64,

    #[serde(default = "get_default_failure_threshold")]
    pub failure_threshold: u32,

    #[serde(default = "get_default_success_threshold")]
    pub success_threshold: u32,

    #[serde(default = "get_default_cooldown_ms")]
    pub cooldown_ms: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LoadBalancing {
    #[serde(rename = "type")]
    pub lb_type: String, // "random","round_robin","consistent_hash"

    // Add support for consistent hash configuration
    #[serde(default)]
    pub key: Option<String>, // For consistent hashing (header, cookie, etc.)
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Log {
    // whisper -> trace
    // haunt -> debug
    // spooky -> info
    // scream -> warn
    // poltergeist -> error
    // silence -> off
    #[serde(default = "get_default_log_level")]
    pub level: String, // "info, warn, error"

    #[serde(default)]
    pub file: LogFile,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LogFile {
    pub enabled: bool,

    #[serde(default = "get_default_log_file_path")]
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Performance {
    #[serde(default = "perf_default_worker_threads")]
    pub worker_threads: usize,

    #[serde(default = "perf_default_control_plane_threads")]
    pub control_plane_threads: usize,

    #[serde(default = "perf_default_reuseport")]
    pub reuseport: bool,

    #[serde(default = "perf_default_pin_workers")]
    pub pin_workers: bool,

    #[serde(default = "perf_default_global_inflight_limit")]
    pub global_inflight_limit: usize,

    #[serde(default = "perf_default_per_upstream_inflight_limit")]
    pub per_upstream_inflight_limit: usize,

    #[serde(default = "perf_default_backend_timeout_ms")]
    pub backend_timeout_ms: u64,

    #[serde(default = "perf_default_backend_connect_timeout_ms")]
    pub backend_connect_timeout_ms: u64,

    #[serde(default = "perf_default_backend_body_idle_timeout_ms")]
    pub backend_body_idle_timeout_ms: u64,

    #[serde(default = "perf_default_backend_body_total_timeout_ms")]
    pub backend_body_total_timeout_ms: u64,

    #[serde(default = "perf_default_backend_total_request_timeout_ms")]
    pub backend_total_request_timeout_ms: u64,

    #[serde(default = "perf_default_udp_recv_buffer_bytes")]
    pub udp_recv_buffer_bytes: usize,

    #[serde(default = "perf_default_udp_send_buffer_bytes")]
    pub udp_send_buffer_bytes: usize,

    #[serde(default = "perf_default_h2_pool_max_idle_per_backend")]
    pub h2_pool_max_idle_per_backend: usize,

    #[serde(default = "perf_default_h2_pool_idle_timeout_ms")]
    pub h2_pool_idle_timeout_ms: u64,

    #[serde(default = "perf_default_per_backend_inflight_limit")]
    pub per_backend_inflight_limit: usize,

    /// Steady-state new QUIC connections allowed per second (token-bucket refill rate).
    #[serde(default = "perf_default_new_connections_per_sec")]
    pub new_connections_per_sec: u32,

    /// Maximum burst of new QUIC connections above the steady-state rate.
    /// Must be >= 1; values below 1 are clamped to 1 at runtime.
    #[serde(default = "perf_default_new_connections_burst")]
    pub new_connections_burst: u32,

    /// QUIC idle timeout: connection is closed after this many ms of inactivity.
    #[serde(default = "perf_default_quic_max_idle_timeout_ms")]
    pub quic_max_idle_timeout_ms: u64,

    /// QUIC connection-level flow control: total bytes the client may send before
    /// receiving a MAX_DATA frame.
    #[serde(default = "perf_default_quic_initial_max_data")]
    pub quic_initial_max_data: u64,

    /// QUIC stream-level flow control: bytes allowed per stream (bidi and uni).
    /// Must be <= `quic_initial_max_data`.
    #[serde(default = "perf_default_quic_initial_max_stream_data")]
    pub quic_initial_max_stream_data: u64,

    /// Maximum number of concurrent bidirectional streams per connection.
    #[serde(default = "perf_default_quic_initial_max_streams_bidi")]
    pub quic_initial_max_streams_bidi: u64,

    /// Maximum number of concurrent unidirectional streams per connection.
    #[serde(default = "perf_default_quic_initial_max_streams_uni")]
    pub quic_initial_max_streams_uni: u64,

    /// Hard cap on upstream response body bytes per stream.
    /// Streams whose response body exceeds this size are terminated with 502.
    /// Protects against runaway or adversarial upstreams streaming unboundedly.
    #[serde(default = "perf_default_max_response_body_bytes")]
    pub max_response_body_bytes: usize,
}

impl Default for Performance {
    fn default() -> Self {
        Self {
            worker_threads: perf_default_worker_threads(),
            control_plane_threads: perf_default_control_plane_threads(),
            reuseport: perf_default_reuseport(),
            pin_workers: perf_default_pin_workers(),
            global_inflight_limit: perf_default_global_inflight_limit(),
            per_upstream_inflight_limit: perf_default_per_upstream_inflight_limit(),
            backend_timeout_ms: perf_default_backend_timeout_ms(),
            backend_connect_timeout_ms: perf_default_backend_connect_timeout_ms(),
            backend_body_idle_timeout_ms: perf_default_backend_body_idle_timeout_ms(),
            backend_body_total_timeout_ms: perf_default_backend_body_total_timeout_ms(),
            backend_total_request_timeout_ms: perf_default_backend_total_request_timeout_ms(),
            udp_recv_buffer_bytes: perf_default_udp_recv_buffer_bytes(),
            udp_send_buffer_bytes: perf_default_udp_send_buffer_bytes(),
            h2_pool_max_idle_per_backend: perf_default_h2_pool_max_idle_per_backend(),
            h2_pool_idle_timeout_ms: perf_default_h2_pool_idle_timeout_ms(),
            per_backend_inflight_limit: perf_default_per_backend_inflight_limit(),
            new_connections_per_sec: perf_default_new_connections_per_sec(),
            new_connections_burst: perf_default_new_connections_burst(),
            quic_max_idle_timeout_ms: perf_default_quic_max_idle_timeout_ms(),
            quic_initial_max_data: perf_default_quic_initial_max_data(),
            quic_initial_max_stream_data: perf_default_quic_initial_max_stream_data(),
            quic_initial_max_streams_bidi: perf_default_quic_initial_max_streams_bidi(),
            quic_initial_max_streams_uni: perf_default_quic_initial_max_streams_uni(),
            max_response_body_bytes: perf_default_max_response_body_bytes(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Resilience {
    #[serde(default)]
    pub adaptive_admission: AdaptiveAdmission,
    #[serde(default)]
    pub route_queue: RouteQueue,
    #[serde(default)]
    pub circuit_breaker: CircuitBreaker,
    #[serde(default)]
    pub hedging: Hedging,
    #[serde(default)]
    pub retry_budget: RetryBudget,
    #[serde(default)]
    pub brownout: Brownout,
    #[serde(default)]
    pub watchdog: Watchdog,
}

impl Default for Resilience {
    fn default() -> Self {
        Self {
            adaptive_admission: AdaptiveAdmission::default(),
            route_queue: RouteQueue::default(),
            circuit_breaker: CircuitBreaker::default(),
            hedging: Hedging::default(),
            retry_budget: RetryBudget::default(),
            brownout: Brownout::default(),
            watchdog: Watchdog::default(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AdaptiveAdmission {
    #[serde(default = "resilience_default_adaptive_enabled")]
    pub enabled: bool,
    #[serde(default = "resilience_default_adaptive_min_limit")]
    pub min_limit: usize,
    #[serde(default = "resilience_default_adaptive_decrease_step")]
    pub decrease_step: usize,
    #[serde(default = "resilience_default_adaptive_increase_step")]
    pub increase_step: usize,
    #[serde(default = "resilience_default_adaptive_high_latency_ms")]
    pub high_latency_ms: u64,
}

impl Default for AdaptiveAdmission {
    fn default() -> Self {
        Self {
            enabled: resilience_default_adaptive_enabled(),
            min_limit: resilience_default_adaptive_min_limit(),
            decrease_step: resilience_default_adaptive_decrease_step(),
            increase_step: resilience_default_adaptive_increase_step(),
            high_latency_ms: resilience_default_adaptive_high_latency_ms(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RouteQueue {
    #[serde(default = "resilience_default_route_queue_default_cap")]
    pub default_cap: usize,
    #[serde(default)]
    pub caps: HashMap<String, usize>,
}

impl Default for RouteQueue {
    fn default() -> Self {
        Self {
            default_cap: resilience_default_route_queue_default_cap(),
            caps: HashMap::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CircuitBreaker {
    #[serde(default = "resilience_default_cb_enabled")]
    pub enabled: bool,
    #[serde(default = "resilience_default_cb_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "resilience_default_cb_open_ms")]
    pub open_ms: u64,
    #[serde(default = "resilience_default_cb_half_open_max_probes")]
    pub half_open_max_probes: u32,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self {
            enabled: resilience_default_cb_enabled(),
            failure_threshold: resilience_default_cb_failure_threshold(),
            open_ms: resilience_default_cb_open_ms(),
            half_open_max_probes: resilience_default_cb_half_open_max_probes(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Hedging {
    #[serde(default = "resilience_default_hedging_enabled")]
    pub enabled: bool,
    #[serde(default = "resilience_default_hedging_delay_ms")]
    pub delay_ms: u64,
    #[serde(default)]
    pub safe_methods: Vec<String>,
    #[serde(default)]
    pub route_allowlist: Vec<String>,
}

impl Default for Hedging {
    fn default() -> Self {
        Self {
            enabled: resilience_default_hedging_enabled(),
            delay_ms: resilience_default_hedging_delay_ms(),
            safe_methods: vec!["GET".to_string(), "HEAD".to_string()],
            route_allowlist: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RetryBudget {
    #[serde(default = "resilience_default_retry_budget_enabled")]
    pub enabled: bool,
    #[serde(default = "resilience_default_retry_budget_ratio_percent")]
    pub ratio_percent: u8,
    #[serde(default)]
    pub per_route_ratio_percent: HashMap<String, u8>,
}

impl Default for RetryBudget {
    fn default() -> Self {
        Self {
            enabled: resilience_default_retry_budget_enabled(),
            ratio_percent: resilience_default_retry_budget_ratio_percent(),
            per_route_ratio_percent: HashMap::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Brownout {
    #[serde(default = "resilience_default_brownout_enabled")]
    pub enabled: bool,
    #[serde(default = "resilience_default_brownout_trigger_inflight_percent")]
    pub trigger_inflight_percent: u8,
    #[serde(default = "resilience_default_brownout_recover_inflight_percent")]
    pub recover_inflight_percent: u8,
    #[serde(default)]
    pub core_routes: Vec<String>,
}

impl Default for Brownout {
    fn default() -> Self {
        Self {
            enabled: resilience_default_brownout_enabled(),
            trigger_inflight_percent: resilience_default_brownout_trigger_inflight_percent(),
            recover_inflight_percent: resilience_default_brownout_recover_inflight_percent(),
            core_routes: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Watchdog {
    #[serde(default = "resilience_default_watchdog_enabled")]
    pub enabled: bool,
    #[serde(default = "resilience_default_watchdog_check_interval_ms")]
    pub check_interval_ms: u64,
    #[serde(default = "resilience_default_watchdog_poll_stall_timeout_ms")]
    pub poll_stall_timeout_ms: u64,
    #[serde(default = "resilience_default_watchdog_timeout_error_rate_percent")]
    pub timeout_error_rate_percent: u8,
    #[serde(default = "resilience_default_watchdog_min_requests_per_window")]
    pub min_requests_per_window: u64,
    #[serde(default = "resilience_default_watchdog_overload_inflight_percent")]
    pub overload_inflight_percent: u8,
    #[serde(default = "resilience_default_watchdog_unhealthy_consecutive_windows")]
    pub unhealthy_consecutive_windows: u32,
    #[serde(default = "resilience_default_watchdog_drain_grace_ms")]
    pub drain_grace_ms: u64,
    #[serde(default = "resilience_default_watchdog_restart_cooldown_ms")]
    pub restart_cooldown_ms: u64,
    #[serde(default)]
    pub restart_hook: Option<String>,
}

impl Default for Watchdog {
    fn default() -> Self {
        Self {
            enabled: resilience_default_watchdog_enabled(),
            check_interval_ms: resilience_default_watchdog_check_interval_ms(),
            poll_stall_timeout_ms: resilience_default_watchdog_poll_stall_timeout_ms(),
            timeout_error_rate_percent: resilience_default_watchdog_timeout_error_rate_percent(),
            min_requests_per_window: resilience_default_watchdog_min_requests_per_window(),
            overload_inflight_percent: resilience_default_watchdog_overload_inflight_percent(),
            unhealthy_consecutive_windows:
                resilience_default_watchdog_unhealthy_consecutive_windows(),
            drain_grace_ms: resilience_default_watchdog_drain_grace_ms(),
            restart_cooldown_ms: resilience_default_watchdog_restart_cooldown_ms(),
            restart_hook: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Observability {
    #[serde(default)]
    pub metrics: MetricsEndpoint,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MetricsEndpoint {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "observe_default_address")]
    pub address: String,

    #[serde(default = "observe_default_port")]
    pub port: u16,

    #[serde(default = "observe_default_metrics_path")]
    pub path: String,
}

impl Default for MetricsEndpoint {
    fn default() -> Self {
        Self {
            enabled: false,
            address: observe_default_address(),
            port: observe_default_port(),
            path: observe_default_metrics_path(),
        }
    }
}
