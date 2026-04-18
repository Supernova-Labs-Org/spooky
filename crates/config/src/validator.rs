use crate::backend_endpoint::{BackendEndpoint, BackendScheme};
use crate::config::Config;
use log::{error, info, warn};

pub const VALID_LOG_LEVELS: &[&str] = &[
    "whisper",
    "haunt",
    "spooky",
    "scream",
    "poltergeist",
    "silence",
    "trace",
    "debug",
    "info",
    "warn",
    "error",
    "off",
];

pub const VALID_LB_TYPES: &[&str] = &[
    "random",
    "round-robin",
    "round_robin",
    "rr",
    "consistent-hash",
    "consistent_hash",
    "ch",
];

pub fn validate(config: &Config) -> bool {
    info!("Starting configuration validation...");

    // --- Validate version ---
    if config.version != 1 {
        error!("Invalid version: expected '1', found '{}'", config.version);
        return false;
    }

    // --- Validate protocol ---
    if config.listen.protocol != "http3" {
        error!(
            "Invalid protocol: expected 'http3', found '{}'",
            config.listen.protocol
        );
        return false;
    }

    // --- Validate log level ---
    if !VALID_LOG_LEVELS
        .iter()
        .any(|lvl| lvl.eq_ignore_ascii_case(&config.log.level))
    {
        error!("Invalid log level: {}", config.log.level);
        return false;
    }

    // --- Validate global load balancing type (if present) ---
    if let Some(ref lb) = config.load_balancing
        && !VALID_LB_TYPES
            .iter()
            .any(|lb_type| lb_type.eq_ignore_ascii_case(&lb.lb_type))
    {
        error!("Invalid global load balancing type: {}", lb.lb_type);
        return false;
    }

    // --- Validate listen address ---
    if config.listen.address.is_empty() {
        error!("Listen address is empty");
        return false;
    }

    // --- Validate listen port ---
    if config.listen.port == 0 || config.listen.port > 65535 {
        error!(
            "Invalid listen port: {} (must be between 1 and 65535)",
            config.listen.port
        );
        return false;
    }

    // --- Validate performance controls ---
    if config.performance.worker_threads == 0 {
        error!("performance.worker_threads must be greater than 0");
        return false;
    }

    if config.performance.control_plane_threads == 0 {
        error!("performance.control_plane_threads must be greater than 0");
        return false;
    }

    if config.performance.packet_shards_per_worker == 0 {
        error!("performance.packet_shards_per_worker must be greater than 0");
        return false;
    }

    if config.performance.packet_shard_queue_capacity == 0 {
        error!("performance.packet_shard_queue_capacity must be greater than 0");
        return false;
    }

    if config.performance.worker_threads > 1 && !config.performance.reuseport {
        error!("performance.reuseport must be true when performance.worker_threads > 1");
        return false;
    }

    if config.performance.global_inflight_limit == 0 {
        error!("performance.global_inflight_limit must be greater than 0");
        return false;
    }

    if config.performance.per_upstream_inflight_limit == 0 {
        error!("performance.per_upstream_inflight_limit must be greater than 0");
        return false;
    }

    if config.performance.backend_timeout_ms == 0 {
        error!("performance.backend_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.backend_connect_timeout_ms == 0 {
        error!("performance.backend_connect_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.backend_body_idle_timeout_ms == 0 {
        error!("performance.backend_body_idle_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.backend_body_total_timeout_ms == 0 {
        error!("performance.backend_body_total_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.backend_total_request_timeout_ms == 0 {
        error!("performance.backend_total_request_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.shutdown_drain_timeout_ms == 0 {
        error!("performance.shutdown_drain_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.udp_recv_buffer_bytes == 0 {
        error!("performance.udp_recv_buffer_bytes must be greater than 0");
        return false;
    }

    if config.performance.udp_send_buffer_bytes == 0 {
        error!("performance.udp_send_buffer_bytes must be greater than 0");
        return false;
    }

    if config.performance.h2_pool_max_idle_per_backend == 0 {
        error!("performance.h2_pool_max_idle_per_backend must be greater than 0");
        return false;
    }

    if config.performance.h2_pool_idle_timeout_ms == 0 {
        error!("performance.h2_pool_idle_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.per_backend_inflight_limit == 0 {
        error!("performance.per_backend_inflight_limit must be greater than 0");
        return false;
    }

    if config.performance.new_connections_per_sec == 0 {
        error!("performance.new_connections_per_sec must be greater than 0");
        return false;
    }

    if config.performance.new_connections_burst == 0 {
        error!("performance.new_connections_burst must be greater than 0");
        return false;
    }

    if config.performance.quic_max_idle_timeout_ms == 0 {
        error!("performance.quic_max_idle_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.quic_initial_max_data == 0 {
        error!("performance.quic_initial_max_data must be greater than 0");
        return false;
    }

    if config.performance.quic_initial_max_stream_data == 0 {
        error!("performance.quic_initial_max_stream_data must be greater than 0");
        return false;
    }

    if config.performance.quic_initial_max_stream_data > config.performance.quic_initial_max_data {
        error!(
            "performance.quic_initial_max_stream_data ({}) must be <= quic_initial_max_data ({})",
            config.performance.quic_initial_max_stream_data,
            config.performance.quic_initial_max_data
        );
        return false;
    }

    if config.performance.quic_initial_max_streams_bidi == 0 {
        error!("performance.quic_initial_max_streams_bidi must be greater than 0");
        return false;
    }

    if config.performance.quic_initial_max_streams_uni == 0 {
        error!("performance.quic_initial_max_streams_uni must be greater than 0");
        return false;
    }

    if config.performance.max_response_body_bytes == 0 {
        error!("performance.max_response_body_bytes must be greater than 0");
        return false;
    }

    if config.performance.backend_connect_timeout_ms > config.performance.backend_timeout_ms {
        error!("performance.backend_connect_timeout_ms must be <= backend_timeout_ms");
        return false;
    }

    if config.performance.backend_timeout_ms > config.performance.backend_body_idle_timeout_ms {
        error!("performance.backend_timeout_ms must be <= backend_body_idle_timeout_ms");
        return false;
    }

    if config.performance.backend_body_idle_timeout_ms
        > config.performance.backend_body_total_timeout_ms
    {
        error!("performance.backend_body_idle_timeout_ms must be <= backend_body_total_timeout_ms");
        return false;
    }

    if config.performance.backend_body_total_timeout_ms
        > config.performance.backend_total_request_timeout_ms
    {
        error!(
            "performance.backend_body_total_timeout_ms must be <= backend_total_request_timeout_ms"
        );
        return false;
    }

    if config.resilience.adaptive_admission.min_limit == 0 {
        error!("resilience.adaptive_admission.min_limit must be greater than 0");
        return false;
    }

    if config.resilience.adaptive_admission.decrease_step == 0 {
        error!("resilience.adaptive_admission.decrease_step must be greater than 0");
        return false;
    }

    if config.resilience.adaptive_admission.increase_step == 0 {
        error!("resilience.adaptive_admission.increase_step must be greater than 0");
        return false;
    }

    if config.resilience.route_queue.default_cap == 0 {
        error!("resilience.route_queue.default_cap must be greater than 0");
        return false;
    }

    if config.resilience.route_queue.global_cap == 0 {
        error!("resilience.route_queue.global_cap must be greater than 0");
        return false;
    }

    if config.resilience.route_queue.shed_retry_after_seconds == 0 {
        error!("resilience.route_queue.shed_retry_after_seconds must be greater than 0");
        return false;
    }

    if config
        .resilience
        .route_queue
        .caps
        .values()
        .any(|cap| *cap == 0)
    {
        error!("resilience.route_queue.caps values must be greater than 0");
        return false;
    }

    if config.resilience.protocol.max_headers_count == 0 {
        error!("resilience.protocol.max_headers_count must be greater than 0");
        return false;
    }

    if config.resilience.protocol.max_headers_bytes == 0 {
        error!("resilience.protocol.max_headers_bytes must be greater than 0");
        return false;
    }

    if config
        .resilience
        .protocol
        .early_data_safe_methods
        .iter()
        .any(|method| method.trim().is_empty())
    {
        error!("resilience.protocol.early_data_safe_methods must not contain empty values");
        return false;
    }

    if config
        .resilience
        .protocol
        .allowed_methods
        .iter()
        .any(|method| method.trim().is_empty())
    {
        error!("resilience.protocol.allowed_methods must not contain empty values");
        return false;
    }

    if config
        .resilience
        .protocol
        .denied_path_prefixes
        .iter()
        .any(|prefix| prefix.is_empty() || !prefix.starts_with('/'))
    {
        error!("resilience.protocol.denied_path_prefixes must contain '/'-prefixed paths");
        return false;
    }

    if config.resilience.protocol.allow_0rtt
        && config
            .resilience
            .protocol
            .early_data_safe_methods
            .is_empty()
    {
        error!(
            "resilience.protocol.early_data_safe_methods must be non-empty when allow_0rtt=true"
        );
        return false;
    }

    if config.resilience.circuit_breaker.failure_threshold == 0 {
        error!("resilience.circuit_breaker.failure_threshold must be greater than 0");
        return false;
    }

    if config.resilience.circuit_breaker.open_ms == 0 {
        error!("resilience.circuit_breaker.open_ms must be greater than 0");
        return false;
    }

    if config.resilience.circuit_breaker.half_open_max_probes == 0 {
        error!("resilience.circuit_breaker.half_open_max_probes must be greater than 0");
        return false;
    }

    if config.resilience.retry_budget.ratio_percent > 100 {
        error!("resilience.retry_budget.ratio_percent must be <= 100");
        return false;
    }

    if config
        .resilience
        .retry_budget
        .per_route_ratio_percent
        .values()
        .any(|ratio| *ratio > 100)
    {
        error!("resilience.retry_budget.per_route_ratio_percent values must be <= 100");
        return false;
    }

    if config.resilience.brownout.trigger_inflight_percent > 100
        || config.resilience.brownout.recover_inflight_percent > 100
    {
        error!("resilience.brownout inflight percentages must be <= 100");
        return false;
    }

    if config.resilience.brownout.recover_inflight_percent
        >= config.resilience.brownout.trigger_inflight_percent
    {
        error!("resilience.brownout.recover_inflight_percent must be < trigger_inflight_percent");
        return false;
    }

    if config.resilience.watchdog.check_interval_ms == 0 {
        error!("resilience.watchdog.check_interval_ms must be greater than 0");
        return false;
    }

    if config.resilience.watchdog.poll_stall_timeout_ms == 0 {
        error!("resilience.watchdog.poll_stall_timeout_ms must be greater than 0");
        return false;
    }

    if config.resilience.watchdog.timeout_error_rate_percent > 100 {
        error!("resilience.watchdog.timeout_error_rate_percent must be <= 100");
        return false;
    }

    if config.resilience.watchdog.min_requests_per_window == 0 {
        error!("resilience.watchdog.min_requests_per_window must be greater than 0");
        return false;
    }

    if config.resilience.watchdog.overload_inflight_percent > 100 {
        error!("resilience.watchdog.overload_inflight_percent must be <= 100");
        return false;
    }

    if config.resilience.watchdog.unhealthy_consecutive_windows == 0 {
        error!("resilience.watchdog.unhealthy_consecutive_windows must be greater than 0");
        return false;
    }

    if config.resilience.watchdog.drain_grace_ms == 0 {
        error!("resilience.watchdog.drain_grace_ms must be greater than 0");
        return false;
    }

    if config.resilience.watchdog.restart_cooldown_ms == 0 {
        error!("resilience.watchdog.restart_cooldown_ms must be greater than 0");
        return false;
    }

    // --- Validate observability ---
    if config.observability.metrics.enabled {
        if config.observability.metrics.address.is_empty() {
            error!("observability.metrics.address cannot be empty when metrics are enabled");
            return false;
        }

        if config.observability.metrics.port == 0 {
            error!("observability.metrics.port must be between 1 and 65535");
            return false;
        }

        if !config.observability.metrics.path.starts_with('/') {
            error!("observability.metrics.path must start with '/'");
            return false;
        }
    }

    // --- Validate TLS certs ---
    if !std::path::Path::new(&config.listen.tls.cert).exists() {
        error!(
            "TLS certificate file does not exist: {}",
            config.listen.tls.cert
        );
        return false;
    }

    if !std::path::Path::new(&config.listen.tls.key).exists() {
        error!(
            "TLS private key file does not exist: {}",
            config.listen.tls.key
        );
        return false;
    }

    // Optional: Try to read the files to ensure they're accessible
    if let Err(e) = std::fs::read(&config.listen.tls.cert) {
        error!(
            "Cannot read TLS certificate file '{}': {}",
            config.listen.tls.cert, e
        );
        return false;
    }

    if let Err(e) = std::fs::read(&config.listen.tls.key) {
        error!(
            "Cannot read TLS private key file '{}': {}",
            config.listen.tls.key, e
        );
        return false;
    }

    // --- Validate upstream routes ---
    for (upstream_name, upstream) in &config.upstream {
        // Validate route matcher has at least one condition
        let has_host = upstream.route.host.is_some();
        let has_path = upstream.route.path_prefix.is_some();

        if !has_host && !has_path {
            error!(
                "Upstream '{}' must have either 'host' or 'path_prefix' route matcher",
                upstream_name
            );
            return false;
        }

        // Validate path_prefix is not empty if present
        if let Some(ref path) = upstream.route.path_prefix {
            if path.is_empty() {
                error!(
                    "Route path_prefix cannot be empty for upstream '{}'",
                    upstream_name
                );
                return false;
            }
            if !path.starts_with('/') {
                error!(
                    "Route path_prefix must start with '/' for upstream '{}': {}",
                    upstream_name, path
                );
                return false;
            }
        }
    }

    // --- Validate upstreams ---
    if config.upstream.is_empty() {
        error!("No upstreams configured");
        return false;
    }

    for (upstream_name, upstream) in &config.upstream {
        if upstream_name.is_empty() {
            error!("Upstream name is empty");
            return false;
        }

        // Validate load balancing type for this upstream
        if !VALID_LB_TYPES
            .iter()
            .any(|lb_type| lb_type.eq_ignore_ascii_case(&upstream.load_balancing.lb_type))
        {
            error!(
                "Invalid load balancing type '{}' for upstream '{}'",
                upstream.load_balancing.lb_type, upstream_name
            );
            return false;
        }

        // Validate backends
        if upstream.backends.is_empty() {
            error!("Upstream '{}' has no backends configured", upstream_name);
            return false;
        }

        for backend in &upstream.backends {
            // Validate backend ID
            if backend.id.is_empty() {
                error!("Backend ID is empty in upstream '{}'", upstream_name);
                return false;
            }

            // Validate backend address
            if backend.address.is_empty() {
                error!(
                    "Backend address is empty for backend '{}' in upstream '{}'",
                    backend.id, upstream_name
                );
                return false;
            }

            let endpoint = match BackendEndpoint::parse(&backend.address) {
                Ok(endpoint) => endpoint,
                Err(reason) => {
                    error!(
                        "Backend address '{}' in upstream '{}' is invalid: {}",
                        backend.address, upstream_name, reason
                    );
                    return false;
                }
            };
            if endpoint.scheme() == BackendScheme::Http {
                warn!(
                    "Backend '{}' in upstream '{}' uses explicit insecure cleartext transport ({})",
                    backend.id, upstream_name, backend.address
                );
            }

            // Validate weight
            if backend.weight == 0 || backend.weight > 1000 {
                error!(
                    "Backend '{}' in upstream '{}' has invalid weight {} (must be 1–1000)",
                    backend.id, upstream_name, backend.weight
                );
                return false;
            }

            // Validate health check
            let hc = &backend.health_check;

            if hc.interval == 0 {
                error!(
                    "Health check interval is invalid (0) for backend '{}' in upstream '{}'",
                    backend.id, upstream_name
                );
                return false;
            }

            if hc.timeout_ms == 0 {
                error!(
                    "Health check timeout is invalid (0) for backend '{}' in upstream '{}'",
                    backend.id, upstream_name
                );
                return false;
            }

            if hc.failure_threshold == 0 {
                error!(
                    "Health check failure threshold is invalid (0) for backend '{}' in upstream '{}'",
                    backend.id, upstream_name
                );
                return false;
            }

            if hc.success_threshold == 0 {
                error!(
                    "Health check success threshold is invalid (0) for backend '{}' in upstream '{}'",
                    backend.id, upstream_name
                );
                return false;
            }

            if hc.cooldown_ms == 0 {
                error!(
                    "Health check cooldown is invalid (0) for backend '{}' in upstream '{}'",
                    backend.id, upstream_name
                );
                return false;
            }
        }
    }

    info!("Configuration validation passed successfully\n");
    true
}

#[cfg(test)]
mod tests {
    use super::validate;
    use crate::config::{
        Backend, Config, HealthCheck, Listen, LoadBalancing, Log, MetricsEndpoint, Observability,
        Performance, Resilience, RouteMatch, Tls, Upstream,
    };
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn base_config(cert: &str, key: &str) -> Config {
        let mut upstream = HashMap::new();
        upstream.insert(
            "test_upstream".to_string(),
            Upstream {
                load_balancing: LoadBalancing {
                    lb_type: "round-robin".to_string(),
                    key: None,
                },
                route: RouteMatch {
                    host: None,
                    path_prefix: Some("/".to_string()),
                    method: None,
                },
                backends: vec![Backend {
                    id: "backend-1".to_string(),
                    address: "127.0.0.1:8080".to_string(),
                    weight: 1,
                    health_check: HealthCheck {
                        path: "/health".to_string(),
                        interval: 1000,
                        timeout_ms: 1000,
                        failure_threshold: 3,
                        success_threshold: 1,
                        cooldown_ms: 1000,
                    },
                }],
            },
        );

        Config {
            version: 1,
            listen: Listen {
                protocol: "http3".to_string(),
                port: 9889,
                address: "127.0.0.1".to_string(),
                tls: Tls {
                    cert: cert.to_string(),
                    key: key.to_string(),
                },
            },
            upstream,
            load_balancing: Some(LoadBalancing {
                lb_type: "random".to_string(),
                key: None,
            }),
            log: Log {
                level: "info".to_string(),
                file: Default::default(),
            },
            performance: Performance::default(),
            observability: Observability::default(),
            resilience: Resilience::default(),
        }
    }

    #[test]
    fn yaml_parse_applies_performance_and_observability_defaults() {
        let dir = tempdir().expect("tempdir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "cert").expect("write cert");
        std::fs::write(&key, "key").expect("write key");

        let yaml = format!(
            r#"
version: 1
listen:
  protocol: http3
  address: "127.0.0.1"
  port: 9889
  tls:
    cert: "{}"
    key: "{}"
upstream:
  test_upstream:
    load_balancing:
      type: round-robin
    route:
      path_prefix: "/"
    backends:
      - id: "b1"
        address: "127.0.0.1:8080"
        weight: 1
        health_check: {{}}
"#,
            cert.display(),
            key.display()
        );

        let cfg: Config = serde_yaml::from_str(&yaml).expect("parse");
        assert_eq!(cfg.performance.worker_threads, 1);
        assert_eq!(cfg.performance.control_plane_threads, 2);
        assert_eq!(cfg.performance.packet_shards_per_worker, 1);
        assert_eq!(cfg.performance.packet_shard_queue_capacity, 2048);
        assert!(cfg.performance.reuseport);
        assert!(!cfg.performance.pin_workers);
        assert_eq!(cfg.performance.global_inflight_limit, 4096);
        assert_eq!(cfg.performance.per_upstream_inflight_limit, 1024);
        assert_eq!(cfg.performance.backend_timeout_ms, 2000);
        assert_eq!(cfg.performance.backend_connect_timeout_ms, 500);
        assert_eq!(cfg.performance.backend_body_idle_timeout_ms, 2000);
        assert_eq!(cfg.performance.backend_body_total_timeout_ms, 30000);
        assert_eq!(cfg.performance.backend_total_request_timeout_ms, 35_000);
        assert_eq!(cfg.performance.shutdown_drain_timeout_ms, 5_000);
        assert_eq!(cfg.performance.udp_recv_buffer_bytes, 8 * 1024 * 1024);
        assert_eq!(cfg.performance.udp_send_buffer_bytes, 8 * 1024 * 1024);
        assert_eq!(cfg.performance.h2_pool_max_idle_per_backend, 256);
        assert_eq!(cfg.performance.h2_pool_idle_timeout_ms, 90_000);
        assert_eq!(cfg.performance.per_backend_inflight_limit, 64);
        assert!(!cfg.observability.metrics.enabled);
        assert_eq!(cfg.observability.metrics.path, "/metrics");
        assert!(cfg.resilience.adaptive_admission.enabled);
        assert_eq!(cfg.resilience.route_queue.default_cap, 512);
        assert_eq!(cfg.resilience.route_queue.global_cap, 2048);
        assert_eq!(cfg.resilience.route_queue.shed_retry_after_seconds, 1);
        assert!(!cfg.resilience.protocol.allow_0rtt);
        assert_eq!(cfg.resilience.protocol.max_headers_count, 128);
        assert_eq!(cfg.resilience.protocol.max_headers_bytes, 16 * 1024);
        assert!(cfg.resilience.protocol.enforce_authority_host_match);
        assert!(!cfg.resilience.watchdog.enabled);
        assert_eq!(cfg.resilience.watchdog.check_interval_ms, 1_000);
    }

    #[test]
    fn rejects_invalid_performance_and_observability_values() {
        let dir = tempdir().expect("tempdir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "cert").expect("write cert");
        std::fs::write(&key, "key").expect("write key");

        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.worker_threads = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.worker_threads = 4;
        cfg.performance.reuseport = false;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.packet_shards_per_worker = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.packet_shard_queue_capacity = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.backend_connect_timeout_ms = 2_001;
        cfg.performance.backend_timeout_ms = 2_000;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.backend_total_request_timeout_ms = 5_000;
        cfg.performance.backend_body_total_timeout_ms = 6_000;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.backend_body_total_timeout_ms = 100;
        cfg.performance.backend_body_idle_timeout_ms = 200;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.shutdown_drain_timeout_ms = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.udp_recv_buffer_bytes = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.udp_send_buffer_bytes = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.h2_pool_max_idle_per_backend = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.h2_pool_idle_timeout_ms = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.per_backend_inflight_limit = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.new_connections_per_sec = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.new_connections_burst = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.quic_max_idle_timeout_ms = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.quic_initial_max_data = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.quic_initial_max_stream_data = 0;
        assert!(!validate(&cfg));

        // stream limit exceeds connection limit — cross-field violation
        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.quic_initial_max_data = 1_000_000;
        cfg.performance.quic_initial_max_stream_data = 2_000_000;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.quic_initial_max_streams_bidi = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.quic_initial_max_streams_uni = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.max_response_body_bytes = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.route_queue.default_cap = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.route_queue.global_cap = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.route_queue.shed_retry_after_seconds = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.protocol.max_headers_count = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.protocol.max_headers_bytes = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.protocol.allow_0rtt = true;
        cfg.resilience.protocol.early_data_safe_methods.clear();
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.protocol.denied_path_prefixes = vec!["admin".to_string()];
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.protocol.allowed_methods = vec!["".to_string()];
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.retry_budget.ratio_percent = 101;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.brownout.trigger_inflight_percent = 50;
        cfg.resilience.brownout.recover_inflight_percent = 50;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.watchdog.timeout_error_rate_percent = 101;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.watchdog.unhealthy_consecutive_windows = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.observability = Observability {
            metrics: MetricsEndpoint {
                enabled: true,
                address: "127.0.0.1".to_string(),
                port: 9901,
                path: "metrics".to_string(),
            },
        };
        assert!(!validate(&cfg));
    }

    #[test]
    fn accepts_valid_metrics_and_performance_configuration() {
        let dir = tempdir().expect("tempdir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "cert").expect("write cert");
        std::fs::write(&key, "key").expect("write key");

        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.worker_threads = 4;
        cfg.performance.control_plane_threads = 2;
        cfg.performance.packet_shards_per_worker = 2;
        cfg.performance.packet_shard_queue_capacity = 1024;
        cfg.performance.reuseport = true;
        cfg.performance.pin_workers = true;
        cfg.performance.global_inflight_limit = 10_000;
        cfg.performance.per_upstream_inflight_limit = 2_000;
        cfg.performance.backend_connect_timeout_ms = 300;
        cfg.performance.backend_timeout_ms = 1500;
        cfg.performance.backend_body_idle_timeout_ms = 2_500;
        cfg.performance.backend_body_total_timeout_ms = 10_000;
        cfg.performance.backend_total_request_timeout_ms = 15_000;
        cfg.performance.shutdown_drain_timeout_ms = 7_500;
        cfg.performance.udp_recv_buffer_bytes = 4 * 1024 * 1024;
        cfg.performance.udp_send_buffer_bytes = 4 * 1024 * 1024;
        cfg.performance.h2_pool_max_idle_per_backend = 128;
        cfg.performance.h2_pool_idle_timeout_ms = 120_000;
        cfg.performance.per_backend_inflight_limit = 32;
        cfg.resilience.route_queue.default_cap = 256;
        cfg.resilience.route_queue.global_cap = 2048;
        cfg.resilience.route_queue.shed_retry_after_seconds = 2;
        cfg.resilience.protocol.allow_0rtt = true;
        cfg.resilience.protocol.early_data_safe_methods = vec!["GET".to_string()];
        cfg.resilience.protocol.max_headers_count = 64;
        cfg.resilience.protocol.max_headers_bytes = 8 * 1024;
        cfg.resilience.protocol.allowed_methods = vec!["GET".to_string(), "POST".to_string()];
        cfg.resilience.protocol.denied_path_prefixes = vec!["/admin".to_string()];
        cfg.resilience.retry_budget.ratio_percent = 30;
        cfg.observability = Observability {
            metrics: MetricsEndpoint {
                enabled: true,
                address: "127.0.0.1".to_string(),
                port: 9901,
                path: "/metrics".to_string(),
            },
        };

        assert!(validate(&cfg));
    }

    #[test]
    fn backend_address_validation_supports_secure_default_and_explicit_http() {
        let dir = tempdir().expect("tempdir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "cert").expect("write cert");
        std::fs::write(&key, "key").expect("write key");

        // Bare host:port defaults to HTTPS policy.
        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.upstream
            .get_mut("test_upstream")
            .expect("upstream")
            .backends[0]
            .address = "api.example.internal:443".to_string();
        assert!(validate(&cfg));

        // Explicit HTTP remains allowed as an opt-out.
        cfg.upstream
            .get_mut("test_upstream")
            .expect("upstream")
            .backends[0]
            .address = "http://127.0.0.1:8080".to_string();
        assert!(validate(&cfg));
    }

    #[test]
    fn backend_address_validation_rejects_invalid_urls() {
        let dir = tempdir().expect("tempdir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "cert").expect("write cert");
        std::fs::write(&key, "key").expect("write key");

        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.upstream
            .get_mut("test_upstream")
            .expect("upstream")
            .backends[0]
            .address = "https://127.0.0.1:8443/path".to_string();
        assert!(!validate(&cfg));

        cfg.upstream
            .get_mut("test_upstream")
            .expect("upstream")
            .backends[0]
            .address = "ftp://127.0.0.1:21".to_string();
        assert!(!validate(&cfg));
    }
}
