use crate::backend_endpoint::{BackendEndpoint, BackendScheme};
use crate::config::{CURRENT_CONFIG_VERSION, Config, SUPPORTED_CONFIG_VERSIONS};
use log::{error, info, warn};
use std::fs::File;
use std::io::BufReader;
use std::net::IpAddr;
use std::path::Path;

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

fn validate_pem_certificates(path: &str, field_name: &str) -> bool {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) => {
            error!("Cannot open {} '{}': {}", field_name, path, err);
            return false;
        }
    };

    let mut reader = BufReader::new(file);
    let certs = match rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>() {
        Ok(certs) => certs,
        Err(err) => {
            error!(
                "Cannot parse PEM certificates from {} '{}': {}",
                field_name, path, err
            );
            return false;
        }
    };

    if certs.is_empty() {
        error!(
            "{} '{}' does not contain any PEM certificate blocks",
            field_name, path
        );
        return false;
    }

    true
}

fn validate_pem_private_key(path: &str, field_name: &str) -> bool {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) => {
            error!("Cannot open {} '{}': {}", field_name, path, err);
            return false;
        }
    };

    let mut reader = BufReader::new(file);
    match rustls_pemfile::private_key(&mut reader) {
        Ok(Some(_)) => true,
        Ok(None) => {
            error!(
                "{} '{}' does not contain a PEM private key",
                field_name, path
            );
            false
        }
        Err(err) => {
            error!(
                "Cannot parse PEM private key from {} '{}': {}",
                field_name, path, err
            );
            false
        }
    }
}

fn is_loopback_bind_address(raw: &str) -> bool {
    let normalized = raw.trim().trim_start_matches('[').trim_end_matches(']');
    if normalized.eq_ignore_ascii_case("localhost") {
        return true;
    }
    normalized
        .parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

pub fn validate(config: &Config) -> bool {
    info!("Starting configuration validation...");

    // --- Validate version ---
    if !SUPPORTED_CONFIG_VERSIONS.contains(&config.version) {
        let supported = SUPPORTED_CONFIG_VERSIONS
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        error!(
            "Invalid version: found '{}', supported versions are [{}]",
            config.version, supported
        );
        return false;
    }
    if config.version != CURRENT_CONFIG_VERSION {
        warn!(
            "Config version '{}' is supported but not current (current={}); please migrate when possible",
            config.version, CURRENT_CONFIG_VERSION
        );
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

    if config.performance.packet_shard_queue_max_bytes == 0 {
        error!("performance.packet_shard_queue_max_bytes must be greater than 0");
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

    if config.performance.max_active_connections == 0 {
        error!("performance.max_active_connections must be greater than 0");
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

    if config.performance.max_request_body_bytes == 0 {
        error!("performance.max_request_body_bytes must be greater than 0");
        return false;
    }

    if config.performance.request_buffer_global_cap_bytes == 0 {
        error!("performance.request_buffer_global_cap_bytes must be greater than 0");
        return false;
    }

    if config.performance.unknown_length_response_prebuffer_bytes == 0 {
        error!("performance.unknown_length_response_prebuffer_bytes must be greater than 0");
        return false;
    }

    if config.performance.client_body_idle_timeout_ms == 0 {
        error!("performance.client_body_idle_timeout_ms must be greater than 0");
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

    if config.performance.max_request_body_bytes
        > config.performance.quic_initial_max_stream_data as usize
    {
        error!(
            "performance.max_request_body_bytes ({}) must be <= quic_initial_max_stream_data ({})",
            config.performance.max_request_body_bytes,
            config.performance.quic_initial_max_stream_data
        );
        return false;
    }

    if config.performance.request_buffer_global_cap_bytes
        < config.performance.max_request_body_bytes
    {
        error!(
            "performance.request_buffer_global_cap_bytes ({}) must be >= max_request_body_bytes ({})",
            config.performance.request_buffer_global_cap_bytes,
            config.performance.max_request_body_bytes
        );
        return false;
    }

    if config.performance.unknown_length_response_prebuffer_bytes
        > config.performance.max_response_body_bytes
    {
        error!(
            "performance.unknown_length_response_prebuffer_bytes ({}) must be <= max_response_body_bytes ({})",
            config.performance.unknown_length_response_prebuffer_bytes,
            config.performance.max_response_body_bytes
        );
        return false;
    }

    if config.resilience.adaptive_admission.min_limit == 0 {
        error!("resilience.adaptive_admission.min_limit must be greater than 0");
        return false;
    }
    if let Some(max_limit) = config.resilience.adaptive_admission.max_limit {
        if max_limit == 0 {
            error!("resilience.adaptive_admission.max_limit must be greater than 0");
            return false;
        }
        if max_limit < config.resilience.adaptive_admission.min_limit {
            error!(
                "resilience.adaptive_admission.max_limit ({}) must be >= min_limit ({})",
                max_limit, config.resilience.adaptive_admission.min_limit
            );
            return false;
        }
        if max_limit > config.performance.global_inflight_limit {
            error!(
                "resilience.adaptive_admission.max_limit ({}) must be <= performance.global_inflight_limit ({})",
                max_limit, config.performance.global_inflight_limit
            );
            return false;
        }
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

    if !config.resilience.watchdog.restart_command.is_empty()
        && config.resilience.watchdog.restart_command[0].trim().is_empty()
    {
        error!("resilience.watchdog.restart_command[0] must be a non-empty executable path");
        return false;
    }

    if config
        .resilience
        .watchdog
        .restart_hook
        .as_ref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        error!(
            "resilience.watchdog.restart_hook is deprecated and unsupported; use restart_command instead"
        );
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

        if config.observability.metrics.max_connections == 0 {
            error!("observability.metrics.max_connections must be greater than 0");
            return false;
        }

        if config.observability.metrics.connection_timeout_ms == 0 {
            error!("observability.metrics.connection_timeout_ms must be greater than 0");
            return false;
        }
    }

    if config.observability.control_api.enabled {
        if config.observability.control_api.address.is_empty() {
            error!("observability.control_api.address cannot be empty when control_api is enabled");
            return false;
        }

        if config.observability.control_api.port == 0 {
            error!("observability.control_api.port must be between 1 and 65535");
            return false;
        }

        let paths = [
            (
                "observability.control_api.health_path",
                config.observability.control_api.health_path.as_str(),
            ),
            (
                "observability.control_api.ready_path",
                config.observability.control_api.ready_path.as_str(),
            ),
            (
                "observability.control_api.runtime_path",
                config.observability.control_api.runtime_path.as_str(),
            ),
            (
                "observability.control_api.restart_path",
                config.observability.control_api.restart_path.as_str(),
            ),
        ];
        for (name, path) in paths {
            if !path.starts_with('/') {
                error!("{} must start with '/'", name);
                return false;
            }
        }

        if config.observability.control_api.max_connections == 0 {
            error!("observability.control_api.max_connections must be greater than 0");
            return false;
        }

        if config.observability.control_api.connection_timeout_ms == 0 {
            error!("observability.control_api.connection_timeout_ms must be greater than 0");
            return false;
        }

        if let Some(token) = config.observability.control_api.auth_token.as_ref()
            && token.trim().is_empty()
        {
            error!("observability.control_api.auth_token cannot be empty when provided");
            return false;
        }

        if !is_loopback_bind_address(&config.observability.control_api.address)
            && config.observability.control_api.auth_token.is_none()
        {
            error!(
                "observability.control_api.auth_token is required when control_api.address is non-loopback ({})",
                config.observability.control_api.address
            );
            return false;
        }
    }

    // --- Validate privilege-drop security controls ---
    if config.security.privileges.enabled {
        if config.security.privileges.user.trim().is_empty() {
            error!("security.privileges.user must be non-empty when privilege drop is enabled");
            return false;
        }
        if config.security.privileges.group.trim().is_empty() {
            error!("security.privileges.group must be non-empty when privilege drop is enabled");
            return false;
        }
    }

    if config.observability.tracing.enabled {
        if config.observability.tracing.service_name.trim().is_empty() {
            error!("observability.tracing.service_name cannot be empty when tracing is enabled");
            return false;
        }
        if !(0.0..=1.0).contains(&config.observability.tracing.sample_ratio) {
            error!("observability.tracing.sample_ratio must be between 0.0 and 1.0");
            return false;
        }
        if let Some(endpoint) = config.observability.tracing.otlp_endpoint.as_ref()
            && endpoint.trim().is_empty()
        {
            error!("observability.tracing.otlp_endpoint cannot be empty when provided");
            return false;
        }
    }

    // --- Validate TLS certs ---
    if !Path::new(&config.listen.tls.cert).exists() {
        error!(
            "TLS certificate file does not exist: {}",
            config.listen.tls.cert
        );
        return false;
    }

    if !Path::new(&config.listen.tls.key).exists() {
        error!(
            "TLS private key file does not exist: {}",
            config.listen.tls.key
        );
        return false;
    }

    if !validate_pem_certificates(&config.listen.tls.cert, "listen.tls.cert") {
        return false;
    }

    if !validate_pem_private_key(&config.listen.tls.key, "listen.tls.key") {
        return false;
    }

    // --- Validate optional downstream client-auth (mTLS) ---
    if config.listen.tls.client_auth.require_client_cert && !config.listen.tls.client_auth.enabled {
        error!("listen.tls.client_auth.require_client_cert requires client_auth.enabled=true");
        return false;
    }

    if config.listen.tls.client_auth.enabled {
        let Some(ca_file) = config.listen.tls.client_auth.ca_file.as_ref() else {
            error!("listen.tls.client_auth.ca_file is required when client_auth.enabled=true");
            return false;
        };
        if ca_file.trim().is_empty() {
            error!("listen.tls.client_auth.ca_file cannot be empty");
            return false;
        }
        if !Path::new(ca_file).exists() {
            error!("listen.tls.client_auth.ca_file does not exist: {}", ca_file);
            return false;
        }
        if !validate_pem_certificates(ca_file, "listen.tls.client_auth.ca_file") {
            return false;
        }
    }

    // --- Validate upstream TLS trust-store configuration ---
    if !config.upstream_tls.verify_certificates {
        warn!(
            "upstream_tls.verify_certificates=false disables TLS certificate validation for HTTPS upstreams"
        );
    }

    if let Some(ca_file) = config.upstream_tls.ca_file.as_ref() {
        if ca_file.trim().is_empty() {
            error!("upstream_tls.ca_file cannot be empty when provided");
            return false;
        }
        if !Path::new(ca_file).exists() {
            error!("upstream_tls.ca_file does not exist: {}", ca_file);
            return false;
        }
        if !validate_pem_certificates(ca_file, "upstream_tls.ca_file") {
            return false;
        }
    }

    if let Some(ca_dir) = config.upstream_tls.ca_dir.as_ref() {
        if ca_dir.trim().is_empty() {
            error!("upstream_tls.ca_dir cannot be empty when provided");
            return false;
        }
        let ca_path = Path::new(ca_dir);
        if !ca_path.exists() {
            error!("upstream_tls.ca_dir does not exist: {}", ca_dir);
            return false;
        }
        if !ca_path.is_dir() {
            error!("upstream_tls.ca_dir must be a directory: {}", ca_dir);
            return false;
        }
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

    let mut seen_backend_origins: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();

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

            let origin = endpoint.origin();
            if let Some((existing_upstream, existing_backend)) = seen_backend_origins
                .insert(origin.clone(), (upstream_name.clone(), backend.id.clone()))
            {
                error!(
                    "Duplicate backend address '{}' detected: upstream '{}' backend '{}' conflicts with upstream '{}' backend '{}'",
                    origin, upstream_name, backend.id, existing_upstream, existing_backend
                );
                return false;
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
        Backend, ClientAuth, Config, ControlApi, HealthCheck, Listen, LoadBalancing, Log,
        LogFormat, MetricsEndpoint, Observability, Performance, Resilience, RouteMatch, Tls,
        Security, Tracing, Upstream, UpstreamTls,
    };
    use rcgen::{Certificate, CertificateParams, SanType};
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn write_test_certs(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let mut params = CertificateParams::new(vec!["localhost".into()]);
        params
            .subject_alt_names
            .push(SanType::IpAddress("127.0.0.1".parse().expect("ip")));
        let cert = Certificate::from_params(params).expect("failed to build cert");

        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");

        std::fs::write(&cert_path, cert.serialize_pem().expect("serialize cert"))
            .expect("write cert");
        std::fs::write(&key_path, cert.serialize_private_key_pem()).expect("write key");

        (cert_path, key_path)
    }

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
                    client_auth: ClientAuth::default(),
                },
            },
            upstream,
            load_balancing: Some(LoadBalancing {
                lb_type: "random".to_string(),
                key: None,
            }),
            upstream_tls: UpstreamTls::default(),
            log: Log {
                level: "info".to_string(),
                file: Default::default(),
                format: LogFormat::Plain,
            },
            performance: Performance::default(),
            observability: Observability::default(),
            resilience: Resilience::default(),
            security: Security::default(),
        }
    }

    #[test]
    fn yaml_parse_applies_performance_and_observability_defaults() {
        let dir = tempdir().expect("tempdir");
        let (cert, key) = write_test_certs(dir.path());

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

        let cfg: Config = serde_yml::from_str(&yaml).expect("parse");
        assert_eq!(cfg.performance.worker_threads, 1);
        assert_eq!(cfg.performance.control_plane_threads, 2);
        assert_eq!(cfg.performance.packet_shards_per_worker, 1);
        assert_eq!(cfg.performance.packet_shard_queue_capacity, 2048);
        assert_eq!(
            cfg.performance.packet_shard_queue_max_bytes,
            64 * 1024 * 1024
        );
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
        assert_eq!(cfg.performance.max_active_connections, 20_000);
        assert_eq!(cfg.performance.max_request_body_bytes, 1_000_000);
        assert_eq!(
            cfg.performance.request_buffer_global_cap_bytes,
            64 * 1024 * 1024
        );
        assert_eq!(
            cfg.performance.unknown_length_response_prebuffer_bytes,
            2 * 1024 * 1024
        );
        assert_eq!(cfg.performance.client_body_idle_timeout_ms, 10_000);
        assert!(!cfg.observability.metrics.enabled);
        assert_eq!(cfg.observability.metrics.path, "/metrics");
        assert_eq!(cfg.observability.metrics.max_connections, 512);
        assert_eq!(cfg.observability.metrics.connection_timeout_ms, 30_000);
        assert!(cfg.upstream_tls.verify_certificates);
        assert!(cfg.upstream_tls.strict_sni);
        assert!(!cfg.listen.tls.client_auth.enabled);
        assert!(!cfg.listen.tls.client_auth.require_client_cert);
        assert!(cfg.listen.tls.client_auth.ca_file.is_none());
        assert!(cfg.resilience.adaptive_admission.enabled);
        assert!(cfg.resilience.adaptive_admission.max_limit.is_none());
        assert_eq!(cfg.resilience.route_queue.default_cap, 512);
        assert_eq!(cfg.resilience.route_queue.global_cap, 2048);
        assert_eq!(cfg.resilience.route_queue.shed_retry_after_seconds, 1);
        assert!(!cfg.resilience.protocol.allow_0rtt);
        assert_eq!(cfg.resilience.protocol.max_headers_count, 128);
        assert_eq!(cfg.resilience.protocol.max_headers_bytes, 16 * 1024);
        assert!(cfg.resilience.protocol.enforce_authority_host_match);
        assert!(!cfg.resilience.watchdog.enabled);
        assert_eq!(cfg.resilience.watchdog.check_interval_ms, 1_000);
        assert_eq!(cfg.observability.control_api.max_connections, 256);
        assert_eq!(cfg.observability.control_api.connection_timeout_ms, 30_000);
    }

    #[test]
    fn rejects_invalid_performance_and_observability_values() {
        let dir = tempdir().expect("tempdir");
        let (cert, key) = write_test_certs(dir.path());

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
        cfg.performance.packet_shard_queue_max_bytes = 0;
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
        cfg.performance.max_active_connections = 0;
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
        cfg.performance.max_request_body_bytes = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.request_buffer_global_cap_bytes = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.unknown_length_response_prebuffer_bytes = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.client_body_idle_timeout_ms = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.max_request_body_bytes =
            (cfg.performance.quic_initial_max_stream_data as usize).saturating_add(1);
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.request_buffer_global_cap_bytes =
            cfg.performance.max_request_body_bytes.saturating_sub(1);
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.unknown_length_response_prebuffer_bytes =
            cfg.performance.max_response_body_bytes.saturating_add(1);
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.adaptive_admission.max_limit = Some(0);
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.adaptive_admission.max_limit = Some(
            cfg.resilience
                .adaptive_admission
                .min_limit
                .saturating_sub(1),
        );
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.adaptive_admission.max_limit =
            Some(cfg.performance.global_inflight_limit.saturating_add(1));
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
        cfg.listen.tls.client_auth.require_client_cert = true;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.listen.tls.client_auth.enabled = true;
        cfg.listen.tls.client_auth.ca_file = None;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.upstream_tls.ca_file = Some("/path/does/not/exist.pem".to_string());
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.upstream_tls.ca_dir = Some("/path/does/not/exist".to_string());
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.observability = Observability {
            metrics: MetricsEndpoint {
                enabled: true,
                address: "127.0.0.1".to_string(),
                port: 9901,
                path: "metrics".to_string(),
                max_connections: 128,
                connection_timeout_ms: 10_000,
            },
            control_api: ControlApi::default(),
            tracing: Tracing::default(),
        };
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.observability.metrics.enabled = true;
        cfg.observability.metrics.max_connections = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.observability.metrics.enabled = true;
        cfg.observability.metrics.connection_timeout_ms = 0;
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.observability.control_api.enabled = true;
        cfg.observability.control_api.max_connections = 0;
        cfg.observability.control_api.auth_token = Some("token".to_string());
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.observability.control_api.enabled = true;
        cfg.observability.control_api.connection_timeout_ms = 0;
        cfg.observability.control_api.auth_token = Some("token".to_string());
        assert!(!validate(&cfg));
    }

    #[test]
    fn rejects_unparseable_tls_material() {
        let dir = tempdir().expect("tempdir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "not-a-pem-cert").expect("write cert");
        std::fs::write(&key, "not-a-pem-key").expect("write key");

        let cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        assert!(!validate(&cfg));
    }

    #[test]
    fn accepts_valid_metrics_and_performance_configuration() {
        let dir = tempdir().expect("tempdir");
        let (cert, key) = write_test_certs(dir.path());

        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.performance.worker_threads = 4;
        cfg.performance.control_plane_threads = 2;
        cfg.performance.packet_shards_per_worker = 2;
        cfg.performance.packet_shard_queue_capacity = 1024;
        cfg.performance.packet_shard_queue_max_bytes = 16 * 1024 * 1024;
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
        cfg.performance.max_active_connections = 50_000;
        cfg.performance.max_request_body_bytes = 512 * 1024;
        cfg.performance.request_buffer_global_cap_bytes = 8 * 1024 * 1024;
        cfg.performance.unknown_length_response_prebuffer_bytes = 512 * 1024;
        cfg.performance.client_body_idle_timeout_ms = 7_500;
        cfg.resilience.adaptive_admission.max_limit = Some(1024);
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
        cfg.upstream_tls.verify_certificates = true;
        cfg.upstream_tls.strict_sni = true;
        cfg.upstream_tls.ca_file = Some(cert.to_string_lossy().to_string());
        cfg.upstream_tls.ca_dir = Some(dir.path().to_string_lossy().to_string());
        cfg.listen.tls.client_auth.enabled = true;
        cfg.listen.tls.client_auth.require_client_cert = true;
        cfg.listen.tls.client_auth.ca_file = Some(cert.to_string_lossy().to_string());
        cfg.observability = Observability {
            metrics: MetricsEndpoint {
                enabled: true,
                address: "127.0.0.1".to_string(),
                port: 9901,
                path: "/metrics".to_string(),
                max_connections: 128,
                connection_timeout_ms: 10_000,
            },
            control_api: ControlApi::default(),
            tracing: Tracing::default(),
        };

        assert!(validate(&cfg));
    }

    #[test]
    fn backend_address_validation_supports_secure_default_and_explicit_http() {
        let dir = tempdir().expect("tempdir");
        let (cert, key) = write_test_certs(dir.path());

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
        let (cert, key) = write_test_certs(dir.path());

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

    #[test]
    fn rejects_duplicate_backend_addresses_across_upstreams() {
        let dir = tempdir().expect("tempdir");
        let (cert, key) = write_test_certs(dir.path());

        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        let mut duplicate = cfg.upstream.get("test_upstream").expect("upstream").clone();
        duplicate.backends[0].id = "backend-2".to_string();
        duplicate.route.path_prefix = Some("/v2".to_string());
        cfg.upstream
            .insert("test_upstream_2".to_string(), duplicate);

        assert!(!validate(&cfg));
    }

    #[test]
    fn rejects_non_loopback_control_api_without_auth_token() {
        let dir = tempdir().expect("tempdir");
        let (cert, key) = write_test_certs(dir.path());
        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.observability.control_api.enabled = true;
        cfg.observability.control_api.address = "0.0.0.0".to_string();
        cfg.observability.control_api.auth_token = None;
        assert!(!validate(&cfg));
    }

    #[test]
    fn rejects_legacy_watchdog_restart_hook() {
        let dir = tempdir().expect("tempdir");
        let (cert, key) = write_test_certs(dir.path());
        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.resilience.watchdog.restart_hook = Some("echo legacy".to_string());
        assert!(!validate(&cfg));
    }

    #[test]
    fn rejects_empty_privilege_drop_user_or_group() {
        let dir = tempdir().expect("tempdir");
        let (cert, key) = write_test_certs(dir.path());
        let mut cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.security.privileges.enabled = true;
        cfg.security.privileges.user = " ".to_string();
        assert!(!validate(&cfg));

        cfg = base_config(&cert.to_string_lossy(), &key.to_string_lossy());
        cfg.security.privileges.enabled = true;
        cfg.security.privileges.group = " ".to_string();
        assert!(!validate(&cfg));
    }
}
