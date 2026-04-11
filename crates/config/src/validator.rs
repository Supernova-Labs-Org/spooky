use crate::config::Config;
use log::{error, info};

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

    if config.performance.backend_body_idle_timeout_ms == 0 {
        error!("performance.backend_body_idle_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.backend_body_total_timeout_ms == 0 {
        error!("performance.backend_body_total_timeout_ms must be greater than 0");
        return false;
    }

    if config.performance.backend_body_total_timeout_ms
        < config.performance.backend_body_idle_timeout_ms
    {
        error!(
            "performance.backend_body_total_timeout_ms must be >= backend_body_idle_timeout_ms"
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

            // Basic address format validation (host:port)
            if !backend.address.contains(':') {
                error!(
                    "Backend address '{}' in upstream '{}' must be in host:port format",
                    backend.address, upstream_name
                );
                return false;
            }

            // Validate weight
            if backend.weight == 0 {
                error!(
                    "Backend '{}' in upstream '{}' has invalid weight (0)",
                    backend.id, upstream_name
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
