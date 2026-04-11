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
        error!("performance.backend_body_total_timeout_ms must be >= backend_body_idle_timeout_ms");
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

#[cfg(test)]
mod tests {
    use super::validate;
    use crate::config::{
        Backend, Config, HealthCheck, Listen, LoadBalancing, Log, MetricsEndpoint, Observability,
        Performance, RouteMatch, Tls, Upstream,
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
        assert_eq!(cfg.performance.global_inflight_limit, 4096);
        assert_eq!(cfg.performance.per_upstream_inflight_limit, 1024);
        assert_eq!(cfg.performance.backend_timeout_ms, 2000);
        assert_eq!(cfg.performance.backend_body_idle_timeout_ms, 2000);
        assert_eq!(cfg.performance.backend_body_total_timeout_ms, 30000);
        assert!(!cfg.observability.metrics.enabled);
        assert_eq!(cfg.observability.metrics.path, "/metrics");
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
        cfg.performance.backend_body_total_timeout_ms = 100;
        cfg.performance.backend_body_idle_timeout_ms = 200;
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
        cfg.performance.global_inflight_limit = 10_000;
        cfg.performance.per_upstream_inflight_limit = 2_000;
        cfg.performance.backend_timeout_ms = 1500;
        cfg.performance.backend_body_idle_timeout_ms = 500;
        cfg.performance.backend_body_total_timeout_ms = 10_000;
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
}
