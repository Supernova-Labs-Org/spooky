use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::default::{
    get_default_address, get_default_cooldown_ms, get_default_failure_threshold,
    get_default_health_timeout, get_default_interval, get_default_load_balancing, get_default_log,
    get_default_log_file_path, get_default_log_level, get_default_path, get_default_port,
    get_default_protocol, get_default_success_threshold, get_default_version, get_default_weight,
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
