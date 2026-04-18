use crate::config::Config;
use serde_yaml::{Mapping, Value};
use std::fs;

pub fn read_config(filename: &str) -> Result<Config, String> {
    let text = fs::read_to_string(filename)
        .map_err(|err| format!("Failed to read config file '{}': {}", filename, err))?;

    parse_config_text(&text)
        .map_err(|err| format!("Could not parse YAML file '{}': {}", filename, err))
}

fn parse_config_text(text: &str) -> Result<Config, serde_yaml::Error> {
    let mut root: Value = serde_yaml::from_str(text)?;
    apply_global_lb_fallback(&mut root);
    serde_yaml::from_value(root)
}

fn apply_global_lb_fallback(root: &mut Value) {
    let Some(root_map) = root.as_mapping_mut() else {
        return;
    };

    let lb_key = Value::String("load_balancing".to_string());
    let upstream_key = Value::String("upstream".to_string());

    let Some(global_lb) = root_map.get(&lb_key).cloned() else {
        return;
    };
    let Some(upstreams) = root_map
        .get_mut(&upstream_key)
        .and_then(Value::as_mapping_mut)
    else {
        return;
    };

    for upstream_value in upstreams.values_mut() {
        let Some(upstream_map) = upstream_value.as_mapping_mut() else {
            continue;
        };
        ensure_mapping_has_lb(upstream_map, &lb_key, global_lb.clone());
    }
}

fn ensure_mapping_has_lb(map: &mut Mapping, lb_key: &Value, global_lb: Value) {
    if !map.contains_key(lb_key) {
        map.insert(lb_key.clone(), global_lb);
    }
}

#[cfg(test)]
mod tests {
    use super::parse_config_text;

    #[test]
    fn applies_global_lb_to_upstream_without_override() {
        let yaml = r#"
version: 1
listen:
  protocol: http3
  address: "127.0.0.1"
  port: 9889
  tls:
    cert: "certs/cert.pem"
    key: "certs/key.pem"
load_balancing:
  type: consistent-hash
upstream:
  inherited:
    route:
      path_prefix: "/"
    backends:
      - id: b1
        address: "http://127.0.0.1:7001"
        weight: 1
        health_check: {}
  explicit:
    load_balancing:
      type: random
    route:
      path_prefix: "/api"
    backends:
      - id: b2
        address: "http://127.0.0.1:7002"
        weight: 1
        health_check: {}
"#;

        let cfg = parse_config_text(yaml).expect("config should parse");
        assert_eq!(
            cfg.upstream
                .get("inherited")
                .map(|u| u.load_balancing.lb_type.as_str()),
            Some("consistent-hash")
        );
        assert_eq!(
            cfg.upstream
                .get("explicit")
                .map(|u| u.load_balancing.lb_type.as_str()),
            Some("random")
        );
    }
}
