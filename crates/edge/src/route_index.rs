use std::collections::HashMap;

use spooky_config::config::Upstream;

pub(crate) fn normalize_host_for_routing(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let host = if let Some(rest) = trimmed.strip_prefix('[') {
        let end = rest.find(']')?;
        &rest[..end]
    } else if let Some((candidate_host, candidate_port)) = trimmed.rsplit_once(':') {
        if !candidate_host.contains(':') && candidate_port.chars().all(|c| c.is_ascii_digit()) {
            candidate_host
        } else {
            trimmed
        }
    } else {
        trimmed
    };

    let normalized = host.trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

/// Route precedence (deterministic):
/// 1) Longest matching path_prefix wins.
/// 2) On equal path length, host-specific routes win over host-agnostic routes.
/// 3) On remaining ties, lexicographically smaller upstream name wins.
///
/// `order` stores the lexicographic rank of upstream name (smaller rank = smaller name),
/// so trie updates are independent of HashMap insertion order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexedRoute {
    upstream_idx: usize,
    path_len: usize,
    host_specific: bool,
    order: usize,
}

#[derive(Default)]
pub struct TrieNode {
    pub route: Option<IndexedRoute>,
    pub children: HashMap<u8, TrieNode>,
}

impl TrieNode {
    fn update_route(&mut self, candidate: IndexedRoute) {
        self.route = prefer_route(self.route, Some(candidate));
    }
}

#[derive(Default)]
struct RouteTrie {
    root: TrieNode,
}

impl RouteTrie {
    fn insert(&mut self, prefix: Option<&str>, route: IndexedRoute) {
        let prefix = prefix.unwrap_or("");
        let mut node = &mut self.root;

        if prefix.is_empty() {
            node.update_route(route);
            return;
        }

        for byte in prefix.as_bytes() {
            node = node.children.entry(*byte).or_default();
        }

        node.update_route(route);
    }

    fn longest_prefix(&self, path: &str) -> Option<IndexedRoute> {
        let mut node = &self.root;
        let mut best = node
            .route
            .filter(|route| prefix_boundary_matches(path, route.path_len));

        for byte in path.as_bytes() {
            let Some(next) = node.children.get(byte) else {
                break;
            };
            node = next;
            let candidate = node
                .route
                .filter(|route| prefix_boundary_matches(path, route.path_len));
            best = prefer_route(best, candidate);
        }

        best
    }
}

fn prefix_boundary_matches(path: &str, prefix_len: usize) -> bool {
    if prefix_len <= 1 {
        return true;
    }
    if path.len() == prefix_len {
        return true;
    }
    path.as_bytes().get(prefix_len) == Some(&b'/')
}

pub(crate) struct RouteIndex {
    host_tries: HashMap<String, RouteTrie>,
    default_trie: RouteTrie,
    default_max_path_len: usize,
    upstream_names: Vec<String>,
}

impl RouteIndex {
    pub(crate) fn from_upstreams(upstreams: &HashMap<String, Upstream>) -> Self {
        let mut host_tries = HashMap::new();
        let mut default_trie = RouteTrie::default();
        let mut default_max_path_len = 0usize;
        let mut upstream_names = Vec::with_capacity(upstreams.len());
        // Build a stable route list first. This keeps tie-breaking deterministic even if
        // upstreams came from a map with non-deterministic iteration order.
        let mut ordered: Vec<(&String, &Upstream)> = upstreams.iter().collect();
        ordered.sort_by_key(|(left, _)| *left);

        for (order, (name, upstream)) in ordered.into_iter().enumerate() {
            let upstream_idx = upstream_names.len();
            upstream_names.push(name.clone());
            let path_len = upstream
                .route
                .path_prefix
                .as_ref()
                .map(|prefix| prefix.len())
                .unwrap_or(0);

            let route = IndexedRoute {
                upstream_idx,
                path_len,
                host_specific: upstream.route.host.is_some(),
                order,
            };

            match upstream.route.host.as_deref() {
                Some(host) => {
                    let normalized_host = normalize_host_for_routing(host)
                        .unwrap_or_else(|| host.to_ascii_lowercase());
                    host_tries
                        .entry(normalized_host)
                        .or_insert_with(RouteTrie::default)
                        .insert(upstream.route.path_prefix.as_deref(), route)
                }
                None => {
                    default_max_path_len = default_max_path_len.max(path_len);
                    default_trie.insert(upstream.route.path_prefix.as_deref(), route);
                }
            }
        }

        Self {
            host_tries,
            default_trie,
            default_max_path_len,
            upstream_names,
        }
    }

    pub(crate) fn lookup<'a>(&'a self, path: &str, host: Option<&str>) -> Option<&'a str> {
        let host_best = host
            .and_then(normalize_host_for_routing)
            .and_then(|host_name| self.host_tries.get(&host_name))
            .and_then(|host_trie| host_trie.longest_prefix(path));

        if let Some(best) = host_best
            && best.path_len >= self.default_max_path_len
        {
            return Some(self.upstream_names[best.upstream_idx].as_str());
        }

        let best = prefer_route(self.default_trie.longest_prefix(path), host_best);
        best.map(|route| self.upstream_names[route.upstream_idx].as_str())
    }
}

fn prefer_route(
    current: Option<IndexedRoute>,
    candidate: Option<IndexedRoute>,
) -> Option<IndexedRoute> {
    match (current, candidate) {
        (None, None) => None,
        (Some(route), None) | (None, Some(route)) => Some(route),
        (Some(current), Some(candidate)) => {
            if candidate.path_len > current.path_len
                || (candidate.path_len == current.path_len
                    && candidate.host_specific
                    && !current.host_specific)
                || (candidate.path_len == current.path_len
                    && candidate.host_specific == current.host_specific
                    && candidate.order < current.order)
            {
                Some(candidate)
            } else {
                Some(current)
            }
        }
    }
}

pub(crate) fn scan_lookup<'a>(
    upstreams: &'a HashMap<String, Upstream>,
    path: &str,
    host: Option<&str>,
) -> Option<&'a str> {
    let path_bytes = path.as_bytes();
    let normalized_request_host = host.and_then(normalize_host_for_routing);
    let mut best_match: Option<(&str, usize, bool)> = None;

    for (upstream_name, upstream) in upstreams {
        let has_host_match = match (&upstream.route.host, normalized_request_host.as_deref()) {
            (Some(route_host), Some(request_host)) => {
                normalize_host_for_routing(route_host).as_deref() == Some(request_host)
            }
            (None, _) => true,
            (Some(_), None) => false,
        };

        let path_match_len = match &upstream.route.path_prefix {
            Some(path_prefix) => {
                let prefix = path_prefix.as_bytes();
                if prefix.len() > path_bytes.len() {
                    continue;
                }
                // Fast reject for same-length-ish prefixes before full starts_with.
                if let Some((&last, idx)) = prefix.last().zip(prefix.len().checked_sub(1))
                    && path_bytes[idx] != last
                {
                    continue;
                }
                if !path_bytes.starts_with(prefix) {
                    continue;
                }
                if !prefix_boundary_matches(path, prefix.len()) {
                    continue;
                }
                prefix.len()
            }
            None => 0,
        };

        if !has_host_match {
            continue;
        }
        let host_specific = upstream.route.host.is_some();

        match best_match {
            Some((best_name, best_len, best_host_specific)) => {
                if path_match_len > best_len
                    || (path_match_len == best_len && host_specific && !best_host_specific)
                    || (path_match_len == best_len
                        && host_specific == best_host_specific
                        && upstream_name.as_str() < best_name)
                {
                    best_match = Some((upstream_name.as_str(), path_match_len, host_specific));
                }
            }
            None => {
                best_match = Some((upstream_name.as_str(), path_match_len, host_specific));
            }
        }
    }

    best_match.map(|(name, _, _)| name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use spooky_config::config::{LoadBalancing, RouteMatch};
    use std::time::Instant;

    fn test_upstream(host: Option<&str>, path_prefix: Option<&str>) -> Upstream {
        Upstream {
            load_balancing: LoadBalancing {
                lb_type: "random".to_string(),
                key: None,
            },
            route: RouteMatch {
                host: host.map(str::to_string),
                path_prefix: path_prefix.map(str::to_string),
                method: None,
            },
            backends: vec![],
        }
    }

    #[test]
    fn longest_prefix_lookup_works() {
        let mut upstreams = HashMap::new();
        upstreams.insert("root".to_string(), test_upstream(None, Some("/")));
        upstreams.insert("api".to_string(), test_upstream(None, Some("/api")));
        upstreams.insert("api-v1".to_string(), test_upstream(None, Some("/api/v1")));

        let index = RouteIndex::from_upstreams(&upstreams);
        let selected = index.lookup("/api/v1/users", None);
        assert_eq!(selected, Some("api-v1"));
    }

    #[test]
    fn indexed_lookup_matches_scan_lookup() {
        let mut upstreams = HashMap::new();
        upstreams.insert("default-root".to_string(), test_upstream(None, Some("/")));
        upstreams.insert("default-api".to_string(), test_upstream(None, Some("/api")));
        upstreams.insert(
            "api-host-only".to_string(),
            test_upstream(Some("api.example.com"), None),
        );
        upstreams.insert(
            "api-host-route".to_string(),
            test_upstream(Some("api.example.com"), Some("/api")),
        );
        upstreams.insert(
            "admin-host-route".to_string(),
            test_upstream(Some("admin.example.com"), Some("/admin")),
        );

        let index = RouteIndex::from_upstreams(&upstreams);
        let queries = vec![
            ("/", None),
            ("/api/users", None),
            ("/api/users", Some("api.example.com")),
            ("/admin/users", Some("admin.example.com")),
            ("/unknown", Some("api.example.com")),
            ("/unknown", Some("missing.example.com")),
        ];

        for (path, host) in queries {
            assert_eq!(
                index.lookup(path, host),
                scan_lookup(&upstreams, path, host)
            );
        }
    }

    #[test]
    fn host_specific_route_wins_on_tie() {
        let mut upstreams = HashMap::new();
        upstreams.insert("a-default".to_string(), test_upstream(None, Some("/api")));
        upstreams.insert(
            "z-host".to_string(),
            test_upstream(Some("api.example.com"), Some("/api")),
        );

        let index = RouteIndex::from_upstreams(&upstreams);
        assert_eq!(
            index.lookup("/api/users", Some("api.example.com")),
            Some("z-host")
        );
        assert_eq!(
            scan_lookup(&upstreams, "/api/users", Some("api.example.com")),
            Some("z-host")
        );
    }

    #[test]
    fn lookup_normalizes_request_host_case_and_port() {
        let mut upstreams = HashMap::new();
        upstreams.insert(
            "api".to_string(),
            test_upstream(Some("api.example.com"), Some("/api")),
        );
        upstreams.insert("default".to_string(), test_upstream(None, Some("/")));
        let index = RouteIndex::from_upstreams(&upstreams);

        assert_eq!(
            index.lookup("/api/v1", Some("API.EXAMPLE.COM:443")),
            Some("api")
        );
        assert_eq!(
            scan_lookup(&upstreams, "/api/v1", Some("API.EXAMPLE.COM:443")),
            Some("api")
        );
    }

    #[test]
    fn lookup_normalizes_configured_host_case() {
        let mut upstreams = HashMap::new();
        upstreams.insert(
            "api".to_string(),
            test_upstream(Some("API.Example.COM"), Some("/api")),
        );
        upstreams.insert("default".to_string(), test_upstream(None, Some("/")));
        let index = RouteIndex::from_upstreams(&upstreams);
        assert_eq!(
            index.lookup("/api/v1", Some("api.example.com")),
            Some("api")
        );
    }

    #[test]
    fn path_prefix_requires_segment_boundary() {
        let mut upstreams = HashMap::new();
        upstreams.insert("api".to_string(), test_upstream(None, Some("/api")));
        upstreams.insert("root".to_string(), test_upstream(None, Some("/")));
        let index = RouteIndex::from_upstreams(&upstreams);
        assert_eq!(index.lookup("/api", None), Some("api"));
        assert_eq!(index.lookup("/api/v1", None), Some("api"));
        assert_eq!(index.lookup("/api2", None), Some("root"));
        assert_eq!(scan_lookup(&upstreams, "/api2", None), Some("root"));
    }

    #[test]
    fn lexical_tie_break_is_deterministic_for_default_routes() {
        let mut upstreams = HashMap::new();
        // Insert in reverse lexical order to prove insertion order does not matter.
        upstreams.insert("zeta".to_string(), test_upstream(None, Some("/api")));
        upstreams.insert("alpha".to_string(), test_upstream(None, Some("/api")));

        let index = RouteIndex::from_upstreams(&upstreams);
        assert_eq!(index.lookup("/api/users", None), Some("alpha"));
        assert_eq!(scan_lookup(&upstreams, "/api/users", None), Some("alpha"));
    }

    #[test]
    fn lexical_tie_break_is_deterministic_for_host_routes() {
        let mut upstreams = HashMap::new();
        // Insert in reverse lexical order to prove insertion order does not matter.
        upstreams.insert(
            "zeta-host".to_string(),
            test_upstream(Some("api.example.com"), Some("/api")),
        );
        upstreams.insert(
            "alpha-host".to_string(),
            test_upstream(Some("api.example.com"), Some("/api")),
        );

        let index = RouteIndex::from_upstreams(&upstreams);
        assert_eq!(
            index.lookup("/api/users", Some("api.example.com")),
            Some("alpha-host")
        );
        assert_eq!(
            scan_lookup(&upstreams, "/api/users", Some("api.example.com")),
            Some("alpha-host")
        );
    }

    #[test]
    fn indexed_lookup_is_insertion_order_invariant() {
        let mut upstreams_a = HashMap::new();
        upstreams_a.insert("zeta".to_string(), test_upstream(None, Some("/")));
        upstreams_a.insert(
            "beta-host".to_string(),
            test_upstream(Some("api.example.com"), Some("/api")),
        );
        upstreams_a.insert("alpha".to_string(), test_upstream(None, Some("/api")));

        let mut upstreams_b = HashMap::new();
        upstreams_b.insert("alpha".to_string(), test_upstream(None, Some("/api")));
        upstreams_b.insert("zeta".to_string(), test_upstream(None, Some("/")));
        upstreams_b.insert(
            "beta-host".to_string(),
            test_upstream(Some("api.example.com"), Some("/api")),
        );

        let index_a = RouteIndex::from_upstreams(&upstreams_a);
        let index_b = RouteIndex::from_upstreams(&upstreams_b);
        let queries = vec![
            ("/api/users", None),
            ("/api/users", Some("api.example.com")),
            ("/", None),
            ("/missing", Some("api.example.com")),
        ];

        for (path, host) in queries {
            assert_eq!(index_a.lookup(path, host), index_b.lookup(path, host));
        }
    }

    fn build_route_table(route_count: usize) -> HashMap<String, Upstream> {
        let mut upstreams = HashMap::with_capacity(route_count);
        for i in 0..route_count {
            let name = format!("upstream-{i:05}");
            let path = format!("/svc/{i:05}");
            let host = (i % 2 == 1).then_some("bench.example.com");
            upstreams.insert(name, test_upstream(host, Some(&path)));
        }
        upstreams
    }

    fn measure_lookup<F>(iterations: usize, mut lookup: F) -> std::time::Duration
    where
        F: FnMut() -> Option<String>,
    {
        let start = Instant::now();
        let mut sink = 0usize;
        for _ in 0..iterations {
            if let Some(value) = lookup() {
                sink ^= value.len();
            }
        }
        std::hint::black_box(sink);
        start.elapsed()
    }

    #[test]
    #[ignore = "microbenchmark"]
    fn route_lookup_microbenchmarks() {
        for route_count in [100usize, 1_000, 10_000] {
            let upstreams = build_route_table(route_count);
            let index = RouteIndex::from_upstreams(&upstreams);
            let query_path = format!("/svc/{:05}/resource", route_count - 1);
            let host = Some("bench.example.com");
            let iterations = match route_count {
                100 => 200_000,
                1_000 => 100_000,
                _ => 20_000,
            };

            assert_eq!(
                index.lookup(&query_path, host),
                scan_lookup(&upstreams, &query_path, host)
            );

            let scan_time = measure_lookup(iterations, || {
                scan_lookup(&upstreams, &query_path, host).map(str::to_string)
            });
            let indexed_time = measure_lookup(iterations, || {
                index.lookup(&query_path, host).map(str::to_string)
            });
            let speedup = scan_time.as_secs_f64() / indexed_time.as_secs_f64();

            eprintln!(
                "routes={route_count:>5} scan={scan_time:?} indexed={indexed_time:?} speedup={speedup:.2}x"
            );

            if route_count >= 1_000 {
                assert!(
                    indexed_time < scan_time,
                    "expected indexed lookup to be faster for {route_count} routes"
                );
            }
        }
    }
}
