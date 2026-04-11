use std::collections::HashMap;

use spooky_config::config::Upstream;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexedRoute {
    upstream_idx: usize,
    path_len: usize,
    order: usize,
}

#[derive(Default)]
pub struct TrieNode {
    pub route: Option<IndexedRoute>,
    pub children: HashMap<u8, TrieNode>,
}

impl TrieNode {
    fn update_route(&mut self, candidate: IndexedRoute) {
        if let Some(current) = self.route {
            if candidate.path_len > current.path_len
                || (candidate.path_len == current.path_len && candidate.order < current.order)
            {
                self.route = Some(candidate);
            }
            return;
        }
        self.route = Some(candidate);
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
        let mut best = node.route;

        for byte in path.as_bytes() {
            let Some(next) = node.children.get(byte) else {
                break;
            };
            node = next;
            best = prefer_route(best, node.route);
        }

        best
    }
}

pub(crate) struct RouteIndex {
    host_tries: HashMap<String, RouteTrie>,
    default_trie: RouteTrie,
    upstream_names: Vec<String>,
}

impl RouteIndex {
    pub(crate) fn from_upstreams(upstreams: &HashMap<String, Upstream>) -> Self {
        let mut host_tries = HashMap::new();
        let mut default_trie = RouteTrie::default();
        let mut upstream_names = Vec::with_capacity(upstreams.len());
        let mut ordered: Vec<(&String, &Upstream)> = upstreams.iter().collect();
        ordered.sort_by(|(left, _), (right, _)| left.cmp(right));

        for (order, (name, upstream)) in ordered.into_iter().enumerate() {
            let upstream_idx = upstream_names.len();
            upstream_names.push(name.clone());

            let route = IndexedRoute {
                upstream_idx,
                path_len: upstream
                    .route
                    .path_prefix
                    .as_ref()
                    .map(|prefix| prefix.len())
                    .unwrap_or(0),
                order,
            };

            match upstream.route.host.as_deref() {
                Some(host) => host_tries
                    .entry(host.to_string())
                    .or_insert_with(RouteTrie::default)
                    .insert(upstream.route.path_prefix.as_deref(), route),
                None => default_trie.insert(upstream.route.path_prefix.as_deref(), route),
            }
        }

        Self {
            host_tries,
            default_trie,
            upstream_names,
        }
    }

    pub(crate) fn lookup<'a>(&'a self, path: &str, host: Option<&str>) -> Option<&'a str> {
        let mut best = self.default_trie.longest_prefix(path);

        if let Some(host) = host
            && let Some(host_trie) = self.host_tries.get(host)
        {
            best = prefer_route(best, host_trie.longest_prefix(path));
        }

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
                || (candidate.path_len == current.path_len && candidate.order < current.order)
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
    let mut best_match: Option<(&str, usize)> = None;

    for (upstream_name, upstream) in upstreams {
        let has_host_match = match (&upstream.route.host, host) {
            (Some(route_host), Some(request_host)) => route_host == request_host,
            (None, _) => true,
            (Some(_), None) => false,
        };

        let path_match_len = match &upstream.route.path_prefix {
            Some(path_prefix) if path.starts_with(path_prefix) => path_prefix.len(),
            None => 0,
            _ => continue,
        };

        if has_host_match
            && (best_match.is_none() || path_match_len > best_match.expect("checked").1)
        {
            best_match = Some((upstream_name.as_str(), path_match_len));
        }
    }

    best_match.map(|(name, _)| name)
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
