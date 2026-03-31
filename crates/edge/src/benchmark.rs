use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use spooky_config::config::{Backend, HealthCheck, LoadBalancing, RouteMatch, Upstream};

use crate::constants::{
    BENCH_CONN_ALIAS_SUFFIX, BENCH_CONN_MISS_ID_FILL, BENCH_CONN_MISS_ID_LEN_BYTES,
    BENCH_CONN_MISS_PORT, BENCH_CONN_PEER_BASE_PORT, BENCH_CONN_PEER_PORT_SPAN,
    BENCH_CONN_PRIMARY_ID_LEN_BYTES, BENCH_CONN_PRIMARY_ID_PREFIX_BYTES,
};
use crate::route_index::{RouteIndex, scan_lookup};

fn default_health_check() -> HealthCheck {
    HealthCheck {
        path: "/health".to_string(),
        interval: 1_000,
        timeout_ms: 1_000,
        failure_threshold: 3,
        success_threshold: 2,
        cooldown_ms: 5_000,
    }
}

fn build_benchmark_upstream(host: Option<String>, path_prefix: String) -> Upstream {
    Upstream {
        load_balancing: LoadBalancing {
            lb_type: "round-robin".to_string(),
            key: None,
        },
        route: RouteMatch {
            host,
            path_prefix: Some(path_prefix),
            method: None,
        },
        // Routing benchmark does not touch backend connectivity.
        backends: vec![Backend {
            id: "placeholder".to_string(),
            address: "127.0.0.1:1".to_string(),
            weight: 1,
            health_check: default_health_check(),
        }],
    }
}

pub struct RouteLookupBench {
    upstreams: HashMap<String, Upstream>,
    index: RouteIndex,
    hit_path: String,
    hit_host: Option<String>,
    miss_path: String,
    miss_host: Option<String>,
}

impl RouteLookupBench {
    pub fn new(scale: usize) -> Self {
        let mut upstreams = HashMap::with_capacity(scale.max(1));
        for i in 0..scale.max(1) {
            let name = format!("upstream-{i:05}");
            let path_prefix = format!("/svc/{i:05}");
            let host = (i % 2 == 1).then_some("bench.example.com".to_string());
            upstreams.insert(name, build_benchmark_upstream(host, path_prefix));
        }

        let index = RouteIndex::from_upstreams(&upstreams);
        let target = scale.max(1) - 1;
        let hit_path = format!("/svc/{target:05}/resource");
        let hit_host = (target % 2 == 1).then_some("bench.example.com".to_string());
        let miss_path = "/not-found/path".to_string();
        let miss_host = Some("missing.example.com".to_string());

        Self {
            upstreams,
            index,
            hit_path,
            hit_host,
            miss_path,
            miss_host,
        }
    }

    pub fn indexed_hit(&self) -> usize {
        self.index
            .lookup(&self.hit_path, self.hit_host.as_deref())
            .map_or(0, str::len)
    }

    pub fn linear_hit(&self) -> usize {
        scan_lookup(&self.upstreams, &self.hit_path, self.hit_host.as_deref()).map_or(0, str::len)
    }

    pub fn indexed_miss(&self) -> usize {
        self.index
            .lookup(&self.miss_path, self.miss_host.as_deref())
            .map_or(0, str::len)
    }
}

pub struct ConnectionLookupBench {
    exact_routes: HashMap<Vec<u8>, SocketAddr>,
    alias_routes: HashMap<Vec<u8>, Vec<u8>>,
    peer_routes: HashMap<SocketAddr, Vec<u8>>,
    hit_exact: Vec<u8>,
    hit_alias: Vec<u8>,
    prefix_miss: Vec<u8>,
    miss_peer: SocketAddr,
}

impl ConnectionLookupBench {
    pub fn new(scale: usize) -> Self {
        let size = scale.max(1);
        let mut exact_routes = HashMap::with_capacity(size);
        let mut alias_routes = HashMap::with_capacity(size);
        let mut peer_routes = HashMap::with_capacity(size);

        for i in 0..size {
            let mut primary = vec![0_u8; BENCH_CONN_PRIMARY_ID_LEN_BYTES];
            primary[..BENCH_CONN_PRIMARY_ID_PREFIX_BYTES]
                .copy_from_slice(&(i as u64).to_be_bytes());
            let peer = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(
                    172,
                    16,
                    ((i >> 8) & 0xff) as u8,
                    (i & 0xff) as u8,
                )),
                BENCH_CONN_PEER_BASE_PORT + (i % BENCH_CONN_PEER_PORT_SPAN) as u16,
            );

            exact_routes.insert(primary.clone(), peer);
            peer_routes.insert(peer, primary.clone());

            let mut alias = primary.clone();
            alias.extend_from_slice(&BENCH_CONN_ALIAS_SUFFIX);
            alias_routes.insert(alias, primary);
        }

        let hit_exact: Vec<u8> = (0_u64)
            .to_be_bytes()
            .iter()
            .copied()
            .chain([0_u8; BENCH_CONN_PRIMARY_ID_PREFIX_BYTES])
            .collect();
        let mut hit_alias = hit_exact.clone();
        hit_alias.extend_from_slice(&BENCH_CONN_ALIAS_SUFFIX);
        let prefix_miss = vec![BENCH_CONN_MISS_ID_FILL; BENCH_CONN_MISS_ID_LEN_BYTES];
        let miss_peer = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 255, 255, 255)),
            BENCH_CONN_MISS_PORT,
        );

        Self {
            exact_routes,
            alias_routes,
            peer_routes,
            hit_exact,
            hit_alias,
            prefix_miss,
            miss_peer,
        }
    }

    pub fn exact_lookup(&self) -> usize {
        self.exact_routes.get(&self.hit_exact).map_or(0, |_| 1)
    }

    pub fn alias_lookup(&self) -> usize {
        self.alias_routes
            .get(&self.hit_alias)
            .and_then(|primary| self.exact_routes.get(primary))
            .map_or(0, |_| 1)
    }

    pub fn prefix_scan_miss_lookup(&self) -> usize {
        self.exact_routes
            .keys()
            .find(|cid| self.prefix_miss.starts_with(cid.as_slice()))
            .map_or(0, |_| 1)
    }

    pub fn peer_scan_miss(&self) -> usize {
        self.exact_routes
            .iter()
            .find_map(|(_cid, peer)| (*peer == self.miss_peer).then_some(1))
            .unwrap_or(0)
    }

    pub fn peer_map_hit(&self) -> usize {
        self.peer_routes
            .get(
                self.exact_routes
                    .get(&self.hit_exact)
                    .expect("seed peer exists"),
            )
            .map_or(0, |_| 1)
    }

    pub fn peer_map_miss(&self) -> usize {
        self.peer_routes.get(&self.miss_peer).map_or(0, |_| 1)
    }
}
