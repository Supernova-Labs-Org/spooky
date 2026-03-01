use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use rand::Rng;
use spooky_config::config::{Backend, HealthCheck};

const DEFAULT_REPLICAS: u32 = 64;

#[derive(Clone)]
pub struct BackendState {
    address: String,
    weight: u32,
    health_check: HealthCheck,
    consecutive_failures: u32,
    health_state: HealthState,
}

#[derive(Clone)]
enum HealthState {
    Healthy,
    Unhealthy { until: Instant, successes: u32 },
}

pub enum HealthTransition {
    BecameHealthy,
    BecameUnhealthy,
}

impl BackendState {
    pub fn new(backend: &Backend) -> Self {
        Self {
            address: backend.address.clone(),
            weight: backend.weight.max(1),
            health_check: backend.health_check.clone(),
            consecutive_failures: 0,
            health_state: HealthState::Healthy,
        }
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.health_state, HealthState::Healthy)
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn health_check(&self) -> &HealthCheck {
        &self.health_check
    }

    pub fn weight(&self) -> u32 {
        self.weight
    }

    pub fn record_success(&mut self) -> Option<HealthTransition> {
        match &mut self.health_state {
            HealthState::Healthy => {
                self.consecutive_failures = 0;
                None
            }
            HealthState::Unhealthy { until, successes } => {
                if Instant::now() < *until {
                    return None;
                }

                *successes += 1;
                if *successes >= self.health_check.success_threshold {
                    self.consecutive_failures = 0;
                    self.health_state = HealthState::Healthy;
                    return Some(HealthTransition::BecameHealthy);
                }
                None
            }
        }
    }

    pub fn record_failure(&mut self) -> Option<HealthTransition> {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let threshold = self.health_check.failure_threshold;
        if self.consecutive_failures < threshold {
            return None;
        }

        self.consecutive_failures = 0;
        let cooldown = Duration::from_millis(self.health_check.cooldown_ms);
        self.health_state = HealthState::Unhealthy {
            until: Instant::now() + cooldown,
            successes: 0,
        };
        Some(HealthTransition::BecameUnhealthy)
    }
}

pub struct BackendPool {
    backends: Vec<BackendState>,
}

pub struct UpstreamPool {
    pub pool: BackendPool,
    pub load_balancer: LoadBalancing,
}

impl UpstreamPool {
    pub fn from_upstream(upstream: &spooky_config::config::Upstream) -> Result<Self, String> {
        let backends = upstream.backends.iter().map(BackendState::new).collect();

        let load_balancer = LoadBalancing::from_config(&upstream.load_balancing.lb_type)?;

        Ok(Self {
            pool: BackendPool::new_from_states(backends),
            load_balancer,
        })
    }

    pub fn pick(&mut self, key: &str) -> Option<usize> {
        self.load_balancer.pick(key, &self.pool)
    }

    pub fn lb_name(&self) -> &'static str {
        self.load_balancer.name()
    }
}

impl BackendPool {
    pub fn new_from_states(backends: Vec<BackendState>) -> Self {
        Self { backends }
    }

    pub fn len(&self) -> usize {
        self.backends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }

    pub fn address(&self, index: usize) -> Option<&str> {
        self.backends.get(index).map(|b| b.address())
    }

    pub fn mark_success(&mut self, index: usize) -> Option<HealthTransition> {
        if let Some(backend) = self.backends.get_mut(index) {
            return backend.record_success();
        }
        None
    }

    pub fn mark_failure(&mut self, index: usize) -> Option<HealthTransition> {
        if let Some(backend) = self.backends.get_mut(index) {
            return backend.record_failure();
        }
        None
    }

    pub fn health_check(&self, index: usize) -> Option<HealthCheck> {
        self.backends.get(index).map(|b| b.health_check().clone())
    }

    pub fn healthy_indices(&self) -> Vec<usize> {
        self.backends
            .iter()
            .enumerate()
            .filter_map(|(idx, backend)| backend.is_healthy().then_some(idx))
            .collect()
    }

    pub fn all_indices(&self) -> Vec<usize> {
        (0..self.backends.len()).collect()
    }

    pub fn backend(&self, index: usize) -> Option<&BackendState> {
        self.backends.get(index)
    }
}

pub enum LoadBalancing {
    RoundRobin(RoundRobin),
    ConsistentHash(ConsistentHash),
    Random(Random),
}

impl LoadBalancing {
    pub fn from_config(value: &str) -> Result<Self, String> {
        let mode = value.trim().to_lowercase();
        match mode.as_str() {
            "round-robin" | "round_robin" | "rr" => Ok(Self::RoundRobin(RoundRobin::new())),
            "consistent-hash" | "consistent_hash" | "ch" => {
                Ok(Self::ConsistentHash(ConsistentHash::new(DEFAULT_REPLICAS)))
            }
            "random" => Ok(Self::Random(Random::new())),
            _ => Err(format!("unsupported load balancing type: {value}")),
        }
    }

    pub fn pick(&mut self, key: &str, pool: &BackendPool) -> Option<usize> {
        match self {
            LoadBalancing::RoundRobin(rr) => rr.pick(pool),
            LoadBalancing::ConsistentHash(ch) => ch.pick(key, pool),
            LoadBalancing::Random(rand) => rand.pick(pool),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            LoadBalancing::RoundRobin(_) => "round-robin",
            LoadBalancing::ConsistentHash(_) => "consistent-hash",
            LoadBalancing::Random(_) => "random",
        }
    }
}

pub struct RoundRobin {
    next: usize,
}

impl RoundRobin {
    pub fn new() -> Self {
        Self { next: 0 }
    }

    pub fn pick(&mut self, pool: &BackendPool) -> Option<usize> {
        let candidates = pool.healthy_indices();
        if candidates.is_empty() {
            return None;
        }

        let idx = candidates[self.next % candidates.len()];
        self.next = self.next.wrapping_add(1);
        Some(idx)
    }
}

impl Default for RoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ConsistentHash {
    replicas: u32,
}

impl ConsistentHash {
    pub fn new(replicas: u32) -> Self {
        Self {
            replicas: replicas.max(1),
        }
    }

    pub fn pick(&self, key: &str, pool: &BackendPool) -> Option<usize> {
        if pool.is_empty() {
            return None;
        }

        let candidates = pool.healthy_indices();
        if candidates.is_empty() {
            return None;
        }

        let ring = build_ring(pool, &candidates, self.replicas);
        let key_hash = hash64(key.as_bytes());

        let (_, idx) = ring
            .range(key_hash..)
            .next()
            .or_else(|| ring.iter().next())?;

        Some(*idx)
    }
}

pub struct Random;

impl Random {
    pub fn new() -> Self {
        Self
    }

    pub fn pick(&mut self, pool: &BackendPool) -> Option<usize> {
        let candidates = pool.healthy_indices();
        if candidates.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..candidates.len());
        Some(candidates[idx])
    }
}

impl Default for Random {
    fn default() -> Self {
        Self::new()
    }
}

fn build_ring(pool: &BackendPool, indices: &[usize], replicas: u32) -> BTreeMap<u64, usize> {
    let mut ring = BTreeMap::new();
    for &idx in indices {
        let backend = match pool.backend(idx) {
            Some(backend) => backend,
            None => continue,
        };

        let weight = backend.weight();
        let replicas = replicas.saturating_mul(weight);
        for replica in 0..replicas {
            let mut key = Vec::new();
            key.extend_from_slice(backend.address().as_bytes());
            key.extend_from_slice(b"-");
            key.extend_from_slice(replica.to_string().as_bytes());
            ring.insert(hash64(&key), idx);
        }
    }
    ring
}

fn hash64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use spooky_config::config::RouteMatch;

    use super::*;

    fn create_backend_state(address: &str, weight: u32) -> BackendState {
        let backend = Backend {
            id: format!("backend-{}", address),
            address: address.to_string(),
            weight,
            health_check: HealthCheck {
                path: "/health".to_string(),
                interval: 1000,
                timeout_ms: 1000,
                failure_threshold: 3,
                success_threshold: 1,
                cooldown_ms: 0,
            },
        };
        BackendState::new(&backend)
    }

    #[test]
    fn round_robin_cycles() {
        let pool = BackendPool::new_from_states(vec![
            create_backend_state("127.0.0.1:1", 1),
            create_backend_state("127.0.0.1:2", 1),
            create_backend_state("127.0.0.1:3", 1),
        ]);
        let mut rr = RoundRobin::new();

        let picks: Vec<usize> = (0..6).filter_map(|_| rr.pick(&pool)).collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn consistent_hash_is_stable() {
        let pool = BackendPool::new_from_states(vec![
            create_backend_state("10.0.0.1:1", 1),
            create_backend_state("10.0.0.2:1", 1),
            create_backend_state("10.0.0.3:1", 1),
        ]);

        let ch = ConsistentHash::new(16);
        let first = ch.pick("user:123", &pool);
        let second = ch.pick("user:123", &pool);
        assert_eq!(first, second);
    }

    #[test]
    fn unhealthy_backends_are_skipped() {
        let mut pool = BackendPool::new_from_states(vec![
            create_backend_state("10.0.0.1:1", 1),
            create_backend_state("10.0.0.2:1", 1),
        ]);

        pool.mark_failure(0);
        pool.mark_failure(0);
        pool.mark_failure(0);

        let mut rr = RoundRobin::new();
        let pick = rr.pick(&pool).unwrap();
        assert_eq!(pick, 1);
    }

    #[test]
    fn load_balancing_from_config() {
        assert!(LoadBalancing::from_config("round-robin").is_ok());
        assert!(LoadBalancing::from_config("consistent-hash").is_ok());
        assert!(LoadBalancing::from_config("random").is_ok());
        assert!(LoadBalancing::from_config("unknown").is_err());
    }

    #[test]
    fn no_healthy_backends_returns_none() {
        let mut pool = BackendPool::new_from_states(vec![create_backend_state("10.0.0.1:1", 1)]);
        pool.mark_failure(0);
        pool.mark_failure(0);
        pool.mark_failure(0);

        let mut rr = RoundRobin::new();
        assert!(rr.pick(&pool).is_none());
    }

    #[test]
    fn backend_recovers_after_success_threshold() {
        let mut pool = BackendPool::new_from_states(vec![create_backend_state("10.0.0.1:1", 1)]);
        pool.mark_failure(0);
        pool.mark_failure(0);
        pool.mark_failure(0);

        assert!(pool.healthy_indices().is_empty());
        pool.mark_success(0);
        assert_eq!(pool.healthy_indices(), vec![0]);
    }

    #[test]
    fn upstream_pool_from_config() {
        let upstream = spooky_config::config::Upstream {
            load_balancing: spooky_config::config::LoadBalancing {
                lb_type: "round-robin".to_string(),
                key: None,
            },
            route: RouteMatch {
                path_prefix: Some("/".to_string()),
                ..Default::default()
            },
            backends: vec![
                Backend {
                    id: "backend1".to_string(),
                    address: "127.0.0.1:8001".to_string(),
                    weight: 100,
                    health_check: HealthCheck {
                        path: "/health".to_string(),
                        interval: 5000,
                        timeout_ms: 2000,
                        failure_threshold: 3,
                        success_threshold: 2,
                        cooldown_ms: 10000,
                    },
                },
                Backend {
                    id: "backend2".to_string(),
                    address: "127.0.0.1:8002".to_string(),
                    weight: 200,
                    health_check: HealthCheck {
                        path: "/health".to_string(),
                        interval: 5000,
                        timeout_ms: 2000,
                        failure_threshold: 3,
                        success_threshold: 2,
                        cooldown_ms: 10000,
                    },
                },
            ],
        };

        let upstream_pool = UpstreamPool::from_upstream(&upstream).unwrap();
        assert!(matches!(
            upstream_pool.load_balancer,
            LoadBalancing::RoundRobin(_)
        ));
        assert_eq!(upstream_pool.pool.len(), 2);
        assert_eq!(upstream_pool.pool.address(0), Some("127.0.0.1:8001"));
        assert_eq!(upstream_pool.pool.address(1), Some("127.0.0.1:8002"));
    }
}
