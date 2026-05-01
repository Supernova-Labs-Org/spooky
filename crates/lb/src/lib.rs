use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use rand::{Rng, SeedableRng, rngs::StdRng};
use spooky_config::config::{Backend, HealthCheck};

thread_local! {
    static LB_RANDOM_RNG: RefCell<StdRng> = RefCell::new(StdRng::from_entropy());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthFailureReason {
    HttpStatus5xx,
    Timeout,
    Transport,
    Tls,
    CircuitOpen,
}

const DEFAULT_REPLICAS: u32 = 64;
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x00000100000001b3;

#[derive(Clone)]
pub struct BackendState {
    address: String,
    weight: u32,
    health_check: Option<HealthCheck>,
    consecutive_failures: u32,
    health_state: HealthState,
    active_requests: Arc<AtomicUsize>,
    ewma_latency_ms: Option<f64>,
}

#[derive(Clone)]
enum HealthState {
    Healthy,
    // `reason` is stored for future introspection; suppressed until wired to metrics
    #[allow(dead_code)]
    Unhealthy {
        until: Instant,
        successes: u32,
        reason: HealthFailureReason,
    },
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
            active_requests: Arc::new(AtomicUsize::new(0)),
            ewma_latency_ms: None,
        }
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.health_state, HealthState::Healthy)
    }

    /// Returns true when an active health-check loop is running for this backend.
    /// When active checks are present, only the health-check loop should drive
    /// consecutive_failures — request-path failures should not contribute.
    pub fn has_active_health_check(&self) -> bool {
        self.health_check.as_ref().is_some_and(|hc| hc.interval > 0)
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn health_check(&self) -> Option<&HealthCheck> {
        self.health_check.as_ref()
    }

    pub fn weight(&self) -> u32 {
        self.weight
    }

    pub fn active_requests(&self) -> usize {
        self.active_requests.load(Ordering::Relaxed)
    }

    pub fn ewma_latency_ms(&self) -> Option<f64> {
        self.ewma_latency_ms
    }

    pub fn record_success(&mut self) -> Option<HealthTransition> {
        match &mut self.health_state {
            HealthState::Healthy => {
                self.consecutive_failures = 0;
                None
            }
            HealthState::Unhealthy {
                until, successes, ..
            } => {
                if Instant::now() < *until {
                    return None;
                }

                *successes += 1;
                let success_threshold = self.health_check.as_ref().map_or(1, |hc| hc.success_threshold);
                if *successes >= success_threshold {
                    self.consecutive_failures = 0;
                    self.health_state = HealthState::Healthy;
                    return Some(HealthTransition::BecameHealthy);
                }
                None
            }
        }
    }

    pub fn record_failure(&mut self, reason: HealthFailureReason) -> Option<HealthTransition> {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let threshold = self.health_check.as_ref().map_or(3, |hc| hc.failure_threshold);
        if self.consecutive_failures < threshold {
            return None;
        }

        self.consecutive_failures = 0;
        let cooldown = Duration::from_millis(self.health_check.as_ref().map_or(10_000, |hc| hc.cooldown_ms));
        self.health_state = HealthState::Unhealthy {
            until: Instant::now() + cooldown,
            successes: 0,
            reason,
        };
        Some(HealthTransition::BecameUnhealthy)
    }
}

pub struct BackendPool {
    backends: Vec<BackendState>,
    healthy: Vec<usize>,
    healthy_pos: Vec<Option<usize>>,
    membership_epoch: u64,
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
        let selected = self.load_balancer.pick(key, &self.pool)?;
        self.pool.begin_request(selected);
        Some(selected)
    }

    pub fn pick_readonly(&self, key: &str) -> Option<usize> {
        self.load_balancer.pick_readonly(key, &self.pool)
    }

    pub fn begin_request_if_healthy(&self, index: usize) -> bool {
        if self.pool.is_healthy_index(index) {
            self.pool.begin_request(index);
            true
        } else {
            false
        }
    }

    pub fn finish_request(&mut self, index: usize, latency: Duration, status: Option<u16>) {
        self.pool.finish_request(index, latency, status);
    }

    pub fn lb_name(&self) -> &'static str {
        self.load_balancer.name()
    }
}

impl BackendPool {
    pub fn new_from_states(backends: Vec<BackendState>) -> Self {
        let mut healthy = Vec::with_capacity(backends.len());
        let mut healthy_pos = vec![None; backends.len()];

        for (idx, backend) in backends.iter().enumerate() {
            if backend.is_healthy() {
                healthy_pos[idx] = Some(healthy.len());
                healthy.push(idx);
            }
        }

        Self {
            backends,
            healthy,
            healthy_pos,
            membership_epoch: 0,
        }
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
        if index >= self.backends.len() {
            return None;
        }

        let (was_healthy, is_healthy, transition) = {
            let backend = &mut self.backends[index];
            let was_healthy = backend.is_healthy();
            let transition = backend.record_success();
            let is_healthy = backend.is_healthy();
            (was_healthy, is_healthy, transition)
        };

        if was_healthy != is_healthy {
            if is_healthy {
                debug_assert!(self.mark_healthy(index));
            } else {
                debug_assert!(self.mark_unhealthy(index));
            }
            self.membership_epoch = self.membership_epoch.wrapping_add(1);
        }

        transition
    }

    /// Mark a failure from the active health-check loop — always recorded.
    pub fn mark_failure(&mut self, index: usize) -> Option<HealthTransition> {
        self.mark_failure_with_reason(index, HealthFailureReason::HttpStatus5xx)
    }

    /// Mark a failure from the request path (passive).
    /// Skipped when an active health-check loop is running for this backend,
    /// because the loop is the sole authority on consecutive_failures in that case.
    pub fn mark_request_failure(
        &mut self,
        index: usize,
        reason: HealthFailureReason,
    ) -> Option<HealthTransition> {
        if index < self.backends.len() && self.backends[index].has_active_health_check() {
            return None;
        }
        self.mark_failure_with_reason(index, reason)
    }

    pub fn mark_failure_with_reason(
        &mut self,
        index: usize,
        reason: HealthFailureReason,
    ) -> Option<HealthTransition> {
        if index >= self.backends.len() {
            return None;
        }

        let (was_healthy, is_healthy, transition) = {
            let backend = &mut self.backends[index];
            let was_healthy = backend.is_healthy();
            let transition = backend.record_failure(reason);
            let is_healthy = backend.is_healthy();
            (was_healthy, is_healthy, transition)
        };

        if was_healthy != is_healthy {
            if is_healthy {
                debug_assert!(self.mark_healthy(index));
            } else {
                debug_assert!(self.mark_unhealthy(index));
            }
            self.membership_epoch = self.membership_epoch.wrapping_add(1);
        }

        transition
    }

    pub fn health_check(&self, index: usize) -> Option<HealthCheck> {
        self.backends.get(index).and_then(|b| b.health_check().cloned())
    }

    pub fn healthy_indices(&self) -> Vec<usize> {
        self.healthy.clone()
    }

    pub fn healthy_len(&self) -> usize {
        self.healthy.len()
    }

    pub fn healthy_indices_iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.healthy.iter().copied()
    }

    pub fn all_indices(&self) -> Vec<usize> {
        (0..self.backends.len()).collect()
    }

    pub fn backend(&self, index: usize) -> Option<&BackendState> {
        self.backends.get(index)
    }

    pub fn membership_epoch(&self) -> u64 {
        self.membership_epoch
    }

    pub fn is_healthy_index(&self, index: usize) -> bool {
        self.healthy_pos.get(index).copied().flatten().is_some()
    }

    pub fn begin_request(&self, index: usize) {
        if let Some(backend) = self.backends.get(index) {
            backend.active_requests.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn finish_request(&mut self, index: usize, latency: Duration, status: Option<u16>) {
        let Some(backend) = self.backends.get_mut(index) else {
            return;
        };

        let _ =
            backend
                .active_requests
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(1))
                });

        if status.is_some_and(|code| (500..=599).contains(&code)) {
            return;
        }

        let observed_ms = latency.as_secs_f64() * 1_000.0;
        let alpha = 0.2_f64;
        backend.ewma_latency_ms = Some(match backend.ewma_latency_ms {
            Some(previous) => alpha * observed_ms + (1.0 - alpha) * previous,
            None => observed_ms,
        });
    }

    fn mark_healthy(&mut self, index: usize) -> bool {
        if index >= self.backends.len() {
            return false;
        }

        if self.healthy_pos[index].is_some() {
            return false;
        }

        let pos = self.healthy.len();
        self.healthy.push(index);
        self.healthy_pos[index] = Some(pos);
        true
    }

    fn mark_unhealthy(&mut self, index: usize) -> bool {
        if index >= self.backends.len() {
            return false;
        }

        let Some(pos) = self.healthy_pos[index] else {
            return false;
        };

        let removed = self.healthy.swap_remove(pos);
        debug_assert_eq!(removed, index);

        if pos < self.healthy.len() {
            let moved_index = self.healthy[pos];
            self.healthy_pos[moved_index] = Some(pos);
        }

        self.healthy_pos[index] = None;
        true
    }
}

pub enum LoadBalancing {
    RoundRobin(RoundRobin),
    ConsistentHash(ConsistentHash),
    Random(Random),
    LeastConnections(LeastConnections),
    LatencyAware(LatencyAware),
    StickyCid(StickyCid),
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
            "least-connections" | "least_connections" | "lc" => {
                Ok(Self::LeastConnections(LeastConnections::new()))
            }
            "latency-aware" | "latency_aware" | "la" => Ok(Self::LatencyAware(LatencyAware::new())),
            "sticky-cid" | "sticky_cid" | "cid-sticky" | "cid_sticky" => {
                Ok(Self::StickyCid(StickyCid::new(DEFAULT_REPLICAS)))
            }
            _ => Err(format!("unsupported load balancing type: {value}")),
        }
    }

    pub fn pick(&mut self, key: &str, pool: &BackendPool) -> Option<usize> {
        match self {
            LoadBalancing::RoundRobin(rr) => rr.pick(pool),
            LoadBalancing::ConsistentHash(ch) => ch.pick(key, pool),
            LoadBalancing::Random(rand) => rand.pick(pool),
            LoadBalancing::LeastConnections(lc) => lc.pick(pool),
            LoadBalancing::LatencyAware(la) => la.pick(pool),
            LoadBalancing::StickyCid(sticky) => sticky.pick(key, pool),
        }
    }

    pub fn pick_readonly(&self, _key: &str, pool: &BackendPool) -> Option<usize> {
        match self {
            LoadBalancing::RoundRobin(rr) => rr.pick_readonly(pool),
            LoadBalancing::Random(rand) => rand.pick_readonly(pool),
            LoadBalancing::LeastConnections(lc) => lc.pick_readonly(pool),
            LoadBalancing::LatencyAware(la) => la.pick_readonly(pool),
            // ConsistentHash and StickyCid keep mutable ring caches.
            LoadBalancing::ConsistentHash(_) | LoadBalancing::StickyCid(_) => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            LoadBalancing::RoundRobin(_) => "round-robin",
            LoadBalancing::ConsistentHash(_) => "consistent-hash",
            LoadBalancing::Random(_) => "random",
            LoadBalancing::LeastConnections(_) => "least-connections",
            LoadBalancing::LatencyAware(_) => "latency-aware",
            LoadBalancing::StickyCid(_) => "sticky-cid",
        }
    }
}

pub struct RoundRobin {
    next: usize,
    next_read: AtomicUsize,
}

impl RoundRobin {
    pub fn new() -> Self {
        Self {
            next: 0,
            next_read: AtomicUsize::new(0),
        }
    }

    pub fn pick(&mut self, pool: &BackendPool) -> Option<usize> {
        if pool.healthy.is_empty() {
            return None;
        }

        let idx = pool.healthy[self.next % pool.healthy.len()];
        self.next = self.next.wrapping_add(1);
        Some(idx)
    }

    pub fn pick_readonly(&self, pool: &BackendPool) -> Option<usize> {
        if pool.healthy.is_empty() {
            return None;
        }

        let next = self.next_read.fetch_add(1, Ordering::Relaxed);
        let idx = pool.healthy[next % pool.healthy.len()];
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
    ring: Vec<(u64, usize)>,
    ring_epoch: Option<u64>,
    ring_rebuilds: u64,
}

impl ConsistentHash {
    pub fn new(replicas: u32) -> Self {
        Self {
            replicas: replicas.max(1),
            ring: Vec::new(),
            ring_epoch: None,
            ring_rebuilds: 0,
        }
    }

    pub fn pick(&mut self, key: &str, pool: &BackendPool) -> Option<usize> {
        if pool.is_empty() {
            return None;
        }

        let epoch = pool.membership_epoch();
        if self.ring_epoch != Some(epoch) {
            self.rebuild_ring(pool);
            self.ring_epoch = Some(epoch);
            self.ring_rebuilds = self.ring_rebuilds.wrapping_add(1);
        }

        if self.ring.is_empty() {
            return None;
        }

        let key_hash = hash64(key.as_bytes());
        let lookup_idx = match self.ring.binary_search_by(|(hash, _)| hash.cmp(&key_hash)) {
            Ok(idx) => idx,
            Err(idx) if idx < self.ring.len() => idx,
            Err(_) => 0,
        };

        Some(self.ring[lookup_idx].1)
    }

    fn rebuild_ring(&mut self, pool: &BackendPool) {
        self.ring.clear();

        let expected = expected_ring_entries(pool, self.replicas);
        if self.ring.capacity() < expected {
            self.ring.reserve(expected - self.ring.capacity());
        }

        for &idx in &pool.healthy {
            let backend = &pool.backends[idx];
            let replicas = self.replicas.saturating_mul(backend.weight());
            for replica in 0..replicas {
                self.ring
                    .push((hash_backend_replica(backend.address(), replica), idx));
            }
        }

        self.ring.sort_unstable();
    }
}

pub struct Random;

impl Random {
    pub fn new() -> Self {
        Self
    }

    pub fn pick(&mut self, pool: &BackendPool) -> Option<usize> {
        self.pick_readonly(pool)
    }

    pub fn pick_readonly(&self, pool: &BackendPool) -> Option<usize> {
        if pool.healthy.is_empty() {
            return None;
        }

        let idx = LB_RANDOM_RNG.with(|state| {
            let mut rng = state.borrow_mut();
            rng.gen_range(0..pool.healthy.len())
        });
        Some(pool.healthy[idx])
    }
}

impl Default for Random {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LeastConnections;

impl LeastConnections {
    pub fn new() -> Self {
        Self
    }

    pub fn pick(&mut self, pool: &BackendPool) -> Option<usize> {
        self.pick_readonly(pool)
    }

    pub fn pick_readonly(&self, pool: &BackendPool) -> Option<usize> {
        let mut best: Option<(usize, usize)> = None;
        for &idx in &pool.healthy {
            let active = pool.backends[idx].active_requests();
            match best {
                Some((best_active, best_idx)) => {
                    if active < best_active || (active == best_active && idx < best_idx) {
                        best = Some((active, idx));
                    }
                }
                None => best = Some((active, idx)),
            }
        }
        best.map(|(_, idx)| idx)
    }
}

impl Default for LeastConnections {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LatencyAware;

impl LatencyAware {
    pub fn new() -> Self {
        Self
    }

    pub fn pick(&mut self, pool: &BackendPool) -> Option<usize> {
        self.pick_readonly(pool)
    }

    pub fn pick_readonly(&self, pool: &BackendPool) -> Option<usize> {
        let mut unsampled_best: Option<(usize, usize)> = None;
        let mut sampled_best: Option<(f64, usize, usize)> = None;

        for &idx in &pool.healthy {
            let backend = &pool.backends[idx];
            let active = backend.active_requests();
            if let Some(ewma) = backend.ewma_latency_ms() {
                let score = ewma + (active as f64 * 10.0);
                match sampled_best {
                    Some((best_score, best_active, best_idx)) => {
                        if score < best_score
                            || (score == best_score
                                && (active < best_active
                                    || (active == best_active && idx < best_idx)))
                        {
                            sampled_best = Some((score, active, idx));
                        }
                    }
                    None => sampled_best = Some((score, active, idx)),
                }
            } else {
                match unsampled_best {
                    Some((best_active, best_idx)) => {
                        if active < best_active || (active == best_active && idx < best_idx) {
                            unsampled_best = Some((active, idx));
                        }
                    }
                    None => unsampled_best = Some((active, idx)),
                }
            }
        }

        if let Some((_, idx)) = unsampled_best {
            return Some(idx);
        }
        sampled_best.map(|(_, _, idx)| idx)
    }
}

impl Default for LatencyAware {
    fn default() -> Self {
        Self::new()
    }
}

pub struct StickyCid {
    inner: ConsistentHash,
}

impl StickyCid {
    pub fn new(replicas: u32) -> Self {
        Self {
            inner: ConsistentHash::new(replicas),
        }
    }

    pub fn pick(&mut self, key: &str, pool: &BackendPool) -> Option<usize> {
        if key.is_empty() {
            return pool.healthy.first().copied();
        }
        self.inner.pick(key, pool)
    }
}

fn expected_ring_entries(pool: &BackendPool, replicas: u32) -> usize {
    pool.healthy
        .iter()
        .map(|&idx| replicas.saturating_mul(pool.backends[idx].weight()) as usize)
        .sum()
}

fn hash_backend_replica(address: &str, replica: u32) -> u64 {
    let mut hash = FNV_OFFSET;
    for &byte in address.as_bytes() {
        hash = hash64_update(hash, byte);
    }
    hash = hash64_update(hash, b'-');

    let mut digits = [0u8; 10];
    let mut value = replica;
    let mut cursor = digits.len();
    loop {
        cursor -= 1;
        digits[cursor] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }

    for &digit in &digits[cursor..] {
        hash = hash64_update(hash, digit);
    }

    hash
}

fn hash64(data: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in data {
        hash = hash64_update(hash, *byte);
    }
    hash
}

fn hash64_update(hash: u64, byte: u8) -> u64 {
    (hash ^ byte as u64).wrapping_mul(FNV_PRIME)
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
            health_check: Some(HealthCheck {
                path: "/health".to_string(),
                interval: 1000,
                timeout_ms: 1000,
                failure_threshold: 3,
                success_threshold: 1,
                cooldown_ms: 0,
            }),
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

        let mut ch = ConsistentHash::new(16);
        let first = ch.pick("user:123", &pool);
        let second = ch.pick("user:123", &pool);
        assert_eq!(first, second);
    }

    #[test]
    fn consistent_hash_rebuilds_only_when_membership_changes() {
        let mut pool = BackendPool::new_from_states(vec![
            create_backend_state("10.0.0.1:1", 1),
            create_backend_state("10.0.0.2:1", 1),
            create_backend_state("10.0.0.3:1", 1),
        ]);

        let mut ch = ConsistentHash::new(16);

        let _ = ch.pick("user:123", &pool);
        let first_rebuilds = ch.ring_rebuilds;
        let first_len = ch.ring.len();
        assert_eq!(first_rebuilds, 1);

        for key in ["user:123", "user:124", "user:125", "user:126"] {
            let _ = ch.pick(key, &pool);
        }
        assert_eq!(ch.ring_rebuilds, first_rebuilds);
        assert_eq!(ch.ring.len(), first_len);

        pool.mark_failure(0);
        pool.mark_failure(0);
        pool.mark_failure(0);

        let _ = ch.pick("user:127", &pool);
        assert_eq!(ch.ring_rebuilds, first_rebuilds + 1);
        assert!(ch.ring.len() < first_len);
    }

    #[test]
    fn consistent_hash_ring_size_matches_weighted_healthy_membership() {
        let mut pool = BackendPool::new_from_states(vec![
            create_backend_state("10.0.0.1:1", 2),
            create_backend_state("10.0.0.2:1", 3),
        ]);

        let mut ch = ConsistentHash::new(8);

        let _ = ch.pick("user:1", &pool);
        assert_eq!(ch.ring.len(), (8 * (2 + 3)) as usize);

        pool.mark_failure(0);
        pool.mark_failure(0);
        pool.mark_failure(0);

        let _ = ch.pick("user:2", &pool);
        assert_eq!(ch.ring.len(), (8 * 3) as usize);
    }

    #[test]
    fn backend_pool_epoch_changes_only_on_health_membership_transition() {
        let mut pool = BackendPool::new_from_states(vec![create_backend_state("10.0.0.1:1", 1)]);
        assert_eq!(pool.membership_epoch(), 0);

        pool.mark_failure(0);
        pool.mark_failure(0);
        assert_eq!(pool.membership_epoch(), 0);

        pool.mark_failure(0);
        assert_eq!(pool.membership_epoch(), 1);

        pool.mark_failure(0);
        assert_eq!(pool.membership_epoch(), 1);

        pool.mark_success(0);
        assert_eq!(pool.membership_epoch(), 2);

        pool.mark_success(0);
        assert_eq!(pool.membership_epoch(), 2);
    }

    #[test]
    fn healthy_cache_tracks_membership_changes_without_duplicates() {
        let mut pool = BackendPool::new_from_states(vec![
            create_backend_state("10.0.0.1:1", 1),
            create_backend_state("10.0.0.2:1", 1),
            create_backend_state("10.0.0.3:1", 1),
        ]);

        assert_eq!(pool.healthy_indices(), vec![0, 1, 2]);

        pool.mark_failure(1);
        pool.mark_failure(1);
        pool.mark_failure(1);
        assert_eq!(pool.healthy_indices(), vec![0, 2]);

        // Repeated failure for an already unhealthy backend should not change cache.
        pool.mark_failure(1);
        assert_eq!(pool.healthy_indices(), vec![0, 2]);

        pool.mark_success(1);
        let healthy = pool.healthy_indices();
        assert_eq!(healthy.len(), 3);
        assert!(healthy.contains(&0));
        assert!(healthy.contains(&1));
        assert!(healthy.contains(&2));
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
        assert!(LoadBalancing::from_config("least-connections").is_ok());
        assert!(LoadBalancing::from_config("latency-aware").is_ok());
        assert!(LoadBalancing::from_config("sticky-cid").is_ok());
        assert!(LoadBalancing::from_config("unknown").is_err());
    }

    #[test]
    fn least_connections_picks_lowest_active() {
        let pool = BackendPool::new_from_states(vec![
            create_backend_state("10.0.0.1:1", 1),
            create_backend_state("10.0.0.2:1", 1),
            create_backend_state("10.0.0.3:1", 1),
        ]);
        pool.begin_request(0);
        pool.begin_request(0);
        pool.begin_request(1);

        let mut lb = LeastConnections::new();
        assert_eq!(lb.pick(&pool), Some(2));
    }

    #[test]
    fn latency_aware_prefers_lower_ewma() {
        let mut pool = BackendPool::new_from_states(vec![
            create_backend_state("10.0.0.1:1", 1),
            create_backend_state("10.0.0.2:1", 1),
        ]);

        pool.finish_request(0, Duration::from_millis(150), Some(200));
        pool.finish_request(1, Duration::from_millis(20), Some(200));

        let mut lb = LatencyAware::new();
        assert_eq!(lb.pick(&pool), Some(1));
    }

    #[test]
    fn sticky_cid_is_deterministic_for_same_key() {
        let pool = BackendPool::new_from_states(vec![
            create_backend_state("10.0.0.1:1", 1),
            create_backend_state("10.0.0.2:1", 1),
            create_backend_state("10.0.0.3:1", 1),
        ]);

        let mut lb = StickyCid::new(16);
        let first = lb.pick("cid:abc123", &pool);
        let second = lb.pick("cid:abc123", &pool);
        assert_eq!(first, second);
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
                    health_check: Some(HealthCheck {
                        path: "/health".to_string(),
                        interval: 5000,
                        timeout_ms: 2000,
                        failure_threshold: 3,
                        success_threshold: 2,
                        cooldown_ms: 10000,
                    }),
                },
                Backend {
                    id: "backend2".to_string(),
                    address: "127.0.0.1:8002".to_string(),
                    weight: 200,
                    health_check: Some(HealthCheck {
                        path: "/health".to_string(),
                        interval: 5000,
                        timeout_ms: 2000,
                        failure_threshold: 3,
                        success_threshold: 2,
                        cooldown_ms: 10000,
                    }),
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
