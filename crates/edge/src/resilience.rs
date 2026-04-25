use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use spooky_config::config::Resilience as ResilienceConfig;

use crate::RetryReason;

pub struct AdaptiveAdmission {
    enabled: bool,
    min_limit: usize,
    max_limit: usize,
    increase_step: usize,
    decrease_step: usize,
    high_latency_ms: u64,
    current_limit: AtomicUsize,
    inflight: AtomicUsize,
}

impl AdaptiveAdmission {
    pub fn new(
        enabled: bool,
        min_limit: usize,
        max_limit: usize,
        increase_step: usize,
        decrease_step: usize,
        high_latency_ms: u64,
    ) -> Self {
        let max_limit = max_limit.max(1);
        let min_limit = min_limit.max(1).min(max_limit);
        Self {
            enabled,
            min_limit,
            max_limit,
            increase_step: increase_step.max(1),
            decrease_step: decrease_step.max(1),
            high_latency_ms: high_latency_ms.max(1),
            current_limit: AtomicUsize::new(max_limit),
            inflight: AtomicUsize::new(0),
        }
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<AdaptivePermit> {
        loop {
            let current = self.inflight.load(Ordering::Relaxed);
            let limit = self.current_limit.load(Ordering::Relaxed).max(1);
            if current >= limit {
                return None;
            }
            if self
                .inflight
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(AdaptivePermit {
                    admission: Arc::clone(self),
                });
            }
        }
    }

    pub fn observe(&self, latency: Duration, overloaded: bool) {
        if !self.enabled {
            return;
        }
        let latency_ms = latency.as_millis() as u64;
        let decrease = overloaded || latency_ms >= self.high_latency_ms;
        loop {
            let cur = self.current_limit.load(Ordering::Relaxed);
            let next = if decrease {
                cur.saturating_sub(self.decrease_step).max(self.min_limit)
            } else {
                cur.saturating_add(self.increase_step).min(self.max_limit)
            };

            if next == cur {
                return;
            }

            if self
                .current_limit
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn current_limit(&self) -> usize {
        self.current_limit.load(Ordering::Relaxed)
    }

    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    pub fn inflight_percent(&self) -> u8 {
        let limit = self.current_limit().max(1);
        ((self.inflight() * 100) / limit).min(100) as u8
    }
}

pub struct AdaptivePermit {
    admission: Arc<AdaptiveAdmission>,
}

impl Drop for AdaptivePermit {
    fn drop(&mut self) {
        self.admission.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct RouteQueueLimiter {
    default_cap: usize,
    global_cap: usize,
    caps: HashMap<String, usize>,
    inflight: Mutex<RouteQueueState>,
}

#[derive(Default)]
struct RouteQueueState {
    total: usize,
    by_route: HashMap<String, usize>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RouteQueueRejection {
    GlobalCap,
    RouteCap,
}

impl RouteQueueLimiter {
    pub fn new(default_cap: usize, global_cap: usize, caps: HashMap<String, usize>) -> Self {
        Self {
            default_cap: default_cap.max(1),
            global_cap: global_cap.max(1),
            caps,
            inflight: Mutex::new(RouteQueueState::default()),
        }
    }

    pub fn try_acquire(
        self: &Arc<Self>,
        route: &str,
    ) -> Result<RouteQueuePermit, RouteQueueRejection> {
        let cap = self
            .caps
            .get(route)
            .copied()
            .unwrap_or(self.default_cap)
            .max(1);
        let mut guard = self
            .inflight
            .lock()
            .map_err(|_| RouteQueueRejection::GlobalCap)?;
        if guard.total >= self.global_cap {
            return Err(RouteQueueRejection::GlobalCap);
        }
        let current = guard.by_route.get(route).copied().unwrap_or(0);
        if current >= cap {
            return Err(RouteQueueRejection::RouteCap);
        }
        guard.total = guard.total.saturating_add(1);
        guard.by_route.insert(route.to_string(), current + 1);
        Ok(RouteQueuePermit {
            limiter: Arc::clone(self),
            route: route.to_string(),
        })
    }
}

pub struct RouteQueuePermit {
    limiter: Arc<RouteQueueLimiter>,
    route: String,
}

impl Drop for RouteQueuePermit {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.limiter.inflight.lock()
            && let Some(current) = guard.by_route.get_mut(&self.route)
        {
            *current = current.saturating_sub(1);
            if *current == 0 {
                guard.by_route.remove(&self.route);
            }
            guard.total = guard.total.saturating_sub(1);
        }
    }
}

#[derive(Default)]
struct BreakerState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    half_open_inflight: u32,
    half_open: bool,
}

pub struct CircuitBreakers {
    enabled: bool,
    failure_threshold: u32,
    open_for: Duration,
    half_open_max_probes: u32,
    states: Mutex<HashMap<String, BreakerState>>,
}

impl CircuitBreakers {
    pub fn new(
        enabled: bool,
        failure_threshold: u32,
        open_for: Duration,
        half_open_max_probes: u32,
    ) -> Self {
        Self {
            enabled,
            failure_threshold: failure_threshold.max(1),
            open_for,
            half_open_max_probes: half_open_max_probes.max(1),
            states: Mutex::new(HashMap::new()),
        }
    }

    pub fn allow_request(&self, backend: &str) -> bool {
        if !self.enabled {
            return true;
        }
        let now = Instant::now();
        let mut states = match self.states.lock() {
            Ok(guard) => guard,
            Err(_) => return true,
        };
        let state = states.entry(backend.to_string()).or_default();

        if let Some(until) = state.open_until {
            if now < until {
                return false;
            }
            state.open_until = None;
            state.half_open = true;
            state.half_open_inflight = 0;
            state.consecutive_failures = 0;
        }

        if state.half_open {
            if state.half_open_inflight >= self.half_open_max_probes {
                return false;
            }
            state.half_open_inflight += 1;
        }

        true
    }

    pub fn record_success(&self, backend: &str) {
        if !self.enabled {
            return;
        }
        let mut states = match self.states.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let state = states.entry(backend.to_string()).or_default();
        if state.half_open && state.half_open_inflight > 0 {
            state.half_open_inflight -= 1;
        }
        state.consecutive_failures = 0;
        state.open_until = None;
        state.half_open = false;
    }

    pub fn record_failure(&self, backend: &str) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let mut states = match self.states.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let state = states.entry(backend.to_string()).or_default();

        if state.half_open {
            if state.half_open_inflight > 0 {
                state.half_open_inflight -= 1;
            }
            state.open_until = Some(now + self.open_for);
            state.half_open = false;
            state.consecutive_failures = 0;
            return;
        }

        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.consecutive_failures >= self.failure_threshold {
            state.open_until = Some(now + self.open_for);
            state.half_open = false;
            state.consecutive_failures = 0;
        }
    }
}

pub struct RetryBudget {
    enabled: bool,
    global_ratio_percent: u8,
    per_route_ratio_percent: HashMap<String, u8>,
    global_primary: AtomicU64,
    global_retries: AtomicU64,
    route_stats: Mutex<HashMap<String, (u64, u64)>>,
}

impl RetryBudget {
    pub fn new(
        enabled: bool,
        global_ratio_percent: u8,
        per_route_ratio_percent: HashMap<String, u8>,
    ) -> Self {
        Self {
            enabled,
            global_ratio_percent,
            per_route_ratio_percent,
            global_primary: AtomicU64::new(0),
            global_retries: AtomicU64::new(0),
            route_stats: Mutex::new(HashMap::new()),
        }
    }

    pub fn mark_primary(&self, route: &str) {
        self.global_primary.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut stats) = self.route_stats.lock() {
            let entry = stats.entry(route.to_string()).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(1);
        }
    }

    pub fn allow_retry(&self, route: &str) -> Result<(), RetryReason> {
        if !self.enabled {
            return Err(RetryReason::BudgetDenied);
        }

        let ratio = self
            .per_route_ratio_percent
            .get(route)
            .copied()
            .unwrap_or(self.global_ratio_percent);

        let primary = self.global_primary.load(Ordering::Relaxed);
        let retries = self.global_retries.load(Ordering::Relaxed);
        let global_limit = ((primary * ratio as u64) / 100).saturating_add(1);
        if retries >= global_limit {
            return Err(RetryReason::BudgetDenied);
        }

        let mut route_allowed = true;
        if let Ok(mut stats) = self.route_stats.lock() {
            let entry = stats.entry(route.to_string()).or_insert((0, 0));
            let route_limit = ((entry.0 * ratio as u64) / 100).saturating_add(1);
            if entry.1 >= route_limit {
                route_allowed = false;
            } else {
                entry.1 = entry.1.saturating_add(1);
            }
        }
        if !route_allowed {
            return Err(RetryReason::BudgetDenied);
        }

        self.global_retries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

pub struct BrownoutController {
    enabled: bool,
    trigger_inflight_percent: u8,
    recover_inflight_percent: u8,
    core_routes: HashSet<String>,
    active: AtomicBool,
}

impl BrownoutController {
    pub fn new(
        enabled: bool,
        trigger_inflight_percent: u8,
        recover_inflight_percent: u8,
        core_routes: Vec<String>,
    ) -> Self {
        Self {
            enabled,
            trigger_inflight_percent: trigger_inflight_percent.min(100),
            recover_inflight_percent: recover_inflight_percent.min(100),
            core_routes: core_routes.into_iter().collect(),
            active: AtomicBool::new(false),
        }
    }

    pub fn observe_admission_pressure(&self, inflight_percent: u8) {
        if !self.enabled {
            self.active.store(false, Ordering::Relaxed);
            return;
        }

        let active = self.active.load(Ordering::Relaxed);
        if !active && inflight_percent >= self.trigger_inflight_percent {
            self.active.store(true, Ordering::Relaxed);
            return;
        }
        if active && inflight_percent <= self.recover_inflight_percent {
            self.active.store(false, Ordering::Relaxed);
        }
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn route_allowed(&self, route: &str) -> bool {
        if !self.enabled || !self.is_active() {
            return true;
        }
        self.core_routes.contains(route)
    }
}

pub struct RuntimeResilience {
    pub adaptive_admission: Arc<AdaptiveAdmission>,
    pub route_queue: Arc<RouteQueueLimiter>,
    pub circuit_breakers: Arc<CircuitBreakers>,
    pub retry_budget: Arc<RetryBudget>,
    pub brownout: Arc<BrownoutController>,
    pub shed_retry_after_seconds: u32,
    pub allow_0rtt: bool,
    pub max_headers_count: usize,
    pub max_headers_bytes: usize,
    pub enforce_authority_host_match: bool,
    pub hedging_enabled: bool,
    pub hedging_delay: Duration,
    hedge_safe_methods: HashSet<String>,
    early_data_safe_methods: HashSet<String>,
    allowed_methods: HashSet<String>,
    denied_path_prefixes: Vec<String>,
    route_allowlist: HashSet<String>,
}

impl RuntimeResilience {
    pub fn from_config(config: &ResilienceConfig, global_limit: usize) -> Self {
        let adaptive = &config.adaptive_admission;
        let adaptive_max_limit = adaptive.max_limit.unwrap_or(global_limit);
        let admission = Arc::new(AdaptiveAdmission::new(
            adaptive.enabled,
            adaptive.min_limit,
            adaptive_max_limit.max(adaptive.min_limit),
            adaptive.increase_step,
            adaptive.decrease_step,
            adaptive.high_latency_ms,
        ));
        let route_queue = Arc::new(RouteQueueLimiter::new(
            config.route_queue.default_cap,
            config.route_queue.global_cap,
            config.route_queue.caps.clone(),
        ));
        let cb = &config.circuit_breaker;
        let circuit_breakers = Arc::new(CircuitBreakers::new(
            cb.enabled,
            cb.failure_threshold,
            Duration::from_millis(cb.open_ms.max(1)),
            cb.half_open_max_probes,
        ));
        let retry_budget = Arc::new(RetryBudget::new(
            config.retry_budget.enabled,
            config.retry_budget.ratio_percent,
            config.retry_budget.per_route_ratio_percent.clone(),
        ));
        let brownout = Arc::new(BrownoutController::new(
            config.brownout.enabled,
            config.brownout.trigger_inflight_percent,
            config.brownout.recover_inflight_percent,
            config.brownout.core_routes.clone(),
        ));

        let hedge_safe_methods = config
            .hedging
            .safe_methods
            .iter()
            .map(|method| method.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let early_data_safe_methods = config
            .protocol
            .early_data_safe_methods
            .iter()
            .map(|method| method.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let allowed_methods = config
            .protocol
            .allowed_methods
            .iter()
            .map(|method| method.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let route_allowlist = config
            .hedging
            .route_allowlist
            .iter()
            .cloned()
            .collect::<HashSet<_>>();

        Self {
            adaptive_admission: admission,
            route_queue,
            circuit_breakers,
            retry_budget,
            brownout,
            shed_retry_after_seconds: config.route_queue.shed_retry_after_seconds.max(1),
            allow_0rtt: config.protocol.allow_0rtt,
            max_headers_count: config.protocol.max_headers_count.max(1),
            max_headers_bytes: config.protocol.max_headers_bytes.max(1),
            enforce_authority_host_match: config.protocol.enforce_authority_host_match,
            hedging_enabled: config.hedging.enabled,
            hedging_delay: Duration::from_millis(config.hedging.delay_ms),
            hedge_safe_methods,
            early_data_safe_methods,
            allowed_methods,
            denied_path_prefixes: config.protocol.denied_path_prefixes.clone(),
            route_allowlist,
        }
    }

    pub fn hedging_allowed_for(&self, method: &str, route: &str, bodyless: bool) -> bool {
        if !self.hedging_enabled || self.brownout.is_active() || !bodyless {
            return false;
        }
        let safe_method = self
            .hedge_safe_methods
            .contains(&method.to_ascii_uppercase());
        if !safe_method {
            return false;
        }
        self.route_allowlist.is_empty() || self.route_allowlist.contains(route)
    }

    pub fn early_data_allowed_for(&self, method: &str) -> bool {
        self.allow_0rtt
            && self
                .early_data_safe_methods
                .contains(&method.to_ascii_uppercase())
    }

    pub fn method_allowed(&self, method: &str) -> bool {
        self.allowed_methods.is_empty()
            || self.allowed_methods.contains(&method.to_ascii_uppercase())
    }

    pub fn path_denied(&self, path: &str) -> bool {
        self.denied_path_prefixes
            .iter()
            .any(|prefix| path.starts_with(prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_admission_adjusts_limit() {
        let admission = AdaptiveAdmission::new(true, 2, 10, 2, 3, 100);
        assert_eq!(admission.current_limit(), 10);
        admission.observe(Duration::from_millis(150), false);
        assert_eq!(admission.current_limit(), 7);
        admission.observe(Duration::from_millis(10), false);
        assert_eq!(admission.current_limit(), 9);
    }

    #[test]
    fn runtime_resilience_honors_adaptive_max_limit_override() {
        let mut cfg = ResilienceConfig::default();
        cfg.adaptive_admission.max_limit = Some(256);
        let runtime = RuntimeResilience::from_config(&cfg, 4096);
        assert_eq!(runtime.adaptive_admission.current_limit(), 256);
    }

    #[test]
    fn route_queue_cap_enforced() {
        let limiter = Arc::new(RouteQueueLimiter::new(1, 10, HashMap::new()));
        let _p1 = limiter.try_acquire("api").expect("first permit");
        assert!(matches!(
            limiter.try_acquire("api"),
            Err(RouteQueueRejection::RouteCap)
        ));
    }

    #[test]
    fn route_queue_global_cap_enforced() {
        let limiter = Arc::new(RouteQueueLimiter::new(10, 2, HashMap::new()));
        let _p1 = limiter.try_acquire("api").expect("first permit");
        let _p2 = limiter.try_acquire("admin").expect("second permit");
        assert!(matches!(
            limiter.try_acquire("api"),
            Err(RouteQueueRejection::GlobalCap)
        ));
    }

    #[test]
    fn circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreakers::new(true, 2, Duration::from_secs(1), 1);
        assert!(cb.allow_request("b1"));
        cb.record_failure("b1");
        assert!(cb.allow_request("b1"));
        cb.record_failure("b1");
        assert!(!cb.allow_request("b1"));
    }

    #[test]
    fn retry_budget_respects_ratio() {
        let rb = RetryBudget::new(true, 50, HashMap::new());
        rb.mark_primary("api");
        assert!(rb.allow_retry("api").is_ok());
        assert!(rb.allow_retry("api").is_err());
    }

    #[test]
    fn brownout_preserves_core_routes() {
        let controller = BrownoutController::new(true, 90, 60, vec!["core".to_string()]);
        controller.observe_admission_pressure(95);
        assert!(controller.is_active());
        assert!(controller.route_allowed("core"));
        assert!(!controller.route_allowed("non_core"));
    }

    #[test]
    fn runtime_resilience_method_and_path_policy_checks() {
        let mut cfg = ResilienceConfig::default();
        cfg.protocol.allowed_methods = vec!["GET".to_string()];
        cfg.protocol.denied_path_prefixes = vec!["/admin".to_string()];
        cfg.protocol.allow_0rtt = true;
        cfg.protocol.early_data_safe_methods = vec!["GET".to_string()];

        let runtime = RuntimeResilience::from_config(&cfg, 64);
        assert!(runtime.method_allowed("GET"));
        assert!(!runtime.method_allowed("POST"));
        assert!(runtime.path_denied("/admin/secret"));
        assert!(!runtime.path_denied("/api"));
        assert!(runtime.early_data_allowed_for("GET"));
        assert!(!runtime.early_data_allowed_for("POST"));
    }
}
