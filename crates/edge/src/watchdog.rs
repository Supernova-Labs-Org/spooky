use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

use spooky_config::config::Watchdog as WatchdogConfig;

#[derive(Debug, Clone)]
pub struct WatchdogRuntimeConfig {
    pub enabled: bool,
    pub check_interval_ms: u64,
    pub poll_stall_timeout_ms: u64,
    pub timeout_error_rate_percent: u8,
    pub min_requests_per_window: u64,
    pub overload_inflight_percent: u8,
    pub unhealthy_consecutive_windows: u32,
    pub drain_grace_ms: u64,
    pub restart_cooldown_ms: u64,
    pub restart_hook: Option<String>,
}

impl From<&WatchdogConfig> for WatchdogRuntimeConfig {
    fn from(value: &WatchdogConfig) -> Self {
        Self {
            enabled: value.enabled,
            check_interval_ms: value.check_interval_ms.max(1),
            poll_stall_timeout_ms: value.poll_stall_timeout_ms.max(1),
            timeout_error_rate_percent: value.timeout_error_rate_percent.min(100),
            min_requests_per_window: value.min_requests_per_window.max(1),
            overload_inflight_percent: value.overload_inflight_percent.min(100),
            unhealthy_consecutive_windows: value.unhealthy_consecutive_windows.max(1),
            drain_grace_ms: value.drain_grace_ms.max(1),
            restart_cooldown_ms: value.restart_cooldown_ms.max(1),
            restart_hook: value.restart_hook.clone(),
        }
    }
}

pub struct WatchdogCoordinator {
    enabled: bool,
    restart_cooldown_ms: u64,
    last_poll_progress_ms: AtomicU64,
    degraded: AtomicBool,
    restart_requested: AtomicBool,
    restart_requested_at_ms: AtomicU64,
    last_restart_at_ms: AtomicU64,
    expected_workers: AtomicUsize,
    drained_workers: AtomicUsize,
    restart_reason: Mutex<String>,
}

impl WatchdogCoordinator {
    pub fn new(config: &WatchdogConfig) -> Self {
        let now_ms = now_millis();
        Self {
            enabled: config.enabled,
            restart_cooldown_ms: config.restart_cooldown_ms.max(1),
            last_poll_progress_ms: AtomicU64::new(now_ms),
            degraded: AtomicBool::new(false),
            restart_requested: AtomicBool::new(false),
            restart_requested_at_ms: AtomicU64::new(0),
            last_restart_at_ms: AtomicU64::new(0),
            expected_workers: AtomicUsize::new(1),
            drained_workers: AtomicUsize::new(0),
            restart_reason: Mutex::new(String::new()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn set_expected_workers(&self, workers: usize) {
        self.expected_workers
            .store(workers.max(1), Ordering::Relaxed);
    }

    pub fn mark_poll_progress(&self) {
        self.last_poll_progress_ms
            .store(now_millis(), Ordering::Relaxed);
    }

    pub fn last_poll_progress_ms(&self) -> u64 {
        self.last_poll_progress_ms.load(Ordering::Relaxed)
    }

    pub fn set_degraded(&self, degraded: bool) {
        self.degraded.store(degraded, Ordering::Relaxed);
    }

    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }

    pub fn request_restart(&self, reason: &str) -> bool {
        if !self.enabled {
            return false;
        }
        let now_ms = now_millis();
        let last_restart = self.last_restart_at_ms.load(Ordering::Relaxed);
        if last_restart != 0 && now_ms.saturating_sub(last_restart) < self.restart_cooldown_ms {
            return false;
        }
        if self
            .restart_requested
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }

        self.restart_requested_at_ms
            .store(now_ms, Ordering::Relaxed);
        self.drained_workers.store(0, Ordering::Relaxed);
        if let Ok(mut current) = self.restart_reason.lock() {
            *current = reason.to_string();
        }
        true
    }

    pub fn restart_requested(&self) -> bool {
        self.restart_requested.load(Ordering::Relaxed)
    }

    pub fn restart_reason(&self) -> String {
        self.restart_reason
            .lock()
            .map(|reason| reason.clone())
            .unwrap_or_else(|_| String::from("watchdog restart requested"))
    }

    pub fn restart_requested_at_ms(&self) -> u64 {
        self.restart_requested_at_ms.load(Ordering::Relaxed)
    }

    pub fn mark_worker_drained(&self) {
        if !self.restart_requested() {
            return;
        }
        self.drained_workers.fetch_add(1, Ordering::Relaxed);
    }

    pub fn workers_drained(&self) -> bool {
        let expected = self.expected_workers.load(Ordering::Relaxed).max(1);
        self.drained_workers.load(Ordering::Relaxed) >= expected
    }

    pub fn complete_restart_cycle(&self) {
        self.last_restart_at_ms
            .store(now_millis(), Ordering::Relaxed);
        self.restart_requested.store(false, Ordering::Relaxed);
        self.restart_requested_at_ms.store(0, Ordering::Relaxed);
        self.drained_workers.store(0, Ordering::Relaxed);
        self.degraded.store(false, Ordering::Relaxed);
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_request_respects_single_pending_cycle() {
        let cfg = WatchdogConfig {
            enabled: true,
            check_interval_ms: 1000,
            poll_stall_timeout_ms: 5000,
            timeout_error_rate_percent: 60,
            min_requests_per_window: 20,
            overload_inflight_percent: 90,
            unhealthy_consecutive_windows: 3,
            drain_grace_ms: 5_000,
            restart_cooldown_ms: 60_000,
            restart_hook: None,
        };
        let watchdog = WatchdogCoordinator::new(&cfg);
        assert!(watchdog.request_restart("overload"));
        assert!(!watchdog.request_restart("stall"));
    }

    #[test]
    fn worker_drain_tracking_uses_expected_worker_count() {
        let cfg = WatchdogConfig {
            enabled: true,
            check_interval_ms: 1000,
            poll_stall_timeout_ms: 5000,
            timeout_error_rate_percent: 60,
            min_requests_per_window: 20,
            overload_inflight_percent: 90,
            unhealthy_consecutive_windows: 3,
            drain_grace_ms: 5_000,
            restart_cooldown_ms: 60_000,
            restart_hook: None,
        };
        let watchdog = WatchdogCoordinator::new(&cfg);
        watchdog.set_expected_workers(2);
        assert!(watchdog.request_restart("stall"));
        watchdog.mark_worker_drained();
        assert!(!watchdog.workers_drained());
        watchdog.mark_worker_drained();
        assert!(watchdog.workers_drained());
    }
}
