use bytes::Bytes;
use std::{
    cell::Cell,
    collections::{HashMap, HashSet, VecDeque},
    convert::Infallible,
    env,
    net::UdpSocket,
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use core::net::SocketAddr;

use hyper::body::{Body, Frame};
use spooky_config::backend_endpoint::BackendEndpoint;
use spooky_config::config::Config;
use spooky_errors::ProxyError;
use spooky_lb::UpstreamPool;
use spooky_transport::h2_pool::H2Pool;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tracing::Span;

use crate::cid_radix::CidRadix;
use crate::constants::MAX_DATAGRAM_SIZE_BYTES;
use crate::resilience::{AdaptivePermit, RouteQueuePermit, RuntimeResilience};
use crate::watchdog::WatchdogCoordinator;

static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
const FNV_OFFSET_BASIS_64: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME_64: u64 = 0x0000_0100_0000_01b3;

pub fn stable_hash64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS_64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME_64);
    }
    hash
}

pub fn stable_hash_socket_addr(addr: &SocketAddr) -> u64 {
    match addr {
        SocketAddr::V4(v4) => {
            let mut bytes = [0u8; 7];
            bytes[0] = 4;
            bytes[1..5].copy_from_slice(&v4.ip().octets());
            bytes[5..7].copy_from_slice(&v4.port().to_be_bytes());
            stable_hash64(&bytes)
        }
        SocketAddr::V6(v6) => {
            let mut bytes = [0u8; 31];
            bytes[0] = 6;
            bytes[1..17].copy_from_slice(&v6.ip().octets());
            bytes[17..19].copy_from_slice(&v6.port().to_be_bytes());
            bytes[19..23].copy_from_slice(&v6.flowinfo().to_be_bytes());
            bytes[23..27].copy_from_slice(&v6.scope_id().to_be_bytes());
            stable_hash64(&bytes)
        }
    }
}

/// A streaming HTTP body backed by a tokio mpsc channel.
/// The quiche Data handler sends chunks through the sender;
/// hyper reads them from the receiver as the H2 request body.
pub struct ChannelBody {
    rx: mpsc::Receiver<Bytes>,
}

impl ChannelBody {
    pub fn channel(buffer: usize) -> (mpsc::Sender<Bytes>, Self) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, Self { rx })
    }
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(chunk)) => Poll::Ready(Some(Ok(Frame::data(chunk)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub mod benchmark;
pub mod cid_radix;
pub mod constants;
pub mod quic_listener;
mod resilience;
mod route_index;
mod watchdog;

pub use quic_listener::configure_async_runtime;

pub struct SharedRuntimeState {
    pub(crate) h2_pool: Arc<H2Pool>,
    pub(crate) backend_endpoints: Arc<HashMap<String, BackendEndpoint>>,
    pub(crate) upstream_pools: HashMap<String, Arc<RwLock<UpstreamPool>>>,
    pub(crate) upstream_inflight: HashMap<String, Arc<Semaphore>>,
    pub(crate) global_inflight: Arc<Semaphore>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) resilience: Arc<RuntimeResilience>,
    pub(crate) watchdog: Arc<WatchdogCoordinator>,
}

impl SharedRuntimeState {
    pub fn bind_metrics_worker_slot(&self, slot: usize) {
        self.metrics.bind_worker_slot(slot);
    }

    pub fn inc_ingress_queue_drop(&self) {
        self.metrics.inc_ingress_queue_drop();
    }

    pub fn inc_ingress_queue_drop_bytes(&self, bytes: usize) {
        self.metrics.inc_ingress_queue_drop_bytes(bytes);
    }

    pub fn set_ingress_queue_bytes(&self, bytes: usize) {
        self.metrics.set_ingress_queue_bytes(bytes);
    }

    pub fn snapshot_backend_health(&self) -> (usize, usize) {
        let mut healthy = 0usize;
        let mut total = 0usize;

        for pool in self.upstream_pools.values() {
            let guard = match pool.read() {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            let pool_total = guard.pool.len();
            total = total.saturating_add(pool_total);
            healthy = healthy.saturating_add(guard.pool.healthy_len().min(pool_total));
        }

        (healthy, total)
    }
}

pub struct QUICListener {
    pub socket: UdpSocket,
    pub local_addr: SocketAddr,
    pub config: Config,
    pub quic_config: quiche::Config,
    pub h3_config: Arc<quiche::h3::Config>,
    pub h2_pool: Arc<H2Pool>,
    pub backend_endpoints: Arc<HashMap<String, BackendEndpoint>>,
    pub upstream_pools: HashMap<String, Arc<RwLock<UpstreamPool>>>,
    pub upstream_inflight: HashMap<String, Arc<Semaphore>>,
    pub global_inflight: Arc<Semaphore>,
    pub(crate) routing_index: route_index::RouteIndex,
    pub metrics: Arc<Metrics>,
    pub resilience: Arc<RuntimeResilience>,
    pub watchdog: Arc<WatchdogCoordinator>,
    pub draining: bool,
    pub drain_start: Option<Instant>,
    pub watchdog_worker_drained: bool,
    pub drain_timeout: Duration,
    pub backend_timeout: Duration,
    pub backend_body_idle_timeout: Duration,
    pub backend_body_total_timeout: Duration,
    pub client_body_idle_timeout: Duration,
    pub backend_total_request_timeout: Duration,
    pub max_active_connections: usize,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub request_buffer_global_cap_bytes: usize,
    pub unknown_length_response_prebuffer_bytes: usize,
    pub require_client_cert: bool,

    pub recv_buf: [u8; MAX_DATAGRAM_SIZE_BYTES],
    pub send_buf: [u8; MAX_DATAGRAM_SIZE_BYTES],

    pub connections: HashMap<Arc<[u8]>, QuicConnection>, // KEY: SCID(server connection id)
    pub cid_routes: HashMap<Arc<[u8]>, Arc<[u8]>>,       // KEY: alias SCID, VALUE: primary SCID
    pub peer_routes: HashMap<SocketAddr, Arc<[u8]>>,     // KEY: peer address, VALUE: primary SCID
    pub cid_radix: CidRadix,
    pub(crate) conn_rate_limiter: crate::quic_listener::TokenBucket,
}

pub struct QuicConnection {
    pub quic: quiche::Connection,
    pub h3: Option<quiche::h3::Connection>,
    pub h3_config: Arc<quiche::h3::Config>,
    pub streams: HashMap<u64, RequestEnvelope>,

    pub peer_address: SocketAddr,
    pub last_activity: Instant,
    pub primary_scid: Arc<[u8]>,
    pub routing_scids: HashSet<Arc<[u8]>>,
    pub packets_since_rotation: u64,
    pub last_scid_rotation: Instant,
}

/// Result type returned by the in-flight H2 forwarding task.
pub type ForwardResult =
    Result<(http::StatusCode, http::HeaderMap, hyper::body::Incoming), ProxyError>;

#[derive(Debug, Clone, Copy, Default)]
pub struct HedgeTelemetry {
    pub launched: bool,
    pub hedge_won: bool,
    pub hedge_wasted: bool,
    pub primary_won_after_trigger: bool,
    pub primary_late_ms: u64,
}

pub struct UpstreamResult {
    pub forward: ForwardResult,
    pub hedge: HedgeTelemetry,
    pub retry_count: u8,
    /// Set when a retry was attempted; the error reason that triggered it.
    pub retry_attempt_reason: Option<RetryReason>,
    /// Set when a retry was denied; the first denial reason encountered.
    pub retry_denial_reason: Option<RetryReason>,
}

/// Lifecycle phase of a single HTTP/3 request stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamPhase {
    /// Still receiving request headers/body from the QUIC client.
    ReceivingRequest,
    /// Request fully received; waiting for the upstream H2 response.
    AwaitingUpstream,
    /// Upstream responded; streaming response back to the QUIC client.
    SendingResponse,
    /// Stream finished cleanly.
    Completed,
    /// Stream terminated with an error.
    Failed,
}

/// A chunk of the upstream response being streamed back to the client.
#[derive(Debug)]
pub enum ResponseChunk {
    /// Emit downstream response headers (used when headers are deferred until
    /// body-size validation completes).
    Start {
        status: http::StatusCode,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Data(Bytes),
    End,
    Error(ProxyError),
}

pub struct RequestEnvelope {
    pub request_id: u64,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub traceparent: Option<String>,
    pub trace_span: Option<Span>,

    pub method: String,
    pub path: String,
    pub authority: Option<String>,
    /// Sender half of the body channel.  Dropping it signals end-of-body to hyper.
    pub body_tx: Option<mpsc::Sender<Bytes>>,
    /// Body chunks that arrived before the channel had capacity.
    pub body_buf: VecDeque<Bytes>,
    /// Current bytes held in `body_buf`.
    pub body_buf_bytes: usize,
    /// Total body bytes received on this stream (buffered + already forwarded).
    pub body_bytes_received: usize,
    /// Last observed request-body byte arrival time.
    pub last_body_activity: Instant,
    /// Resolved backend address and index (for health marking on response).
    pub backend_addr: Option<String>,
    pub backend_index: Option<usize>,
    pub upstream_name: Option<String>,
    pub route_reason: Option<String>,
    pub route_path_len: Option<usize>,
    pub route_host_specific: Option<bool>,
    pub backend_lb: Option<String>,
    pub upstream_pool: Option<Arc<RwLock<UpstreamPool>>>,
    pub routing_transparency_enabled: bool,
    pub routing_transparency_include_reason: bool,
    pub response_status: Option<u16>,
    pub backend_request_finished: bool,
    pub global_inflight_permit: Option<OwnedSemaphorePermit>,
    pub upstream_inflight_permit: Option<OwnedSemaphorePermit>,
    pub adaptive_admission_permit: Option<AdaptivePermit>,
    pub route_queue_permit: Option<RouteQueuePermit>,
    pub start: Instant,
    pub total_request_deadline: Instant,
    pub bodyless_mode: bool,

    pub retry_count: u8,
    pub error_kind: Option<&'static str>,

    /// Current lifecycle phase of this stream.
    pub phase: StreamPhase,
    /// True once the client has sent FIN on the request stream.
    pub request_fin_received: bool,
    /// Receives the upstream H2 response (status, headers, body stream).
    pub upstream_result_rx: Option<oneshot::Receiver<UpstreamResult>>,
    /// Receives response body chunks to write back over QUIC.
    pub response_chunk_rx: Option<mpsc::Receiver<ResponseChunk>>,
    /// True once downstream response headers are emitted on this stream.
    pub response_headers_sent: bool,
    /// A chunk that could not be written due to QUIC send backpressure; retried next poll.
    pub pending_chunk: Option<ResponseChunk>,
}

impl RequestEnvelope {
    pub fn request_id(&self) -> u64 {
        self.request_id
    }
}

#[derive(Debug)]
pub enum HealthClassification {
    Success, // 2xx, 3xx responses
    Failure, // 5xx responses, Transport/Pool/Timeout errors
    Neutral, // 4xx responses, Bridge/TLS errors
}

pub fn outcome_from_status(status: http::StatusCode) -> HealthClassification {
    if status.is_server_error() {
        // 5xx
        HealthClassification::Failure
    } else if status.is_client_error() {
        // 4xx
        HealthClassification::Neutral
    } else {
        // 2xx, 3xx
        HealthClassification::Success
    }
}

pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_success: AtomicU64,
    pub requests_failure: AtomicU64,
    pub request_validation_rejects: AtomicU64,
    pub policy_denied: AtomicU64,
    pub early_data_accepted: AtomicU64,
    pub early_data_rejected: AtomicU64,
    pub health_checks_total: AtomicU64,
    pub health_checks_success: AtomicU64,
    pub health_checks_failure: AtomicU64,
    pub backend_timeouts: AtomicU64,
    pub backend_errors: AtomicU64,
    pub overload_shed: AtomicU64,
    pub overload_shed_brownout: AtomicU64,
    pub overload_shed_adaptive: AtomicU64,
    pub overload_shed_route_cap: AtomicU64,
    pub overload_shed_route_global_cap: AtomicU64,
    pub overload_shed_global_inflight: AtomicU64,
    pub overload_shed_upstream_inflight: AtomicU64,
    pub overload_shed_backend_inflight: AtomicU64,
    pub overload_shed_request_buffer: AtomicU64,
    pub overload_shed_response_prebuffer: AtomicU64,
    pub overload_shed_connection_cap: AtomicU64,
    pub active_connections: AtomicU64,
    pub connection_cap_rejects: AtomicU64,
    pub hedge_triggered: AtomicU64,
    pub hedge_won: AtomicU64,
    pub hedge_wasted: AtomicU64,
    pub hedge_primary_won_after_trigger: AtomicU64,
    pub hedge_primary_late_ms_total: AtomicU64,
    pub hedge_primary_late_samples: AtomicU64,
    pub ingress_packets_total: AtomicU64,
    pub ingress_queue_drops: AtomicU64,
    pub ingress_queue_drop_bytes: AtomicU64,
    pub ingress_queue_bytes: AtomicU64,
    pub ingress_bad_header_total: AtomicU64,
    pub ingress_rate_limited_total: AtomicU64,
    pub ingress_unroutable_total: AtomicU64,
    pub ingress_draining_drops_total: AtomicU64,
    pub ingress_connection_create_failed_total: AtomicU64,
    pub ingress_version_neg_failed_total: AtomicU64,
    pub request_buffered_bytes: AtomicU64,
    pub request_buffered_high_watermark_bytes: AtomicU64,
    pub request_buffer_limit_rejects: AtomicU64,
    pub response_prebuffer_limit_rejects: AtomicU64,
    pub scid_rotations: AtomicU64,
    pub watchdog_restart_requests: AtomicU64,
    pub watchdog_restart_hooks: AtomicU64,
    pub watchdog_degraded_windows: AtomicU64,
    pub runtime_panics: AtomicU64,
    pub retries_total: AtomicU64,
    pub retry_denied_budget: AtomicU64,
    pub retry_denied_no_bodyless: AtomicU64,
    pub retry_denied_no_alternate: AtomicU64,
    pub retry_reason_timeout: AtomicU64,
    pub retry_reason_transport: AtomicU64,
    pub retry_reason_pool: AtomicU64,
    pub circuit_breaker_rejected_total: AtomicU64,
    pub brownout_active: AtomicU64,
    pub health_failure_5xx: AtomicU64,
    pub health_failure_timeout: AtomicU64,
    pub health_failure_transport: AtomicU64,
    pub health_failure_tls: AtomicU64,
    route_latency_sample_every: u64,
    route_latency_sample_counter: AtomicU64,
    route_labels: Vec<String>,
    route_label_to_id: HashMap<String, usize>,
    route_stats: Vec<RouteStatsAtomic>,
    unrouted_route_id: usize,
    worker_labels: Vec<String>,
    worker_stats: Vec<WorkerStatsAtomic>,
}

const LATENCY_BUCKETS_MS: [u64; 14] = [
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000,
];
const ROUTE_LATENCY_SAMPLE_EVERY_ENV: &str = "SPOOKY_ROUTE_LATENCY_SAMPLE_EVERY";

#[derive(Default, Clone)]
struct RouteStats {
    requests_total: u64,
    success: u64,
    failure: u64,
    timeout: u64,
    backend_error: u64,
    overload_shed: u64,
    latency_buckets: [u64; LATENCY_BUCKETS_MS.len() + 1],
}

#[derive(Default, Clone)]
struct WorkerStats {
    requests_total: u64,
    requests_success: u64,
    requests_failure: u64,
    ingress_packets_total: u64,
    ingress_queue_drops: u64,
    ingress_queue_drop_bytes: u64,
}

struct RouteStatsAtomic {
    requests_total: AtomicU64,
    success: AtomicU64,
    failure: AtomicU64,
    timeout: AtomicU64,
    backend_error: AtomicU64,
    overload_shed: AtomicU64,
    latency_buckets: [AtomicU64; LATENCY_BUCKETS_MS.len() + 1],
}

impl RouteStatsAtomic {
    fn new() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            success: AtomicU64::new(0),
            failure: AtomicU64::new(0),
            timeout: AtomicU64::new(0),
            backend_error: AtomicU64::new(0),
            overload_shed: AtomicU64::new(0),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn snapshot(&self) -> RouteStats {
        let mut latency_buckets = [0u64; LATENCY_BUCKETS_MS.len() + 1];
        for (idx, bucket) in self.latency_buckets.iter().enumerate() {
            latency_buckets[idx] = bucket.load(Ordering::Relaxed);
        }

        RouteStats {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            failure: self.failure.load(Ordering::Relaxed),
            timeout: self.timeout.load(Ordering::Relaxed),
            backend_error: self.backend_error.load(Ordering::Relaxed),
            overload_shed: self.overload_shed.load(Ordering::Relaxed),
            latency_buckets,
        }
    }
}

struct WorkerStatsAtomic {
    requests_total: AtomicU64,
    requests_success: AtomicU64,
    requests_failure: AtomicU64,
    ingress_packets_total: AtomicU64,
    ingress_queue_drops: AtomicU64,
    ingress_queue_drop_bytes: AtomicU64,
}

impl WorkerStatsAtomic {
    fn new() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            requests_success: AtomicU64::new(0),
            requests_failure: AtomicU64::new(0),
            ingress_packets_total: AtomicU64::new(0),
            ingress_queue_drops: AtomicU64::new(0),
            ingress_queue_drop_bytes: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> WorkerStats {
        WorkerStats {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            requests_success: self.requests_success.load(Ordering::Relaxed),
            requests_failure: self.requests_failure.load(Ordering::Relaxed),
            ingress_packets_total: self.ingress_packets_total.load(Ordering::Relaxed),
            ingress_queue_drops: self.ingress_queue_drops.load(Ordering::Relaxed),
            ingress_queue_drop_bytes: self.ingress_queue_drop_bytes.load(Ordering::Relaxed),
        }
    }
}

pub enum RouteOutcome {
    Success,
    Failure,
    Timeout,
    BackendError,
    OverloadShed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverloadShedReason {
    Brownout,
    AdaptiveAdmission,
    RouteCap,
    RouteGlobalCap,
    GlobalInflight,
    UpstreamInflight,
    BackendInflight,
    RequestBufferCap,
    ResponsePrebufferCap,
    ConnectionCap,
}

#[derive(Clone, Copy, Debug)]
pub enum RetryReason {
    BackendTimeout,
    BackendTransport,
    BackendPool,
    BudgetDenied,
    NotBodylessMode,
    NoAlternateBackend,
}

pub use spooky_lb::HealthFailureReason;

impl Default for Metrics {
    fn default() -> Self {
        Self::new(1, [String::from("unrouted")])
    }
}

thread_local! {
    static WORKER_METRICS_SLOT: Cell<usize> = const { Cell::new(0) };
}

impl Metrics {
    pub fn new<I>(worker_slots: usize, route_labels: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let route_latency_sample_every = env::var(ROUTE_LATENCY_SAMPLE_EVERY_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1);

        let mut route_labels_dedup = Vec::new();
        let mut route_label_to_id = HashMap::new();
        for raw in route_labels {
            let label = raw.trim();
            if label.is_empty() || route_label_to_id.contains_key(label) {
                continue;
            }
            let id = route_labels_dedup.len();
            route_labels_dedup.push(label.to_string());
            route_label_to_id.insert(label.to_string(), id);
        }
        if !route_label_to_id.contains_key("unrouted") {
            let id = route_labels_dedup.len();
            route_labels_dedup.push("unrouted".to_string());
            route_label_to_id.insert("unrouted".to_string(), id);
        }
        let unrouted_route_id = route_label_to_id.get("unrouted").copied().unwrap_or(0);

        let worker_slots = worker_slots.max(1);
        let worker_labels = (0..worker_slots)
            .map(|idx| format!("worker-{idx}"))
            .collect::<Vec<_>>();
        let worker_stats = (0..worker_slots)
            .map(|_| WorkerStatsAtomic::new())
            .collect::<Vec<_>>();
        let route_stats = route_labels_dedup
            .iter()
            .map(|_| RouteStatsAtomic::new())
            .collect::<Vec<_>>();

        Self {
            requests_total: AtomicU64::new(0),
            requests_success: AtomicU64::new(0),
            requests_failure: AtomicU64::new(0),
            request_validation_rejects: AtomicU64::new(0),
            policy_denied: AtomicU64::new(0),
            early_data_accepted: AtomicU64::new(0),
            early_data_rejected: AtomicU64::new(0),
            health_checks_total: AtomicU64::new(0),
            health_checks_success: AtomicU64::new(0),
            health_checks_failure: AtomicU64::new(0),
            backend_timeouts: AtomicU64::new(0),
            backend_errors: AtomicU64::new(0),
            overload_shed: AtomicU64::new(0),
            overload_shed_brownout: AtomicU64::new(0),
            overload_shed_adaptive: AtomicU64::new(0),
            overload_shed_route_cap: AtomicU64::new(0),
            overload_shed_route_global_cap: AtomicU64::new(0),
            overload_shed_global_inflight: AtomicU64::new(0),
            overload_shed_upstream_inflight: AtomicU64::new(0),
            overload_shed_backend_inflight: AtomicU64::new(0),
            overload_shed_request_buffer: AtomicU64::new(0),
            overload_shed_response_prebuffer: AtomicU64::new(0),
            overload_shed_connection_cap: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            connection_cap_rejects: AtomicU64::new(0),
            hedge_triggered: AtomicU64::new(0),
            hedge_won: AtomicU64::new(0),
            hedge_wasted: AtomicU64::new(0),
            hedge_primary_won_after_trigger: AtomicU64::new(0),
            hedge_primary_late_ms_total: AtomicU64::new(0),
            hedge_primary_late_samples: AtomicU64::new(0),
            ingress_packets_total: AtomicU64::new(0),
            ingress_queue_drops: AtomicU64::new(0),
            ingress_queue_drop_bytes: AtomicU64::new(0),
            ingress_queue_bytes: AtomicU64::new(0),
            ingress_bad_header_total: AtomicU64::new(0),
            ingress_rate_limited_total: AtomicU64::new(0),
            ingress_unroutable_total: AtomicU64::new(0),
            ingress_draining_drops_total: AtomicU64::new(0),
            ingress_connection_create_failed_total: AtomicU64::new(0),
            ingress_version_neg_failed_total: AtomicU64::new(0),
            request_buffered_bytes: AtomicU64::new(0),
            request_buffered_high_watermark_bytes: AtomicU64::new(0),
            request_buffer_limit_rejects: AtomicU64::new(0),
            response_prebuffer_limit_rejects: AtomicU64::new(0),
            scid_rotations: AtomicU64::new(0),
            watchdog_restart_requests: AtomicU64::new(0),
            watchdog_restart_hooks: AtomicU64::new(0),
            watchdog_degraded_windows: AtomicU64::new(0),
            runtime_panics: AtomicU64::new(0),
            retries_total: AtomicU64::new(0),
            retry_denied_budget: AtomicU64::new(0),
            retry_denied_no_bodyless: AtomicU64::new(0),
            retry_denied_no_alternate: AtomicU64::new(0),
            retry_reason_timeout: AtomicU64::new(0),
            retry_reason_transport: AtomicU64::new(0),
            retry_reason_pool: AtomicU64::new(0),
            circuit_breaker_rejected_total: AtomicU64::new(0),
            brownout_active: AtomicU64::new(0),
            health_failure_5xx: AtomicU64::new(0),
            health_failure_timeout: AtomicU64::new(0),
            health_failure_transport: AtomicU64::new(0),
            health_failure_tls: AtomicU64::new(0),
            route_latency_sample_every,
            route_latency_sample_counter: AtomicU64::new(0),
            route_labels: route_labels_dedup,
            route_label_to_id,
            route_stats,
            unrouted_route_id,
            worker_labels,
            worker_stats,
        }
    }

    pub fn bind_worker_slot(&self, slot: usize) {
        let max_index = self.worker_stats.len().saturating_sub(1);
        WORKER_METRICS_SLOT.with(|current| current.set(slot.min(max_index)));
    }

    pub fn inc_total(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.inc_worker_requests_total();
    }

    pub fn inc_success(&self) {
        self.requests_success.fetch_add(1, Ordering::Relaxed);
        self.inc_worker_requests_success();
    }

    pub fn inc_failure(&self) {
        self.requests_failure.fetch_add(1, Ordering::Relaxed);
        self.inc_worker_requests_failure();
    }

    pub fn inc_request_validation_reject(&self) {
        self.request_validation_rejects
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_policy_denied(&self) {
        self.policy_denied.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_early_data_accepted(&self) {
        self.early_data_accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_early_data_rejected(&self) {
        self.early_data_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_health_check_success(&self) {
        self.health_checks_total.fetch_add(1, Ordering::Relaxed);
        self.health_checks_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_health_check_failure(&self) {
        self.health_checks_total.fetch_add(1, Ordering::Relaxed);
        self.health_checks_failure.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_timeout(&self) {
        self.backend_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_backend_error(&self) {
        self.backend_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_overload_shed(&self) {
        self.overload_shed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_overload_shed_reason(&self, reason: OverloadShedReason) {
        self.overload_shed.fetch_add(1, Ordering::Relaxed);
        match reason {
            OverloadShedReason::Brownout => {
                self.overload_shed_brownout.fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::AdaptiveAdmission => {
                self.overload_shed_adaptive.fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::RouteCap => {
                self.overload_shed_route_cap.fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::RouteGlobalCap => {
                self.overload_shed_route_global_cap
                    .fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::GlobalInflight => {
                self.overload_shed_global_inflight
                    .fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::UpstreamInflight => {
                self.overload_shed_upstream_inflight
                    .fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::BackendInflight => {
                self.overload_shed_backend_inflight
                    .fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::RequestBufferCap => {
                self.overload_shed_request_buffer
                    .fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::ResponsePrebufferCap => {
                self.overload_shed_response_prebuffer
                    .fetch_add(1, Ordering::Relaxed);
            }
            OverloadShedReason::ConnectionCap => {
                self.overload_shed_connection_cap
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn set_active_connections(&self, count: usize) {
        self.active_connections
            .store(count as u64, Ordering::Relaxed);
    }

    pub fn inc_connection_cap_reject(&self) {
        self.connection_cap_rejects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_hedge_triggered(&self) {
        self.hedge_triggered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_hedge_won(&self) {
        self.hedge_won.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_hedge_wasted(&self) {
        self.hedge_wasted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_hedge_primary_won_after_trigger(&self) {
        self.hedge_primary_won_after_trigger
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_hedge_primary_late_ms(&self, late_ms: u64) {
        self.hedge_primary_late_ms_total
            .fetch_add(late_ms, Ordering::Relaxed);
        self.hedge_primary_late_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ingress_packet(&self) {
        self.ingress_packets_total.fetch_add(1, Ordering::Relaxed);
        self.inc_worker_ingress_packets_total();
    }

    pub fn inc_ingress_queue_drop(&self) {
        self.ingress_queue_drops.fetch_add(1, Ordering::Relaxed);
        self.inc_worker_ingress_queue_drops();
    }

    pub fn inc_ingress_queue_drop_bytes(&self, bytes: usize) {
        self.ingress_queue_drop_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.inc_worker_ingress_queue_drop_bytes(bytes as u64);
    }

    pub fn set_ingress_queue_bytes(&self, bytes: usize) {
        self.ingress_queue_bytes
            .store(bytes as u64, Ordering::Relaxed);
    }

    pub fn inc_ingress_bad_header(&self) {
        self.ingress_bad_header_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ingress_rate_limited(&self) {
        self.ingress_rate_limited_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ingress_unroutable(&self) {
        self.ingress_unroutable_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ingress_draining_drop(&self) {
        self.ingress_draining_drops_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ingress_connection_create_failed(&self) {
        self.ingress_connection_create_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ingress_version_neg_failed(&self) {
        self.ingress_version_neg_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn current_worker_stats(&self) -> Option<&WorkerStatsAtomic> {
        let idx = WORKER_METRICS_SLOT.with(|current| current.get());
        self.worker_stats
            .get(idx)
            .or_else(|| self.worker_stats.first())
    }

    fn inc_worker_requests_total(&self) {
        if let Some(stats) = self.current_worker_stats() {
            stats.requests_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn inc_worker_requests_success(&self) {
        if let Some(stats) = self.current_worker_stats() {
            stats.requests_success.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn inc_worker_requests_failure(&self) {
        if let Some(stats) = self.current_worker_stats() {
            stats.requests_failure.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn inc_worker_ingress_packets_total(&self) {
        if let Some(stats) = self.current_worker_stats() {
            stats.ingress_packets_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn inc_worker_ingress_queue_drops(&self) {
        if let Some(stats) = self.current_worker_stats() {
            stats.ingress_queue_drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn inc_worker_ingress_queue_drop_bytes(&self, bytes: u64) {
        if let Some(stats) = self.current_worker_stats() {
            stats
                .ingress_queue_drop_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn try_reserve_request_buffer(&self, bytes: usize, cap_bytes: usize) -> bool {
        let add = bytes as u64;
        let cap = cap_bytes as u64;
        loop {
            let current = self.request_buffered_bytes.load(Ordering::Relaxed);
            let next = current.saturating_add(add);
            if next > cap {
                return false;
            }
            if self
                .request_buffered_bytes
                .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.observe_request_buffer_high_water(next);
                return true;
            }
        }
    }

    pub fn release_request_buffer(&self, bytes: usize) {
        let sub = bytes as u64;
        loop {
            let current = self.request_buffered_bytes.load(Ordering::Relaxed);
            let next = current.saturating_sub(sub);
            if self
                .request_buffered_bytes
                .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn inc_request_buffer_limit_reject(&self) {
        self.request_buffer_limit_rejects
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_response_prebuffer_limit_reject(&self) {
        self.response_prebuffer_limit_rejects
            .fetch_add(1, Ordering::Relaxed);
    }

    fn observe_request_buffer_high_water(&self, candidate: u64) {
        loop {
            let current = self
                .request_buffered_high_watermark_bytes
                .load(Ordering::Relaxed);
            if candidate <= current {
                return;
            }
            if self
                .request_buffered_high_watermark_bytes
                .compare_exchange(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn inc_scid_rotation(&self) {
        self.scid_rotations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_watchdog_restart_request(&self) {
        self.watchdog_restart_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_watchdog_restart_hook(&self) {
        self.watchdog_restart_hooks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_watchdog_degraded_window(&self) {
        self.watchdog_degraded_windows
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_runtime_panic(&self) {
        self.runtime_panics.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_retry(&self, reason: RetryReason) {
        self.retries_total.fetch_add(1, Ordering::Relaxed);
        match reason {
            RetryReason::BackendTimeout => {
                self.retry_reason_timeout.fetch_add(1, Ordering::Relaxed);
            }
            RetryReason::BackendTransport => {
                self.retry_reason_transport.fetch_add(1, Ordering::Relaxed);
            }
            RetryReason::BackendPool => {
                self.retry_reason_pool.fetch_add(1, Ordering::Relaxed);
            }
            RetryReason::BudgetDenied => {
                self.retry_denied_budget.fetch_add(1, Ordering::Relaxed);
            }
            RetryReason::NotBodylessMode => {
                self.retry_denied_no_bodyless
                    .fetch_add(1, Ordering::Relaxed);
            }
            RetryReason::NoAlternateBackend => {
                self.retry_denied_no_alternate
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn inc_circuit_breaker_rejected(&self) {
        self.circuit_breaker_rejected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_brownout_active(&self, active: bool) {
        self.brownout_active
            .store(if active { 1 } else { 0 }, Ordering::Relaxed);
    }

    pub fn inc_health_failure(&self, reason: HealthFailureReason) {
        match reason {
            HealthFailureReason::HttpStatus5xx => {
                self.health_failure_5xx.fetch_add(1, Ordering::Relaxed);
            }
            HealthFailureReason::Timeout => {
                self.health_failure_timeout.fetch_add(1, Ordering::Relaxed);
            }
            HealthFailureReason::Transport => {
                self.health_failure_transport
                    .fetch_add(1, Ordering::Relaxed);
            }
            HealthFailureReason::Tls => {
                self.health_failure_tls.fetch_add(1, Ordering::Relaxed);
            }
            HealthFailureReason::CircuitOpen => {}
        }
    }

    pub fn record_route(&self, route: &str, latency: Duration, outcome: RouteOutcome) {
        let route_id = self
            .route_label_to_id
            .get(route)
            .copied()
            .unwrap_or(self.unrouted_route_id);
        let Some(entry) = self.route_stats.get(route_id) else {
            return;
        };
        entry.requests_total.fetch_add(1, Ordering::Relaxed);

        match outcome {
            RouteOutcome::Success => {
                entry.success.fetch_add(1, Ordering::Relaxed);
            }
            RouteOutcome::Failure => {
                entry.failure.fetch_add(1, Ordering::Relaxed);
            }
            RouteOutcome::Timeout => {
                entry.timeout.fetch_add(1, Ordering::Relaxed);
            }
            RouteOutcome::BackendError => {
                entry.backend_error.fetch_add(1, Ordering::Relaxed);
            }
            RouteOutcome::OverloadShed => {
                entry.overload_shed.fetch_add(1, Ordering::Relaxed);
            }
        }

        if self.route_latency_sample_every > 1 {
            let seq = self
                .route_latency_sample_counter
                .fetch_add(1, Ordering::Relaxed);
            if !seq.is_multiple_of(self.route_latency_sample_every) {
                return;
            }
        }

        let latency_ms = latency.as_millis() as u64;
        let bucket = LATENCY_BUCKETS_MS
            .iter()
            .position(|cutoff| latency_ms <= *cutoff)
            .unwrap_or(LATENCY_BUCKETS_MS.len());
        entry.latency_buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(8 * 1024);
        out.push_str("# HELP spooky_requests_total Total requests seen by spooky.\n");
        out.push_str("# TYPE spooky_requests_total counter\n");
        out.push_str(&format!(
            "spooky_requests_total {}\n",
            self.requests_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_requests_success Total successful upstream responses.\n");
        out.push_str("# TYPE spooky_requests_success counter\n");
        out.push_str(&format!(
            "spooky_requests_success {}\n",
            self.requests_success.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_requests_failure Total failed requests.\n");
        out.push_str("# TYPE spooky_requests_failure counter\n");
        out.push_str(&format!(
            "spooky_requests_failure {}\n",
            self.requests_failure.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_request_validation_rejects Total requests rejected by protocol validation.\n",
        );
        out.push_str("# TYPE spooky_request_validation_rejects counter\n");
        out.push_str(&format!(
            "spooky_request_validation_rejects {}\n",
            self.request_validation_rejects.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_policy_denied Total requests denied by runtime method/path policies.\n",
        );
        out.push_str("# TYPE spooky_policy_denied counter\n");
        out.push_str(&format!(
            "spooky_policy_denied {}\n",
            self.policy_denied.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_early_data_accepted Total requests accepted in early data.\n");
        out.push_str("# TYPE spooky_early_data_accepted counter\n");
        out.push_str(&format!(
            "spooky_early_data_accepted {}\n",
            self.early_data_accepted.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_early_data_rejected Total requests rejected in early data.\n");
        out.push_str("# TYPE spooky_early_data_rejected counter\n");
        out.push_str(&format!(
            "spooky_early_data_rejected {}\n",
            self.early_data_rejected.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_health_checks_total Total active health checks executed.\n");
        out.push_str("# TYPE spooky_health_checks_total counter\n");
        out.push_str(&format!(
            "spooky_health_checks_total {}\n",
            self.health_checks_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_health_checks_success Total successful active health checks.\n",
        );
        out.push_str("# TYPE spooky_health_checks_success counter\n");
        out.push_str(&format!(
            "spooky_health_checks_success {}\n",
            self.health_checks_success.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_health_checks_failure Total failed active health checks.\n");
        out.push_str("# TYPE spooky_health_checks_failure counter\n");
        out.push_str(&format!(
            "spooky_health_checks_failure {}\n",
            self.health_checks_failure.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_backend_timeouts Total backend timeout events.\n");
        out.push_str("# TYPE spooky_backend_timeouts counter\n");
        out.push_str(&format!(
            "spooky_backend_timeouts {}\n",
            self.backend_timeouts.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_backend_errors Total backend error events.\n");
        out.push_str("# TYPE spooky_backend_errors counter\n");
        out.push_str(&format!(
            "spooky_backend_errors {}\n",
            self.backend_errors.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_overload_shed Total requests dropped due to overload controls.\n",
        );
        out.push_str("# TYPE spooky_overload_shed counter\n");
        out.push_str(&format!(
            "spooky_overload_shed {}\n",
            self.overload_shed.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_overload_shed_by_reason_total Total overload shed decisions grouped by reason.\n",
        );
        out.push_str("# TYPE spooky_overload_shed_by_reason_total counter\n");
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"brownout\"}} {}\n",
            self.overload_shed_brownout.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"adaptive_admission\"}} {}\n",
            self.overload_shed_adaptive.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"route_cap\"}} {}\n",
            self.overload_shed_route_cap.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"route_global_cap\"}} {}\n",
            self.overload_shed_route_global_cap.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"global_inflight\"}} {}\n",
            self.overload_shed_global_inflight.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"upstream_inflight\"}} {}\n",
            self.overload_shed_upstream_inflight.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"backend_inflight\"}} {}\n",
            self.overload_shed_backend_inflight.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"request_buffer_cap\"}} {}\n",
            self.overload_shed_request_buffer.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"response_prebuffer_cap\"}} {}\n",
            self.overload_shed_response_prebuffer
                .load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_overload_shed_by_reason_total{{reason=\"connection_cap\"}} {}\n",
            self.overload_shed_connection_cap.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_active_connections Current active QUIC connections.\n");
        out.push_str("# TYPE spooky_active_connections gauge\n");
        out.push_str(&format!(
            "spooky_active_connections {}\n",
            self.active_connections.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_connection_cap_rejects Total new-connection attempts rejected by max_active_connections cap.\n",
        );
        out.push_str("# TYPE spooky_connection_cap_rejects counter\n");
        out.push_str(&format!(
            "spooky_connection_cap_rejects {}\n",
            self.connection_cap_rejects.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_hedge_triggered_total Total hedge attempts started.\n");
        out.push_str("# TYPE spooky_hedge_triggered_total counter\n");
        out.push_str(&format!(
            "spooky_hedge_triggered_total {}\n",
            self.hedge_triggered.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_hedge_won_total Total requests where hedge response arrived first.\n",
        );
        out.push_str("# TYPE spooky_hedge_won_total counter\n");
        out.push_str(&format!(
            "spooky_hedge_won_total {}\n",
            self.hedge_won.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_hedge_wasted_total Total hedge attempts that did not win the race.\n",
        );
        out.push_str("# TYPE spooky_hedge_wasted_total counter\n");
        out.push_str(&format!(
            "spooky_hedge_wasted_total {}\n",
            self.hedge_wasted.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_hedge_primary_won_after_trigger_total Total hedged requests where primary still won.\n",
        );
        out.push_str("# TYPE spooky_hedge_primary_won_after_trigger_total counter\n");
        out.push_str(&format!(
            "spooky_hedge_primary_won_after_trigger_total {}\n",
            self.hedge_primary_won_after_trigger.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_hedge_primary_late_ms_total Aggregate milliseconds primary was late after hedge trigger.\n",
        );
        out.push_str("# TYPE spooky_hedge_primary_late_ms_total counter\n");
        out.push_str(&format!(
            "spooky_hedge_primary_late_ms_total {}\n",
            self.hedge_primary_late_ms_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_hedge_primary_late_samples_total Number of late-primary observations used in hedge tuning.\n",
        );
        out.push_str("# TYPE spooky_hedge_primary_late_samples_total counter\n");
        out.push_str(&format!(
            "spooky_hedge_primary_late_samples_total {}\n",
            self.hedge_primary_late_samples.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_packets_total Total UDP packets processed by ingress.\n",
        );
        out.push_str("# TYPE spooky_ingress_packets_total counter\n");
        out.push_str(&format!(
            "spooky_ingress_packets_total {}\n",
            self.ingress_packets_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_queue_drops Total ingress packets dropped due to full shard queues.\n",
        );
        out.push_str("# TYPE spooky_ingress_queue_drops counter\n");
        out.push_str(&format!(
            "spooky_ingress_queue_drops {}\n",
            self.ingress_queue_drops.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_queue_drop_bytes Total UDP datagram bytes dropped due to full shard queues.\n",
        );
        out.push_str("# TYPE spooky_ingress_queue_drop_bytes counter\n");
        out.push_str(&format!(
            "spooky_ingress_queue_drop_bytes {}\n",
            self.ingress_queue_drop_bytes.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_queue_bytes Current bytes buffered in ingress shard queues.\n",
        );
        out.push_str("# TYPE spooky_ingress_queue_bytes gauge\n");
        out.push_str(&format!(
            "spooky_ingress_queue_bytes {}\n",
            self.ingress_queue_bytes.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_bad_header_total Ingress packets dropped due to unparseable QUIC header.\n",
        );
        out.push_str("# TYPE spooky_ingress_bad_header_total counter\n");
        out.push_str(&format!(
            "spooky_ingress_bad_header_total {}\n",
            self.ingress_bad_header_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_rate_limited_total Initial packets dropped by the new-connection rate limiter.\n",
        );
        out.push_str("# TYPE spooky_ingress_rate_limited_total counter\n");
        out.push_str(&format!(
            "spooky_ingress_rate_limited_total {}\n",
            self.ingress_rate_limited_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_unroutable_total Non-Initial packets received for unknown connections.\n",
        );
        out.push_str("# TYPE spooky_ingress_unroutable_total counter\n");
        out.push_str(&format!(
            "spooky_ingress_unroutable_total {}\n",
            self.ingress_unroutable_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_draining_drops_total Packets dropped because the listener is draining.\n",
        );
        out.push_str("# TYPE spooky_ingress_draining_drops_total counter\n");
        out.push_str(&format!(
            "spooky_ingress_draining_drops_total {}\n",
            self.ingress_draining_drops_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_connection_create_failed_total Packets dropped because quiche::accept() failed to create a new connection.\n",
        );
        out.push_str("# TYPE spooky_ingress_connection_create_failed_total counter\n");
        out.push_str(&format!(
            "spooky_ingress_connection_create_failed_total {}\n",
            self.ingress_connection_create_failed_total
                .load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_ingress_version_neg_failed_total Packets dropped because version negotiation response could not be constructed.\n",
        );
        out.push_str("# TYPE spooky_ingress_version_neg_failed_total counter\n");
        out.push_str(&format!(
            "spooky_ingress_version_neg_failed_total {}\n",
            self.ingress_version_neg_failed_total
                .load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_request_buffered_bytes Current bytes buffered in request backpressure queues.\n",
        );
        out.push_str("# TYPE spooky_request_buffered_bytes gauge\n");
        out.push_str(&format!(
            "spooky_request_buffered_bytes {}\n",
            self.request_buffered_bytes.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_request_buffered_high_watermark_bytes Peak request-buffered bytes since process start.\n",
        );
        out.push_str("# TYPE spooky_request_buffered_high_watermark_bytes gauge\n");
        out.push_str(&format!(
            "spooky_request_buffered_high_watermark_bytes {}\n",
            self.request_buffered_high_watermark_bytes
                .load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_request_buffer_limit_rejects Total requests rejected due to request buffer byte caps.\n",
        );
        out.push_str("# TYPE spooky_request_buffer_limit_rejects counter\n");
        out.push_str(&format!(
            "spooky_request_buffer_limit_rejects {}\n",
            self.request_buffer_limit_rejects.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_response_prebuffer_limit_rejects Total unknown-length upstream responses rejected due to prebuffer cap.\n",
        );
        out.push_str("# TYPE spooky_response_prebuffer_limit_rejects counter\n");
        out.push_str(&format!(
            "spooky_response_prebuffer_limit_rejects {}\n",
            self.response_prebuffer_limit_rejects
                .load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_scid_rotations Total SCID rotations.\n");
        out.push_str("# TYPE spooky_scid_rotations counter\n");
        out.push_str(&format!(
            "spooky_scid_rotations {}\n",
            self.scid_rotations.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_watchdog_restart_requests Total watchdog restart requests.\n");
        out.push_str("# TYPE spooky_watchdog_restart_requests counter\n");
        out.push_str(&format!(
            "spooky_watchdog_restart_requests {}\n",
            self.watchdog_restart_requests.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_watchdog_restart_hooks Total executed watchdog restart hooks.\n",
        );
        out.push_str("# TYPE spooky_watchdog_restart_hooks counter\n");
        out.push_str(&format!(
            "spooky_watchdog_restart_hooks {}\n",
            self.watchdog_restart_hooks.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_runtime_panics Total runtime task panics observed.\n");
        out.push_str("# TYPE spooky_runtime_panics counter\n");
        out.push_str(&format!(
            "spooky_runtime_panics {}\n",
            self.runtime_panics.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_watchdog_degraded_windows Total degraded watchdog evaluation windows.\n",
        );
        out.push_str("# TYPE spooky_watchdog_degraded_windows counter\n");
        out.push_str(&format!(
            "spooky_watchdog_degraded_windows {}\n",
            self.watchdog_degraded_windows.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_retries_total Total retry attempts across all routes.\n");
        out.push_str("# TYPE spooky_retries_total counter\n");
        out.push_str(&format!(
            "spooky_retries_total {}\n",
            self.retries_total.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_retry_denied_total Total retry attempts blocked, by denial reason.\n",
        );
        out.push_str("# TYPE spooky_retry_denied_total counter\n");
        out.push_str(&format!(
            "spooky_retry_denied_total{{reason=\"budget\"}} {}\n",
            self.retry_denied_budget.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_retry_denied_total{{reason=\"no_bodyless\"}} {}\n",
            self.retry_denied_no_bodyless.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_retry_denied_total{{reason=\"no_alternate\"}} {}\n",
            self.retry_denied_no_alternate.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_retry_attempts_total Total retries triggered, by error reason.\n",
        );
        out.push_str("# TYPE spooky_retry_attempts_total counter\n");
        out.push_str(&format!(
            "spooky_retry_attempts_total{{reason=\"timeout\"}} {}\n",
            self.retry_reason_timeout.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_retry_attempts_total{{reason=\"transport\"}} {}\n",
            self.retry_reason_transport.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_retry_attempts_total{{reason=\"pool\"}} {}\n",
            self.retry_reason_pool.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_circuit_breaker_rejected_total Requests rejected by an open circuit breaker.\n");
        out.push_str("# TYPE spooky_circuit_breaker_rejected_total counter\n");
        out.push_str(&format!(
            "spooky_circuit_breaker_rejected_total {}\n",
            self.circuit_breaker_rejected_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_brownout_active Whether brownout mode is currently active (1=active, 0=inactive).\n");
        out.push_str("# TYPE spooky_brownout_active gauge\n");
        out.push_str(&format!(
            "spooky_brownout_active {}\n",
            self.brownout_active.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP spooky_health_failures_total Backend health failures, by failure reason.\n",
        );
        out.push_str("# TYPE spooky_health_failures_total counter\n");
        out.push_str(&format!(
            "spooky_health_failures_total{{reason=\"5xx\"}} {}\n",
            self.health_failure_5xx.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_health_failures_total{{reason=\"timeout\"}} {}\n",
            self.health_failure_timeout.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_health_failures_total{{reason=\"transport\"}} {}\n",
            self.health_failure_transport.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "spooky_health_failures_total{{reason=\"tls\"}} {}\n",
            self.health_failure_tls.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP spooky_route_latency_sample_every Route latency histogram sampling interval (1 = every request).\n",
        );
        out.push_str("# TYPE spooky_route_latency_sample_every gauge\n");
        out.push_str(&format!(
            "spooky_route_latency_sample_every {}\n",
            self.route_latency_sample_every
        ));

        let mut snapshot: Vec<(String, RouteStats)> = self
            .route_labels
            .iter()
            .enumerate()
            .filter_map(|(idx, route)| {
                self.route_stats
                    .get(idx)
                    .map(|stats| (route.clone(), stats.snapshot()))
            })
            .collect();
        snapshot.sort_by(|(left, _), (right, _)| left.cmp(right));

        for (route, stats) in snapshot {
            let route = escape_prometheus_label(&route);
            out.push_str(&format!(
                "spooky_route_requests_total{{route=\"{}\"}} {}\n",
                route, stats.requests_total
            ));
            out.push_str(&format!(
                "spooky_route_success_total{{route=\"{}\"}} {}\n",
                route, stats.success
            ));
            out.push_str(&format!(
                "spooky_route_failure_total{{route=\"{}\"}} {}\n",
                route, stats.failure
            ));
            out.push_str(&format!(
                "spooky_route_timeout_total{{route=\"{}\"}} {}\n",
                route, stats.timeout
            ));
            out.push_str(&format!(
                "spooky_route_backend_error_total{{route=\"{}\"}} {}\n",
                route, stats.backend_error
            ));
            out.push_str(&format!(
                "spooky_route_overload_shed_total{{route=\"{}\"}} {}\n",
                route, stats.overload_shed
            ));
            out.push_str(&format!(
                "spooky_route_latency_ms_p50{{route=\"{}\"}} {:.2}\n",
                route,
                percentile_ms(&stats, 0.50)
            ));
            out.push_str(&format!(
                "spooky_route_latency_ms_p95{{route=\"{}\"}} {:.2}\n",
                route,
                percentile_ms(&stats, 0.95)
            ));
            out.push_str(&format!(
                "spooky_route_latency_ms_p99{{route=\"{}\"}} {:.2}\n",
                route,
                percentile_ms(&stats, 0.99)
            ));
        }

        let mut worker_snapshot: Vec<(String, WorkerStats)> = self
            .worker_labels
            .iter()
            .enumerate()
            .filter_map(|(idx, worker)| {
                self.worker_stats
                    .get(idx)
                    .map(|stats| (worker.clone(), stats.snapshot()))
            })
            .collect();
        worker_snapshot.sort_by(|(left, _), (right, _)| left.cmp(right));

        out.push_str(
            "# HELP spooky_worker_requests_total Total requests handled by each worker thread.\n",
        );
        out.push_str("# TYPE spooky_worker_requests_total counter\n");
        out.push_str(
            "# HELP spooky_worker_requests_success Total successful requests by worker thread.\n",
        );
        out.push_str("# TYPE spooky_worker_requests_success counter\n");
        out.push_str(
            "# HELP spooky_worker_requests_failure Total failed requests by worker thread.\n",
        );
        out.push_str("# TYPE spooky_worker_requests_failure counter\n");
        out.push_str(
            "# HELP spooky_worker_ingress_packets_total Total ingress packets by worker thread.\n",
        );
        out.push_str("# TYPE spooky_worker_ingress_packets_total counter\n");
        out.push_str(
            "# HELP spooky_worker_ingress_queue_drops Total ingress queue drops by worker thread.\n",
        );
        out.push_str("# TYPE spooky_worker_ingress_queue_drops counter\n");
        out.push_str(
            "# HELP spooky_worker_ingress_queue_drop_bytes Total ingress queue drop bytes by worker thread.\n",
        );
        out.push_str("# TYPE spooky_worker_ingress_queue_drop_bytes counter\n");

        for (worker, stats) in worker_snapshot {
            let worker = escape_prometheus_label(&worker);
            out.push_str(&format!(
                "spooky_worker_requests_total{{worker=\"{}\"}} {}\n",
                worker, stats.requests_total
            ));
            out.push_str(&format!(
                "spooky_worker_requests_success{{worker=\"{}\"}} {}\n",
                worker, stats.requests_success
            ));
            out.push_str(&format!(
                "spooky_worker_requests_failure{{worker=\"{}\"}} {}\n",
                worker, stats.requests_failure
            ));
            out.push_str(&format!(
                "spooky_worker_ingress_packets_total{{worker=\"{}\"}} {}\n",
                worker, stats.ingress_packets_total
            ));
            out.push_str(&format!(
                "spooky_worker_ingress_queue_drops{{worker=\"{}\"}} {}\n",
                worker, stats.ingress_queue_drops
            ));
            out.push_str(&format!(
                "spooky_worker_ingress_queue_drop_bytes{{worker=\"{}\"}} {}\n",
                worker, stats.ingress_queue_drop_bytes
            ));
        }

        out
    }
}

fn percentile_ms(stats: &RouteStats, quantile: f64) -> f64 {
    if stats.requests_total == 0 {
        return 0.0;
    }

    let target = ((stats.requests_total as f64) * quantile).ceil() as u64;
    let mut running = 0u64;

    for (idx, count) in stats.latency_buckets.iter().enumerate() {
        running = running.saturating_add(*count);
        if running >= target {
            return if idx < LATENCY_BUCKETS_MS.len() {
                LATENCY_BUCKETS_MS[idx] as f64
            } else {
                *LATENCY_BUCKETS_MS.last().unwrap_or(&60_000) as f64
            };
        }
    }

    *LATENCY_BUCKETS_MS.last().unwrap_or(&60_000) as f64
}

fn escape_prometheus_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_render_includes_route_percentiles() {
        let metrics = Metrics::new(1, [String::from("api_pool")]);
        metrics.record_route("api_pool", Duration::from_millis(12), RouteOutcome::Success);
        metrics.record_route(
            "api_pool",
            Duration::from_millis(320),
            RouteOutcome::Timeout,
        );
        metrics.record_route(
            "api_pool",
            Duration::from_millis(900),
            RouteOutcome::BackendError,
        );

        let output = metrics.render_prometheus();
        assert!(output.contains("spooky_route_requests_total{route=\"api_pool\"} 3"));
        assert!(output.contains("spooky_route_latency_ms_p50{route=\"api_pool\"}"));
        assert!(output.contains("spooky_route_latency_ms_p95{route=\"api_pool\"}"));
        assert!(output.contains("spooky_route_latency_ms_p99{route=\"api_pool\"}"));
    }

    #[test]
    fn metrics_render_collects_routes_from_multiple_shards() {
        let routes: Vec<String> = (0..128).map(|idx| format!("route-{idx:03}")).collect();
        let metrics = Metrics::new(1, routes.clone());
        for (idx, route) in routes.iter().enumerate().take(128) {
            metrics.record_route(
                route,
                Duration::from_millis(5 + idx as u64),
                RouteOutcome::Success,
            );
        }

        let output = metrics.render_prometheus();
        assert!(output.contains("spooky_route_requests_total{route=\"route-000\"} 1"));
        assert!(output.contains("spooky_route_requests_total{route=\"route-127\"} 1"));
    }

    #[test]
    fn request_buffer_reservation_respects_cap_and_releases() {
        let metrics = Metrics::default();
        assert!(metrics.try_reserve_request_buffer(512, 1024));
        assert!(!metrics.try_reserve_request_buffer(600, 1024));
        assert_eq!(metrics.request_buffered_bytes.load(Ordering::Relaxed), 512);
        assert_eq!(
            metrics
                .request_buffered_high_watermark_bytes
                .load(Ordering::Relaxed),
            512
        );

        metrics.release_request_buffer(256);
        assert_eq!(metrics.request_buffered_bytes.load(Ordering::Relaxed), 256);

        metrics.release_request_buffer(512);
        assert_eq!(metrics.request_buffered_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn metrics_render_includes_overload_reasons_and_hedge_counters() {
        let metrics = Metrics::default();
        metrics.inc_overload_shed_reason(OverloadShedReason::GlobalInflight);
        metrics.inc_overload_shed_reason(OverloadShedReason::BackendInflight);
        metrics.set_active_connections(7);
        metrics.inc_connection_cap_reject();
        metrics.inc_hedge_triggered();
        metrics.inc_hedge_won();
        metrics.inc_hedge_wasted();
        metrics.inc_hedge_primary_won_after_trigger();
        metrics.observe_hedge_primary_late_ms(42);

        let output = metrics.render_prometheus();
        assert!(
            output.contains("spooky_overload_shed_by_reason_total{reason=\"global_inflight\"} 1")
        );
        assert!(
            output.contains("spooky_overload_shed_by_reason_total{reason=\"backend_inflight\"} 1")
        );
        assert!(output.contains("spooky_active_connections 7"));
        assert!(output.contains("spooky_connection_cap_rejects 1"));
        assert!(output.contains("spooky_hedge_triggered_total 1"));
        assert!(output.contains("spooky_hedge_won_total 1"));
        assert!(output.contains("spooky_hedge_wasted_total 1"));
        assert!(output.contains("spooky_hedge_primary_won_after_trigger_total 1"));
        assert!(output.contains("spooky_hedge_primary_late_ms_total 42"));
        assert!(output.contains("spooky_hedge_primary_late_samples_total 1"));
        assert!(output.contains("spooky_ingress_queue_bytes 0\n"));
        assert!(output.contains("spooky_ingress_bad_header_total 0\n"));
        assert!(output.contains("spooky_ingress_rate_limited_total 0\n"));
        assert!(output.contains("spooky_ingress_unroutable_total 0\n"));
        assert!(output.contains("spooky_ingress_draining_drops_total 0\n"));
        assert!(output.contains("spooky_ingress_connection_create_failed_total 0\n"));
        assert!(output.contains("spooky_ingress_version_neg_failed_total 0\n"));
        assert!(output.contains("spooky_circuit_breaker_rejected_total 0\n"));
        assert!(output.contains("spooky_brownout_active 0\n"));
    }

    #[test]
    fn resilience_metrics_increment_correctly() {
        let metrics = Metrics::default();
        metrics.inc_retry(RetryReason::BackendTimeout);
        metrics.inc_retry(RetryReason::BudgetDenied);
        metrics.inc_circuit_breaker_rejected();
        metrics.inc_circuit_breaker_rejected();
        metrics.set_brownout_active(true);
        let output = metrics.render_prometheus();
        assert!(output.contains("spooky_retries_total 2\n"));
        assert!(output.contains("spooky_retry_attempts_total{reason=\"timeout\"} 1\n"));
        assert!(output.contains("spooky_retry_denied_total{reason=\"budget\"} 1\n"));
        assert!(output.contains("spooky_circuit_breaker_rejected_total 2\n"));
        assert!(output.contains("spooky_brownout_active 1\n"));
        metrics.set_brownout_active(false);
        let output2 = metrics.render_prometheus();
        assert!(output2.contains("spooky_brownout_active 0\n"));
    }

    #[test]
    fn ingress_drop_counters_increment_correctly() {
        let metrics = Metrics::default();
        metrics.inc_ingress_bad_header();
        metrics.inc_ingress_bad_header();
        metrics.inc_ingress_rate_limited();
        metrics.inc_ingress_unroutable();
        metrics.inc_ingress_unroutable();
        metrics.inc_ingress_unroutable();
        metrics.inc_ingress_draining_drop();
        metrics.inc_ingress_connection_create_failed();
        metrics.inc_ingress_version_neg_failed();
        metrics.inc_ingress_version_neg_failed();
        let output = metrics.render_prometheus();
        assert!(output.contains("spooky_ingress_bad_header_total 2\n"));
        assert!(output.contains("spooky_ingress_rate_limited_total 1\n"));
        assert!(output.contains("spooky_ingress_unroutable_total 3\n"));
        assert!(output.contains("spooky_ingress_draining_drops_total 1\n"));
        assert!(output.contains("spooky_ingress_connection_create_failed_total 1\n"));
        assert!(output.contains("spooky_ingress_version_neg_failed_total 2\n"));
    }

    #[test]
    fn metrics_render_includes_worker_labels() {
        let metrics = Metrics::default();
        metrics.inc_total();
        metrics.inc_success();
        metrics.inc_failure();
        metrics.inc_ingress_packet();
        metrics.inc_ingress_queue_drop();
        metrics.inc_ingress_queue_drop_bytes(128);

        let output = metrics.render_prometheus();
        assert!(output.contains("spooky_worker_requests_total{worker=\""));
        assert!(output.contains("spooky_worker_requests_success{worker=\""));
        assert!(output.contains("spooky_worker_requests_failure{worker=\""));
        assert!(output.contains("spooky_worker_ingress_packets_total{worker=\""));
        assert!(output.contains("spooky_worker_ingress_queue_drops{worker=\""));
        assert!(output.contains("spooky_worker_ingress_queue_drop_bytes{worker=\""));
        assert!(
            output.contains("spooky_worker_requests_total{worker=\"") && output.contains("} 1")
        );
        assert!(
            output.contains("spooky_worker_ingress_queue_drop_bytes{worker=\"")
                && output.contains("} 128")
        );
    }

    #[test]
    fn stable_hash64_is_deterministic() {
        let first = stable_hash64(b"/api/orders");
        let second = stable_hash64(b"/api/orders");
        assert_eq!(first, second);
    }

    #[test]
    fn stable_hash_socket_addr_distinguishes_addresses() {
        let addr_one: SocketAddr = "127.0.0.1:9889".parse().expect("addr one");
        let addr_two: SocketAddr = "127.0.0.2:9889".parse().expect("addr two");

        assert_ne!(
            stable_hash_socket_addr(&addr_one),
            stable_hash_socket_addr(&addr_two)
        );
    }
}
