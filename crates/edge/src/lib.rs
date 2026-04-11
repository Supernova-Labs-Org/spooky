use bytes::Bytes;
use smallvec::SmallVec;
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    net::UdpSocket,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use core::net::SocketAddr;

use hyper::body::{Body, Frame};
use spooky_config::config::Config;
use spooky_errors::ProxyError;
use spooky_lb::UpstreamPool;
use spooky_transport::h2_pool::H2Pool;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::cid_radix::CidRadix;
use crate::constants::MAX_DATAGRAM_SIZE_BYTES;

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
mod route_index;

pub struct QUICListener {
    pub socket: UdpSocket,
    pub config: Config,
    pub quic_config: quiche::Config,
    pub h3_config: Arc<quiche::h3::Config>,
    pub h2_pool: Arc<H2Pool>,
    pub upstream_pools: HashMap<String, Arc<Mutex<UpstreamPool>>>,
    pub upstream_inflight: HashMap<String, Arc<Semaphore>>,
    pub global_inflight: Arc<Semaphore>,
    pub(crate) routing_index: route_index::RouteIndex,
    pub metrics: Arc<Metrics>,
    pub draining: bool,
    pub drain_start: Option<Instant>,
    pub backend_timeout: Duration,
    pub backend_body_idle_timeout: Duration,
    pub backend_body_total_timeout: Duration,

    pub recv_buf: [u8; MAX_DATAGRAM_SIZE_BYTES],
    pub send_buf: [u8; MAX_DATAGRAM_SIZE_BYTES],

    pub connections: HashMap<Arc<[u8]>, QuicConnection>, // KEY: SCID(server connection id)
    pub cid_routes: HashMap<Vec<u8>, Vec<u8>>,           // KEY: alias SCID, VALUE: primary SCID
    pub peer_routes: HashMap<SocketAddr, Arc<[u8]>>,     // KEY: peer address, VALUE: primary SCID
    pub cid_radix: CidRadix,
}

pub struct QuicConnection {
    pub quic: quiche::Connection,
    pub h3: Option<quiche::h3::Connection>,
    pub h3_config: Arc<quiche::h3::Config>,
    pub streams: HashMap<u64, RequestEnvelope>,

    pub peer_address: SocketAddr,
    pub last_activity: Instant,
    pub primary_scid: Vec<u8>,
    pub routing_scids: HashSet<Vec<u8>>,
    pub packets_since_rotation: u64,
    pub last_scid_rotation: Instant,
}

/// Result type returned by the in-flight H2 forwarding task.
pub type ForwardResult =
    Result<(http::StatusCode, http::HeaderMap, hyper::body::Incoming), ProxyError>;

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
    Data(Bytes),
    End,
    Error(ProxyError),
}

pub struct RequestEnvelope {
    pub method: String,
    pub path: String,
    pub authority: Option<String>,
    pub headers: SmallVec<[(Vec<u8>, Vec<u8>); 16]>,
    /// Sender half of the body channel.  Dropping it signals end-of-body to hyper.
    pub body_tx: Option<mpsc::Sender<Bytes>>,
    /// Body chunks that arrived before the channel had capacity.
    pub body_buf: Vec<Bytes>,
    /// Total body bytes received on this stream (buffered + already forwarded).
    pub body_bytes_received: usize,
    /// Resolved backend address and index (for health marking on response).
    pub backend_addr: Option<String>,
    pub backend_index: Option<usize>,
    pub upstream_name: Option<String>,
    pub global_inflight_permit: Option<OwnedSemaphorePermit>,
    pub upstream_inflight_permit: Option<OwnedSemaphorePermit>,
    pub start: Instant,

    /// Current lifecycle phase of this stream.
    pub phase: StreamPhase,
    /// True once the client has sent FIN on the request stream.
    pub request_fin_received: bool,
    /// Receives the upstream H2 response (status, headers, body stream).
    pub upstream_result_rx: Option<oneshot::Receiver<ForwardResult>>,
    /// Receives response body chunks to write back over QUIC.
    pub response_chunk_rx: Option<mpsc::Receiver<ResponseChunk>>,
    /// A chunk that could not be written due to QUIC send backpressure; retried next poll.
    pub pending_chunk: Option<ResponseChunk>,
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

#[derive(Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_success: AtomicU64,
    pub requests_failure: AtomicU64,
    pub backend_timeouts: AtomicU64,
    pub backend_errors: AtomicU64,
    pub overload_shed: AtomicU64,
    pub scid_rotations: AtomicU64,
    route_stats: Mutex<HashMap<String, RouteStats>>,
}

const LATENCY_BUCKETS_MS: [u64; 14] = [
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000,
];

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

pub enum RouteOutcome {
    Success,
    Failure,
    Timeout,
    BackendError,
    OverloadShed,
}

impl Metrics {
    pub fn inc_total(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_success(&self) {
        self.requests_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failure(&self) {
        self.requests_failure.fetch_add(1, Ordering::Relaxed);
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

    pub fn inc_scid_rotation(&self) {
        self.scid_rotations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_route(&self, route: &str, latency: Duration, outcome: RouteOutcome) {
        let mut guard = match self.route_stats.lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        let entry = guard.entry(route.to_string()).or_default();
        entry.requests_total = entry.requests_total.saturating_add(1);

        match outcome {
            RouteOutcome::Success => {
                entry.success = entry.success.saturating_add(1);
            }
            RouteOutcome::Failure => {
                entry.failure = entry.failure.saturating_add(1);
            }
            RouteOutcome::Timeout => {
                entry.timeout = entry.timeout.saturating_add(1);
            }
            RouteOutcome::BackendError => {
                entry.backend_error = entry.backend_error.saturating_add(1);
            }
            RouteOutcome::OverloadShed => {
                entry.overload_shed = entry.overload_shed.saturating_add(1);
            }
        }

        let latency_ms = latency.as_millis() as u64;
        let bucket = LATENCY_BUCKETS_MS
            .iter()
            .position(|cutoff| latency_ms <= *cutoff)
            .unwrap_or(LATENCY_BUCKETS_MS.len());
        entry.latency_buckets[bucket] = entry.latency_buckets[bucket].saturating_add(1);
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

        out.push_str("# HELP spooky_overload_shed Total requests dropped due to overload controls.\n");
        out.push_str("# TYPE spooky_overload_shed counter\n");
        out.push_str(&format!(
            "spooky_overload_shed {}\n",
            self.overload_shed.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP spooky_scid_rotations Total SCID rotations.\n");
        out.push_str("# TYPE spooky_scid_rotations counter\n");
        out.push_str(&format!(
            "spooky_scid_rotations {}\n",
            self.scid_rotations.load(Ordering::Relaxed)
        ));

        if let Ok(route_stats) = self.route_stats.lock() {
            for (route, stats) in route_stats.iter() {
                let route = escape_prometheus_label(route);
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
                    percentile_ms(stats, 0.50)
                ));
                out.push_str(&format!(
                    "spooky_route_latency_ms_p95{{route=\"{}\"}} {:.2}\n",
                    route,
                    percentile_ms(stats, 0.95)
                ));
                out.push_str(&format!(
                    "spooky_route_latency_ms_p99{{route=\"{}\"}} {:.2}\n",
                    route,
                    percentile_ms(stats, 0.99)
                ));
            }
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
        let metrics = Metrics::default();
        metrics.record_route(
            "api_pool",
            Duration::from_millis(12),
            RouteOutcome::Success,
        );
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
}
