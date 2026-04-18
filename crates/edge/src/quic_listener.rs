use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::{ToSocketAddrs, UdpSocket},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use core::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use log::{debug, error, info, warn};
use quiche::Config;
use quiche::h3::NameValue;
use rand::RngCore;
use socket2::{Domain, Protocol, Socket, Type};
use spooky_bridge::h3_to_h2::build_h2_request;
use spooky_errors::{PoolError, ProxyError};
use spooky_lb::{HealthTransition, UpstreamPool};
use spooky_transport::h2_client::H2Client;
use spooky_transport::h2_pool::H2Pool;
use tokio::runtime::Handle;
use tokio::sync::{
    Semaphore, mpsc,
    mpsc::error::{TryRecvError, TrySendError},
    oneshot,
};

use spooky_config::{backend_endpoint::BackendEndpoint, config::Config as SpookyConfig};

use crate::{
    ChannelBody, ForwardResult, Metrics, QUICListener, QuicConnection, RequestEnvelope,
    ResponseChunk, RouteOutcome, SharedRuntimeState, StreamPhase,
    cid_radix::CidRadix,
    constants::{
        DEFAULT_SCID_LEN_BYTES, MAX_DATAGRAM_SIZE_BYTES, MAX_REQUEST_BODY_BYTES,
        MAX_STREAMS_PER_CONNECTION, MAX_UDP_PAYLOAD_BYTES, MIN_SCID_LEN_BYTES,
        REQUEST_BUFFERED_CHUNK_BYTES_LIMIT, REQUEST_CHUNK_BYTES_LIMIT,
        REQUEST_CHUNK_CHANNEL_CAPACITY, RESET_TOKEN_LEN_BYTES, RESPONSE_CHUNK_BYTES_LIMIT,
        RESPONSE_CHUNK_CHANNEL_CAPACITY, SCID_ROTATION_PACKET_THRESHOLD, UDP_READ_TIMEOUT_MS,
        scid_rotation_interval,
    },
    outcome_from_status,
    resilience::{RouteQueueRejection, RuntimeResilience},
    route_index::RouteIndex,
    watchdog::{WatchdogCoordinator, WatchdogRuntimeConfig, now_millis},
};

/// A leaky token-bucket rate limiter for new QUIC connection accepts.
///
/// Tokens refill at `rate_per_sec` tokens/second up to a cap of `burst`.
/// Each new `quiche::accept` call consumes one token; if the bucket is empty
/// the packet is silently dropped (no panic, no connection state allocated).
pub(crate) struct TokenBucket {
    /// Maximum tokens the bucket can hold (burst capacity).
    burst: f64,
    /// Tokens added per nanosecond (= rate_per_sec / 1_000_000_000).
    tokens_per_ns: f64,
    /// Current available tokens.
    tokens: f64,
    /// Last time tokens were refilled.
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate_per_sec: u32, burst: u32) -> Self {
        let burst = (burst.max(1)) as f64;
        let rate_per_sec = rate_per_sec.max(1) as f64;
        Self {
            burst,
            tokens_per_ns: rate_per_sec / 1_000_000_000.0,
            tokens: burst, // start full so the first burst of legitimate connections succeeds
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns `true` if a token was available
    /// (connection may proceed), `false` if the bucket is empty (drop).
    fn try_consume(&mut self) -> bool {
        let now = Instant::now();
        let elapsed_ns = now.saturating_duration_since(self.last_refill).as_nanos() as f64;
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed_ns * self.tokens_per_ns).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

fn is_hop_header(name: &str) -> bool {
    matches!(
        name,
        "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding" | "upgrade"
    )
}

type ResolvedBackend = (String, String, usize, Arc<Mutex<UpstreamPool>>);

fn request_content_length(headers: &[quiche::h3::Header]) -> Option<usize> {
    for header in headers {
        if !header.name().eq_ignore_ascii_case(b"content-length") {
            continue;
        }
        let value = std::str::from_utf8(header.value()).ok()?;
        let parsed = value.trim().parse::<usize>().ok()?;
        return Some(parsed);
    }
    None
}

fn resolve_primary_from_radix_prefix<T>(
    dcid: &[u8],
    connections: &HashMap<Arc<[u8]>, T>,
    cid_routes: &mut HashMap<Arc<[u8]>, Arc<[u8]>>,
    cid_radix: &mut CidRadix,
) -> Option<Arc<[u8]>> {
    let matched_cid = cid_radix.longest_prefix_match(dcid)?;

    if connections.contains_key(matched_cid.as_ref()) {
        return Some(matched_cid);
    }

    if let Some(primary) = cid_routes.get(matched_cid.as_ref()).cloned() {
        if connections.contains_key(primary.as_ref()) {
            return Some(primary);
        }
        // Alias points to a missing primary, clean up stale alias map entry.
        cid_routes.remove(matched_cid.as_ref());
    }

    // Stale radix entry (either alias or retired primary).
    cid_radix.remove(matched_cid.as_ref());
    None
}

impl QUICListener {
    pub fn new(config: SpookyConfig) -> Result<Self, ProxyError> {
        let shared_state = Arc::new(Self::build_shared_state(&config)?);
        Self::spawn_control_plane_tasks(&config, &shared_state, 1);
        let socket = Self::bind_socket(&config, false)?;
        Self::new_with_socket_and_shared_state(config, socket, shared_state)
    }

    pub fn build_shared_state(config: &SpookyConfig) -> Result<SharedRuntimeState, ProxyError> {
        let worker_threads = config.performance.worker_threads.max(1);
        let per_upstream_limit = config.performance.per_upstream_inflight_limit.max(1);
        let global_inflight_limit = config.performance.global_inflight_limit.max(1);
        let max_inflight_per_backend = config
            .performance
            .per_backend_inflight_limit
            .saturating_mul(worker_threads);

        info!(
            "Performance profile: worker_threads={} control_plane_threads={} reuseport={} pin_workers={} global_inflight_limit={} per_upstream_inflight_limit={} per_backend_inflight_limit={} backend_connect_timeout_ms={} backend_timeout_ms={} backend_body_idle_timeout_ms={} backend_body_total_timeout_ms={} backend_total_request_timeout_ms={} udp_recv_buffer_bytes={} udp_send_buffer_bytes={} h2_pool_max_idle_per_backend={} h2_pool_idle_timeout_ms={}",
            worker_threads,
            config.performance.control_plane_threads.max(1),
            config.performance.reuseport,
            config.performance.pin_workers,
            global_inflight_limit,
            per_upstream_limit,
            config.performance.per_backend_inflight_limit,
            config.performance.backend_connect_timeout_ms,
            config.performance.backend_timeout_ms,
            config.performance.backend_body_idle_timeout_ms,
            config.performance.backend_body_total_timeout_ms,
            config.performance.backend_total_request_timeout_ms,
            config.performance.udp_recv_buffer_bytes,
            config.performance.udp_send_buffer_bytes,
            config.performance.h2_pool_max_idle_per_backend,
            config.performance.h2_pool_idle_timeout_ms
        );

        let mut backend_addresses = Vec::new();
        for (upstream_name, upstream) in &config.upstream {
            for backend in &upstream.backends {
                if let Err(err) = BackendEndpoint::parse(&backend.address) {
                    return Err(ProxyError::Transport(format!(
                        "invalid backend address '{}' in upstream '{}' (backend '{}'): {}",
                        backend.address, upstream_name, backend.id, err
                    )));
                }
                backend_addresses.push(backend.address.clone());
            }
        }

        let h2_pool = Arc::new(H2Pool::new(
            backend_addresses,
            max_inflight_per_backend,
            config.performance.h2_pool_max_idle_per_backend,
            Duration::from_millis(config.performance.h2_pool_idle_timeout_ms),
            Duration::from_millis(config.performance.backend_connect_timeout_ms),
        ));
        let mut upstream_pools = HashMap::new();
        let mut upstream_inflight = HashMap::new();

        for (name, upstream) in &config.upstream {
            let upstream_pool = UpstreamPool::from_upstream(upstream).map_err(|err| {
                ProxyError::Transport(format!(
                    "failed to create upstream pool '{}': {}",
                    name, err
                ))
            })?;
            upstream_pools.insert(name.clone(), Arc::new(Mutex::new(upstream_pool)));
            upstream_inflight.insert(name.clone(), Arc::new(Semaphore::new(per_upstream_limit)));
        }

        let resilience = Arc::new(RuntimeResilience::from_config(
            &config.resilience,
            global_inflight_limit,
        ));
        let watchdog = Arc::new(WatchdogCoordinator::new(&config.resilience.watchdog));

        Ok(SharedRuntimeState {
            h2_pool,
            upstream_pools,
            upstream_inflight,
            global_inflight: Arc::new(Semaphore::new(global_inflight_limit)),
            metrics: Arc::new(Metrics::default()),
            resilience,
            watchdog,
        })
    }

    pub fn spawn_control_plane_tasks(
        config: &SpookyConfig,
        shared_state: &SharedRuntimeState,
        worker_count: usize,
    ) {
        shared_state
            .watchdog
            .set_expected_workers(worker_count.max(1));
        Self::spawn_health_checks(
            shared_state.upstream_pools.clone(),
            Arc::new(H2Client::new(
                config.performance.h2_pool_max_idle_per_backend.max(1),
                Duration::from_millis(config.performance.h2_pool_idle_timeout_ms.max(1)),
                Duration::from_millis(config.performance.backend_connect_timeout_ms.max(1)),
            )),
            Arc::clone(&shared_state.metrics),
        );
        Self::spawn_metrics_endpoint(config, Arc::clone(&shared_state.metrics));
        Self::spawn_watchdog(
            config,
            Arc::clone(&shared_state.metrics),
            Arc::clone(&shared_state.resilience),
            Arc::clone(&shared_state.watchdog),
        );
    }

    pub fn bind_reuseport_sockets(
        config: &SpookyConfig,
        workers: usize,
    ) -> Result<Vec<UdpSocket>, ProxyError> {
        let workers = workers.max(1);
        let mut sockets = Vec::with_capacity(workers);
        for _ in 0..workers {
            sockets.push(Self::bind_socket(config, true)?);
        }
        Ok(sockets)
    }

    pub fn bind_socket(config: &SpookyConfig, reuse_port: bool) -> Result<UdpSocket, ProxyError> {
        let bind_addr = Self::resolve_bind_addr(config)?;
        let socket = Self::create_udp_socket(
            bind_addr,
            reuse_port,
            config.performance.udp_recv_buffer_bytes,
            config.performance.udp_send_buffer_bytes,
        )?;
        socket
            .set_read_timeout(Some(Duration::from_millis(UDP_READ_TIMEOUT_MS)))
            .map_err(|err| {
                ProxyError::Transport(format!("failed to set UDP read timeout: {}", err))
            })?;

        Ok(socket)
    }

    pub fn new_with_socket_and_shared_state(
        config: SpookyConfig,
        socket: UdpSocket,
        shared_state: Arc<SharedRuntimeState>,
    ) -> Result<Self, ProxyError> {
        let local_addr = socket.local_addr().map_err(|err| {
            ProxyError::Transport(format!("failed to read UDP socket local address: {}", err))
        })?;
        debug!("Listening on {}", local_addr);

        let quic_config = Self::build_quic_config(&config)?;
        let h3_config =
            Arc::new(quiche::h3::Config::new().map_err(|err| {
                ProxyError::Transport(format!("failed to create h3 config: {err}"))
            })?);
        let routing_index = RouteIndex::from_upstreams(&config.upstream);
        let backend_timeout = Duration::from_millis(config.performance.backend_timeout_ms);
        let backend_body_idle_timeout =
            Duration::from_millis(config.performance.backend_body_idle_timeout_ms);
        let backend_body_total_timeout =
            Duration::from_millis(config.performance.backend_body_total_timeout_ms);
        let backend_total_request_timeout =
            Duration::from_millis(config.performance.backend_total_request_timeout_ms);
        let drain_timeout = Duration::from_millis(config.performance.shutdown_drain_timeout_ms);
        let max_response_body_bytes = config.performance.max_response_body_bytes;
        let conn_rate_limiter = TokenBucket::new(
            config.performance.new_connections_per_sec,
            config.performance.new_connections_burst,
        );

        Ok(Self {
            socket,
            config,
            quic_config,
            h3_config,
            h2_pool: Arc::clone(&shared_state.h2_pool),
            upstream_pools: shared_state.upstream_pools.clone(),
            upstream_inflight: shared_state.upstream_inflight.clone(),
            global_inflight: Arc::clone(&shared_state.global_inflight),
            routing_index,
            metrics: Arc::clone(&shared_state.metrics),
            resilience: Arc::clone(&shared_state.resilience),
            watchdog: Arc::clone(&shared_state.watchdog),
            draining: false,
            drain_start: None,
            watchdog_worker_drained: false,
            drain_timeout,
            backend_timeout,
            backend_body_idle_timeout,
            backend_body_total_timeout,
            backend_total_request_timeout,
            max_response_body_bytes,
            recv_buf: [0; MAX_DATAGRAM_SIZE_BYTES],
            send_buf: [0; MAX_DATAGRAM_SIZE_BYTES],
            connections: HashMap::new(),
            cid_routes: HashMap::new(),
            peer_routes: HashMap::new(),
            cid_radix: CidRadix::new(),
            conn_rate_limiter,
        })
    }

    fn resolve_bind_addr(config: &SpookyConfig) -> Result<SocketAddr, ProxyError> {
        let socket_address = format!("{}:{}", config.listen.address, config.listen.port);
        socket_address
            .to_socket_addrs()
            .map_err(|err| {
                ProxyError::Transport(format!(
                    "failed to resolve listen address '{}': {}",
                    socket_address, err
                ))
            })?
            .next()
            .ok_or_else(|| {
                ProxyError::Transport(format!("no socket addresses found for '{socket_address}'"))
            })
    }

    fn create_udp_socket(
        bind_addr: SocketAddr,
        reuse_port: bool,
        udp_recv_buffer_bytes: usize,
        udp_send_buffer_bytes: usize,
    ) -> Result<UdpSocket, ProxyError> {
        let domain = if bind_addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(|err| {
            ProxyError::Transport(format!("failed to create UDP socket: {}", err))
        })?;
        socket
            .set_reuse_address(true)
            .map_err(|err| ProxyError::Transport(format!("failed to set SO_REUSEADDR: {}", err)))?;
        socket
            .set_recv_buffer_size(udp_recv_buffer_bytes)
            .map_err(|err| {
                ProxyError::Transport(format!(
                    "failed to set UDP recv buffer size ({}): {}",
                    udp_recv_buffer_bytes, err
                ))
            })?;
        socket
            .set_send_buffer_size(udp_send_buffer_bytes)
            .map_err(|err| {
                ProxyError::Transport(format!(
                    "failed to set UDP send buffer size ({}): {}",
                    udp_send_buffer_bytes, err
                ))
            })?;

        #[cfg(all(
            unix,
            not(target_os = "solaris"),
            not(target_os = "illumos"),
            not(target_os = "cygwin")
        ))]
        {
            socket.set_reuse_port(reuse_port).map_err(|err| {
                ProxyError::Transport(format!("failed to set SO_REUSEPORT: {}", err))
            })?;
        }

        socket.bind(&bind_addr.into()).map_err(|err| {
            ProxyError::Transport(format!(
                "failed to bind UDP socket on '{}': {}",
                bind_addr, err
            ))
        })?;

        match (socket.recv_buffer_size(), socket.send_buffer_size()) {
            (Ok(actual_recv), Ok(actual_send)) => {
                debug!(
                    "UDP socket buffers on {}: recv={} (requested={}) send={} (requested={}) reuseport={}",
                    bind_addr,
                    actual_recv,
                    udp_recv_buffer_bytes,
                    actual_send,
                    udp_send_buffer_bytes,
                    reuse_port
                );
            }
            _ => {
                debug!(
                    "UDP socket bound on {} with requested buffers recv={} send={} reuseport={}",
                    bind_addr, udp_recv_buffer_bytes, udp_send_buffer_bytes, reuse_port
                );
            }
        }

        Ok(socket.into())
    }

    fn build_quic_config(config: &SpookyConfig) -> Result<Config, ProxyError> {
        let mut quic_config = Config::new(quiche::PROTOCOL_VERSION)
            .map_err(|err| ProxyError::Transport(format!("failed to create QUIC config: {err}")))?;

        match quic_config.load_cert_chain_from_pem_file(&config.listen.tls.cert) {
            Ok(_) => debug!("Certificate loaded successfully"),
            Err(e) => {
                return Err(ProxyError::Tls(format!(
                    "Failed to load certificate '{}': {}",
                    config.listen.tls.cert, e
                )));
            }
        }

        match quic_config.load_priv_key_from_pem_file(&config.listen.tls.key) {
            Ok(_) => debug!("Private key loaded successfully"),
            Err(e) => {
                return Err(ProxyError::Tls(format!(
                    "Failed to load key '{}': {}",
                    config.listen.tls.key, e
                )));
            }
        }

        quic_config
            .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
            .map_err(|err| {
                ProxyError::Transport(format!("failed to set ALPN protocols: {:?}", err))
            })?;
        quic_config.set_max_idle_timeout(config.performance.quic_max_idle_timeout_ms);
        quic_config.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
        quic_config.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
        quic_config.set_initial_max_data(config.performance.quic_initial_max_data);
        quic_config.set_initial_max_stream_data_bidi_local(
            config.performance.quic_initial_max_stream_data,
        );
        quic_config.set_initial_max_stream_data_bidi_remote(
            config.performance.quic_initial_max_stream_data,
        );
        quic_config
            .set_initial_max_stream_data_uni(config.performance.quic_initial_max_stream_data);
        quic_config.set_initial_max_streams_bidi(config.performance.quic_initial_max_streams_bidi);
        quic_config.set_initial_max_streams_uni(config.performance.quic_initial_max_streams_uni);
        quic_config.set_disable_active_migration(true);
        quic_config.verify_peer(false);

        Ok(quic_config)
    }

    pub fn start_draining(&mut self) {
        if self.draining {
            return;
        }
        self.draining = true;
        self.drain_start = Some(Instant::now());
        info!("Draining connections");
    }

    pub fn drain_complete(&mut self) -> bool {
        if !self.draining {
            return self.connections.is_empty();
        }

        if self.connections.is_empty() {
            return true;
        }

        // Once all in-flight streams are terminal, drain can complete without
        // waiting for clients to idle-close their QUIC connections.
        let has_active_streams = self
            .connections
            .values()
            .any(|conn| !conn.streams.is_empty());
        if !has_active_streams {
            self.close_all();
            return true;
        }

        if let Some(start) = self.drain_start
            && start.elapsed() >= self.drain_timeout
        {
            self.close_all();
            return true;
        }

        false
    }

    fn close_all(&mut self) {
        let mut send_buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];
        for connection in self.connections.values_mut() {
            let _ = connection.quic.close(true, 0x0, b"draining");
            Self::flush_send(&self.socket, &mut send_buf, connection);
        }

        self.connections.clear();
        self.cid_routes.clear();
        self.peer_routes.clear();
        self.cid_radix.clear();
    }

    fn take_or_create_connection(
        &mut self,
        peer: SocketAddr,
        local_addr: SocketAddr,
        packets: &[u8],
    ) -> Option<(QuicConnection, Arc<[u8]>)> {
        let mut buf = packets.to_vec();
        let header = match quiche::Header::from_slice(&mut buf, quiche::MAX_CONN_ID_LEN) {
            Ok(hdr) => hdr,
            Err(_) => {
                error!("Wrong QUIC HEADER");
                return None;
            }
        };

        let dcid_bytes: Arc<[u8]> = Arc::from(header.dcid.as_ref());
        debug!(
            "Packet DCID (len={}): {:02x?}, type: {:?}, active connections: {}",
            dcid_bytes.len(),
            &dcid_bytes,
            header.ty,
            self.connections.len()
        );

        // Try exact match first
        if let Some(mut connection) = self.connections.remove(&dcid_bytes) {
            debug!("Found existing connection for DCID: {:02x?}", &dcid_bytes);
            self.peer_routes.remove(&connection.peer_address);
            connection.peer_address = peer;
            return Some((connection, dcid_bytes));
        }

        // For Short packets, try prefix match (client may append bytes to our SCID)
        // This handles cases where client uses longer DCIDs based on server's SCID
        if header.ty == quiche::Type::Short
            && dcid_bytes.len() > MIN_SCID_LEN_BYTES
            && let Some(primary_cid) = resolve_primary_from_radix_prefix(
                &dcid_bytes,
                &self.connections,
                &mut self.cid_routes,
                &mut self.cid_radix,
            )
        {
            debug!(
                "Found connection via prefix match. Resolved CID: {:02x?}, Packet DCID: {:02x?}",
                primary_cid, &dcid_bytes
            );
            if let Some(mut connection) = self.connections.remove(primary_cid.as_ref()) {
                self.peer_routes.remove(&connection.peer_address);
                connection.peer_address = peer;
                return Some((connection, primary_cid));
            }
        }

        if self.draining {
            return None;
        }

        // Only create new connections for Initial packets
        if header.ty != quiche::Type::Initial {
            debug!("Non-Initial packet for unknown connection, ignoring");
            return None;
        }

        // If this is a 0-RTT packet without a valid token, we need to reject it
        if header.token.is_some() {
            debug!("Received 0-RTT attempt, will negotiate fresh connection");
            // return None;
        }

        // Rate-limit new connection creation to prevent unbounded memory growth
        // under connection floods. Existing connections are never affected.
        if !self.conn_rate_limiter.try_consume() {
            debug!(
                "New connection rate limit exceeded, dropping Initial packet from {}",
                peer
            );
            return None;
        }

        let mut scid_bytes = [0u8; DEFAULT_SCID_LEN_BYTES];
        rand::thread_rng().fill_bytes(&mut scid_bytes);

        let scid = quiche::ConnectionId::from_ref(&scid_bytes);

        let quic_connection =
            quiche::accept(&scid, None, local_addr, peer, &mut self.quic_config).ok()?;

        let connection = QuicConnection {
            quic: quic_connection,
            h3: None,
            h3_config: self.h3_config.clone(),
            streams: HashMap::new(),
            peer_address: peer,
            last_activity: Instant::now(),
            primary_scid: Arc::from(&scid_bytes[..]),
            routing_scids: HashSet::from([Arc::from(&scid_bytes[..])]),
            packets_since_rotation: 0,
            last_scid_rotation: Instant::now(),
        };

        // Store connection using server's SCID (not client's DCID)
        // After handshake, client will use server's SCID as DCID in subsequent packets
        debug!(
            "Creating new connection with server SCID: {:02x?}",
            &scid_bytes
        );
        Some((connection, Arc::from(&scid_bytes[..])))
    }

    fn random_reset_token() -> u128 {
        let mut token = [0u8; RESET_TOKEN_LEN_BYTES];
        rand::thread_rng().fill_bytes(&mut token);
        u128::from_be_bytes(token)
    }

    fn maybe_rotate_scid(connection: &mut QuicConnection, metrics: &Metrics) {
        if !connection.quic.is_established() {
            return;
        }

        let now = Instant::now();
        let elapsed = now.saturating_duration_since(connection.last_scid_rotation);
        if connection.packets_since_rotation < SCID_ROTATION_PACKET_THRESHOLD
            && elapsed < scid_rotation_interval()
        {
            return;
        }

        if connection.quic.scids_left() == 0 {
            return;
        }

        let cid_len = connection
            .quic
            .source_id()
            .as_ref()
            .len()
            .max(MIN_SCID_LEN_BYTES);
        let mut cid_bytes = vec![0u8; cid_len];
        rand::thread_rng().fill_bytes(&mut cid_bytes);

        let new_scid = quiche::ConnectionId::from_ref(&cid_bytes);
        let reset_token = Self::random_reset_token();

        match connection.quic.new_scid(&new_scid, reset_token, true) {
            Ok(seq) => {
                connection.last_scid_rotation = now;
                connection.packets_since_rotation = 0;
                metrics.inc_scid_rotation();
                debug!(
                    "Issued new SCID seq={} cid={}",
                    seq,
                    hex::encode(&cid_bytes)
                );
            }
            Err(e) => {
                debug!("SCID rotation skipped: {:?}", e);
            }
        }
    }

    fn remove_connection_routes(&mut self, connection: &QuicConnection) {
        self.cid_radix.remove(connection.primary_scid.as_ref());
        self.cid_routes.remove(connection.primary_scid.as_ref());
        for cid in &connection.routing_scids {
            self.cid_radix.remove(cid.as_ref());
            self.cid_routes.remove(cid.as_ref());
        }
        self.peer_routes.remove(&connection.peer_address);
    }

    fn sync_connection_routes(&mut self, connection: &mut QuicConnection) -> Arc<[u8]> {
        let mut active_scids: HashSet<Arc<[u8]>> = connection
            .quic
            .source_ids()
            .map(|cid| Arc::from(cid.as_ref()))
            .collect();

        if active_scids.is_empty() {
            active_scids.insert(Arc::clone(&connection.primary_scid));
        }

        let active_source_id: Arc<[u8]> = Arc::from(connection.quic.source_id().as_ref());
        let primary = if active_scids.contains(&active_source_id) {
            active_source_id
        } else if active_scids.contains(&connection.primary_scid) {
            Arc::clone(&connection.primary_scid)
        } else {
            active_scids
                .iter()
                .min_by(|left, right| left.as_ref().cmp(right.as_ref()))
                .cloned()
                .unwrap_or_else(|| Arc::clone(&connection.primary_scid))
        };

        let retired_scids: Vec<Arc<[u8]>> = connection
            .routing_scids
            .difference(&active_scids)
            .cloned()
            .collect();

        // Phase 1: make active SCIDs prefix-matchable before retirements.
        for cid in &active_scids {
            self.cid_radix.insert(Arc::clone(cid));
        }

        // Phase 2: clear previous aliases for this connection.
        for cid in &connection.routing_scids {
            self.cid_routes.remove(cid.as_ref());
        }

        // Phase 3: install aliases for active non-primary SCIDs.
        for cid in &active_scids {
            if *cid == primary {
                continue;
            }
            self.cid_routes
                .insert(Arc::clone(cid), Arc::clone(&primary));
        }

        // Phase 4: retire stale SCIDs after active set is fully installed.
        for retired in retired_scids {
            self.cid_radix.remove(retired.as_ref());
        }

        connection.routing_scids = active_scids;
        connection.primary_scid = Arc::clone(&primary);
        primary
    }

    fn poll_preamble(&mut self) -> bool {
        self.watchdog.mark_poll_progress();
        if !self.watchdog.restart_requested() {
            self.watchdog_worker_drained = false;
        }
        if self.watchdog.restart_requested() && !self.draining {
            warn!("Watchdog requested restart; entering draining mode");
            self.start_draining();
        }
        if self.draining && self.drain_complete() {
            if self.watchdog.restart_requested() && !self.watchdog_worker_drained {
                self.watchdog.mark_worker_drained();
                self.watchdog_worker_drained = true;
            }
            return false;
        }
        true
    }

    pub fn poll(&mut self) {
        if !self.poll_preamble() {
            return;
        }

        // Read a UDP datagram and feed it into quiche.
        let (len, peer) = match self.socket.recv_from(&mut self.recv_buf) {
            Ok(v) => v,
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                self.handle_timeouts();
                return;
            }
            Err(_) => return,
        };

        debug!("Received UDP datagram ({} bytes)", len);

        let local_addr = match self.socket.local_addr() {
            Ok(addr) => addr,
            Err(_) => return,
        };

        let packet = self.recv_buf[..len].to_vec();
        self.process_datagram_inner(peer, local_addr, &packet);
    }

    pub fn poll_idle(&mut self) {
        if !self.poll_preamble() {
            return;
        }
        self.handle_timeouts();
    }

    pub fn process_datagram(&mut self, peer: SocketAddr, local_addr: SocketAddr, packet: &[u8]) {
        if !self.poll_preamble() {
            return;
        }
        self.process_datagram_inner(peer, local_addr, packet);
    }

    fn process_datagram_inner(&mut self, peer: SocketAddr, local_addr: SocketAddr, packet: &[u8]) {
        self.metrics.inc_ingress_packet();

        let mut recv_data = BytesMut::from(packet);
        let header = match quiche::Header::from_slice(&mut recv_data, quiche::MAX_CONN_ID_LEN) {
            Ok(hdr) => hdr,
            Err(_) => {
                error!("Wrong QUIC HEADER");
                return;
            }
        };

        if header.ty == quiche::Type::VersionNegotiation {
            let len =
                match quiche::negotiate_version(&header.scid, &header.dcid, &mut self.send_buf) {
                    Ok(len) => len,
                    Err(e) => {
                        error!("Version negotiation failed: {:?}", e);
                        return;
                    }
                };

            if let Err(e) = self.socket.send_to(&self.send_buf[..len], peer) {
                error!("Failed to send version negotiation: {:?}", e);
            }
            return;
        }

        let h2_pool = self.h2_pool.clone();

        // First, try to find existing connection by DCID
        let lookup_key: Arc<[u8]> = Arc::from(header.dcid.as_ref());
        debug!(
            "Looking up connection with DCID: {:?}",
            hex::encode(&lookup_key)
        );
        let (mut connection, current_primary) =
            if let Some(mut conn) = self.connections.remove(&lookup_key) {
                self.peer_routes.remove(&conn.peer_address);
                conn.peer_address = peer;
                debug!("Found existing connection for {}", peer);
                (conn, lookup_key)
            } else if let Some(primary) = self.cid_routes.get(lookup_key.as_ref()).cloned() {
                if let Some(mut conn) = self.connections.remove(&primary) {
                    self.peer_routes.remove(&conn.peer_address);
                    conn.peer_address = peer;
                    debug!(
                        "Found existing connection via SCID alias {} -> {}",
                        hex::encode(&lookup_key),
                        hex::encode(&primary)
                    );
                    (conn, primary)
                } else {
                    // Stale alias entry.
                    self.cid_routes.remove(lookup_key.as_ref());
                    match self.take_or_create_connection(peer, local_addr, &recv_data) {
                        Some(conn) => {
                            debug!("Created new connection for {}", peer);
                            conn
                        }
                        None => {
                            debug!(
                                "Dropping packet for unknown connection from {} (DCID: {:?})",
                                peer,
                                hex::encode(&lookup_key)
                            );
                            return;
                        }
                    }
                }
            } else if let Some(primary) = self.peer_routes.get(&peer).cloned() {
                if let Some(mut conn) = self.connections.remove(&primary) {
                    self.peer_routes.remove(&conn.peer_address);
                    conn.peer_address = peer;
                    debug!(
                        "Found existing connection via peer map {} -> {}",
                        peer,
                        hex::encode(&primary)
                    );
                    (conn, primary)
                } else {
                    // Stale peer map entry.
                    self.peer_routes.remove(&peer);
                    match self.take_or_create_connection(peer, local_addr, &recv_data) {
                        Some(conn_pair) => {
                            debug!("Created new connection for {}", peer);
                            conn_pair
                        }
                        None => {
                            debug!(
                                "Dropping packet for unknown connection from {} (DCID: {:?})",
                                peer,
                                hex::encode(&lookup_key)
                            );
                            return;
                        }
                    }
                }
            } else {
                // No existing connection found, try to create new one.
                match self.take_or_create_connection(peer, local_addr, &recv_data) {
                    Some(conn_pair) => {
                        debug!("Created new connection for {}", peer);
                        conn_pair
                    }
                    None => {
                        debug!(
                            "Dropping packet for unknown connection from {} (DCID: {:?})",
                            peer,
                            hex::encode(&lookup_key)
                        );
                        return;
                    }
                }
            };

        let recv_info = quiche::RecvInfo {
            from: peer,
            to: local_addr,
        };

        if let Err(e) = connection.quic.recv(&mut recv_data, recv_info) {
            error!("QUIC recv failed: {:?}", e);
            self.remove_connection_routes(&connection);
            return;
        }

        if let Some(err) = connection.quic.peer_error() {
            error!("🔴 Peer error: {:?}", err);
        }

        if let Some(err) = connection.quic.local_error() {
            error!("🔴 Local error: {:?}", err);
        }

        connection.last_activity = Instant::now();
        connection.packets_since_rotation = connection.packets_since_rotation.saturating_add(1);

        // Debug logs
        debug!(
            "QUIC connection state - established: {}, in_early_data: {}, closed: {}",
            connection.quic.is_established(),
            connection.quic.is_in_early_data(),
            connection.quic.is_closed()
        );

        if (connection.quic.is_established() || connection.quic.is_in_early_data())
            && let Err(e) = Self::handle_h3(
                &mut connection,
                Arc::clone(&h2_pool),
                &self.upstream_pools,
                &self.upstream_inflight,
                Arc::clone(&self.global_inflight),
                self.backend_timeout,
                self.backend_body_idle_timeout,
                self.backend_body_total_timeout,
                self.backend_total_request_timeout,
                &self.routing_index,
                &self.metrics,
                &self.resilience,
                self.max_response_body_bytes,
            )
        {
            error!("HTTP/3 handling failed: {:?}", e);
            let _ = connection
                .quic
                .close(true, 0x1, b"http3 protocol handling error");
        }

        Self::maybe_rotate_scid(&mut connection, &self.metrics);

        let mut send_buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

        Self::flush_send(&self.socket, &mut send_buf, &mut connection);
        Self::handle_timeout(&self.socket, &mut send_buf, &mut connection);

        if !connection.quic.is_closed() {
            let new_primary = self.sync_connection_routes(&mut connection);
            debug!(
                "Storing connection with key: {:02x?} (previous: {:02x?})",
                &new_primary, &current_primary
            );
            self.peer_routes
                .insert(connection.peer_address, Arc::clone(&new_primary));
            self.connections
                .insert(Arc::clone(&new_primary), connection);
        } else {
            self.remove_connection_routes(&connection);
            debug!("Connection closed, not storing");
        }
    }

    fn handle_timeouts(&mut self) {
        if self.connections.is_empty() {
            return;
        }

        let mut send_buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];
        let mut to_remove = Vec::new();

        for (scid, connection) in self.connections.iter_mut() {
            let timeout = match connection.quic.timeout() {
                Some(timeout) => timeout,
                None => {
                    if connection.quic.is_closed() {
                        to_remove.push(scid.clone());
                    }
                    continue;
                }
            };

            if connection.last_activity.elapsed() >= timeout {
                connection.quic.on_timeout();
                connection.last_activity = Instant::now();
                Self::flush_send(&self.socket, &mut send_buf, connection);
            }

            if connection.quic.is_closed() {
                to_remove.push(scid.clone());
                continue;
            }

            // Advance in-flight streams independent of inbound packets.
            if let Some(mut h3) = connection.h3.take() {
                if let Err(e) = Self::advance_streams_non_blocking(
                    &mut connection.streams,
                    &mut connection.quic,
                    &mut h3,
                    &self.upstream_pools,
                    &self.routing_index,
                    self.backend_body_idle_timeout,
                    self.backend_body_total_timeout,
                    &self.metrics,
                    self.backend_total_request_timeout,
                    &self.resilience,
                    self.max_response_body_bytes,
                ) {
                    error!("advance_streams_non_blocking in timeout path: {:?}", e);
                }
                connection.h3 = Some(h3);
                Self::flush_send(&self.socket, &mut send_buf, connection);
            }
        }

        for scid in to_remove {
            if let Some(connection) = self.connections.remove(&scid) {
                self.remove_connection_routes(&connection);
            }
        }
    }

    fn handle_timeout(socket: &UdpSocket, send_buf: &mut [u8], connection: &mut QuicConnection) {
        let timeout = match connection.quic.timeout() {
            Some(timeout) => timeout,
            None => return,
        };

        if connection.last_activity.elapsed() >= timeout {
            connection.quic.on_timeout();
            connection.last_activity = Instant::now();
            Self::flush_send(socket, send_buf, connection);
        }
    }

    fn push_request_chunk(req: &mut RequestEnvelope, chunk: Bytes) -> Result<(), ()> {
        let next = req.body_buf_bytes.saturating_add(chunk.len());
        if next > REQUEST_BUFFERED_CHUNK_BYTES_LIMIT {
            return Err(());
        }
        req.body_buf_bytes = next;
        req.body_buf.push_back(chunk);
        Ok(())
    }

    fn enqueue_request_chunk(req: &mut RequestEnvelope, chunk: Bytes) -> Result<(), ()> {
        if let Some(tx) = &req.body_tx {
            match tx.try_send(chunk) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(chunk)) => Self::push_request_chunk(req, chunk),
                Err(TrySendError::Closed(_chunk)) => {
                    req.body_tx = None;
                    req.body_buf.clear();
                    req.body_buf_bytes = 0;
                    Ok(())
                }
            }
        } else {
            Self::push_request_chunk(req, chunk)
        }
    }

    fn flush_request_buffer(req: &mut RequestEnvelope) {
        let Some(tx) = req.body_tx.as_ref() else {
            return;
        };

        loop {
            let Some(chunk) = req.body_buf.pop_front() else {
                break;
            };
            let len = chunk.len();
            match tx.try_send(chunk) {
                Ok(()) => {
                    req.body_buf_bytes = req.body_buf_bytes.saturating_sub(len);
                }
                Err(TrySendError::Full(chunk)) => {
                    req.body_buf.push_front(chunk);
                    break;
                }
                Err(TrySendError::Closed(_chunk)) => {
                    req.body_buf.clear();
                    req.body_buf_bytes = 0;
                    req.body_tx = None;
                    break;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_h3(
        connection: &mut QuicConnection,
        h2_pool: Arc<H2Pool>,
        upstream_pools: &HashMap<String, Arc<Mutex<UpstreamPool>>>,
        upstream_inflight: &HashMap<String, Arc<Semaphore>>,
        global_inflight: Arc<Semaphore>,
        backend_timeout: Duration,
        backend_body_idle_timeout: Duration,
        backend_body_total_timeout: Duration,
        backend_total_request_timeout: Duration,
        routing_index: &RouteIndex,
        metrics: &Metrics,
        resilience: &RuntimeResilience,
        max_response_body_bytes: usize,
    ) -> Result<(), quiche::h3::Error> {
        let mut body_buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

        if connection.h3.is_none() {
            connection.h3 = Some(quiche::h3::Connection::with_transport(
                &mut connection.quic,
                &connection.h3_config,
            )?);
        }

        let h3 = match connection.h3.as_mut() {
            Some(h3) => h3,
            None => return Ok(()),
        };

        loop {
            match h3.poll(&mut connection.quic) {
                Ok((stream_id, quiche::h3::Event::Headers { list, .. })) => {
                    let mut method = String::new();
                    let mut path = String::new();
                    let mut authority = None;

                    for header in &list {
                        match header.name() {
                            b":method" => {
                                method = String::from_utf8_lossy(header.value()).to_string()
                            }
                            b":path" => path = String::from_utf8_lossy(header.value()).to_string(),
                            b":authority" | b"host" => {
                                authority =
                                    Some(String::from_utf8_lossy(header.value()).to_string())
                            }
                            _ => {}
                        }
                    }

                    if !method.is_empty() && !path.is_empty() {
                        info!("HTTP/3 request {} {}", method, path);
                    }

                    metrics.inc_total();
                    let request_start = Instant::now();

                    // Route lookup — needed to start the H2 request immediately.
                    let resolved = Self::resolve_backend(
                        &method,
                        &path,
                        authority.as_deref(),
                        upstream_pools,
                        routing_index,
                    );

                    let (
                        body_tx,
                        upstream_result_rx,
                        backend_addr,
                        backend_index,
                        upstream_name,
                        global_inflight_permit,
                        upstream_inflight_permit,
                        adaptive_admission_permit,
                        route_queue_permit,
                        request_fin_received,
                        bodyless_mode,
                    ) = match resolved {
                        Ok((upstream_name, addr, idx, upstream_pool)) => {
                            resilience.brownout.observe_admission_pressure(
                                resilience.adaptive_admission.inflight_percent(),
                            );
                            if !resilience.brownout.route_allowed(&upstream_name) {
                                metrics.inc_failure();
                                metrics.inc_overload_shed();
                                metrics.record_route(
                                    &upstream_name,
                                    request_start.elapsed(),
                                    RouteOutcome::OverloadShed,
                                );
                                Self::send_overload_response(
                                    h3,
                                    &mut connection.quic,
                                    stream_id,
                                    b"brownout active, non-core route shed\n",
                                    resilience.shed_retry_after_seconds,
                                )?;
                                resilience
                                    .adaptive_admission
                                    .observe(request_start.elapsed(), true);
                                continue;
                            }

                            let adaptive_permit = match resilience.adaptive_admission.try_acquire()
                            {
                                Some(permit) => permit,
                                None => {
                                    metrics.inc_failure();
                                    metrics.inc_overload_shed();
                                    metrics.record_route(
                                        &upstream_name,
                                        request_start.elapsed(),
                                        RouteOutcome::OverloadShed,
                                    );
                                    Self::send_overload_response(
                                        h3,
                                        &mut connection.quic,
                                        stream_id,
                                        b"adaptive admission overload\n",
                                        resilience.shed_retry_after_seconds,
                                    )?;
                                    resilience
                                        .adaptive_admission
                                        .observe(request_start.elapsed(), true);
                                    continue;
                                }
                            };

                            let route_queue_permit =
                                match resilience.route_queue.try_acquire(&upstream_name) {
                                    Ok(permit) => permit,
                                    Err(RouteQueueRejection::RouteCap) => {
                                        metrics.inc_failure();
                                        metrics.inc_overload_shed();
                                        metrics.record_route(
                                            &upstream_name,
                                            request_start.elapsed(),
                                            RouteOutcome::OverloadShed,
                                        );
                                        Self::send_overload_response(
                                            h3,
                                            &mut connection.quic,
                                            stream_id,
                                            b"route queue cap exceeded\n",
                                            resilience.shed_retry_after_seconds,
                                        )?;
                                        resilience
                                            .adaptive_admission
                                            .observe(request_start.elapsed(), true);
                                        continue;
                                    }
                                    Err(RouteQueueRejection::GlobalCap) => {
                                        metrics.inc_failure();
                                        metrics.inc_overload_shed();
                                        metrics.record_route(
                                            &upstream_name,
                                            request_start.elapsed(),
                                            RouteOutcome::OverloadShed,
                                        );
                                        Self::send_overload_response(
                                            h3,
                                            &mut connection.quic,
                                            stream_id,
                                            b"global queue cap exceeded\n",
                                            resilience.shed_retry_after_seconds,
                                        )?;
                                        resilience
                                            .adaptive_admission
                                            .observe(request_start.elapsed(), true);
                                        continue;
                                    }
                                };

                            let global_permit =
                                match Arc::clone(&global_inflight).try_acquire_owned() {
                                    Ok(permit) => permit,
                                    Err(_) => {
                                        metrics.inc_failure();
                                        metrics.inc_overload_shed();
                                        metrics.record_route(
                                            &upstream_name,
                                            request_start.elapsed(),
                                            RouteOutcome::OverloadShed,
                                        );
                                        Self::send_overload_response(
                                            h3,
                                            &mut connection.quic,
                                            stream_id,
                                            b"overloaded, retry later\n",
                                            resilience.shed_retry_after_seconds,
                                        )?;
                                        resilience
                                            .adaptive_admission
                                            .observe(request_start.elapsed(), true);
                                        continue;
                                    }
                                };

                            let upstream_permit =
                                match upstream_inflight.get(&upstream_name).cloned() {
                                    Some(semaphore) => match semaphore.try_acquire_owned() {
                                        Ok(permit) => permit,
                                        Err(_) => {
                                            drop(global_permit);
                                            metrics.inc_failure();
                                            metrics.inc_overload_shed();
                                            metrics.record_route(
                                                &upstream_name,
                                                request_start.elapsed(),
                                                RouteOutcome::OverloadShed,
                                            );
                                            Self::send_overload_response(
                                                h3,
                                                &mut connection.quic,
                                                stream_id,
                                                b"upstream overloaded, retry later\n",
                                                resilience.shed_retry_after_seconds,
                                            )?;
                                            resilience
                                                .adaptive_admission
                                                .observe(request_start.elapsed(), true);
                                            continue;
                                        }
                                    },
                                    None => {
                                        drop(global_permit);
                                        metrics.inc_failure();
                                        metrics.inc_overload_shed();
                                        metrics.record_route(
                                            &upstream_name,
                                            request_start.elapsed(),
                                            RouteOutcome::OverloadShed,
                                        );
                                        Self::send_simple_response(
                                            h3,
                                            &mut connection.quic,
                                            stream_id,
                                            http::StatusCode::SERVICE_UNAVAILABLE,
                                            b"no upstream throttle configured\n",
                                        )?;
                                        resilience
                                            .adaptive_admission
                                            .observe(request_start.elapsed(), true);
                                        continue;
                                    }
                                };

                            match h2_pool.has_capacity(&addr) {
                                Ok(true) => {}
                                Ok(false) => {
                                    drop(upstream_permit);
                                    drop(global_permit);
                                    metrics.inc_failure();
                                    metrics.inc_overload_shed();
                                    metrics.record_route(
                                        &upstream_name,
                                        request_start.elapsed(),
                                        RouteOutcome::OverloadShed,
                                    );
                                    Self::send_overload_response(
                                        h3,
                                        &mut connection.quic,
                                        stream_id,
                                        b"backend overloaded, retry later\n",
                                        resilience.shed_retry_after_seconds,
                                    )?;
                                    resilience
                                        .adaptive_admission
                                        .observe(request_start.elapsed(), true);
                                    continue;
                                }
                                Err(_) => {
                                    drop(upstream_permit);
                                    drop(global_permit);
                                    metrics.inc_failure();
                                    metrics.record_route(
                                        &upstream_name,
                                        request_start.elapsed(),
                                        RouteOutcome::Failure,
                                    );
                                    Self::send_simple_response(
                                        h3,
                                        &mut connection.quic,
                                        stream_id,
                                        http::StatusCode::SERVICE_UNAVAILABLE,
                                        b"no upstream available\n",
                                    )?;
                                    resilience
                                        .adaptive_admission
                                        .observe(request_start.elapsed(), true);
                                    continue;
                                }
                            }

                            let content_length = request_content_length(&list);
                            let bodyless_mode = content_length.unwrap_or(0) == 0
                                && (method.eq_ignore_ascii_case("GET")
                                    || method.eq_ignore_ascii_case("HEAD"));
                            let (tx, boxed, request_fin_received) = if bodyless_mode {
                                (None, BoxBody::new(Full::new(Bytes::new())), true)
                            } else {
                                // Create a channel body so quiche Data chunks stream
                                // directly into the in-flight H2 request.
                                let (tx, channel_body) =
                                    ChannelBody::channel(REQUEST_CHUNK_CHANNEL_CAPACITY);
                                (Some(tx), channel_body.boxed(), false)
                            };
                            let request =
                                match build_h2_request(&addr, &method, &path, &list, boxed, None) {
                                    Ok(request) => request,
                                    Err(err) => {
                                        drop(upstream_permit);
                                        drop(global_permit);
                                        metrics.inc_failure();
                                        metrics.record_route(
                                            &upstream_name,
                                            request_start.elapsed(),
                                            RouteOutcome::Failure,
                                        );
                                        Self::send_simple_response(
                                            h3,
                                            &mut connection.quic,
                                            stream_id,
                                            http::StatusCode::BAD_REQUEST,
                                            b"invalid request\n",
                                        )?;
                                        error!("failed to build upstream request: {}", err);
                                        resilience
                                            .adaptive_admission
                                            .observe(request_start.elapsed(), true);
                                        continue;
                                    }
                                };

                            let h2 = h2_pool.clone();
                            let fwd_addr = addr.clone();
                            let cb = Arc::clone(&resilience.circuit_breakers);
                            let retry_budget = Arc::clone(&resilience.retry_budget);
                            let route_name = upstream_name.clone();
                            let allow_hedge = bodyless_mode
                                && resilience.hedging_allowed_for(&method, &upstream_name, true);
                            let hedge_delay = resilience.hedging_delay;
                            let alternate_backend =
                                Self::pick_alternate_backend(&upstream_pool, idx);
                            let method_owned = method.clone();
                            let path_owned = path.clone();
                            let headers_owned = list.clone();
                            let (result_tx, result_rx) = oneshot::channel::<ForwardResult>();
                            let fut = async move {
                                let result = async {
                                    retry_budget.mark_primary(&route_name);

                                    let send_once =
                                        |backend: String,
                                         req: http::Request<
                                            BoxBody<Bytes, std::convert::Infallible>,
                                        >,
                                         cb: Arc<crate::resilience::CircuitBreakers>,
                                         h2: Arc<H2Pool>| async move {
                                            if !cb.allow_request(&backend) {
                                                return Err(ProxyError::Pool(
                                                    PoolError::CircuitOpen(backend),
                                                ));
                                            }
                                            let send_result = tokio::time::timeout(
                                                backend_timeout,
                                                h2.send(&backend, req),
                                            )
                                            .await
                                            .map_err(|_| ProxyError::Timeout);
                                            match &send_result {
                                                Ok(Ok(_)) => cb.record_success(&backend),
                                                _ => cb.record_failure(&backend),
                                            }
                                            Ok(send_result??)
                                        };

                                    let response: Response<Incoming> = if allow_hedge {
                                        let hedge_candidate = alternate_backend.clone().and_then(
                                            |(backend, _idx)| {
                                                build_h2_request(
                                                    &backend,
                                                    &method_owned,
                                                    &path_owned,
                                                    &headers_owned,
                                                    BoxBody::new(Full::new(Bytes::new())),
                                                    Some(0),
                                                )
                                                .ok()
                                                .map(|req| (backend, req))
                                            },
                                        );

                                        if let Some((hedge_backend, hedge_request)) =
                                            hedge_candidate
                                        {
                                            let primary_backend = fwd_addr.clone();
                                            let primary_fut = send_once(
                                                primary_backend,
                                                request,
                                                Arc::clone(&cb),
                                                Arc::clone(&h2),
                                            );
                                            tokio::pin!(primary_fut);
                                            let hedge_sleep = tokio::time::sleep(hedge_delay);
                                            tokio::pin!(hedge_sleep);

                                            if let Some(result) = tokio::select! {
                                                result = &mut primary_fut => Some(result),
                                                _ = &mut hedge_sleep => None,
                                            } {
                                                result?
                                            } else if retry_budget.allow_retry(&route_name) {
                                                let hedge_fut = send_once(
                                                    hedge_backend,
                                                    hedge_request,
                                                    Arc::clone(&cb),
                                                    Arc::clone(&h2),
                                                );
                                                tokio::pin!(hedge_fut);
                                                tokio::select! {
                                                    result = &mut primary_fut => result?,
                                                    result = &mut hedge_fut => result?,
                                                }
                                            } else {
                                                primary_fut.await?
                                            }
                                        } else {
                                            send_once(
                                                fwd_addr.clone(),
                                                request,
                                                Arc::clone(&cb),
                                                Arc::clone(&h2),
                                            )
                                            .await?
                                        }
                                    } else {
                                        match send_once(
                                            fwd_addr.clone(),
                                            request,
                                            Arc::clone(&cb),
                                            Arc::clone(&h2),
                                        )
                                        .await
                                        {
                                            Ok(response) => response,
                                            Err(primary_err) => {
                                                if bodyless_mode
                                                    && retry_budget.allow_retry(&route_name)
                                                    && let Some((retry_backend, _)) =
                                                        alternate_backend.clone()
                                                    && let Ok(retry_request) = build_h2_request(
                                                        &retry_backend,
                                                        &method_owned,
                                                        &path_owned,
                                                        &headers_owned,
                                                        BoxBody::new(Full::new(Bytes::new())),
                                                        Some(0),
                                                    )
                                                {
                                                    send_once(
                                                        retry_backend,
                                                        retry_request,
                                                        Arc::clone(&cb),
                                                        Arc::clone(&h2),
                                                    )
                                                    .await?
                                                } else {
                                                    return Err(primary_err);
                                                }
                                            }
                                        }
                                    };

                                    let (parts, body) = response.into_parts();
                                    Ok((parts.status, parts.headers, body))
                                }
                                .await;
                                // Ignore send error: receiver dropped means the stream was reset.
                                let _ = result_tx.send(result);
                            };
                            if !spawn_async_task(fut, "upstream") {
                                error!("dropping upstream task: no runtime available");
                            }
                            (
                                tx,
                                Some(result_rx),
                                Some(addr),
                                Some(idx),
                                Some(upstream_name),
                                Some(global_permit),
                                Some(upstream_permit),
                                Some(adaptive_permit),
                                Some(route_queue_permit),
                                request_fin_received,
                                bodyless_mode,
                            )
                        }
                        Err(err) => {
                            metrics.inc_failure();
                            metrics.record_route(
                                "unrouted",
                                request_start.elapsed(),
                                RouteOutcome::Failure,
                            );
                            let (status, body): (http::StatusCode, &[u8]) = match err {
                                ProxyError::Transport(_) => (
                                    http::StatusCode::SERVICE_UNAVAILABLE,
                                    b"no upstream available\n",
                                ),
                                ProxyError::Bridge(_) => {
                                    (http::StatusCode::BAD_REQUEST, b"invalid request\n")
                                }
                                _ => (
                                    http::StatusCode::INTERNAL_SERVER_ERROR,
                                    b"internal proxy error\n",
                                ),
                            };
                            Self::send_simple_response(
                                h3,
                                &mut connection.quic,
                                stream_id,
                                status,
                                body,
                            )?;
                            resilience
                                .adaptive_admission
                                .observe(request_start.elapsed(), true);
                            continue;
                        }
                    };

                    // App-level stream count cap: mirrors the QUIC max_streams_bidi
                    // limit so the streams HashMap can never grow beyond what the
                    // transport layer allows even if a race or misconfiguration
                    // delivers a stream-open event before the flow-control frame
                    // reaches the client.
                    if connection.streams.len() >= MAX_STREAMS_PER_CONNECTION {
                        warn!(
                            "stream limit reached ({} streams), rejecting stream {}",
                            MAX_STREAMS_PER_CONNECTION, stream_id
                        );
                        // Dropping the permits and body_tx here releases inflight
                        // semaphore slots and signals the upstream task to abort.
                        drop(body_tx);
                        drop(global_inflight_permit);
                        drop(upstream_inflight_permit);
                        drop(adaptive_admission_permit);
                        drop(route_queue_permit);
                        drop(upstream_result_rx);
                        Self::send_simple_response(
                            h3,
                            &mut connection.quic,
                            stream_id,
                            http::StatusCode::SERVICE_UNAVAILABLE,
                            b"too many concurrent streams\n",
                        )?;
                        continue;
                    }

                    connection.streams.insert(
                        stream_id,
                        RequestEnvelope {
                            method,
                            path,
                            authority,
                            body_tx,
                            body_buf: std::collections::VecDeque::new(),
                            body_buf_bytes: 0,
                            body_bytes_received: 0,
                            backend_addr,
                            backend_index,
                            upstream_name,
                            global_inflight_permit,
                            upstream_inflight_permit,
                            adaptive_admission_permit,
                            route_queue_permit,
                            start: request_start,
                            total_request_deadline: request_start + backend_total_request_timeout,
                            bodyless_mode,
                            phase: StreamPhase::ReceivingRequest,
                            request_fin_received,
                            upstream_result_rx,
                            response_chunk_rx: None,
                            response_headers_sent: false,
                            pending_chunk: None,
                        },
                    );
                }
                Ok((stream_id, quiche::h3::Event::Data)) => loop {
                    match h3.recv_body(&mut connection.quic, stream_id, &mut body_buf) {
                        Ok(read) => {
                            let mut shed_due_to_buffer_pressure = false;
                            let mut reject_body_for_bodyless = None::<(String, Duration)>;
                            let mut payload_too_large = None::<(String, Duration)>;
                            if let Some(req) = connection.streams.get_mut(&stream_id) {
                                if req.bodyless_mode && read > 0 {
                                    reject_body_for_bodyless = Some((
                                        req.upstream_name
                                            .clone()
                                            .unwrap_or_else(|| "unrouted".to_string()),
                                        req.start.elapsed(),
                                    ));
                                }
                                if reject_body_for_bodyless.is_none() {
                                    // Enforce cap on total bytes received for the stream,
                                    // including chunks already forwarded to the H2 body channel.
                                    let next_total = req.body_bytes_received.saturating_add(read);
                                    if next_total > MAX_REQUEST_BODY_BYTES {
                                        payload_too_large = Some((
                                            req.upstream_name
                                                .clone()
                                                .unwrap_or_else(|| "unrouted".to_string()),
                                            req.start.elapsed(),
                                        ));
                                    } else {
                                        req.body_bytes_received = next_total;

                                        for chunk_slice in
                                            body_buf[..read].chunks(REQUEST_CHUNK_BYTES_LIMIT)
                                        {
                                            let chunk = Bytes::copy_from_slice(chunk_slice);
                                            if Self::enqueue_request_chunk(req, chunk).is_err() {
                                                shed_due_to_buffer_pressure = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some((route_label, elapsed)) = reject_body_for_bodyless {
                                metrics.inc_failure();
                                metrics.record_route(&route_label, elapsed, RouteOutcome::Failure);
                                Self::send_simple_response(
                                    h3,
                                    &mut connection.quic,
                                    stream_id,
                                    http::StatusCode::BAD_REQUEST,
                                    b"request body not allowed for this request\n",
                                )?;
                                connection.streams.remove(&stream_id);
                                resilience.adaptive_admission.observe(elapsed, true);
                                break;
                            }
                            if let Some((route_label, elapsed)) = payload_too_large {
                                metrics.inc_failure();
                                metrics.record_route(&route_label, elapsed, RouteOutcome::Failure);
                                Self::send_simple_response(
                                    h3,
                                    &mut connection.quic,
                                    stream_id,
                                    http::StatusCode::PAYLOAD_TOO_LARGE,
                                    b"request body too large\n",
                                )?;
                                connection.streams.remove(&stream_id);
                                resilience.adaptive_admission.observe(elapsed, true);
                                break;
                            }
                            if shed_due_to_buffer_pressure
                                && let Some(req) = connection.streams.remove(&stream_id)
                            {
                                metrics.inc_failure();
                                metrics.inc_overload_shed();
                                let route_label =
                                    req.upstream_name.as_deref().unwrap_or("unrouted");
                                metrics.record_route(
                                    route_label,
                                    req.start.elapsed(),
                                    RouteOutcome::OverloadShed,
                                );
                                Self::send_overload_response(
                                    h3,
                                    &mut connection.quic,
                                    stream_id,
                                    b"request body backpressure overload\n",
                                    resilience.shed_retry_after_seconds,
                                )?;
                                resilience
                                    .adaptive_admission
                                    .observe(req.start.elapsed(), true);
                                break;
                            }
                        }
                        Err(quiche::h3::Error::Done) => break,
                        Err(err) => {
                            error!(
                                "HTTP/3 recv_body protocol error on stream {}: {:?}",
                                stream_id, err
                            );
                            if let Some(req) = connection.streams.remove(&stream_id) {
                                metrics.inc_failure();
                                let route_label =
                                    req.upstream_name.as_deref().unwrap_or("unrouted");
                                metrics.record_route(
                                    route_label,
                                    req.start.elapsed(),
                                    RouteOutcome::Failure,
                                );
                                resilience
                                    .adaptive_admission
                                    .observe(req.start.elapsed(), true);
                            }
                            let _ = Self::send_simple_response(
                                h3,
                                &mut connection.quic,
                                stream_id,
                                http::StatusCode::BAD_REQUEST,
                                b"malformed request stream\n",
                            );
                            break;
                        }
                    }
                },
                Ok((stream_id, quiche::h3::Event::Finished)) => {
                    if let Some(req) = connection.streams.get_mut(&stream_id) {
                        req.request_fin_received = true;

                        Self::flush_request_buffer(req);
                        // If buffer is now empty, drop body_tx to signal end-of-body.
                        if req.body_buf.is_empty() {
                            req.body_tx = None;
                        }
                        // Request body fully handed off — now waiting on upstream.
                        req.phase = StreamPhase::AwaitingUpstream;
                        // Upstream polling and response dispatch are handled entirely
                        // by advance_streams_non_blocking, called unconditionally below.
                    }
                }
                Ok((stream_id, quiche::h3::Event::Reset(_))) => {
                    connection.streams.remove(&stream_id);
                }
                Ok((_stream_id, quiche::h3::Event::PriorityUpdate)) => {}
                Ok((_stream_id, quiche::h3::Event::GoAway)) => {}
                Err(quiche::h3::Error::Done) => break,
                Err(e) => return Err(e),
            }
        }

        Self::advance_streams_non_blocking(
            &mut connection.streams,
            &mut connection.quic,
            h3,
            upstream_pools,
            routing_index,
            backend_body_idle_timeout,
            backend_body_total_timeout,
            metrics,
            backend_total_request_timeout,
            resilience,
            max_response_body_bytes,
        )?;

        Ok(())
    }

    /// Advance all in-flight streams without blocking.
    ///
    /// Called after every packet-driven `handle_h3` pass and from
    /// `handle_timeouts` so progress continues even when no new client
    /// packets arrive.
    ///
    /// Per stream, in order:
    /// 1. Drain request body buffer → body channel (`try_send`).
    /// 2. Close body channel once FIN received and buffer empty.
    /// 3. Poll `upstream_result_rx` (`try_recv`).
    ///    - Error result  → send error response, mark terminal.
    ///    - Ok result     → send H3 response headers, spawn body-pump task,
    ///      store `response_chunk_rx`, transition to SendingResponse.
    /// 4. Flush `response_chunk_rx` chunks into H3 (`try_recv` loop).
    ///    - `Data`  → `h3.send_body(..., false)`
    ///    - `End`   → `h3.send_body(..., true)`, mark Completed
    ///    - `Error` → send 502, mark Failed
    /// 5. Remove streams in terminal phase (Completed / Failed).
    #[allow(clippy::too_many_arguments)]
    fn advance_streams_non_blocking(
        streams: &mut HashMap<u64, RequestEnvelope>,
        quic: &mut quiche::Connection,
        h3: &mut quiche::h3::Connection,
        upstream_pools: &HashMap<String, Arc<Mutex<UpstreamPool>>>,
        routing_index: &RouteIndex,
        backend_body_idle_timeout: Duration,
        backend_body_total_timeout: Duration,
        metrics: &Metrics,
        _backend_total_request_timeout: Duration,
        resilience: &RuntimeResilience,
        max_response_body_bytes: usize,
    ) -> Result<(), quiche::h3::Error> {
        let stream_ids: Vec<u64> = streams.keys().copied().collect();

        for stream_id in stream_ids {
            if let Some(req) = streams.get(&stream_id)
                && Instant::now() >= req.total_request_deadline
            {
                if let Err(protocol_err) = Self::handle_forward_result(
                    h3,
                    quic,
                    stream_id,
                    req,
                    Err(ProxyError::Timeout),
                    upstream_pools,
                    routing_index,
                    metrics,
                    resilience.shed_retry_after_seconds,
                ) {
                    error!(
                        "failed to emit timeout response for stream {}: {:?}",
                        stream_id, protocol_err
                    );
                }
                resilience
                    .adaptive_admission
                    .observe(req.start.elapsed(), true);
                streams.remove(&stream_id);
                continue;
            }

            // ── 1 & 2: request body drain ────────────────────────────────────
            if let Some(req) = streams.get_mut(&stream_id) {
                Self::flush_request_buffer(req);
                if req.request_fin_received && req.body_buf.is_empty() {
                    req.body_tx = None; // signals EOF to the upstream H2 task
                }
            }

            // ── 3: poll upstream oneshot ──────────────────────────────────────
            // Only transition to response handling once request-body ingestion is
            // complete. This preserves request-size enforcement semantics:
            // oversized requests must still be able to terminate with 413 even if
            // upstream produced an early response.
            let can_poll_upstream = streams.get(&stream_id).is_some_and(|req| {
                req.phase == StreamPhase::AwaitingUpstream
                    && req.request_fin_received
                    && req.body_tx.is_none()
                    && req.body_buf.is_empty()
            });

            // upstream_ready: Option<ForwardResult>
            //   None          → oneshot not yet resolved (or not eligible), skip
            //   Some(Ok(...)) → upstream responded successfully
            //   Some(Err(.))  → upstream error (or sender dropped)
            let upstream_ready: Option<ForwardResult> = if can_poll_upstream {
                streams
                    .get_mut(&stream_id)
                    .and_then(|req| req.upstream_result_rx.as_mut())
                    .and_then(|rx| match rx.try_recv() {
                        Ok(result) => Some(result),
                        Err(oneshot::error::TryRecvError::Empty) => None,
                        Err(oneshot::error::TryRecvError::Closed) => Some(Err(
                            ProxyError::Transport("upstream task dropped sender".into()),
                        )),
                    })
            } else {
                None
            };

            if let Some(forward_result) = upstream_ready {
                if let Some(req) = streams.get_mut(&stream_id) {
                    req.upstream_result_rx = None;
                }
                match forward_result {
                    Ok((status, resp_headers, body)) => {
                        // If upstream advertised a response length beyond our hard cap,
                        // fail fast with 503 before sending any downstream headers/body.
                        let upstream_content_length = resp_headers
                            .get(http::header::CONTENT_LENGTH)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<usize>().ok());
                        if upstream_content_length.is_some_and(|len| len > max_response_body_bytes)
                        {
                            if let Some(req) = streams.get(&stream_id) {
                                metrics.inc_failure();
                                metrics.inc_overload_shed();
                                let route_label =
                                    req.upstream_name.as_deref().unwrap_or("unrouted");
                                metrics.record_route(
                                    route_label,
                                    req.start.elapsed(),
                                    RouteOutcome::OverloadShed,
                                );
                                resilience
                                    .adaptive_admission
                                    .observe(req.start.elapsed(), true);
                                warn!(
                                    "upstream declared content-length over cap ({} > {}) on stream {}",
                                    upstream_content_length.unwrap_or_default(),
                                    max_response_body_bytes,
                                    stream_id
                                );
                                let _ = Self::send_simple_response(
                                    h3,
                                    quic,
                                    stream_id,
                                    http::StatusCode::SERVICE_UNAVAILABLE,
                                    b"upstream response body too large\n",
                                );
                            }
                            streams.remove(&stream_id);
                            continue;
                        }

                        let mut owned_h3_headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                        for (name, value) in resp_headers.iter() {
                            if is_hop_header(name.as_str()) || name == http::header::CONTENT_LENGTH
                            {
                                continue;
                            }
                            owned_h3_headers.push((
                                name.as_str().as_bytes().to_vec(),
                                value.as_bytes().to_vec(),
                            ));
                        }

                        let defer_headers_until_body_validated = upstream_content_length.is_none();

                        if !defer_headers_until_body_validated {
                            // For declared-length responses within cap, emit headers immediately
                            // and stream body progressively.
                            let mut h3_headers = Vec::with_capacity(owned_h3_headers.len() + 1);
                            h3_headers.push(quiche::h3::Header::new(
                                b":status",
                                status.as_str().as_bytes(),
                            ));
                            for (name, value) in &owned_h3_headers {
                                h3_headers.push(quiche::h3::Header::new(name, value));
                            }
                            if let Err(err) = h3.send_response(quic, stream_id, &h3_headers, false)
                            {
                                if let Some(req) = streams.get(&stream_id) {
                                    let protocol = ProxyError::Protocol(format!(
                                        "failed to send HTTP/3 response headers: {:?}",
                                        err
                                    ));
                                    if let Err(protocol_err) = Self::handle_forward_result(
                                        h3,
                                        quic,
                                        stream_id,
                                        req,
                                        Err(protocol),
                                        upstream_pools,
                                        routing_index,
                                        metrics,
                                        resilience.shed_retry_after_seconds,
                                    ) {
                                        error!(
                                            "failed to emit protocol recovery response on stream {}: {:?}",
                                            stream_id, protocol_err
                                        );
                                    }
                                    resilience
                                        .adaptive_admission
                                        .observe(req.start.elapsed(), true);
                                }
                                streams.remove(&stream_id);
                                continue;
                            }
                        }

                        // Spawn a task that pumps body frames into a ResponseChunk channel.
                        // Enforces body deadlines; for unknown-length responses it first
                        // validates total body size against cap before emitting any headers.
                        let (chunk_tx, chunk_rx) =
                            mpsc::channel::<ResponseChunk>(RESPONSE_CHUNK_CHANNEL_CAPACITY);
                        let fail_tx = chunk_tx.clone();
                        // `backend_body_total_timeout` is used as a pre-first-byte guard:
                        // once the upstream starts making body progress, the idle timeout
                        // governs pacing and the stream may continue until request deadline.
                        let first_byte_deadline =
                            tokio::time::Instant::now() + backend_body_total_timeout;
                        let deferred_status = status;
                        let deferred_headers = owned_h3_headers.clone();
                        let fut = async move {
                            use http_body_util::BodyExt;
                            let mut body: hyper::body::Incoming = body;
                            let mut response_bytes_received: usize = 0;
                            let mut buffered_chunks: Vec<Bytes> = Vec::new();
                            let mut saw_body_progress = false;
                            loop {
                                let frame_fut = BodyExt::frame(&mut body);
                                let now = tokio::time::Instant::now();
                                if !saw_body_progress && now >= first_byte_deadline {
                                    let _ = chunk_tx
                                        .send(ResponseChunk::Error(ProxyError::Timeout))
                                        .await;
                                    return;
                                }
                                let wait_timeout = if saw_body_progress {
                                    backend_body_idle_timeout
                                } else {
                                    first_byte_deadline
                                        .saturating_duration_since(now)
                                        .min(backend_body_idle_timeout)
                                };
                                let result = tokio::time::timeout(wait_timeout, frame_fut).await;
                                match result {
                                    Err(_elapsed) => {
                                        // Body read idle timeout — signal timeout to flush loop.
                                        let _ = chunk_tx
                                            .send(ResponseChunk::Error(ProxyError::Timeout))
                                            .await;
                                        return;
                                    }
                                    Ok(Some(Ok(f))) => {
                                        if let Ok(data) = f.into_data() {
                                            if !data.is_empty() {
                                                saw_body_progress = true;
                                            }
                                            if defer_headers_until_body_validated {
                                                response_bytes_received = response_bytes_received
                                                    .saturating_add(data.len());
                                                if response_bytes_received > max_response_body_bytes
                                                {
                                                    let _ = chunk_tx
                                                        .send(ResponseChunk::Error(ProxyError::Pool(
                                                            PoolError::BackendOverloaded(
                                                                "upstream response body too large".into(),
                                                            ),
                                                        )))
                                                        .await;
                                                    return;
                                                }
                                                for start in (0..data.len())
                                                    .step_by(RESPONSE_CHUNK_BYTES_LIMIT)
                                                {
                                                    let end = (start + RESPONSE_CHUNK_BYTES_LIMIT)
                                                        .min(data.len());
                                                    buffered_chunks.push(data.slice(start..end));
                                                }
                                            } else {
                                                for start in (0..data.len())
                                                    .step_by(RESPONSE_CHUNK_BYTES_LIMIT)
                                                {
                                                    let end = (start + RESPONSE_CHUNK_BYTES_LIMIT)
                                                        .min(data.len());
                                                    if chunk_tx
                                                        .send(ResponseChunk::Data(
                                                            data.slice(start..end),
                                                        ))
                                                        .await
                                                        .is_err()
                                                    {
                                                        return;
                                                    }
                                                }
                                            }
                                        }
                                        // skip trailers / other frame types
                                    }
                                    Ok(Some(Err(_))) => {
                                        let _ = chunk_tx
                                            .send(ResponseChunk::Error(ProxyError::Transport(
                                                "upstream body error".into(),
                                            )))
                                            .await;
                                        return;
                                    }
                                    Ok(None) => {
                                        if defer_headers_until_body_validated {
                                            if chunk_tx
                                                .send(ResponseChunk::Start {
                                                    status: deferred_status,
                                                    headers: deferred_headers,
                                                })
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                            for chunk in buffered_chunks {
                                                if chunk_tx
                                                    .send(ResponseChunk::Data(chunk))
                                                    .await
                                                    .is_err()
                                                {
                                                    return;
                                                }
                                            }
                                        }
                                        let _ = chunk_tx.send(ResponseChunk::End).await;
                                        return;
                                    }
                                }
                            }
                        };
                        let spawned = spawn_async_task(fut, "body-pump");
                        if !spawned {
                            let _ = fail_tx.try_send(ResponseChunk::Error(ProxyError::Transport(
                                "runtime unavailable".into(),
                            )));
                        }

                        if let Some(req) = streams.get_mut(&stream_id) {
                            req.response_chunk_rx = Some(chunk_rx);
                            req.response_headers_sent = !defer_headers_until_body_validated;
                            req.phase = StreamPhase::SendingResponse;
                        }

                        // Update health/metrics for successful upstream response.
                        if let Some(req) = streams.get(&stream_id) {
                            if let (Some(addr), Some(idx)) = (&req.backend_addr, req.backend_index)
                            {
                                let upstream_name =
                                    routing_index.lookup(&req.path, req.authority.as_deref());
                                if let Some(pool) =
                                    upstream_name.and_then(|n| upstream_pools.get(n))
                                {
                                    let transition =
                                        pool.lock().ok().and_then(
                                            |mut p| match outcome_from_status(status) {
                                                crate::HealthClassification::Success => {
                                                    p.pool.mark_success(idx)
                                                }
                                                crate::HealthClassification::Failure => {
                                                    p.pool.mark_failure(idx)
                                                }
                                                crate::HealthClassification::Neutral => None,
                                            },
                                        );
                                    if let Some(t) = transition {
                                        Self::log_health_transition(addr, t);
                                    }
                                }
                            }
                            metrics.inc_success();
                            let route_label = req.upstream_name.as_deref().unwrap_or("unrouted");
                            metrics.record_route(
                                route_label,
                                req.start.elapsed(),
                                RouteOutcome::Success,
                            );
                            resilience
                                .adaptive_admission
                                .observe(req.start.elapsed(), false);
                            info!(
                                "Upstream {} status {} latency_ms {}",
                                req.backend_addr.as_deref().unwrap_or("?"),
                                status,
                                req.start.elapsed().as_millis()
                            );
                        }
                    }
                    Err(err) => {
                        // Send error response first, then remove the stream so
                        // cleanup only happens after the response has been emitted.
                        if let Some(req) = streams.get(&stream_id) {
                            if let Err(protocol_err) = Self::handle_forward_result(
                                h3,
                                quic,
                                stream_id,
                                req,
                                Err(err),
                                upstream_pools,
                                routing_index,
                                metrics,
                                resilience.shed_retry_after_seconds,
                            ) {
                                error!(
                                    "failed to emit recoverable forward error response on stream {}: {:?}",
                                    stream_id, protocol_err
                                );
                            }
                            resilience
                                .adaptive_admission
                                .observe(req.start.elapsed(), true);
                        }
                        streams.remove(&stream_id);
                        continue;
                    }
                }
            }

            // ── 4: flush response chunks ──────────────────────────────────────
            let mut terminal = false;
            if let Some(req) = streams.get_mut(&stream_id)
                && let Some(rx) = &mut req.response_chunk_rx
            {
                // Drain as many chunks as quiche will accept this iteration.
                loop {
                    // Retry any chunk that previously hit backpressure.
                    let chunk = match req.pending_chunk.take() {
                        Some(c) => c,
                        None => match rx.try_recv() {
                            Ok(c) => c,
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => {
                                req.phase = StreamPhase::Failed;
                                terminal = true;
                                break;
                            }
                        },
                    };
                    match chunk {
                        ResponseChunk::Start { status, headers } => {
                            let mut h3_headers = Vec::with_capacity(headers.len() + 1);
                            h3_headers.push(quiche::h3::Header::new(
                                b":status",
                                status.as_str().as_bytes(),
                            ));
                            for (name, value) in &headers {
                                h3_headers.push(quiche::h3::Header::new(name, value));
                            }
                            match h3.send_response(quic, stream_id, &h3_headers, false) {
                                Ok(_) => {
                                    req.response_headers_sent = true;
                                }
                                Err(quiche::h3::Error::StreamBlocked) => {
                                    req.pending_chunk =
                                        Some(ResponseChunk::Start { status, headers });
                                    break;
                                }
                                Err(err) => {
                                    error!(
                                        "HTTP/3 send_response protocol error on stream {}: {:?}",
                                        stream_id, err
                                    );
                                    req.phase = StreamPhase::Failed;
                                    metrics.inc_failure();
                                    metrics.inc_backend_error();
                                    let route_label =
                                        req.upstream_name.as_deref().unwrap_or("unrouted");
                                    metrics.record_route(
                                        route_label,
                                        req.start.elapsed(),
                                        RouteOutcome::BackendError,
                                    );
                                    resilience
                                        .adaptive_admission
                                        .observe(req.start.elapsed(), true);
                                    terminal = true;
                                    break;
                                }
                            }
                        }
                        ResponseChunk::Data(data) => {
                            match h3.send_body(quic, stream_id, &data, false) {
                                Ok(_) => {}
                                Err(quiche::h3::Error::StreamBlocked) => {
                                    // QUIC flow-control backpressure — retry next poll.
                                    req.pending_chunk = Some(ResponseChunk::Data(data));
                                    break;
                                }
                                Err(err) => {
                                    error!(
                                        "HTTP/3 send_body data protocol error on stream {}: {:?}",
                                        stream_id, err
                                    );
                                    req.phase = StreamPhase::Failed;
                                    metrics.inc_failure();
                                    metrics.inc_backend_error();
                                    let route_label =
                                        req.upstream_name.as_deref().unwrap_or("unrouted");
                                    metrics.record_route(
                                        route_label,
                                        req.start.elapsed(),
                                        RouteOutcome::BackendError,
                                    );
                                    resilience
                                        .adaptive_admission
                                        .observe(req.start.elapsed(), true);
                                    terminal = true;
                                    break;
                                }
                            }
                        }
                        ResponseChunk::End => match h3.send_body(quic, stream_id, b"", true) {
                            Ok(_) => {
                                req.phase = StreamPhase::Completed;
                                terminal = true;
                                break;
                            }
                            Err(quiche::h3::Error::StreamBlocked) => {
                                req.pending_chunk = Some(ResponseChunk::End);
                                break;
                            }
                            Err(err) => {
                                error!(
                                    "HTTP/3 send_body end protocol error on stream {}: {:?}",
                                    stream_id, err
                                );
                                req.phase = StreamPhase::Failed;
                                metrics.inc_failure();
                                metrics.inc_backend_error();
                                let route_label =
                                    req.upstream_name.as_deref().unwrap_or("unrouted");
                                metrics.record_route(
                                    route_label,
                                    req.start.elapsed(),
                                    RouteOutcome::BackendError,
                                );
                                resilience
                                    .adaptive_admission
                                    .observe(req.start.elapsed(), true);
                                terminal = true;
                                break;
                            }
                        },
                        ResponseChunk::Error(err) => {
                            // If headers are not emitted yet, return a deterministic
                            // HTTP error status instead of resetting or truncating.
                            if !req.response_headers_sent {
                                let (status, body): (http::StatusCode, &[u8]) = match &err {
                                    ProxyError::Timeout => (
                                        http::StatusCode::SERVICE_UNAVAILABLE,
                                        b"upstream timeout\n",
                                    ),
                                    ProxyError::Pool(PoolError::BackendOverloaded(_)) => (
                                        http::StatusCode::SERVICE_UNAVAILABLE,
                                        b"upstream response body too large\n",
                                    ),
                                    _ => (http::StatusCode::BAD_GATEWAY, b"upstream error\n"),
                                };
                                let _ =
                                    Self::send_simple_response(h3, quic, stream_id, status, body);
                            } else {
                                // Best-effort: close the stream.
                                let _ = h3.send_body(quic, stream_id, b"", true);
                            }
                            req.phase = StreamPhase::Failed;
                            // Mirror the health/metrics updates from the old
                            // send_backend_response timeout/error paths.
                            let upstream_name =
                                routing_index.lookup(&req.path, req.authority.as_deref());
                            if let (Some(idx), Some(pool)) = (
                                req.backend_index,
                                upstream_name.and_then(|n| upstream_pools.get(n)),
                            ) && let Some(t) =
                                pool.lock().ok().and_then(|mut p| p.pool.mark_failure(idx))
                                && let Some(addr) = &req.backend_addr
                            {
                                Self::log_health_transition(addr, t);
                            }
                            match err {
                                ProxyError::Timeout => {
                                    metrics.inc_failure();
                                    metrics.inc_timeout();
                                    let route_label =
                                        req.upstream_name.as_deref().unwrap_or("unrouted");
                                    metrics.record_route(
                                        route_label,
                                        req.start.elapsed(),
                                        RouteOutcome::Timeout,
                                    );
                                    resilience
                                        .adaptive_admission
                                        .observe(req.start.elapsed(), true);
                                    info!(
                                        "Upstream {} body timeout latency_ms {}",
                                        req.backend_addr.as_deref().unwrap_or("?"),
                                        req.start.elapsed().as_millis()
                                    );
                                }
                                _ => {
                                    metrics.inc_failure();
                                    metrics.inc_backend_error();
                                    let route_label =
                                        req.upstream_name.as_deref().unwrap_or("unrouted");
                                    metrics.record_route(
                                        route_label,
                                        req.start.elapsed(),
                                        RouteOutcome::BackendError,
                                    );
                                    resilience
                                        .adaptive_admission
                                        .observe(req.start.elapsed(), true);
                                    error!(
                                        "Upstream {} body error: {:?}",
                                        req.backend_addr.as_deref().unwrap_or("?"),
                                        err
                                    );
                                }
                            }
                            terminal = true;
                            break;
                        }
                    }
                }
            }

            // ── 5: remove terminal streams ────────────────────────────────────
            if terminal {
                streams.remove(&stream_id);
            }
        }

        Ok(())
    }

    /// Resolve routing + LB for a request, returning `(backend_addr, backend_index, pool)`.
    fn resolve_backend(
        method: &str,
        path: &str,
        authority: Option<&str>,
        upstream_pools: &HashMap<String, Arc<Mutex<UpstreamPool>>>,
        routing_index: &RouteIndex,
    ) -> Result<ResolvedBackend, ProxyError> {
        if method.is_empty() || path.is_empty() {
            return Err(ProxyError::Transport("empty method or path".into()));
        }

        let upstream_name = routing_index
            .lookup(path, authority)
            .ok_or_else(|| ProxyError::Transport(format!("no route for {path}")))?;

        let upstream_pool = upstream_pools
            .get(upstream_name)
            .ok_or_else(|| ProxyError::Transport(format!("pool not found: {upstream_name}")))?
            .clone();

        let key: &str = authority.unwrap_or(if !path.is_empty() { path } else { method });

        let (backend_index, lb_type, backend_addr) = {
            let mut pool = upstream_pool
                .lock()
                .map_err(|_| ProxyError::Transport("upstream pool lock poisoned".into()))?;
            if pool.pool.is_empty() {
                return Err(ProxyError::Transport("no servers in upstream".into()));
            }
            let lb_type = pool.lb_name();
            let idx = pool.pick(key);
            let idx = idx.ok_or_else(|| ProxyError::Transport("no healthy servers".into()))?;
            let backend_addr = pool
                .pool
                .address(idx)
                .map(str::to_string)
                .ok_or_else(|| ProxyError::Transport("invalid server address".into()))?;
            (idx, lb_type, backend_addr)
        };

        info!("Selected backend {} via {}", backend_addr, lb_type);
        Ok((
            upstream_name.to_string(),
            backend_addr,
            backend_index,
            upstream_pool,
        ))
    }

    fn pick_alternate_backend(
        upstream_pool: &Arc<Mutex<UpstreamPool>>,
        primary_index: usize,
    ) -> Option<(String, usize)> {
        let pool = upstream_pool.lock().ok()?;
        let healthy = pool.pool.healthy_indices();
        for index in healthy {
            if index == primary_index {
                continue;
            }
            if let Some(address) = pool.pool.address(index) {
                return Some((address.to_string(), index));
            }
        }
        None
    }

    /// Handle an already-resolved `ForwardResult`, applying health transitions
    /// and sending the H3 response.
    #[allow(clippy::too_many_arguments)]
    fn handle_forward_result(
        h3: &mut quiche::h3::Connection,
        quic: &mut quiche::Connection,
        stream_id: u64,
        req: &RequestEnvelope,
        result: ForwardResult,
        upstream_pools: &HashMap<String, Arc<Mutex<UpstreamPool>>>,
        routing_index: &RouteIndex,
        metrics: &Metrics,
        overload_retry_after_seconds: u32,
    ) -> Result<(), quiche::h3::Error> {
        let start = req.start;
        let route_label = req.upstream_name.as_deref().unwrap_or("unrouted");

        // If routing failed at Headers time, return an appropriate error now.
        let (backend_addr, backend_index) = match (&req.backend_addr, req.backend_index) {
            (Some(a), Some(i)) => (a.as_str(), i),
            _ => {
                metrics.inc_failure();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::Failure);
                return Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    if req.method.is_empty() || req.path.is_empty() {
                        http::StatusCode::BAD_REQUEST
                    } else {
                        http::StatusCode::SERVICE_UNAVAILABLE
                    },
                    b"no upstream available\n",
                );
            }
        };

        // Re-acquire the upstream pool for health marking.
        let upstream_name = routing_index.lookup(&req.path, req.authority.as_deref());
        let upstream_pool = upstream_name.and_then(|n| upstream_pools.get(n)).cloned();

        match result {
            Ok(_) => {
                error!("Unexpected successful forward result in error handler path");
                metrics.inc_failure();
                metrics.inc_backend_error();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::BackendError);
                Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    http::StatusCode::BAD_GATEWAY,
                    b"unexpected upstream state\n",
                )
            }
            Err(ProxyError::Bridge(err)) => {
                error!("Bridge error: {:?}", err);
                metrics.inc_failure();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::Failure);
                info!(
                    "Upstream {} status 400 latency_ms {}",
                    backend_addr,
                    start.elapsed().as_millis()
                );
                Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    http::StatusCode::BAD_REQUEST,
                    b"invalid request\n",
                )
            }
            Err(ProxyError::Pool(PoolError::BackendOverloaded(_))) => {
                info!("Backend overloaded");
                metrics.inc_failure();
                metrics.inc_overload_shed();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::OverloadShed);
                info!(
                    "Upstream {} status 503 latency_ms {}",
                    backend_addr,
                    start.elapsed().as_millis()
                );
                Self::send_overload_response(
                    h3,
                    quic,
                    stream_id,
                    b"backend overloaded, retry later\n",
                    overload_retry_after_seconds,
                )
            }
            Err(ProxyError::Pool(PoolError::CircuitOpen(_))) => {
                info!("Backend circuit open");
                metrics.inc_failure();
                metrics.inc_overload_shed();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::OverloadShed);
                info!(
                    "Upstream {} status 503 latency_ms {}",
                    backend_addr,
                    start.elapsed().as_millis()
                );
                Self::send_overload_response(
                    h3,
                    quic,
                    stream_id,
                    b"backend circuit open, retry later\n",
                    overload_retry_after_seconds,
                )
            }
            Err(ProxyError::Transport(_))
            | Err(ProxyError::Pool(PoolError::Send(_)))
            | Err(ProxyError::Pool(PoolError::InflightLimiterClosed))
            | Err(ProxyError::Pool(PoolError::UnknownBackend(_))) => {
                error!("Transport error");
                if let Some(pool) = &upstream_pool
                    && let Some(t) = pool
                        .lock()
                        .ok()
                        .and_then(|mut p| p.pool.mark_failure(backend_index))
                {
                    Self::log_health_transition(backend_addr, t);
                }
                metrics.inc_failure();
                metrics.inc_backend_error();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::BackendError);
                info!(
                    "Upstream {} status 502 latency_ms {}",
                    backend_addr,
                    start.elapsed().as_millis()
                );
                Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    http::StatusCode::BAD_GATEWAY,
                    b"upstream error\n",
                )
            }
            Err(ProxyError::Protocol(err)) => {
                error!("Protocol error: {}", err);
                metrics.inc_failure();
                metrics.inc_backend_error();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::BackendError);
                Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    http::StatusCode::BAD_GATEWAY,
                    b"upstream protocol error\n",
                )
            }
            Err(ProxyError::Timeout) => {
                error!("Server timeout");
                if let Some(pool) = &upstream_pool
                    && let Some(t) = pool
                        .lock()
                        .ok()
                        .and_then(|mut p| p.pool.mark_failure(backend_index))
                {
                    Self::log_health_transition(backend_addr, t);
                }
                metrics.inc_failure();
                metrics.inc_timeout();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::Timeout);
                info!(
                    "Upstream {} status 503 latency_ms {}",
                    backend_addr,
                    start.elapsed().as_millis()
                );
                Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    http::StatusCode::SERVICE_UNAVAILABLE,
                    b"upstream timeout\n",
                )
            }
            Err(ProxyError::Tls(err)) => {
                error!("TLS error: {}", err);
                metrics.inc_failure();
                metrics.record_route(route_label, start.elapsed(), RouteOutcome::Failure);
                info!(
                    "TLS error for stream {} latency_ms {}",
                    stream_id,
                    start.elapsed().as_millis()
                );
                Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    b"internal server error\n",
                )
            }
        }
    }

    fn send_simple_response(
        h3: &mut quiche::h3::Connection,
        quic: &mut quiche::Connection,
        stream_id: u64,
        status: http::StatusCode,
        body: &[u8],
    ) -> Result<(), quiche::h3::Error> {
        let resp_headers = vec![
            quiche::h3::Header::new(b":status", status.as_str().as_bytes()),
            quiche::h3::Header::new(b"content-type", b"text/plain"),
            quiche::h3::Header::new(b"content-length", body.len().to_string().as_bytes()),
        ];

        h3.send_response(quic, stream_id, &resp_headers, false)?;
        h3.send_body(quic, stream_id, body, true)?;
        Ok(())
    }

    fn send_overload_response(
        h3: &mut quiche::h3::Connection,
        quic: &mut quiche::Connection,
        stream_id: u64,
        body: &[u8],
        retry_after_seconds: u32,
    ) -> Result<(), quiche::h3::Error> {
        let retry_after = retry_after_seconds.max(1).to_string();
        let resp_headers = vec![
            quiche::h3::Header::new(
                b":status",
                http::StatusCode::SERVICE_UNAVAILABLE.as_str().as_bytes(),
            ),
            quiche::h3::Header::new(b"content-type", b"text/plain"),
            quiche::h3::Header::new(b"retry-after", retry_after.as_bytes()),
            quiche::h3::Header::new(b"content-length", body.len().to_string().as_bytes()),
        ];

        h3.send_response(quic, stream_id, &resp_headers, false)?;
        h3.send_body(quic, stream_id, body, true)?;
        Ok(())
    }

    fn flush_send(socket: &UdpSocket, send_buf: &mut [u8], connection: &mut QuicConnection) {
        let mut packet_count = 0;

        loop {
            match connection.quic.send(send_buf) {
                Ok((write, send_info)) => {
                    packet_count += 1;
                    debug!("Sending {} bytes to {}", write, send_info.to);
                    if let Err(e) = socket.send_to(&send_buf[..write], send_info.to) {
                        error!("Failed to send UDP packet: {:?}", e);
                        break;
                    }
                }
                Err(quiche::Error::Done) => break,
                Err(e) => {
                    error!("QUIC send failed: {:?}", e);
                    break;
                }
            }
        }

        if packet_count > 0 {
            debug!("Sent {} packets", packet_count);
        }
    }

    fn log_health_transition(addr: &str, transition: HealthTransition) {
        match transition {
            HealthTransition::BecameHealthy => {
                info!("Backend {} became healthy", addr);
            }
            HealthTransition::BecameUnhealthy => {
                error!("Backend {} became unhealthy", addr);
            }
        }
    }

    fn spawn_metrics_endpoint(config: &SpookyConfig, metrics: Arc<Metrics>) {
        let endpoint = &config.observability.metrics;
        if !endpoint.enabled {
            return;
        }

        let bind = format!("{}:{}", endpoint.address, endpoint.port);
        let metrics_path = endpoint.path.clone();

        let handle = match runtime_handle() {
            Some(handle) => handle,
            None => {
                error!("Metrics endpoint disabled (no Tokio runtime available)");
                return;
            }
        };

        // Bind synchronously so endpoint readiness does not race with task scheduling.
        let std_listener = match std::net::TcpListener::bind(&bind) {
            Ok(listener) => listener,
            Err(err) => {
                error!("Failed to bind metrics endpoint {}: {}", bind, err);
                return;
            }
        };
        if let Err(err) = std_listener.set_nonblocking(true) {
            error!(
                "Failed to set metrics endpoint listener nonblocking ({}): {}",
                bind, err
            );
            return;
        }
        let listener = match tokio::net::TcpListener::from_std(std_listener) {
            Ok(listener) => listener,
            Err(err) => {
                error!(
                    "Failed to register metrics endpoint listener {}: {}",
                    bind, err
                );
                return;
            }
        };

        spawn_supervised_async_task(
            &handle,
            "metrics-endpoint",
            Some(Arc::clone(&metrics)),
            async move {
                info!(
                    "Metrics endpoint listening on http://{}{}",
                    bind, metrics_path
                );

                loop {
                    let (stream, _peer) = match listener.accept().await {
                        Ok(v) => v,
                        Err(err) => {
                            error!("Metrics endpoint accept failed: {}", err);
                            continue;
                        }
                    };

                    let io = TokioIo::new(stream);
                    let metrics = Arc::clone(&metrics);
                    let metrics_path = metrics_path.clone();

                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<Incoming>| {
                            let metrics = Arc::clone(&metrics);
                            let metrics_path = metrics_path.clone();
                            async move {
                                Ok::<_, hyper::Error>(Self::handle_metrics_request(
                                    req,
                                    &metrics_path,
                                    metrics,
                                ))
                            }
                        });

                        if let Err(err) = http1::Builder::new().serve_connection(io, service).await
                        {
                            error!("Metrics endpoint connection failed: {}", err);
                        }
                    });
                }
            },
        );
    }

    fn handle_metrics_request(
        req: Request<Incoming>,
        metrics_path: &str,
        metrics: Arc<Metrics>,
    ) -> Response<Full<Bytes>> {
        if req.uri().path() != metrics_path {
            return match Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found\n")))
            {
                Ok(resp) => resp,
                Err(_) => Response::new(Full::new(Bytes::from_static(b"not found\n"))),
            };
        }

        let body = metrics.render_prometheus();
        match Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; version=0.0.4")
            .body(Full::new(Bytes::from(body)))
        {
            Ok(resp) => resp,
            Err(_) => Response::new(Full::new(Bytes::from_static(b"failed to render metrics\n"))),
        }
    }

    fn spawn_watchdog(
        config: &SpookyConfig,
        metrics: Arc<Metrics>,
        resilience: Arc<RuntimeResilience>,
        watchdog: Arc<WatchdogCoordinator>,
    ) {
        let watchdog_config = WatchdogRuntimeConfig::from(&config.resilience.watchdog);
        if !watchdog_config.enabled || !watchdog.enabled() {
            return;
        }

        let handle = match runtime_handle() {
            Some(handle) => handle,
            None => {
                error!("Watchdog disabled: no Tokio runtime available");
                return;
            }
        };

        spawn_supervised_async_task(
            &handle,
            "watchdog",
            Some(Arc::clone(&metrics)),
            async move {
                info!(
                    "Watchdog enabled: check_interval_ms={} poll_stall_timeout_ms={} timeout_error_rate_percent={} overload_inflight_percent={} unhealthy_windows={} drain_grace_ms={} restart_cooldown_ms={}",
                    watchdog_config.check_interval_ms,
                    watchdog_config.poll_stall_timeout_ms,
                    watchdog_config.timeout_error_rate_percent,
                    watchdog_config.overload_inflight_percent,
                    watchdog_config.unhealthy_consecutive_windows,
                    watchdog_config.drain_grace_ms,
                    watchdog_config.restart_cooldown_ms,
                );

                let mut interval =
                    tokio::time::interval(Duration::from_millis(watchdog_config.check_interval_ms));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let has_restart_hook = watchdog_config
                    .restart_hook
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|value| !value.is_empty());

                let mut previous_requests = metrics.requests_total.load(Ordering::Relaxed);
                let mut previous_timeouts = metrics.backend_timeouts.load(Ordering::Relaxed);
                let mut degraded_windows = 0u32;

                loop {
                    interval.tick().await;
                    let now = now_millis();
                    let stalled = now.saturating_sub(watchdog.last_poll_progress_ms())
                        > watchdog_config.poll_stall_timeout_ms;

                    let current_requests = metrics.requests_total.load(Ordering::Relaxed);
                    let current_timeouts = metrics.backend_timeouts.load(Ordering::Relaxed);
                    let request_delta = current_requests.saturating_sub(previous_requests);
                    let timeout_delta = current_timeouts.saturating_sub(previous_timeouts);
                    previous_requests = current_requests;
                    previous_timeouts = current_timeouts;

                    let timeout_rate_percent = if request_delta == 0 {
                        0
                    } else {
                        timeout_delta.saturating_mul(100) / request_delta
                    };

                    let timeout_pressure = request_delta >= watchdog_config.min_requests_per_window
                        && timeout_rate_percent
                            >= watchdog_config.timeout_error_rate_percent as u64;
                    let overload_pressure = resilience.adaptive_admission.inflight_percent()
                        >= watchdog_config.overload_inflight_percent;

                    if stalled || timeout_pressure || overload_pressure {
                        degraded_windows = degraded_windows.saturating_add(1);
                        watchdog.set_degraded(true);
                        metrics.inc_watchdog_degraded_window();
                    } else {
                        degraded_windows = 0;
                        watchdog.set_degraded(false);
                    }

                    if degraded_windows >= watchdog_config.unhealthy_consecutive_windows {
                        if !has_restart_hook {
                            warn!(
                                "Watchdog detected unhealthy runtime state, but restart_hook is not configured"
                            );
                            degraded_windows = 0;
                            continue;
                        }
                        let mut reasons = Vec::new();
                        if stalled {
                            reasons.push("poll_stall");
                        }
                        if timeout_pressure {
                            reasons.push("timeout_spike");
                        }
                        if overload_pressure {
                            reasons.push("inflight_overload");
                        }
                        let reason = reasons.join("+");
                        if watchdog.request_restart(&reason) {
                            metrics.inc_watchdog_restart_request();
                            warn!("Watchdog requested safe restart: {}", reason);
                        }
                        degraded_windows = 0;
                    }

                    if !watchdog.restart_requested() {
                        continue;
                    }

                    let requested_at = watchdog.restart_requested_at_ms();
                    let grace_elapsed = requested_at != 0
                        && now.saturating_sub(requested_at) >= watchdog_config.drain_grace_ms;
                    if !watchdog.workers_drained() && !grace_elapsed {
                        continue;
                    }

                    let restart_reason = watchdog.restart_reason();
                    if watchdog.workers_drained() {
                        info!(
                            "Watchdog safe restart condition reached (all workers drained): {}",
                            restart_reason
                        );
                    } else {
                        warn!(
                            "Watchdog restart drain grace elapsed; executing hook without full drain: {}",
                            restart_reason
                        );
                    }

                    let cmd = watchdog_config
                        .restart_hook
                        .as_deref()
                        .map(str::trim)
                        .unwrap_or_default();
                    let status = tokio::process::Command::new("/bin/sh")
                        .arg("-c")
                        .arg(cmd)
                        .env("SPOOKY_WATCHDOG_REASON", &restart_reason)
                        .status()
                        .await;
                    match status {
                        Ok(status) => {
                            info!(
                                "Watchdog restart hook exited with status {}",
                                status
                                    .code()
                                    .map(|code| code.to_string())
                                    .unwrap_or_else(|| "signal".to_string())
                            );
                        }
                        Err(err) => {
                            error!("Watchdog restart hook execution failed: {}", err);
                        }
                    }
                    metrics.inc_watchdog_restart_hook();

                    watchdog.complete_restart_cycle();
                }
            },
        );
    }

    fn spawn_health_checks(
        upstream_pools: HashMap<String, Arc<Mutex<UpstreamPool>>>,
        health_client: Arc<H2Client>,
        metrics: Arc<Metrics>,
    ) {
        let entries = {
            let mut all_entries = Vec::new();
            for (_upstream_name, upstream_pool) in upstream_pools.iter() {
                let pool = match upstream_pool.lock() {
                    Ok(pool) => pool,
                    Err(_) => continue,
                };
                for index in pool.pool.all_indices() {
                    if let (Some(address), Some(health)) =
                        (pool.pool.address(index), pool.pool.health_check(index))
                    {
                        all_entries.push((
                            upstream_pool.clone(),
                            index,
                            address.to_string(),
                            health,
                        ));
                    }
                }
            }
            all_entries
        };

        let handle = match runtime_handle() {
            Some(handle) => handle,
            None => {
                error!("Health checks disabled: no Tokio runtime available");
                return;
            }
        };

        for (upstream_pool, index, address, health) in entries {
            let health_client = Arc::clone(&health_client);
            let task_metrics = Arc::clone(&metrics);
            let handle = handle.clone();
            let supervise_metrics = Arc::clone(&task_metrics);
            spawn_supervised_async_task(&handle, "health-check", Some(supervise_metrics), async move {
                let interval = Duration::from_millis(health.interval.max(1));
                let timeout = Duration::from_millis(health.timeout_ms.max(1));
                let path: &str = if health.path.is_empty() {
                    "/"
                } else {
                    &health.path
                };
                let endpoint = match BackendEndpoint::parse(&address) {
                    Ok(endpoint) => endpoint,
                    Err(err) => {
                        error!(
                            "disabling health checks for backend '{}' due to invalid endpoint: {}",
                            address, err
                        );
                        return;
                    }
                };
                let health_uri = endpoint.uri_for_path(path);

                loop {
                    tokio::time::sleep(interval).await;

                    let request = match http::Request::builder()
                        .method("GET")
                        .uri(&health_uri)
                        .body(BoxBody::new(Full::new(Bytes::new())))
                    {
                        Ok(req) => req,
                        Err(_) => continue,
                    };

                    let result = tokio::time::timeout(timeout, health_client.send(request)).await;

                    let healthy = match result {
                        Ok(Ok(response)) => response.status().is_success(),
                        _ => false,
                    };
                    if healthy {
                        task_metrics.inc_health_check_success();
                    } else {
                        task_metrics.inc_health_check_failure();
                    }

                    let transition = match upstream_pool.lock() {
                        Ok(mut pool) => {
                            if healthy {
                                pool.pool.mark_success(index)
                            } else {
                                pool.pool.mark_failure(index)
                            }
                        }
                        Err(_) => None,
                    };

                    if let Some(transition) = transition {
                        Self::log_health_transition(&address, transition);
                    }
                }
            });
        }
    }
}

pub fn configure_async_runtime(worker_threads: usize) {
    let threads = worker_threads.max(1);
    if FALLBACK_RT.get().is_some() {
        warn!(
            "async runtime already initialized; ignoring new worker_threads={}",
            threads
        );
        return;
    }
    FALLBACK_RT_THREADS.store(threads, Ordering::Relaxed);
}

fn runtime_handle() -> Option<Handle> {
    if let Ok(handle) = Handle::try_current() {
        return Some(handle);
    }
    fallback_runtime().map(|rt| rt.handle().clone())
}

fn spawn_async_task<F>(fut: F, _task_name: &str) -> bool
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if let Some(handle) = runtime_handle() {
        handle.spawn(fut);
        true
    } else {
        false
    }
}

fn spawn_supervised_async_task<F>(
    handle: &Handle,
    task_name: &'static str,
    metrics: Option<Arc<Metrics>>,
    fut: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    let task_name = task_name.to_string();
    let join = handle.spawn(fut);
    let monitor_handle = handle.clone();
    monitor_handle.spawn(async move {
        match join.await {
            Ok(()) => {}
            Err(err) => {
                if let Some(metrics) = metrics {
                    metrics.inc_runtime_panic();
                }
                if err.is_panic() {
                    error!("Background task '{}' panicked", task_name);
                } else {
                    warn!("Background task '{}' cancelled", task_name);
                }
            }
        }
    });
}

fn fallback_runtime() -> Option<&'static tokio::runtime::Runtime> {
    FALLBACK_RT
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(FALLBACK_RT_THREADS.load(Ordering::Relaxed))
                .thread_name("spooky-edge-fallback-rt")
                .build()
                .ok()
        })
        .as_ref()
}

static FALLBACK_RT: OnceLock<Option<tokio::runtime::Runtime>> = OnceLock::new();
static FALLBACK_RT_THREADS: AtomicUsize = AtomicUsize::new(2);

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::cid_radix::CidRadix;

    use super::{TokenBucket, resolve_primary_from_radix_prefix};

    fn cid(bytes: &[u8]) -> Arc<[u8]> {
        Arc::from(bytes)
    }

    #[test]
    fn prefix_match_on_alias_resolves_to_primary_connection() {
        let primary = cid(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let alias = cid(&[9, 10, 11, 12, 13, 14, 15, 16]);

        let mut connections: HashMap<Arc<[u8]>, ()> = HashMap::new();
        connections.insert(Arc::clone(&primary), ());

        let mut cid_routes = HashMap::new();
        cid_routes.insert(Arc::clone(&alias), Arc::clone(&primary));

        let mut cid_radix = CidRadix::new();
        cid_radix.insert(Arc::clone(&alias));

        let mut dcid = alias.as_ref().to_vec();
        dcid.extend_from_slice(&[0xAA, 0xBB]);

        let resolved =
            resolve_primary_from_radix_prefix(&dcid, &connections, &mut cid_routes, &mut cid_radix)
                .expect("prefix lookup should resolve to active primary");

        assert_eq!(resolved.as_ref(), primary.as_ref());
        assert!(
            cid_routes.get(alias.as_ref()).is_some(),
            "live alias should remain mapped to active primary"
        );
        assert!(
            cid_radix.longest_prefix_match(&dcid).is_some(),
            "live alias should remain indexed in radix"
        );
    }

    #[test]
    fn stale_alias_prefix_match_is_cleaned_up() {
        let primary = cid(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let alias = cid(&[9, 10, 11, 12, 13, 14, 15, 16]);

        let connections: HashMap<Arc<[u8]>, ()> = HashMap::new();

        let mut cid_routes = HashMap::new();
        cid_routes.insert(Arc::clone(&alias), Arc::clone(&primary));

        let mut cid_radix = CidRadix::new();
        cid_radix.insert(Arc::clone(&alias));

        let mut dcid = alias.as_ref().to_vec();
        dcid.extend_from_slice(&[0xAA, 0xBB]);

        let resolved =
            resolve_primary_from_radix_prefix(&dcid, &connections, &mut cid_routes, &mut cid_radix);
        assert!(resolved.is_none(), "stale alias must not resolve");
        assert!(
            cid_routes.get(alias.as_ref()).is_none(),
            "stale alias mapping should be removed"
        );
        assert!(
            cid_radix.longest_prefix_match(alias.as_ref()).is_none(),
            "stale alias should be removed from radix"
        );
    }

    // -----------------------------------------------------------------------
    // TokenBucket unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn token_bucket_allows_up_to_burst_immediately() {
        let mut tb = TokenBucket::new(100, 5);
        // Bucket starts full; first 5 tokens should all succeed.
        for i in 0..5 {
            assert!(
                tb.try_consume(),
                "token {} should be available (burst=5)",
                i
            );
        }
        // 6th token must fail — bucket is empty.
        assert!(
            !tb.try_consume(),
            "6th token must be denied when burst exhausted"
        );
    }

    #[test]
    fn token_bucket_refills_over_time() {
        let mut tb = TokenBucket::new(1_000_000, 2); // 1 M tokens/sec = 1 per µs
        // Drain the bucket.
        assert!(tb.try_consume());
        assert!(tb.try_consume());
        assert!(!tb.try_consume());

        // Sleep slightly longer than 1 token's worth at 1M/s (1µs).
        std::thread::sleep(std::time::Duration::from_micros(5));

        // At least one token must have been refilled.
        assert!(
            tb.try_consume(),
            "bucket should have refilled at least one token after sleep"
        );
    }

    #[test]
    fn token_bucket_rate_zero_clamps_to_one() {
        // rate=0 is clamped to 1; burst=0 is clamped to 1.
        let mut tb = TokenBucket::new(0, 0);
        // Starts with 1 token (burst=1).
        assert!(
            tb.try_consume(),
            "first token should succeed with clamped burst=1"
        );
        assert!(!tb.try_consume(), "second token must fail when burst=1");
    }

    #[test]
    fn token_bucket_never_exceeds_burst() {
        // With rate=1/s a burst of 3 should yield exactly 3 tokens on a fresh
        // bucket, then nothing more (refill is 1ns per second — negligible in a
        // tight loop running for microseconds).
        let burst = 3u32;
        let mut tb = TokenBucket::new(1, burst); // 1 token/sec → ~1ns per token
        let mut consumed = 0;
        for _ in 0..(burst + 10) {
            if tb.try_consume() {
                consumed += 1;
            }
        }
        assert_eq!(
            consumed, burst as usize,
            "fresh bucket must yield exactly burst={} tokens in a tight loop, got {}",
            burst, consumed
        );
    }
}
