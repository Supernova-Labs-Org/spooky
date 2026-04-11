use std::{
    collections::{HashMap, HashSet},
    net::UdpSocket,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use core::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use log::{debug, error, info};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use quiche::Config;
use quiche::h3::NameValue;
use rand::RngCore;
use smallvec::SmallVec;
use spooky_bridge::h3_to_h2::build_h2_request;
use spooky_errors::ProxyError;
use spooky_lb::{HealthTransition, UpstreamPool};
use spooky_transport::h2_pool::H2Pool;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

use spooky_config::config::Config as SpookyConfig;

use crate::{
    ChannelBody, ForwardResult, Metrics, QUICListener, QuicConnection, RequestEnvelope,
    ResponseChunk, RouteOutcome, StreamPhase,
    cid_radix::CidRadix,
    constants::{
        DEFAULT_SCID_LEN_BYTES, MAX_DATAGRAM_SIZE_BYTES, MAX_INFLIGHT_PER_BACKEND,
        MAX_REQUEST_BODY_BYTES, MAX_UDP_PAYLOAD_BYTES, MIN_SCID_LEN_BYTES, QUIC_IDLE_TIMEOUT_MS,
        QUIC_INITIAL_MAX_DATA, QUIC_INITIAL_MAX_STREAMS_BIDI, QUIC_INITIAL_MAX_STREAMS_UNI,
        QUIC_INITIAL_STREAM_DATA, RESET_TOKEN_LEN_BYTES, SCID_ROTATION_PACKET_THRESHOLD,
        UDP_READ_TIMEOUT_MS, backend_timeout, drain_timeout, scid_rotation_interval,
    },
    outcome_from_status,
    route_index::RouteIndex,
};

fn is_hop_header(name: &str) -> bool {
    matches!(
        name,
        "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding" | "upgrade"
    )
}

impl QUICListener {
    pub fn new(config: SpookyConfig) -> Result<Self, ProxyError> {
        let socket_address = format!("{}:{}", &config.listen.address, &config.listen.port);

        let socket = UdpSocket::bind(socket_address.as_str()).expect("Failed to bind UDP socker");
        socket
            .set_read_timeout(Some(Duration::from_millis(UDP_READ_TIMEOUT_MS)))
            .expect("Failed to set UDP read timeout");

        let mut quic_config = Config::new(quiche::PROTOCOL_VERSION).expect("REASON");

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
            .unwrap();
        quic_config.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
        quic_config.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
        quic_config.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
        quic_config.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
        quic_config.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
        quic_config.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
        quic_config.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
        quic_config.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
        quic_config.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
        quic_config.set_disable_active_migration(true);
        quic_config.verify_peer(false);

        // CRITICAL FIX: Explicitly disable 0-RTT/early data
        // This prevents clients from attempting 0-RTT that we can't handle

        debug!("Listening on {}", socket_address);

        let h3_config =
            Arc::new(quiche::h3::Config::new().expect("Failed to create HTTP/3 config"));
        let backend_addresses = config
            .upstream
            .values()
            .flat_map(|upstream| {
                upstream
                    .backends
                    .iter()
                    .map(|backend| backend.address.clone())
            })
            .collect::<Vec<_>>();
        let h2_pool = Arc::new(H2Pool::new(backend_addresses, MAX_INFLIGHT_PER_BACKEND));

        let mut upstream_pools = HashMap::new();
        for (name, upstream) in &config.upstream {
            let upstream_pool =
                UpstreamPool::from_upstream(upstream).expect("Failed to create upstream pool");
            upstream_pools.insert(name.clone(), Arc::new(Mutex::new(upstream_pool)));
        }
        let routing_index = RouteIndex::from_upstreams(&config.upstream);

        let metrics = Arc::new(Metrics::default());

        Self::spawn_health_checks(upstream_pools.clone(), h2_pool.clone());
        Self::spawn_metrics_endpoint(&config, Arc::clone(&metrics));

        Ok(Self {
            socket,
            config,
            quic_config,
            h3_config,
            h2_pool,
            upstream_pools,
            routing_index,
            metrics,
            draining: false,
            drain_start: None,
            recv_buf: [0; MAX_DATAGRAM_SIZE_BYTES],
            send_buf: [0; MAX_DATAGRAM_SIZE_BYTES],
            connections: HashMap::new(),
            cid_routes: HashMap::new(),
            peer_routes: HashMap::new(),
            cid_radix: CidRadix::new(),
        })
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

        if let Some(start) = self.drain_start
            && start.elapsed() >= drain_timeout()
        {
            self.close_all();
            return true;
        }

        false
    }

    fn close_all(&mut self) {
        let socket = match self.socket.try_clone() {
            Ok(sock) => sock,
            Err(e) => {
                error!("Failed to clone UDP socket: {:?}", e);
                return;
            }
        };

        let mut send_buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];
        for connection in self.connections.values_mut() {
            let _ = connection.quic.close(true, 0x0, b"draining");
            Self::flush_send(&socket, &mut send_buf, connection);
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
        if header.ty == quiche::Type::Short && dcid_bytes.len() > MIN_SCID_LEN_BYTES {
            if let Some(matched_cid) = self.cid_radix.longest_prefix_match(&dcid_bytes) {
                debug!(
                    "Found connection via prefix match. Stored CID: {:02x?}, Packet DCID: {:02x?}",
                    matched_cid, &dcid_bytes
                );
                let stored_cid_copy: Arc<[u8]> = Arc::clone(&matched_cid);
                if let Some(mut connection) = self.connections.remove(&stored_cid_copy) {
                    self.peer_routes.remove(&connection.peer_address);
                    connection.peer_address = peer;
                    return Some((connection, stored_cid_copy));
                }
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
            primary_scid: scid_bytes.to_vec(),
            routing_scids: HashSet::from([scid_bytes.to_vec()]),
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
        self.cid_radix.remove(&connection.primary_scid);
        self.cid_routes.remove(&connection.primary_scid);
        for cid in &connection.routing_scids {
            self.cid_radix.remove(cid);
            self.cid_routes.remove(cid);
        }
        self.peer_routes.remove(&connection.peer_address);
    }

    fn sync_connection_routes(&mut self, connection: &mut QuicConnection) -> Vec<u8> {
        let mut active_scids: HashSet<Vec<u8>> = connection
            .quic
            .source_ids()
            .map(|cid| cid.as_ref().to_vec())
            .collect();

        if active_scids.is_empty() {
            active_scids.insert(connection.primary_scid.clone());
        }

        let active_source_id = connection.quic.source_id().as_ref().to_vec();
        let primary = if active_scids.contains(&active_source_id) {
            active_source_id
        } else if active_scids.contains(&connection.primary_scid) {
            connection.primary_scid.clone()
        } else {
            active_scids
                .iter()
                .next()
                .cloned()
                .unwrap_or_else(|| connection.primary_scid.clone())
        };

        for retired in connection.routing_scids.difference(&active_scids) {
            self.cid_radix.remove(retired);
            self.cid_routes.remove(retired);
        }

        self.cid_routes.remove(&primary);
        for cid in &active_scids {
            if *cid == primary {
                continue;
            }
            self.cid_routes.insert(cid.clone(), primary.clone());
        }

        // Add new SCIDs to radix trie
        for cid in &active_scids {
            // Get Arc<[u8]> reference from connections HashMap
            if let Some(arc_cid) = self
                .connections
                .keys()
                .find(|k| k.as_ref() == cid.as_slice())
            {
                self.cid_radix.insert(Arc::clone(arc_cid)); // NEW
            }
        }

        connection.routing_scids = active_scids;
        connection.primary_scid = primary.clone();
        primary
    }

    pub fn poll(&mut self) {
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

        info!("Length of data recived: {}", len);

        let local_addr = match self.socket.local_addr() {
            Ok(addr) => addr,
            Err(_) => return,
        };

        let socket = match self.socket.try_clone() {
            Ok(sock) => sock,
            Err(e) => {
                error!("Failed to clone UDP socket: {:?}", e);
                return;
            }
        };

        // let mut recv_data = self.recv_buf[..len].to_vec();
        let mut recv_data = BytesMut::from(&self.recv_buf[..len]);

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

            if let Err(e) = socket.send_to(&self.send_buf[..len], peer) {
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
            } else if let Some(primary_vec) = self.cid_routes.get(lookup_key.as_ref()).cloned() {
                let primary: Arc<[u8]> = Arc::from(primary_vec.as_slice());
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
                &self.routing_index,
                &self.metrics,
            )
        {
            error!("HTTP/3 handling failed: {:?}", e);
        }

        Self::maybe_rotate_scid(&mut connection, &self.metrics);

        let mut send_buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

        Self::flush_send(&socket, &mut send_buf, &mut connection);
        Self::handle_timeout(&socket, &mut send_buf, &mut connection);

        if !connection.quic.is_closed() {
            let new_primary_vec = self.sync_connection_routes(&mut connection);
            let new_primary: Arc<[u8]> = Arc::from(new_primary_vec.as_slice());
            debug!(
                "Storing connection with key: {:02x?} (previous: {:02x?})",
                &new_primary, &current_primary
            );
            self.peer_routes
                .insert(connection.peer_address, Arc::clone(&new_primary));
            self.connections.insert(new_primary, connection);
        } else {
            self.remove_connection_routes(&connection);
            debug!("Connection closed, not storing");
        }
    }

    fn handle_timeouts(&mut self) {
        if self.connections.is_empty() {
            return;
        }

        let socket = match self.socket.try_clone() {
            Ok(sock) => sock,
            Err(e) => {
                error!("Failed to clone UDP socket: {:?}", e);
                return;
            }
        };

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
                Self::flush_send(&socket, &mut send_buf, connection);
            }

            if connection.quic.is_closed() {
                to_remove.push(scid.clone());
                continue;
            }

            // Advance in-flight streams independent of inbound packets.
            if connection.h3.is_some() {
                let mut h3 = connection.h3.take().unwrap();
                if let Err(e) = Self::advance_streams_non_blocking(
                    &mut connection.streams,
                    &mut connection.quic,
                    &mut h3,
                    &self.upstream_pools,
                    &self.routing_index,
                    &self.metrics,
                ) {
                    error!("advance_streams_non_blocking in timeout path: {:?}", e);
                }
                connection.h3 = Some(h3);
                Self::flush_send(&socket, &mut send_buf, connection);
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

    fn handle_h3(
        connection: &mut QuicConnection,
        h2_pool: Arc<H2Pool>,
        upstream_pools: &HashMap<String, Arc<Mutex<UpstreamPool>>>,
        routing_index: &RouteIndex,
        metrics: &Metrics,
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
                    let mut headers =
                        SmallVec::<[(Vec<u8>, Vec<u8>); 16]>::with_capacity(list.len());

                    for header in list {
                        headers.push((header.name().to_vec(), header.value().to_vec()));
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

                    // Route lookup — needed to start the H2 request immediately.
                    let resolved = Self::resolve_backend(
                        &method,
                        &path,
                        authority.as_deref(),
                        upstream_pools,
                        routing_index,
                    );

                    let (body_tx, upstream_result_rx, backend_addr, backend_index, upstream_name) =
                        match resolved {
                            Ok((upstream_name, addr, idx, _pool)) => {
                            // Create a channel body so quiche Data chunks stream
                            // directly into the in-flight H2 request.
                            let (tx, channel_body) = ChannelBody::channel(8);
                            let boxed = channel_body.boxed();
                            let h2 = h2_pool.clone();
                            let req_headers = headers.clone();
                            let req_method = method.clone();
                            let req_path = path.clone();
                            let fwd_addr = addr.clone();
                            let (result_tx, result_rx) = oneshot::channel::<ForwardResult>();
                            let fut = async move {
                                let result = async {
                                    let request = build_h2_request(
                                        &fwd_addr,
                                        &req_method,
                                        &req_path,
                                        &req_headers,
                                        boxed,
                                        None,
                                    )
                                    .map_err(ProxyError::Bridge)?;

                                    let response = tokio::time::timeout(
                                        backend_timeout(),
                                        h2.send(&fwd_addr, request),
                                    )
                                    .await
                                    .map_err(|_| ProxyError::Timeout)??;

                                    let (parts, body) = response.into_parts();
                                    Ok((parts.status, parts.headers, body))
                                }
                                .await;
                                // Ignore send error: receiver dropped means the stream was reset.
                                let _ = result_tx.send(result);
                            };
                            match Handle::try_current() {
                                Ok(handle) => {
                                    handle.spawn(fut);
                                }
                                Err(_) => {
                                    fallback_runtime().spawn(fut);
                                }
                            }
                                (Some(tx), Some(result_rx), Some(addr), Some(idx), Some(upstream_name))
                        }
                            Err(_) => (None, None, None, None, None),
                        };

                    metrics.inc_total();
                    connection.streams.insert(
                        stream_id,
                        RequestEnvelope {
                            method,
                            path,
                            authority,
                            headers,
                            body_tx,
                            body_buf: Vec::new(),
                            body_bytes_received: 0,
                            backend_addr,
                            backend_index,
                            upstream_name,
                            start: Instant::now(),
                            phase: StreamPhase::ReceivingRequest,
                            request_fin_received: false,
                            upstream_result_rx,
                            response_chunk_rx: None,
                            pending_chunk: None,
                        },
                    );
                }
                Ok((stream_id, quiche::h3::Event::Data)) => loop {
                    match h3.recv_body(&mut connection.quic, stream_id, &mut body_buf) {
                        Ok(read) => {
                            if let Some(req) = connection.streams.get_mut(&stream_id) {
                                // Enforce cap on total bytes received for the stream,
                                // including chunks already forwarded to the H2 body channel.
                                let next_total = req.body_bytes_received.saturating_add(read);
                                if next_total > MAX_REQUEST_BODY_BYTES {
                                    connection.streams.remove(&stream_id);
                                    Self::send_simple_response(
                                        h3,
                                        &mut connection.quic,
                                        stream_id,
                                        http::StatusCode::PAYLOAD_TOO_LARGE,
                                        b"request body too large\n",
                                    )?;
                                    break;
                                }
                                req.body_bytes_received = next_total;
                                let chunk = Bytes::copy_from_slice(&body_buf[..read]);
                                if let Some(tx) = &req.body_tx {
                                    // Non-blocking send: if the channel is full, fall
                                    // back to buffering so the poll loop is not stalled.
                                    match tx.try_send(chunk.clone()) {
                                        Ok(_) => {}
                                        Err(_) => req.body_buf.push(chunk),
                                    }
                                } else {
                                    req.body_buf.push(chunk);
                                }
                            }
                        }
                        Err(quiche::h3::Error::Done) => break,
                        Err(e) => return Err(e),
                    }
                },
                Ok((stream_id, quiche::h3::Event::Finished)) => {
                    if let Some(req) = connection.streams.get_mut(&stream_id) {
                        req.request_fin_received = true;

                        // Non-blocking body flush: push buffered chunks into the
                        // channel with try_send.  Any that don't fit stay in body_buf
                        // and will be retried on the next poll iteration.
                        if let Some(tx) = &req.body_tx {
                            let buf = std::mem::take(&mut req.body_buf);
                            let mut overflow = Vec::new();
                            for chunk in buf {
                                if tx.try_send(chunk.clone()).is_err() {
                                    overflow.push(chunk);
                                }
                            }
                            req.body_buf = overflow;
                        }
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
            metrics,
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
    ///                      store `response_chunk_rx`, transition to SendingResponse.
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
        metrics: &Metrics,
    ) -> Result<(), quiche::h3::Error> {
        let stream_ids: Vec<u64> = streams.keys().copied().collect();

        for stream_id in stream_ids {
            // ── 1 & 2: request body drain ────────────────────────────────────
            if let Some(req) = streams.get_mut(&stream_id) {
                if let Some(tx) = &req.body_tx {
                    let buf = std::mem::take(&mut req.body_buf);
                    let mut overflow = Vec::new();
                    for chunk in buf {
                        if tx.try_send(chunk.clone()).is_err() {
                            overflow.push(chunk);
                        }
                    }
                    req.body_buf = overflow;
                }
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
                        // Send H3 response headers immediately (non-blocking).
                        let mut h3_headers = Vec::with_capacity(resp_headers.len() + 1);
                        h3_headers.push(quiche::h3::Header::new(
                            b":status",
                            status.as_str().as_bytes(),
                        ));
                        for (name, value) in resp_headers.iter() {
                            if is_hop_header(name.as_str()) || name == http::header::CONTENT_LENGTH
                            {
                                continue;
                            }
                            h3_headers.push(quiche::h3::Header::new(
                                name.as_str().as_bytes(),
                                value.as_bytes(),
                            ));
                        }
                        h3.send_response(quic, stream_id, &h3_headers, false)?;

                        // Spawn a task that pumps body frames into a ResponseChunk channel.
                        // Enforces backend_timeout() so a slow upstream body does not
                        // leave the stream open indefinitely.
                        let (chunk_tx, chunk_rx) = mpsc::channel::<ResponseChunk>(16);
                        let deadline = tokio::time::Instant::now() + backend_timeout();
                        let fut = async move {
                            use http_body_util::BodyExt;
                            let mut body: hyper::body::Incoming = body;
                            loop {
                                let frame_fut = BodyExt::frame(&mut body);
                                let result = tokio::time::timeout_at(deadline, frame_fut).await;
                                match result {
                                    Err(_elapsed) => {
                                        // Body read timed out — signal timeout to flush loop.
                                        let _ = chunk_tx
                                            .send(ResponseChunk::Error(ProxyError::Timeout))
                                            .await;
                                        return;
                                    }
                                    Ok(Some(Ok(f))) => {
                                        if let Ok(data) = f.into_data() {
                                            if chunk_tx
                                                .send(ResponseChunk::Data(data))
                                                .await
                                                .is_err()
                                            {
                                                return;
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
                                        let _ = chunk_tx.send(ResponseChunk::End).await;
                                        return;
                                    }
                                }
                            }
                        };
                        match Handle::try_current() {
                            Ok(h) => {
                                h.spawn(fut);
                            }
                            Err(_) => {
                                fallback_runtime().spawn(fut);
                            }
                        }

                        if let Some(req) = streams.get_mut(&stream_id) {
                            req.response_chunk_rx = Some(chunk_rx);
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
                            let route_label =
                                req.upstream_name.as_deref().unwrap_or("unrouted");
                            metrics.record_route(
                                route_label,
                                req.start.elapsed(),
                                RouteOutcome::Success,
                            );
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
                            Self::handle_forward_result(
                                h3,
                                quic,
                                stream_id,
                                req,
                                Err(err),
                                upstream_pools,
                                routing_index,
                                metrics,
                            )?;
                        }
                        streams.remove(&stream_id);
                        continue;
                    }
                }
            }

            // ── 4: flush response chunks ──────────────────────────────────────
            let mut terminal = false;
            if let Some(req) = streams.get_mut(&stream_id) {
                if let Some(rx) = &mut req.response_chunk_rx {
                    // Drain as many chunks as quiche will accept this iteration.
                    loop {
                        // Retry any chunk that previously hit backpressure.
                        let chunk = match req.pending_chunk.take() {
                            Some(c) => c,
                            None => match rx.try_recv() {
                                Ok(c) => c,
                                Err(_) => break, // Empty or closed — nothing to flush
                            },
                        };
                        match chunk {
                            ResponseChunk::Data(data) => {
                                match h3.send_body(quic, stream_id, &data, false) {
                                    Ok(_) => {}
                                    Err(quiche::h3::Error::StreamBlocked) => {
                                        // QUIC flow-control backpressure — retry next poll.
                                        req.pending_chunk = Some(ResponseChunk::Data(data));
                                        break;
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                            ResponseChunk::End => {
                                h3.send_body(quic, stream_id, b"", true)?;
                                req.phase = StreamPhase::Completed;
                                terminal = true;
                                break;
                            }
                            ResponseChunk::Error(err) => {
                                // Best-effort: close the stream.
                                let _ = h3.send_body(quic, stream_id, b"", true);
                                req.phase = StreamPhase::Failed;
                                // Mirror the health/metrics updates from the old
                                // send_backend_response timeout/error paths.
                                let upstream_name =
                                    routing_index.lookup(&req.path, req.authority.as_deref());
                                if let (Some(idx), Some(pool)) = (
                                    req.backend_index,
                                    upstream_name.and_then(|n| upstream_pools.get(n)),
                                ) {
                                    if let Some(t) =
                                        pool.lock().ok().and_then(|mut p| p.pool.mark_failure(idx))
                                    {
                                        if let Some(addr) = &req.backend_addr {
                                            Self::log_health_transition(addr, t);
                                        }
                                    }
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
    ) -> Result<(String, String, usize, Arc<Mutex<UpstreamPool>>), ProxyError> {
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

        let upstream_len = upstream_pool.lock().map(|p| p.pool.len()).unwrap_or(0);
        if upstream_len == 0 {
            return Err(ProxyError::Transport("no servers in upstream".into()));
        }

        let key: &str = authority.unwrap_or(if !path.is_empty() { path } else { method });

        let (backend_index, lb_type) = {
            let mut pool = upstream_pool.lock().expect("upstream pool lock");
            let lb_type = pool.lb_name();
            let idx = pool.pick(key);
            (idx, lb_type)
        };
        let backend_index =
            backend_index.ok_or_else(|| ProxyError::Transport("no healthy servers".into()))?;

        let backend_addr = {
            let pool = upstream_pool.lock().expect("upstream pool lock");
            pool.pool
                .address(backend_index)
                .map(|a| a.to_string())
                .ok_or_else(|| ProxyError::Transport("invalid server address".into()))?
        };

        info!("Selected backend {} via {}", backend_addr, lb_type);
        Ok((
            upstream_name.to_string(),
            backend_addr,
            backend_index,
            upstream_pool,
        ))
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
            // Ok variant is handled upstream by advance_streams_non_blocking;
            // handle_forward_result is only called with Err variants.
            Ok(_) => unreachable!("handle_forward_result called with Ok result"),
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
            Err(ProxyError::Transport(_)) | Err(ProxyError::Pool(_)) => {
                error!("Transport error");
                if let Some(pool) = &upstream_pool {
                    if let Some(t) = pool
                        .lock()
                        .ok()
                        .and_then(|mut p| p.pool.mark_failure(backend_index))
                    {
                        Self::log_health_transition(backend_addr, t);
                    }
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
            Err(ProxyError::Timeout) => {
                error!("Server timeout");
                if let Some(pool) = &upstream_pool {
                    if let Some(t) = pool
                        .lock()
                        .ok()
                        .and_then(|mut p| p.pool.mark_failure(backend_index))
                    {
                        Self::log_health_transition(backend_addr, t);
                    }
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

        let handle = match Handle::try_current() {
            Ok(h) => h,
            Err(err) => {
                error!("Metrics endpoint disabled (no Tokio runtime): {}", err);
                return;
            }
        };

        handle.spawn(async move {
            let listener = match tokio::net::TcpListener::bind(&bind).await {
                Ok(l) => l,
                Err(err) => {
                    error!("Failed to bind metrics endpoint {}: {}", bind, err);
                    return;
                }
            };
            info!("Metrics endpoint listening on http://{}{}", bind, metrics_path);

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

                    if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                        error!("Metrics endpoint connection failed: {}", err);
                    }
                });
            }
        });
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
            Err(_) => Response::new(Full::new(Bytes::from_static(
                b"failed to render metrics\n",
            ))),
        }
    }

    fn spawn_health_checks(
        upstream_pools: HashMap<String, Arc<Mutex<UpstreamPool>>>,
        h2_pool: Arc<H2Pool>,
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

        let handle = match Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                error!("Health checks disabled: no Tokio runtime available");
                return;
            }
        };

        for (upstream_pool, index, address, health) in entries {
            let h2_pool = h2_pool.clone();
            let handle = handle.clone();
            handle.spawn(async move {
                let interval = Duration::from_millis(health.interval.max(1));
                let timeout = Duration::from_millis(health.timeout_ms.max(1));
                let path: &str = if health.path.is_empty() {
                    "/"
                } else {
                    &health.path
                };

                loop {
                    tokio::time::sleep(interval).await;

                    let request = match http::Request::builder()
                        .method("GET")
                        .uri(format!("http://{address}{path}"))
                        .body(BoxBody::new(Full::new(Bytes::new())))
                    {
                        Ok(req) => req,
                        Err(_) => continue,
                    };

                    let result =
                        tokio::time::timeout(timeout, h2_pool.send(&address, request)).await;

                    let healthy = match result {
                        Ok(Ok(response)) => response.status().is_success(),
                        _ => false,
                    };

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

fn fallback_runtime() -> &'static tokio::runtime::Runtime {
    static FALLBACK_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    FALLBACK_RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("spooky-edge-fallback-rt")
            .build()
            .expect("failed to create fallback tokio runtime")
    })
}
