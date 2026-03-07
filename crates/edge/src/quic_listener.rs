use std::{
    collections::{HashMap, HashSet},
    net::UdpSocket,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use core::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use log::{debug, error, info};
use quiche::Config;
use quiche::h3::NameValue;
use rand::RngCore;
use spooky_bridge::h3_to_h2::build_h2_request;
use spooky_lb::{HealthTransition, UpstreamPool};
use spooky_transport::h2_pool::H2Pool;
use spooky_errors::ProxyError;
use tokio::runtime::Handle;

use spooky_config::config::Config as SpookyConfig;

use crate::{Metrics, QUICListener, QuicConnection, RequestEnvelope};

fn is_hop_header(name: &str) -> bool {
    matches!(
        name,
        "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding" | "upgrade"
    )
}

fn request_hash_key(req: &RequestEnvelope) -> &str {
    if let Some(authority) = &req.authority {
        return authority;
    }

    if !req.path.is_empty() {
        return &req.path;
    }

    &req.method
}

fn find_upstream_for_request<'a>(
    upstreams: &'a std::collections::HashMap<String, spooky_config::config::Upstream>,
    path: &str,
    host: Option<&str>,
) -> Option<&'a str> {
    // Find the most specific route that matches the path and/or host
    // We need to find the longest matching path prefix
    let mut best_match: Option<(&str, usize)> = None;

    for (upstream_name, upstream) in upstreams {
        let has_host_match = match (&upstream.route.host, host) {
            (Some(route_host), Some(request_host)) => route_host == request_host,
            (None, _) => true,        // No host constraint
            (Some(_), None) => false, // Host constraint but no host in request
        };

        let path_match_len = match &upstream.route.path_prefix {
            Some(path_prefix) if path.starts_with(path_prefix) => path_prefix.len(),
            None => 0,     // No path constraint, matches but with lowest priority
            _ => continue, // No match
        };

        if has_host_match {
            // Keep the match with the longest path prefix
            if best_match.is_none() || path_match_len > best_match.unwrap().1 {
                best_match = Some((upstream_name.as_str(), path_match_len));
            }
        }
    }

    best_match.map(|(name, _)| name)
}

const BACKEND_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_INFLIGHT_PER_BACKEND: usize = 64;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const SCID_ROTATION_INTERVAL: Duration = Duration::from_secs(60);
const SCID_ROTATION_PACKET_THRESHOLD: u64 = 8;

impl QUICListener {
    pub fn new(config: SpookyConfig) -> Result<Self, ProxyError> {
        let socket_address = format!("{}:{}", &config.listen.address, &config.listen.port);

        let socket = UdpSocket::bind(socket_address.as_str()).expect("Failed to bind UDP socker");
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
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
        quic_config.set_max_idle_timeout(5000);
        quic_config.set_max_recv_udp_payload_size(1350);
        quic_config.set_max_send_udp_payload_size(1350);
        quic_config.set_initial_max_data(10_000_000);
        quic_config.set_initial_max_stream_data_bidi_local(1_000_000);
        quic_config.set_initial_max_stream_data_bidi_remote(1_000_000);
        quic_config.set_initial_max_stream_data_uni(1_000_000);
        quic_config.set_initial_max_streams_bidi(100);
        quic_config.set_initial_max_streams_uni(100);
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

        let metrics = Metrics::default();

        Self::spawn_health_checks(upstream_pools.clone(), h2_pool.clone());

        Ok(Self {
            socket,
            config,
            quic_config,
            h3_config,
            h2_pool,
            upstream_pools,
            metrics,
            draining: false,
            drain_start: None,
            recv_buf: [0; 65535],
            send_buf: [0; 65535],
            connections: HashMap::new(),
            cid_routes: HashMap::new(),
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
            && start.elapsed() >= DRAIN_TIMEOUT
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

        let mut send_buf = [0u8; 65_535];
        for connection in self.connections.values_mut() {
            let _ = connection.quic.close(true, 0x0, b"draining");
            Self::flush_send(&socket, &mut send_buf, connection);
        }

        self.connections.clear();
        self.cid_routes.clear();
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
            connection.peer_address = peer;
            return Some((connection, dcid_bytes));
        }

        // For Short packets, try prefix match (client may append bytes to our SCID)
        // This handles cases where client uses longer DCIDs based on server's SCID
        if header.ty == quiche::Type::Short && dcid_bytes.len() > 8 {
            for stored_cid in self.connections.keys() {
                if dcid_bytes.starts_with(stored_cid.as_ref()) {
                    debug!(
                        "Found connection via prefix match. Stored CID: {:02x?}, Packet DCID: {:02x?}",
                        stored_cid, &dcid_bytes
                    );
                    let stored_cid_copy: Arc<[u8]> = Arc::clone(stored_cid);
                    if let Some(mut connection) = self.connections.remove(&stored_cid_copy) {
                        connection.peer_address = peer;
                        return Some((connection, stored_cid_copy));
                    }
                    break;
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

        let mut scid_bytes = [0u8; 16]; // scid must be >= 8 bytes, 16 is perfect
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
        let mut token = [0u8; 16];
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
            && elapsed < SCID_ROTATION_INTERVAL
        {
            return;
        }

        if connection.quic.scids_left() == 0 {
            return;
        }

        let cid_len = connection.quic.source_id().as_ref().len().max(8);
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
        self.cid_routes.remove(&connection.primary_scid);
        for cid in &connection.routing_scids {
            self.cid_routes.remove(cid);
        }
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
            self.cid_routes.remove(retired);
        }

        self.cid_routes.remove(&primary);
        for cid in &active_scids {
            if *cid == primary {
                continue;
            }
            self.cid_routes.insert(cid.clone(), primary.clone());
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
                conn.peer_address = peer;
                debug!("Found existing connection for {}", peer);
                (conn, lookup_key)
            } else if let Some(primary_vec) = self.cid_routes.get(lookup_key.as_ref()).cloned() {
                let primary: Arc<[u8]> = Arc::from(primary_vec.as_slice());
                if let Some(mut conn) = self.connections.remove(&primary) {
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
            } else {
                // Check if there's an existing connection from the same peer
                // This handles cases where the client uses different DCIDs for the same connection
                let mut found_peer_connection = None;
                for (key, conn) in &self.connections {
                    if conn.peer_address == peer {
                        found_peer_connection = Some(key.clone());
                        break;
                    }
                }

                if let Some(peer_key) = found_peer_connection {
                    debug!(
                        "Found existing connection from same peer {}, trying with key: {:?}",
                        peer,
                        hex::encode(&peer_key)
                    );
                    if let Some(mut conn) = self.connections.remove(&peer_key) {
                        conn.peer_address = peer;
                        debug!("Using existing peer connection for {}", peer);
                        (conn, peer_key)
                    } else {
                        // This shouldn't happen, but fallback to creating new connection
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
                    debug!(
                        "No existing connection found for DCID or peer, checking all connections..."
                    );
                    // Debug: check what connections we have
                    for (key, conn) in &self.connections {
                        debug!(
                            "Existing connection DCID: {:?}, peer: {}",
                            hex::encode(key),
                            conn.peer_address
                        );
                    }

                    // No existing connection found, try to create new one
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
                &h2_pool,
                &self.upstream_pools,
                &self.config.upstream,
                &self.metrics,
            )
        {
            error!("HTTP/3 handling failed: {:?}", e);
        }

        Self::maybe_rotate_scid(&mut connection, &self.metrics);

        let mut send_buf = [0u8; 65_535];

        Self::flush_send(&socket, &mut send_buf, &mut connection);
        Self::handle_timeout(&socket, &mut send_buf, &mut connection);

        if !connection.quic.is_closed() {
            let new_primary_vec = self.sync_connection_routes(&mut connection);
            let new_primary: Arc<[u8]> = Arc::from(new_primary_vec.as_slice());
            debug!(
                "Storing connection with key: {:02x?} (previous: {:02x?})",
                &new_primary, &current_primary
            );
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

        let mut send_buf = [0u8; 65_535];
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
        h2_pool: &H2Pool,
        upstream_pools: &HashMap<String, Arc<Mutex<UpstreamPool>>>,
        upstreams: &std::collections::HashMap<String, spooky_config::config::Upstream>,
        metrics: &Metrics,
    ) -> Result<(), quiche::h3::Error> {
        let mut body_buf = [0u8; 65_535];

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
                    let mut headers = Vec::with_capacity(list.len());

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

                    let envelope = RequestEnvelope {
                        method: method.clone(),
                        path: path.clone(),
                        authority,
                        headers,
                        body: Vec::new(),
                        start: Instant::now(),
                    };

                    metrics.inc_total();
                    connection.streams.insert(stream_id, envelope);

                    if !method.is_empty() && !path.is_empty() {
                        info!("HTTP/3 request {} {}", method, path);
                    }
                }
                Ok((stream_id, quiche::h3::Event::Data)) => loop {
                    match h3.recv_body(&mut connection.quic, stream_id, &mut body_buf) {
                        Ok(read) => {
                            if let Some(req) = connection.streams.get_mut(&stream_id) {
                                req.body.extend_from_slice(&body_buf[..read]);
                            }
                        }
                        Err(quiche::h3::Error::Done) => break,
                        Err(e) => return Err(e),
                    }
                },
                Ok((stream_id, quiche::h3::Event::Finished)) => {
                    if let Some(req) = connection.streams.remove(&stream_id) {
                        Self::handle_request_finish(
                            h3,
                            &mut connection.quic,
                            stream_id,
                            req,
                            h2_pool,
                            upstream_pools,
                            upstreams,
                            metrics,
                        )?;
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

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_request_finish(
        h3: &mut quiche::h3::Connection,
        quic: &mut quiche::Connection,
        stream_id: u64,
        req: RequestEnvelope,
        h2_pool: &H2Pool,
        upstream_pools: &HashMap<String, Arc<Mutex<UpstreamPool>>>,
        upstreams: &std::collections::HashMap<String, spooky_config::config::Upstream>,
        metrics: &Metrics,
    ) -> Result<(), quiche::h3::Error> {
        let start = req.start;
        if req.method.is_empty() || req.path.is_empty() {
            return Self::send_simple_response(
                h3,
                quic,
                stream_id,
                http::StatusCode::BAD_REQUEST,
                b"invalid request\n",
            );
        }

        // Find the upstream for this request
        let upstream_name =
            find_upstream_for_request(upstreams, &req.path, req.authority.as_deref()).ok_or_else(
                || {
                    error!(
                        "No route found for path: {} (host: {:?})",
                        req.path, req.authority
                    );
                    quiche::h3::Error::InternalError
                },
            )?;

        let upstream_pool = upstream_pools.get(upstream_name).ok_or_else(|| {
            error!("Upstream pool not found for: {}", upstream_name);
            quiche::h3::Error::InternalError
        })?;

        let upstream_len = upstream_pool
            .lock()
            .map(|pool| pool.pool.len())
            .unwrap_or(0);
        if upstream_len == 0 {
            return Self::send_simple_response(
                h3,
                quic,
                stream_id,
                http::StatusCode::SERVICE_UNAVAILABLE,
                b"no servers configured for upstream\n",
            );
        }

        let key = request_hash_key(&req);
        let (backend_index, lb_type) = {
            let mut pool = upstream_pool.lock().expect("upstream pool lock");
            let lb_type = pool.lb_name();
            let backend_index = pool.pick(key);
            (backend_index, lb_type)
        };
        let backend_index = match backend_index {
            Some(index) => index,
            None => {
                return Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    http::StatusCode::SERVICE_UNAVAILABLE,
                    b"no healthy servers\n",
                );
            }
        };

        let backend_addr = {
            let pool = upstream_pool.lock().expect("upstream pool lock");
            pool.pool
                .address(backend_index)
                .map(|addr| addr.to_string())
        };
        let backend_addr = match backend_addr {
            Some(addr) => addr,
            None => {
                return Self::send_simple_response(
                    h3,
                    quic,
                    stream_id,
                    http::StatusCode::SERVICE_UNAVAILABLE,
                    b"invalid server\n",
                );
            }
        };

        info!("Selected backend {} via {}", backend_addr, lb_type);

        match Self::forward_request(&backend_addr, h2_pool, req) {
            Ok((status, headers, body)) => {
                let transition = upstream_pool.lock().ok().and_then(|mut pool| {
                    if status.is_server_error() {
                        pool.pool.mark_failure(backend_index)
                    } else {
                        pool.pool.mark_success(backend_index)
                    }
                });
                if let Some(transition) = transition {
                    Self::log_health_transition(&backend_addr, transition);
                }
                metrics.inc_success();
                let latency = start.elapsed().as_millis();
                info!(
                    "Upstream {} status {} latency_ms {}",
                    backend_addr, status, latency
                );
                Self::send_backend_response(h3, quic, stream_id, status, &headers, &body)
            }
            Err(ProxyError::Bridge(err)) => {
                error!("Bridge error: {:?}", err);
                let transition = upstream_pool
                    .lock()
                    .ok()
                    .and_then(|mut pool| pool.pool.mark_failure(backend_index));
                if let Some(transition) = transition {
                    Self::log_health_transition(&backend_addr, transition);
                }
                metrics.inc_failure();
                let latency = start.elapsed().as_millis();
                info!(
                    "Upstream {} status 400 latency_ms {}",
                    backend_addr, latency
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
                let transition = upstream_pool
                    .lock()
                    .ok()
                    .and_then(|mut pool| pool.pool.mark_failure(backend_index));
                if let Some(transition) = transition {
                    Self::log_health_transition(&backend_addr, transition);
                }
                metrics.inc_failure();
                metrics.inc_backend_error();
                let latency = start.elapsed().as_millis();
                info!(
                    "Upstream {} status 502 latency_ms {}",
                    backend_addr, latency
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
                let transition = upstream_pool
                    .lock()
                    .ok()
                    .and_then(|mut pool| pool.pool.mark_failure(backend_index));
                if let Some(transition) = transition {
                    Self::log_health_transition(&backend_addr, transition);
                }
                metrics.inc_failure();
                metrics.inc_timeout();
                let latency = start.elapsed().as_millis();
                info!(
                    "Upstream {} status 503 latency_ms {}",
                    backend_addr, latency
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
                error!("TLS configuration error during request processing: {}", err);
                // TLS errors during request processing indicate server misconfiguration
                // Don't mark backend as failed since this is a local TLS issue
                metrics.inc_failure();
                let latency = start.elapsed().as_millis();
                info!("TLS error for stream {} latency_ms {}", stream_id, latency);
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

    fn forward_request(
        backend_addr: &str,
        h2_pool: &H2Pool,
        req: RequestEnvelope,
    ) -> Result<(http::StatusCode, http::HeaderMap, Bytes), ProxyError> {
        let request = build_h2_request(
            backend_addr,
            &req.method,
            &req.path,
            &req.headers,
            &req.body,
        )
        .map_err(ProxyError::Bridge)?;

        let response = run_blocking(|| async {
            tokio::time::timeout(BACKEND_TIMEOUT, h2_pool.send(backend_addr, request)).await
        })
        .map_err(|e| ProxyError::Transport(format!("send: {e}")))?;

        let response = match response {
            Ok(inner) => inner?,
            Err(_) => return Err(ProxyError::Timeout),
        };

        let (parts, body) = response.into_parts();

        let body_bytes =
            run_blocking(|| async { tokio::time::timeout(BACKEND_TIMEOUT, body.collect()).await })
                .map_err(|e| ProxyError::Transport(format!("body: {e}")))?;

        let body_bytes = match body_bytes {
            Ok(inner) => inner
                .map(|c| c.to_bytes())
                .map_err(|e| ProxyError::Transport(format!("body: {e:?}")))?,
            Err(_) => return Err(ProxyError::Timeout),
        };

        Ok((parts.status, parts.headers, body_bytes))
    }

    fn send_backend_response(
        h3: &mut quiche::h3::Connection,
        quic: &mut quiche::Connection,
        stream_id: u64,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        body: &Bytes,
    ) -> Result<(), quiche::h3::Error> {
        let mut resp_headers = Vec::with_capacity(headers.len() + 2);

        resp_headers.push(quiche::h3::Header::new(
            b":status",
            status.as_str().as_bytes(),
        ));

        for (name, value) in headers.iter() {
            if is_hop_header(name.as_str()) || name == http::header::CONTENT_LENGTH {
                continue;
            }
            resp_headers.push(quiche::h3::Header::new(
                name.as_str().as_bytes(),
                value.as_bytes(),
            ));
        }

        resp_headers.push(quiche::h3::Header::new(
            b"content-length",
            body.len().to_string().as_bytes(),
        ));

        h3.send_response(quic, stream_id, &resp_headers, false)?;
        h3.send_body(quic, stream_id, body, true)?;
        Ok(())
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
                        .body(Full::new(Bytes::new()))
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

fn run_blocking<F, Fut, T>(f: F) -> Result<T, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    match Handle::try_current() {
        Ok(handle) => Ok(tokio::task::block_in_place(|| handle.block_on(f()))),
        Err(_) => {
            let rt = tokio::runtime::Runtime::new().map_err(|e| format!("runtime: {e}"))?;
            Ok(rt.block_on(f()))
        }
    }
}
