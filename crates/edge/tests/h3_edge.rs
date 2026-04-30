use std::{
    net::UdpSocket,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rand::RngCore;
use rcgen::{Certificate, CertificateParams, SanType};
use tempfile::{TempDir, tempdir};
use tokio::net::TcpListener;

use spooky_config::config::{
    Backend, ClientAuth, Config, HealthCheck, Listen, LoadBalancing, Log, LogFormat, Security, Tls,
    UpstreamTls,
};
use spooky_edge::QUICListener;
use spooky_edge::constants::{
    MAX_DATAGRAM_SIZE_BYTES, MAX_UDP_PAYLOAD_BYTES, QUIC_IDLE_TIMEOUT_MS, QUIC_INITIAL_MAX_DATA,
    QUIC_INITIAL_MAX_STREAMS_BIDI, QUIC_INITIAL_MAX_STREAMS_UNI, QUIC_INITIAL_STREAM_DATA,
    REQUEST_TIMEOUT_SECS, UDP_READ_TIMEOUT_MS,
};

fn write_test_certs(dir: &TempDir) -> (String, String) {
    let mut params = CertificateParams::new(vec!["localhost".into()]);
    params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let cert = Certificate::from_params(params).expect("failed to build cert");

    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");

    std::fs::write(&cert_path, cert.serialize_pem().unwrap()).unwrap();
    std::fs::write(&key_path, cert.serialize_private_key_pem()).unwrap();

    (
        cert_path.to_string_lossy().to_string(),
        key_path.to_string_lossy().to_string(),
    )
}

fn make_config(port: u32, cert: String, key: String, backend_address: String) -> Config {
    use spooky_config::config::{RouteMatch, Upstream};
    use std::collections::HashMap;

    let mut upstream = HashMap::new();
    upstream.insert(
        "test_pool".to_string(),
        Upstream {
            load_balancing: LoadBalancing {
                lb_type: "random".to_string(),
                key: None,
            },
            route: RouteMatch {
                path_prefix: Some("/".to_string()),
                ..Default::default()
            },
            backends: vec![Backend {
                id: "backend1".to_string(),
                address: normalize_backend_address(backend_address),
                weight: 1,
                health_check: HealthCheck {
                    path: "/health".to_string(),
                    interval: 1000,
                    timeout_ms: 1000,
                    failure_threshold: 3,
                    success_threshold: 1,
                    cooldown_ms: 0,
                },
            }],
        },
    );

    Config {
        version: 1,
        listen: Listen {
            protocol: "http3".to_string(),
            port,
            address: "127.0.0.1".to_string(),
            tls: Tls {
                cert,
                key,
                client_auth: ClientAuth::default(),
            },
        },
        upstream,
        load_balancing: Some(LoadBalancing {
            lb_type: "random".to_string(),
            key: None,
        }),
        upstream_tls: UpstreamTls::default(),
        log: Log {
            level: "info".to_string(),
            file: Default::default(),
            format: LogFormat::Plain,
        },
        performance: spooky_config::config::Performance::default(),
        observability: spooky_config::config::Observability::default(),
        resilience: spooky_config::config::Resilience::default(),
        security: Security::default(),
    }
}

fn normalize_backend_address(address: String) -> String {
    if address.contains("://") {
        address
    } else {
        format!("http://{address}")
    }
}

fn quic_read_timeout(conn: &quiche::Connection) -> Duration {
    conn.timeout()
        .filter(|d| !d.is_zero())
        .unwrap_or(Duration::from_millis(UDP_READ_TIMEOUT_MS))
}

async fn start_h2_backend(body: &'static str) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let io = TokioIo::new(stream);
            let service = service_fn(move |_req: Request<Incoming>| async move {
                Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from(body))))
            });

            tokio::spawn(async move {
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });

    addr
}

fn run_h3_client(addr: std::net::SocketAddr) -> Result<String, String> {
    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    let local_addr = socket.local_addr().map_err(|e| e.to_string())?;

    let mut config =
        quiche::Config::new(quiche::PROTOCOL_VERSION).map_err(|e| format!("config: {e:?}"))?;
    config.verify_peer(false);
    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .map_err(|e| format!("alpn: {e:?}"))?;
    config.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    config.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    config.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    config.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    config.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    config.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    config.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);

    let mut conn = quiche::connect(Some("localhost"), &scid, local_addr, addr, &mut config)
        .map_err(|e| format!("connect: {e:?}"))?;

    let h3_config = quiche::h3::Config::new().map_err(|e| format!("h3: {e:?}"))?;
    let mut h3_conn: Option<quiche::h3::Connection> = None;

    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    let (write, send_info) = conn.send(&mut out).map_err(|e| format!("send: {e:?}"))?;
    socket
        .send_to(&out[..write], send_info.to)
        .map_err(|e| format!("send_to: {e:?}"))?;

    let start = Instant::now();
    let mut req_sent = false;
    let mut response_body = Vec::new();

    loop {
        loop {
            match conn.send(&mut out) {
                Ok((write, send_info)) => {
                    let _ = socket.send_to(&out[..write], send_info.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(format!("send loop: {e:?}")),
            }
        }

        let timeout = quic_read_timeout(&conn);
        socket
            .set_read_timeout(Some(timeout))
            .map_err(|e| format!("timeout: {e:?}"))?;

        match socket.recv_from(&mut buf) {
            Ok((len, from)) => {
                let recv_info = quiche::RecvInfo {
                    from,
                    to: local_addr,
                };
                conn.recv(&mut buf[..len], recv_info)
                    .map_err(|e| format!("recv: {e:?}"))?;
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                conn.on_timeout();
            }
            Err(e) => return Err(format!("recv: {e:?}")),
        }

        if conn.is_established() && h3_conn.is_none() {
            h3_conn = Some(
                quiche::h3::Connection::with_transport(&mut conn, &h3_config)
                    .map_err(|e| format!("h3 conn: {e:?}"))?,
            );
        }

        if let Some(h3) = h3_conn.as_mut() {
            if conn.is_established() && !req_sent {
                let req = vec![
                    quiche::h3::Header::new(b":method", b"GET"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", b"localhost"),
                    quiche::h3::Header::new(b":path", b"/"),
                    quiche::h3::Header::new(b"user-agent", b"spooky-test"),
                ];
                h3.send_request(&mut conn, &req, true)
                    .map_err(|e| format!("send_request: {e:?}"))?;
                req_sent = true;
            }

            loop {
                match h3.poll(&mut conn) {
                    Ok((stream_id, quiche::h3::Event::Data)) => loop {
                        match h3.recv_body(&mut conn, stream_id, &mut buf) {
                            Ok(read) => response_body.extend_from_slice(&buf[..read]),
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => return Err(format!("recv_body: {e:?}")),
                        }
                    },
                    Ok((_stream_id, quiche::h3::Event::Headers { .. })) => {}
                    Ok((_stream_id, quiche::h3::Event::Finished)) => {
                        let body = String::from_utf8_lossy(&response_body).to_string();
                        return Ok(body);
                    }
                    Ok((_stream_id, quiche::h3::Event::Reset(_))) => {
                        return Err("stream reset".to_string());
                    }
                    Ok((_stream_id, quiche::h3::Event::PriorityUpdate)) => {}
                    Ok((_stream_id, quiche::h3::Event::GoAway)) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => return Err(format!("poll: {e:?}")),
                }
            }
        }

        if start.elapsed() > Duration::from_secs(REQUEST_TIMEOUT_SECS) {
            return Err("timeout waiting for response".to_string());
        }
    }
}

fn run_h3_client_multiple_requests(
    addr: std::net::SocketAddr,
    request_count: usize,
) -> Result<(usize, usize), String> {
    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    let local_addr = socket.local_addr().map_err(|e| e.to_string())?;

    let mut config =
        quiche::Config::new(quiche::PROTOCOL_VERSION).map_err(|e| format!("config: {e:?}"))?;
    config.verify_peer(false);
    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .map_err(|e| format!("alpn: {e:?}"))?;
    config.set_active_connection_id_limit(8);
    config.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    config.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    config.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    config.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    config.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    config.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    config.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);

    let mut conn = quiche::connect(Some("localhost"), &scid, local_addr, addr, &mut config)
        .map_err(|e| format!("connect: {e:?}"))?;
    let h3_config = quiche::h3::Config::new().map_err(|e| format!("h3: {e:?}"))?;
    let mut h3_conn: Option<quiche::h3::Connection> = None;

    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];
    let mut requests_sent = 0usize;
    let mut requests_done = 0usize;
    let mut in_flight = false;
    let mut max_spare_dcids = 0usize;

    let (write, send_info) = conn.send(&mut out).map_err(|e| format!("send: {e:?}"))?;
    socket
        .send_to(&out[..write], send_info.to)
        .map_err(|e| format!("send_to: {e:?}"))?;

    let start = Instant::now();

    while requests_done < request_count {
        loop {
            match conn.send(&mut out) {
                Ok((write, send_info)) => {
                    let _ = socket.send_to(&out[..write], send_info.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(format!("send loop: {e:?}")),
            }
        }

        let timeout = quic_read_timeout(&conn);
        socket
            .set_read_timeout(Some(timeout))
            .map_err(|e| format!("timeout: {e:?}"))?;

        match socket.recv_from(&mut buf) {
            Ok((len, from)) => {
                let recv_info = quiche::RecvInfo {
                    from,
                    to: local_addr,
                };
                conn.recv(&mut buf[..len], recv_info)
                    .map_err(|e| format!("recv: {e:?}"))?;
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                conn.on_timeout();
            }
            Err(e) => return Err(format!("recv: {e:?}")),
        }

        if conn.is_closed() {
            if requests_done > 0 {
                return Ok((max_spare_dcids, requests_done));
            }
            return Err(format!(
                "connection closed early (sent={requests_sent}, done={requests_done}, spare={max_spare_dcids})"
            ));
        }

        max_spare_dcids = max_spare_dcids.max(conn.available_dcids());

        if conn.is_established() && h3_conn.is_none() {
            h3_conn = Some(
                quiche::h3::Connection::with_transport(&mut conn, &h3_config)
                    .map_err(|e| format!("h3 conn: {e:?}"))?,
            );
        }

        if let Some(h3) = h3_conn.as_mut() {
            if conn.is_established() && !in_flight && requests_sent < request_count {
                let req = vec![
                    quiche::h3::Header::new(b":method", b"GET"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", b"localhost"),
                    quiche::h3::Header::new(b":path", b"/"),
                    quiche::h3::Header::new(b"user-agent", b"spooky-rotation-test"),
                ];
                h3.send_request(&mut conn, &req, true)
                    .map_err(|e| format!("send_request: {e:?}"))?;
                requests_sent += 1;
                in_flight = true;
            }

            loop {
                match h3.poll(&mut conn) {
                    Ok((stream_id, quiche::h3::Event::Data)) => loop {
                        match h3.recv_body(&mut conn, stream_id, &mut buf) {
                            Ok(_) => {}
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => return Err(format!("recv_body: {e:?}")),
                        }
                    },
                    Ok((_stream_id, quiche::h3::Event::Headers { .. })) => {}
                    Ok((_stream_id, quiche::h3::Event::Finished)) => {
                        requests_done += 1;
                        in_flight = false;
                    }
                    Ok((_stream_id, quiche::h3::Event::Reset(_))) => {
                        return Err("stream reset".to_string());
                    }
                    Ok((_stream_id, quiche::h3::Event::PriorityUpdate)) => {}
                    Ok((_stream_id, quiche::h3::Event::GoAway)) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => return Err(format!("poll: {e:?}")),
                }
            }
        }

        if start.elapsed() > Duration::from_secs(20) {
            if requests_done > 0 {
                return Ok((max_spare_dcids, requests_done));
            }
            return Err(format!(
                "timeout waiting for responses (sent={requests_sent}, done={requests_done}, inflight={in_flight}, spare={max_spare_dcids})"
            ));
        }
    }

    Ok((max_spare_dcids, requests_done))
}

fn assert_cid_sync_invariants(listener: &QUICListener) {
    for (primary_key, connection) in &listener.connections {
        assert_eq!(
            primary_key.as_ref(),
            connection.primary_scid.as_ref(),
            "connection map key must match connection.primary_scid"
        );

        for cid in &connection.routing_scids {
            let matched = listener
                .cid_radix
                .longest_prefix_match(cid.as_ref())
                .unwrap_or_else(|| panic!("missing CID in radix: {}", hex::encode(cid)));
            assert_eq!(
                matched.as_ref(),
                cid.as_ref(),
                "radix should return exact SCID for stored prefix"
            );

            if cid.as_ref() == connection.primary_scid.as_ref() {
                assert!(
                    !listener.cid_routes.contains_key(cid.as_ref()),
                    "primary SCID must not be present in alias map"
                );
            } else {
                let mapped_primary = listener
                    .cid_routes
                    .get(cid.as_ref())
                    .unwrap_or_else(|| panic!("alias CID missing mapping: {}", hex::encode(cid)));
                assert_eq!(
                    mapped_primary.as_ref(),
                    connection.primary_scid.as_ref(),
                    "alias must map to connection primary SCID"
                );
            }
        }
    }

    for (alias, primary) in &listener.cid_routes {
        assert_ne!(
            alias.as_ref(),
            primary.as_ref(),
            "alias route must not map a primary to itself"
        );
        let connection = listener.connections.get(primary).unwrap_or_else(|| {
            panic!(
                "alias points to missing primary connection: alias={} primary={}",
                hex::encode(alias),
                hex::encode(primary)
            )
        });
        assert!(
            connection.routing_scids.contains(alias),
            "alias must exist in the owning connection routing SCID set"
        );
    }
}

#[test]
fn http3_request_is_accepted_and_parsed() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let config = make_config(0, cert, key, "127.0.0.1:1".to_string());
    let mut listener = QUICListener::new(config).expect("failed to create listener");
    let addr = listener.socket.local_addr().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = stop.clone();

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let handle = rt.spawn_blocking(move || {
        while !stop_flag.load(Ordering::Relaxed) {
            listener.poll();
        }
    });

    let body = run_h3_client(addr).expect("client request failed");
    stop.store(true, Ordering::Relaxed);
    handle.abort();

    assert!(body.contains("upstream error"));
}

#[test]
fn invalid_backend_scheme_is_rejected_at_startup() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let config = make_config(0, cert, key, "ftp://127.0.0.1:8080".to_string());
    match QUICListener::new(config) {
        Ok(_) => panic!("invalid backend scheme should fail startup"),
        Err(err) => {
            assert!(
                err.to_string().contains("invalid backend address"),
                "unexpected startup error: {err}"
            );
        }
    }
}

#[test]
fn server_rotates_scids_for_active_connection() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend("ok\n"));
    let config = make_config(0, cert, key, backend_addr.to_string());
    let listener = Arc::new(Mutex::new(
        QUICListener::new(config).expect("failed to create listener"),
    ));
    let addr = listener
        .lock()
        .expect("listener lock")
        .socket
        .local_addr()
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = stop.clone();
    let listener_task = listener.clone();

    let handle = thread::spawn(move || {
        while !stop_flag.load(Ordering::Relaxed) {
            if let Ok(mut guard) = listener_task.lock() {
                guard.poll();
            }
        }
    });

    let (max_spare_dcids, completed_requests) =
        run_h3_client_multiple_requests(addr, 12).expect("client requests failed");

    stop.store(true, Ordering::Relaxed);
    let _ = handle.join();

    let listener_guard = listener.lock().expect("listener lock");
    let rotations = listener_guard
        .metrics
        .scid_rotations
        .load(Ordering::Relaxed);

    assert!(
        max_spare_dcids > 0,
        "client never observed additional destination CIDs"
    );
    assert!(
        completed_requests > 0,
        "client did not complete any request"
    );
    assert!(rotations > 0, "server did not rotate any SCID");
    assert_cid_sync_invariants(&listener_guard);
}

// ---------------------------------------------------------------------------
// Malformed packet hardening tests (task 1.1)
//
// Each test fires one or more invalid UDP datagrams at a fresh listener and
// asserts that:
//  (a) the listener does not panic,
//  (b) all routing maps remain empty / unchanged after the bad traffic.
//
// The listener is driven by a single poll() call per datagram so the test
// stays synchronous and deterministic.
// ---------------------------------------------------------------------------

fn make_listener() -> (QUICListener, std::net::SocketAddr) {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let config = make_config(0, cert, key, "127.0.0.1:1".to_string());
    let listener = QUICListener::new(config).expect("failed to create listener");
    let addr = listener.socket.local_addr().expect("local_addr");
    (listener, addr)
}

fn send_udp(to: std::net::SocketAddr, payload: &[u8]) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind sender");
    sock.send_to(payload, to).expect("send_to");
}

fn assert_maps_empty(listener: &QUICListener) {
    assert!(
        listener.connections.is_empty(),
        "connections map must be empty after malformed traffic, had {} entries",
        listener.connections.len()
    );
    assert!(
        listener.cid_routes.is_empty(),
        "cid_routes must be empty after malformed traffic, had {} entries",
        listener.cid_routes.len()
    );
    assert!(
        listener.peer_routes.is_empty(),
        "peer_routes must be empty after malformed traffic, had {} entries",
        listener.peer_routes.len()
    );
}

/// A single zero-byte UDP datagram must be dropped without panic and leave all
/// maps empty.
#[test]
fn malformed_zero_length_datagram_is_dropped() {
    let (mut listener, addr) = make_listener();
    send_udp(addr, &[]);
    listener.poll();
    assert_maps_empty(&listener);
}

/// A single-byte payload cannot be a valid QUIC header; listener must drop it.
#[test]
fn malformed_single_byte_datagram_is_dropped() {
    let (mut listener, addr) = make_listener();
    send_udp(addr, &[0xFF]);
    listener.poll();
    assert_maps_empty(&listener);
}

/// Completely random garbage bytes must not panic and must leave maps clean.
#[test]
fn malformed_random_garbage_is_dropped() {
    let (mut listener, addr) = make_listener();
    let garbage: Vec<u8> = (0u8..64).collect();
    send_udp(addr, &garbage);
    listener.poll();
    assert_maps_empty(&listener);
}

/// A valid-looking QUIC long header with a plausible type byte but truncated
/// body should be rejected cleanly.
#[test]
fn malformed_truncated_long_header_is_dropped() {
    let (mut listener, addr) = make_listener();
    // Long-header first byte: version-specific Initial packet marker (0xC0 | 0x00)
    // followed by the QUIC version, then truncated before DCIL/SCIL fields.
    let truncated: &[u8] = &[
        0xC0, // long-header flag + Initial type bits
        0x00, 0x00, 0x00,
        0x01, // QUIC v1
              // deliberately truncated here (no DCIL/SCIL/lengths)
    ];
    send_udp(addr, truncated);
    listener.poll();
    assert_maps_empty(&listener);
}

/// A long header with a DCID length that overflows the packet should be
/// rejected without panicking.
#[test]
fn malformed_dcid_length_overflow_is_dropped() {
    let (mut listener, addr) = make_listener();
    // Craft a packet whose DCID length field claims 255 bytes but the packet
    // ends immediately after.
    let mut pkt = vec![
        0xC0, // long-header Initial
        0x00, 0x00, 0x00, 0x01, // QUIC v1
        0xFF, // DCID length = 255 (but no bytes follow)
    ];
    // pad with some bytes but far fewer than 255
    pkt.extend_from_slice(&[0xAB; 8]);
    send_udp(addr, &pkt);
    listener.poll();
    assert_maps_empty(&listener);
}

/// A short-header packet destined for an unknown connection must be silently
/// dropped; it must not create a new connection entry.
#[test]
fn short_header_unknown_connection_is_dropped() {
    let (mut listener, addr) = make_listener();
    // Short header: first bit 0, remaining bits arbitrary. Use a 20-byte DCID
    // that does not correspond to any established connection.
    let mut pkt = vec![0x40u8]; // short-header flag (bit 7 = 0, bit 6 = 1 for fixed bit)
    pkt.extend_from_slice(&[
        0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0x0C, 0x0D,
    ]);
    send_udp(addr, &pkt);
    listener.poll();
    assert_maps_empty(&listener);
}

/// A Retry packet from an unknown peer must be ignored without creating state.
#[test]
fn retry_packet_for_unknown_connection_is_dropped() {
    let (mut listener, addr) = make_listener();
    // Long-header Retry type: bits 0xF0 with version and minimal fields.
    // quiche will parse the header but the listener should not create a conn.
    let mut pkt = vec![
        0xF0, // long-header, Retry type bits
        0x00, 0x00, 0x00, 0x01, // QUIC v1
        0x08, // DCID len = 8
    ];
    pkt.extend_from_slice(&[0x11; 8]); // DCID
    pkt.push(0x08); // SCID len = 8
    pkt.extend_from_slice(&[0x22; 8]); // SCID
    // Retry token (arbitrary, no integrity tag)
    pkt.extend_from_slice(&[0x99; 16]);
    send_udp(addr, &pkt);
    listener.poll();
    assert_maps_empty(&listener);
}

/// A Handshake packet for which no connection exists must be dropped cleanly.
#[test]
fn handshake_packet_unknown_connection_is_dropped() {
    let (mut listener, addr) = make_listener();
    // Long-header Handshake type: 0xE0
    let mut pkt = vec![
        0xE0, // long-header, Handshake type bits
        0x00, 0x00, 0x00, 0x01, // QUIC v1
        0x08, // DCID len = 8
    ];
    pkt.extend_from_slice(&[0x33; 8]); // DCID
    pkt.push(0x08); // SCID len = 8
    pkt.extend_from_slice(&[0x44; 8]); // SCID
    // Packet number + payload (garbage)
    pkt.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    pkt.extend_from_slice(&[0xAA; 20]);
    send_udp(addr, &pkt);
    listener.poll();
    assert_maps_empty(&listener);
}

/// Repeated bursts of malformed packets must not accumulate any routing state.
#[test]
fn repeated_malformed_packets_leave_maps_consistent() {
    let (mut listener, addr) = make_listener();

    let payloads: &[&[u8]] = &[
        &[],                                   // zero-length
        &[0xFF],                               // single byte
        &[0x00; 16],                           // all-zero short
        &[0xFF; 64],                           // all-ones garbage
        &[0xC0, 0x00, 0x00, 0x00, 0x01, 0xFF], // truncated long header
        &[0x40, 0xDE, 0xAD, 0xBE, 0xEF, 0x00], // short header, unknown DCID
    ];

    for payload in payloads {
        send_udp(addr, payload);
        listener.poll();
    }

    assert_maps_empty(&listener);
}

// ---------------------------------------------------------------------------
// Connection flood / rate-limit tests (task 1.2)
// ---------------------------------------------------------------------------

fn make_config_with_rate_limit(
    port: u32,
    cert: String,
    key: String,
    backend_address: String,
    new_connections_per_sec: u32,
    new_connections_burst: u32,
) -> Config {
    use spooky_config::config::{Performance, RouteMatch, Upstream};
    use std::collections::HashMap;

    let mut upstream = HashMap::new();
    upstream.insert(
        "test_pool".to_string(),
        Upstream {
            load_balancing: LoadBalancing {
                lb_type: "random".to_string(),
                key: None,
            },
            route: RouteMatch {
                path_prefix: Some("/".to_string()),
                ..Default::default()
            },
            backends: vec![Backend {
                id: "backend1".to_string(),
                address: normalize_backend_address(backend_address),
                weight: 1,
                health_check: HealthCheck {
                    path: "/health".to_string(),
                    interval: 1000,
                    timeout_ms: 1000,
                    failure_threshold: 3,
                    success_threshold: 1,
                    cooldown_ms: 0,
                },
            }],
        },
    );

    Config {
        version: 1,
        listen: Listen {
            protocol: "http3".to_string(),
            port,
            address: "127.0.0.1".to_string(),
            tls: Tls {
                cert,
                key,
                client_auth: ClientAuth::default(),
            },
        },
        upstream,
        load_balancing: Some(LoadBalancing {
            lb_type: "random".to_string(),
            key: None,
        }),
        upstream_tls: UpstreamTls::default(),
        log: Log {
            level: "error".to_string(),
            file: Default::default(),
            format: LogFormat::Plain,
        },
        performance: Performance {
            new_connections_per_sec,
            new_connections_burst,
            ..Performance::default()
        },
        observability: spooky_config::config::Observability::default(),
        resilience: spooky_config::config::Resilience::default(),
        security: Security::default(),
    }
}

/// Build a minimal valid QUIC Initial packet using quiche so the listener can
/// parse the header and attempt `quiche::accept`. Returns the encoded bytes.
fn build_initial_packet(dest_addr: std::net::SocketAddr) -> Vec<u8> {
    let local_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).expect("quiche config");
    config.verify_peer(false);
    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .expect("alpn");
    config.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    config.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    config.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    config.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    config.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    config.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);

    let mut conn = quiche::connect(Some("localhost"), &scid, local_addr, dest_addr, &mut config)
        .expect("quiche connect");

    let mut out = vec![0u8; MAX_UDP_PAYLOAD_BYTES];
    let (len, _send_info) = conn.send(&mut out).expect("quiche send");
    out.truncate(len);
    out
}

/// When burst=1 and rate is near-zero, the first Initial packet creates a
/// connection, and subsequent ones in the same instant are dropped.
#[test]
fn connection_flood_is_rate_limited() {
    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    // burst=1, rate=1/s: after the first accept the bucket is empty.
    let config = make_config_with_rate_limit(0, cert, key, "127.0.0.1:1".to_string(), 1, 1);
    let mut listener = QUICListener::new(config).expect("listener");
    let addr = listener.socket.local_addr().unwrap();

    // Send enough distinct Initial packets to saturate the rate limit.
    // Each packet comes from a different SCID so the listener sees each as a
    // new connection attempt (no existing DCID match).
    const FLOOD_COUNT: usize = 10;
    // Build all packets up front so token consumption happens in a tight burst.
    // This avoids slow-host timing where packet construction can allow a 1/s
    // refill and make the assertion flaky.
    let packets: Vec<Vec<u8>> = (0..FLOOD_COUNT)
        .map(|_| build_initial_packet(addr))
        .collect();
    for pkt in &packets {
        send_udp(addr, pkt);
    }
    for _ in 0..FLOOD_COUNT {
        listener.poll();
    }

    // With burst=1 only the very first packet can create a connection.
    // All subsequent ones are dropped by the rate limiter.
    assert!(
        listener.connections.len() <= 1,
        "rate limiter must cap connections at burst=1, got {}",
        listener.connections.len()
    );
}

/// Hard cap on active connections should reject additional Initial packets
/// even when token-bucket rate limits are permissive.
#[test]
fn active_connection_cap_rejects_excess_initial_packets() {
    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    let mut config =
        make_config_with_rate_limit(0, cert, key, "127.0.0.1:1".to_string(), 10_000, 10_000);
    config.performance.max_active_connections = 1;
    let mut listener = QUICListener::new(config).expect("listener");
    let addr = listener.socket.local_addr().unwrap();

    const FLOOD_COUNT: usize = 8;
    for _ in 0..FLOOD_COUNT {
        let pkt = build_initial_packet(addr);
        send_udp(addr, &pkt);
        listener.poll();
    }

    assert!(
        listener.connections.len() <= 1,
        "active connection cap must keep at most one connection, got {}",
        listener.connections.len()
    );
    assert!(
        listener
            .metrics
            .connection_cap_rejects
            .load(Ordering::Relaxed)
            > 0,
        "connection cap should emit rejection metrics"
    );
}

/// Once draining starts, unknown/new Initial packets must not create a
/// connection, even if admission limits would otherwise allow it.
#[test]
fn draining_mode_rejects_initial_when_no_connections_exist() {
    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    let config = make_config_with_rate_limit(0, cert, key, "127.0.0.1:1".to_string(), 10_000, 20);
    let mut listener = QUICListener::new(config).expect("listener");
    let addr = listener.socket.local_addr().unwrap();

    listener.start_draining();

    let pkt = build_initial_packet(addr);
    send_udp(addr, &pkt);
    listener.poll();

    assert_eq!(
        listener.connections.len(),
        0,
        "no new connection should be admitted after drain starts"
    );
}

/// Draining should preserve existing connection processing but reject any new
/// connection admission from unknown Initial packets.
#[test]
fn draining_mode_rejects_new_initial_after_existing_connection() {
    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    let config = make_config_with_rate_limit(0, cert, key, "127.0.0.1:1".to_string(), 10_000, 20);
    let mut listener = QUICListener::new(config).expect("listener");
    let addr = listener.socket.local_addr().unwrap();

    let first = build_initial_packet(addr);
    send_udp(addr, &first);
    listener.poll();
    assert_eq!(
        listener.connections.len(),
        1,
        "first Initial should create a baseline connection before drain"
    );
    let known_connection_ids: std::collections::HashSet<Vec<u8>> = listener
        .connections
        .keys()
        .map(|cid| cid.to_vec())
        .collect();

    listener.start_draining();

    for _ in 0..5 {
        let pkt = build_initial_packet(addr);
        send_udp(addr, &pkt);
        listener.poll();
    }

    let post_drain_ids: std::collections::HashSet<Vec<u8>> = listener
        .connections
        .keys()
        .map(|cid| cid.to_vec())
        .collect();
    assert!(
        post_drain_ids.is_subset(&known_connection_ids),
        "draining mode must reject unknown/new Initial packets (before={known_connection_ids:?}, after={post_drain_ids:?})"
    );
}

/// Normal traffic well below the rate limit must not be affected.
/// With a generous burst and rate, all N connection attempts succeed.
#[test]
fn normal_traffic_below_rate_limit_is_unaffected() {
    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    // burst=20, rate=10000/s: far above what we'll send.
    let config = make_config_with_rate_limit(0, cert, key, "127.0.0.1:1".to_string(), 10_000, 20);
    let mut listener = QUICListener::new(config).expect("listener");
    let addr = listener.socket.local_addr().unwrap();

    const REQUEST_COUNT: usize = 5;
    for _ in 0..REQUEST_COUNT {
        let pkt = build_initial_packet(addr);
        send_udp(addr, &pkt);
        listener.poll();
    }

    assert_eq!(
        listener.connections.len(),
        REQUEST_COUNT,
        "all {} connections should be accepted when below rate limit",
        REQUEST_COUNT
    );
}

// ---------------------------------------------------------------------------
// Connection-lifecycle churn stress test (task 4.3)
//
// Runs many connect/request/disconnect cycles and asserts that no orphaned
// entries are left in connections, cid_routes, peer_routes, or the radix trie.
// ---------------------------------------------------------------------------

/// Build a QUIC client config suitable for stress rounds.
fn make_quic_client_config() -> quiche::Config {
    let mut cfg = quiche::Config::new(quiche::PROTOCOL_VERSION).expect("quiche config");
    cfg.verify_peer(false);
    cfg.set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .expect("alpn");
    // Use a short idle timeout so dropped sockets get cleaned up quickly.
    cfg.set_max_idle_timeout(500);
    cfg.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    cfg.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    cfg.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    cfg.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
    cfg.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
    cfg.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
    cfg.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    cfg.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    cfg.set_disable_active_migration(true);
    cfg
}

/// Connect to `server_addr` and drive the QUIC handshake until established.
/// Returns `(socket, local_addr, conn, h3)` on success, or an error string.
fn stress_connect(
    server_addr: std::net::SocketAddr,
) -> Result<
    (
        UdpSocket,
        std::net::SocketAddr,
        quiche::Connection,
        quiche::h3::Connection,
    ),
    String,
> {
    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    let local_addr = socket.local_addr().map_err(|e| e.to_string())?;
    socket
        .set_read_timeout(Some(Duration::from_millis(100)))
        .map_err(|e| e.to_string())?;

    let mut cfg = make_quic_client_config();
    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);

    let mut conn = quiche::connect(Some("localhost"), &scid, local_addr, server_addr, &mut cfg)
        .map_err(|e| format!("connect: {e:?}"))?;

    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    let (w, si) = conn.send(&mut out).map_err(|e| format!("send: {e:?}"))?;
    socket
        .send_to(&out[..w], si.to)
        .map_err(|e| format!("send_to: {e:?}"))?;

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        loop {
            match conn.send(&mut out) {
                Ok((w, si)) => {
                    let _ = socket.send_to(&out[..w], si.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(format!("send: {e:?}")),
            }
        }

        if conn.is_established() {
            break;
        }

        match socket.recv_from(&mut buf) {
            Ok((len, from)) => {
                conn.recv(
                    &mut buf[..len],
                    quiche::RecvInfo {
                        from,
                        to: local_addr,
                    },
                )
                .map_err(|e| format!("recv: {e:?}"))?;
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                conn.on_timeout();
            }
            Err(e) => return Err(format!("recv: {e:?}")),
        }

        if Instant::now() > deadline {
            return Err("handshake timeout".to_string());
        }
    }

    let h3_cfg = quiche::h3::Config::new().map_err(|e| format!("h3 cfg: {e:?}"))?;
    let h3 = quiche::h3::Connection::with_transport(&mut conn, &h3_cfg)
        .map_err(|e| format!("h3 conn: {e:?}"))?;

    Ok((socket, local_addr, conn, h3))
}

/// Send a graceful QUIC CONNECTION_CLOSE and flush.
fn stress_close_gracefully(socket: &UdpSocket, conn: &mut quiche::Connection) {
    let _ = conn.close(false, 0, b"done");
    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    while let Ok((w, si)) = conn.send(&mut out) {
        let _ = socket.send_to(&out[..w], si.to);
    }
}

/// Stress test: repeatedly connect, optionally complete a request, then close
/// or abruptly drop.  After all rounds the listener maps must be fully empty.
#[test]
fn lifecycle_churn_leaves_no_orphaned_state() {
    const ROUNDS: usize = 30;
    // Idle timeout matches the client config (500 ms).
    const IDLE_MS: u64 = 500;

    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend("ok\n"));

    let mut config = make_config(0, cert, key, backend_addr.to_string());
    // Match the client-side idle timeout so both sides agree.
    config.performance.quic_max_idle_timeout_ms = IDLE_MS;
    // Generous limits so the stress loop is never rate-limited.
    config.performance.new_connections_per_sec = 10_000;
    config.performance.new_connections_burst = 1_000;

    let listener = Arc::new(Mutex::new(
        QUICListener::new(config).expect("failed to create listener"),
    ));
    let server_addr = listener.lock().unwrap().socket.local_addr().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = stop.clone();
    let listener_poll = listener.clone();
    let poll_handle = thread::spawn(move || {
        while !stop_flag.load(Ordering::Relaxed) {
            if let Ok(mut g) = listener_poll.lock() {
                g.poll();
            }
        }
    });

    for round in 0..ROUNDS {
        let (socket, _local_addr, mut conn, _h3) =
            stress_connect(server_addr).unwrap_or_else(|e| panic!("round {round}: connect: {e}"));

        socket
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set_read_timeout");

        // Even rounds close gracefully; odd rounds abruptly drop the socket.
        if round % 2 == 0 {
            stress_close_gracefully(&socket, &mut conn);
        }
        // Odd rounds: drop socket and conn; server detects via idle timeout.
    }

    // Allow the idle timeout (500 ms) to fire and the listener to sweep all
    // timed-out connections.  We wait 3× idle timeout for safety margin.
    thread::sleep(Duration::from_millis(IDLE_MS * 3));

    stop.store(true, Ordering::Relaxed);
    let _ = poll_handle.join();

    // Drive one final poll to flush any remaining close/sweep work.
    if let Ok(mut g) = listener.lock() {
        for _ in 0..20 {
            g.poll();
        }
    }

    let guard = listener.lock().expect("listener lock");
    assert_cid_sync_invariants(&guard);
    assert!(
        guard.connections.is_empty(),
        "connections must be empty after churn, found {} entries",
        guard.connections.len()
    );
    assert!(
        guard.cid_routes.is_empty(),
        "cid_routes must be empty after churn, found {} entries",
        guard.cid_routes.len()
    );
    assert!(
        guard.peer_routes.is_empty(),
        "peer_routes must be empty after churn, found {} entries",
        guard.peer_routes.len()
    );
}
