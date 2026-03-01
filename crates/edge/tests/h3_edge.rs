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

use spooky_config::config::{Backend, Config, HealthCheck, Listen, LoadBalancing, Log, Tls};
use spooky_edge::QUICListener;

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
                address: backend_address,
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
            tls: Tls { cert, key },
        },
        upstream,
        load_balancing: Some(LoadBalancing {
            lb_type: "random".to_string(),
            key: None,
        }),
        log: Log {
            level: "info".to_string(),
            file: Default::default(),
        },
    }
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
    config.set_max_idle_timeout(5_000);
    config.set_max_recv_udp_payload_size(1350);
    config.set_max_send_udp_payload_size(1350);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_stream_data_uni(1_000_000);
    config.set_initial_max_streams_bidi(100);
    config.set_initial_max_streams_uni(100);
    config.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);

    let mut conn = quiche::connect(Some("localhost"), &scid, local_addr, addr, &mut config)
        .map_err(|e| format!("connect: {e:?}"))?;

    let h3_config = quiche::h3::Config::new().map_err(|e| format!("h3: {e:?}"))?;
    let mut h3_conn: Option<quiche::h3::Connection> = None;

    let mut out = [0u8; 1350];
    let mut buf = [0u8; 65_535];

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

        let timeout = conn.timeout().unwrap_or(Duration::from_millis(50));
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

        if start.elapsed() > Duration::from_secs(5) {
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
    config.set_max_idle_timeout(5_000);
    config.set_max_recv_udp_payload_size(1350);
    config.set_max_send_udp_payload_size(1350);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_stream_data_uni(1_000_000);
    config.set_initial_max_streams_bidi(100);
    config.set_initial_max_streams_uni(100);
    config.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);

    let mut conn = quiche::connect(Some("localhost"), &scid, local_addr, addr, &mut config)
        .map_err(|e| format!("connect: {e:?}"))?;
    let h3_config = quiche::h3::Config::new().map_err(|e| format!("h3: {e:?}"))?;
    let mut h3_conn: Option<quiche::h3::Connection> = None;

    let mut out = [0u8; 1350];
    let mut buf = [0u8; 65_535];
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

        let timeout = conn.timeout().unwrap_or(Duration::from_millis(50));
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
}
