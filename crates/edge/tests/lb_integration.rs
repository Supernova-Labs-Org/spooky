use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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
    Backend, ClientAuth, Config, HealthCheck, Listen, LoadBalancing, Log, LogFormat, RouteMatch,
    Tls, Upstream, UpstreamTls,
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

async fn start_h2_backend(body: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(value) => value,
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

fn make_config(
    port: u32,
    backends: Vec<Backend>,
    lb_type: &str,
    cert: String,
    key: String,
) -> Config {
    use std::collections::HashMap;

    let mut upstream = HashMap::new();
    upstream.insert(
        "test_pool".to_string(),
        Upstream {
            load_balancing: LoadBalancing {
                lb_type: lb_type.to_string(),
                key: None,
            },
            route: RouteMatch {
                path_prefix: Some("/".to_string()),
                ..Default::default()
            },
            backends: backends
                .into_iter()
                .map(|mut backend| {
                    backend.address = normalize_backend_address(backend.address);
                    backend
                })
                .collect(),
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
            lb_type: lb_type.to_string(),
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
    }
}

fn normalize_backend_address(address: String) -> String {
    if address.contains("://") {
        address
    } else {
        format!("http://{address}")
    }
}

fn run_h3_client(addr: SocketAddr, authority: &str) -> Result<String, String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
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

        let timeout = conn
            .timeout()
            .unwrap_or(Duration::from_millis(UDP_READ_TIMEOUT_MS));
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
                    quiche::h3::Header::new(b":authority", authority.as_bytes()),
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

#[test]
fn round_robin_across_backends() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_a = rt.block_on(start_h2_backend("backend-a\n"));
    let backend_b = rt.block_on(start_h2_backend("backend-b\n"));

    let backends = vec![
        Backend {
            id: "a".to_string(),
            address: backend_a.to_string(),
            weight: 1,
            health_check: HealthCheck {
                path: "/health".to_string(),
                interval: 1000,
                timeout_ms: 1000,
                failure_threshold: 3,
                success_threshold: 1,
                cooldown_ms: 0,
            },
        },
        Backend {
            id: "b".to_string(),
            address: backend_b.to_string(),
            weight: 1,
            health_check: HealthCheck {
                path: "/health".to_string(),
                interval: 1000,
                timeout_ms: 1000,
                failure_threshold: 3,
                success_threshold: 1,
                cooldown_ms: 0,
            },
        },
    ];

    let config = make_config(0, backends, "round-robin", cert, key);
    let mut listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = stop.clone();

    let handle = rt.spawn_blocking(move || {
        while !stop_flag.load(Ordering::Relaxed) {
            listener.poll();
        }
    });

    let r1 = run_h3_client(listen_addr, "rr-test").expect("request 1");
    let r2 = run_h3_client(listen_addr, "rr-test").expect("request 2");
    let r3 = run_h3_client(listen_addr, "rr-test").expect("request 3");
    let r4 = run_h3_client(listen_addr, "rr-test").expect("request 4");

    stop.store(true, Ordering::Relaxed);
    handle.abort();

    let sequence = vec![r1, r2, r3, r4]
        .into_iter()
        .map(|body| body.trim().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        sequence,
        vec!["backend-a", "backend-b", "backend-a", "backend-b"]
    );
}

#[test]
fn consistent_hash_is_stable_per_authority() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_a = rt.block_on(start_h2_backend("node-a\n"));
    let backend_b = rt.block_on(start_h2_backend("node-b\n"));

    let backends = vec![
        Backend {
            id: "a".to_string(),
            address: backend_a.to_string(),
            weight: 1,
            health_check: HealthCheck {
                path: "/health".to_string(),
                interval: 1000,
                timeout_ms: 1000,
                failure_threshold: 3,
                success_threshold: 1,
                cooldown_ms: 0,
            },
        },
        Backend {
            id: "b".to_string(),
            address: backend_b.to_string(),
            weight: 1,
            health_check: HealthCheck {
                path: "/health".to_string(),
                interval: 1000,
                timeout_ms: 1000,
                failure_threshold: 3,
                success_threshold: 1,
                cooldown_ms: 0,
            },
        },
    ];

    let config = make_config(0, backends, "consistent-hash", cert, key);
    let mut listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = stop.clone();

    let handle = rt.spawn_blocking(move || {
        while !stop_flag.load(Ordering::Relaxed) {
            listener.poll();
        }
    });

    let a1 = run_h3_client(listen_addr, "alpha").expect("alpha 1");
    let a2 = run_h3_client(listen_addr, "alpha").expect("alpha 2");
    let b1 = run_h3_client(listen_addr, "beta").expect("beta 1");

    stop.store(true, Ordering::Relaxed);
    handle.abort();

    assert_eq!(a1.trim(), a2.trim());
    assert!(a1.trim() == "node-a" || a1.trim() == "node-b");
    assert!(b1.trim() == "node-a" || b1.trim() == "node-b");
}
