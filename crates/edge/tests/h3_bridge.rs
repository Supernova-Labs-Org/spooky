use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, HeaderValue};
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::{
    Request, Response, Uri,
    body::{Body, Frame, Incoming},
    service::service_fn,
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo},
};
use quiche::h3::NameValue;
use rand::RngCore;
use rcgen::{Certificate, CertificateParams, SanType};
use tempfile::{TempDir, tempdir};
use tokio::net::TcpListener;

use spooky_config::config::{
    Backend, ClientAuth, Config, HealthCheck, Listen, LoadBalancing, Log, LogFormat, Tls,
    UpstreamTls,
};
use spooky_edge::QUICListener;
use spooky_edge::constants::{
    BACKEND_TIMEOUT_SECS, MAX_DATAGRAM_SIZE_BYTES, MAX_REQUEST_BODY_BYTES,
    MAX_STREAMS_PER_CONNECTION, MAX_UDP_PAYLOAD_BYTES, QUIC_IDLE_TIMEOUT_MS, QUIC_INITIAL_MAX_DATA,
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

fn make_config(port: u32, backend_addr: String, cert: String, key: String) -> Config {
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
                address: normalize_backend_address(backend_addr),
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

struct ListenerTaskGuard {
    stop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
}

impl ListenerTaskGuard {
    fn spawn(rt: &tokio::runtime::Runtime, mut listener: QUICListener) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let handle = rt.spawn_blocking(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                listener.poll();
            }
        });
        Self { stop, handle }
    }
}

impl Drop for ListenerTaskGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.abort();
    }
}

async fn start_h2_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let io = TokioIo::new(stream);
            let service = service_fn(|_req: Request<Incoming>| async move {
                Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from("backend ok\n"))))
            });

            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await;
        }
    });

    addr
}

/// Backend that drains the full request body before responding.
/// Required for large-body tests where H2 flow control would otherwise stall.
async fn start_h2_backend_draining() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(|req: Request<Incoming>| async move {
                    // Drain body so H2 flow control doesn't stall.
                    let _ = req.into_body().collect().await;
                    Ok::<_, hyper::Error>(Response::new(
                        Full::new(Bytes::from_static(b"ok\n")).boxed(),
                    ))
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });

    addr
}

fn run_h3_client(addr: SocketAddr) -> Result<String, String> {
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
    let client_stream_window = QUIC_INITIAL_STREAM_DATA.saturating_add(128 * 1024);
    config.set_initial_max_stream_data_bidi_local(client_stream_window);
    config.set_initial_max_stream_data_bidi_remote(client_stream_window);
    config.set_initial_max_stream_data_uni(client_stream_window);
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

        let read_timeout = quic_read_timeout(&conn);
        socket
            .set_read_timeout(Some(read_timeout))
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

fn run_h3_client_chunked_post(
    addr: SocketAddr,
    path: &str,
    total_len: usize,
    chunk_size: usize,
    inter_chunk_delay: Duration,
    read_delay: Duration,
    timeout: Duration,
) -> Result<(String, Vec<u8>, bool), String> {
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
    let mut h3: Option<quiche::h3::Connection> = None;

    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];
    let start = Instant::now();

    let mut req_stream_id: Option<u64> = None;
    let mut remaining = total_len;
    let mut chunk_written = 0usize;
    let mut current_chunk_len = 0usize;
    let mut should_fin_for_chunk = false;
    let mut body_done = total_len == 0;
    let mut status = String::new();
    let mut body = Vec::new();
    let payload = vec![0u8; chunk_size.max(1)];

    let (w, si) = conn.send(&mut out).map_err(|e| format!("send: {e:?}"))?;
    socket
        .send_to(&out[..w], si.to)
        .map_err(|e| format!("send_to: {e:?}"))?;

    loop {
        loop {
            match conn.send(&mut out) {
                Ok((w, si)) => {
                    let _ = socket.send_to(&out[..w], si.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(format!("send loop: {e:?}")),
            }
        }

        let read_timeout = quic_read_timeout(&conn);
        socket
            .set_read_timeout(Some(read_timeout))
            .map_err(|e| format!("timeout: {e:?}"))?;
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

        if conn.is_established() && h3.is_none() {
            h3 = Some(
                quiche::h3::Connection::with_transport(&mut conn, &h3_config)
                    .map_err(|e| format!("h3 conn: {e:?}"))?,
            );
        }

        if let Some(h3c) = h3.as_mut() {
            if req_stream_id.is_none() && conn.is_established() {
                let content_length = total_len.to_string();
                let req = vec![
                    quiche::h3::Header::new(b":method", b"POST"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", b"localhost"),
                    quiche::h3::Header::new(b":path", path.as_bytes()),
                    quiche::h3::Header::new(b"user-agent", b"spooky-pressure-test"),
                    quiche::h3::Header::new(b"content-length", content_length.as_bytes()),
                ];
                let sid = h3c
                    .send_request(&mut conn, &req, total_len == 0)
                    .map_err(|e| format!("send_request: {e:?}"))?;
                req_stream_id = Some(sid);
            }

            if let Some(sid) = req_stream_id
                && !body_done
            {
                if current_chunk_len == 0 {
                    current_chunk_len = chunk_size.min(remaining);
                    chunk_written = 0;
                    should_fin_for_chunk = current_chunk_len == remaining;
                }
                let end = current_chunk_len;
                match h3c.send_body(
                    &mut conn,
                    sid,
                    &payload[chunk_written..end],
                    should_fin_for_chunk,
                ) {
                    Ok(written) => {
                        chunk_written += written;
                        if chunk_written == current_chunk_len {
                            remaining -= current_chunk_len;
                            current_chunk_len = 0;
                            if remaining == 0 {
                                body_done = true;
                            } else if !inter_chunk_delay.is_zero() {
                                std::thread::sleep(inter_chunk_delay);
                            }
                        }
                    }
                    Err(quiche::h3::Error::Done | quiche::h3::Error::StreamBlocked) => {}
                    Err(e) => return Err(format!("send_body: {e:?}")),
                }
            }

            loop {
                match h3c.poll(&mut conn) {
                    Ok((_sid, quiche::h3::Event::Headers { list, .. })) => {
                        for header in &list {
                            if header.name() == b":status" {
                                status = String::from_utf8_lossy(header.value()).to_string();
                            }
                        }
                    }
                    Ok((sid, quiche::h3::Event::Data)) => loop {
                        if !read_delay.is_zero() {
                            std::thread::sleep(read_delay);
                        }
                        match h3c.recv_body(&mut conn, sid, &mut buf) {
                            Ok(read) => body.extend_from_slice(&buf[..read]),
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => return Err(format!("recv_body: {e:?}")),
                        }
                    },
                    Ok((_sid, quiche::h3::Event::Finished)) => {
                        return Ok((status, body, false));
                    }
                    Ok((_sid, quiche::h3::Event::Reset(_))) => {
                        return Ok((status, body, true));
                    }
                    Ok((_sid, quiche::h3::Event::PriorityUpdate)) => {}
                    Ok((_sid, quiche::h3::Event::GoAway)) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => return Err(format!("poll: {e:?}")),
                }
            }
        }

        if start.elapsed() > timeout {
            return Err(format!(
                "timeout waiting for response (status='{}', body_len={})",
                status,
                body.len()
            ));
        }
    }
}

fn run_h3_client_two_chunk_post(
    addr: SocketAddr,
    path: &str,
    chunk1: Vec<u8>,
    chunk2: Vec<u8>,
    inter_chunk_delay: Duration,
    timeout: Duration,
) -> Result<(String, Vec<u8>, bool), String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    let local_addr = socket.local_addr().map_err(|e| e.to_string())?;

    let mut qconfig =
        quiche::Config::new(quiche::PROTOCOL_VERSION).map_err(|e| format!("config: {e:?}"))?;
    qconfig.verify_peer(false);
    qconfig
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .map_err(|e| format!("alpn: {e:?}"))?;
    qconfig.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    qconfig.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    qconfig.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    qconfig.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    qconfig.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);
    let mut conn = quiche::connect(Some("localhost"), &scid, local_addr, addr, &mut qconfig)
        .map_err(|e| format!("connect: {e:?}"))?;
    let h3_config = quiche::h3::Config::new().map_err(|e| format!("h3: {e:?}"))?;
    let mut h3: Option<quiche::h3::Connection> = None;

    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];
    let mut stream_id: Option<u64> = None;
    let mut chunk1_written = 0usize;
    let mut chunk2_written = 0usize;
    let mut delayed_once = false;
    let total_len = chunk1.len() + chunk2.len();
    let mut status = String::new();
    let mut response_body = Vec::new();
    let start = Instant::now();

    let (write, send_info) = conn.send(&mut out).map_err(|e| format!("send: {e:?}"))?;
    socket
        .send_to(&out[..write], send_info.to)
        .map_err(|e| format!("send_to: {e:?}"))?;

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

        let read_timeout = quic_read_timeout(&conn);
        socket
            .set_read_timeout(Some(read_timeout))
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

        if conn.is_established() && h3.is_none() {
            h3 = Some(
                quiche::h3::Connection::with_transport(&mut conn, &h3_config)
                    .map_err(|e| format!("h3 conn: {e:?}"))?,
            );
        }

        if let Some(h3c) = h3.as_mut() {
            if stream_id.is_none() && conn.is_established() {
                let content_length = total_len.to_string();
                let headers = vec![
                    quiche::h3::Header::new(b":method", b"POST"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", b"localhost"),
                    quiche::h3::Header::new(b":path", path.as_bytes()),
                    quiche::h3::Header::new(b"content-length", content_length.as_bytes()),
                ];
                let sid = h3c
                    .send_request(&mut conn, &headers, total_len == 0)
                    .map_err(|e| format!("send_request: {e:?}"))?;
                stream_id = Some(sid);
            }

            if let Some(sid) = stream_id {
                if chunk1_written < chunk1.len() {
                    match h3c.send_body(&mut conn, sid, &chunk1[chunk1_written..], false) {
                        Ok(written) => chunk1_written += written,
                        Err(quiche::h3::Error::Done | quiche::h3::Error::StreamBlocked) => {}
                        Err(e) => return Err(format!("send_body chunk1: {e:?}")),
                    }
                } else {
                    if !delayed_once && !inter_chunk_delay.is_zero() {
                        std::thread::sleep(inter_chunk_delay);
                        delayed_once = true;
                    }
                    if chunk2_written < chunk2.len() {
                        match h3c.send_body(&mut conn, sid, &chunk2[chunk2_written..], true) {
                            Ok(written) => chunk2_written += written,
                            Err(quiche::h3::Error::Done | quiche::h3::Error::StreamBlocked) => {}
                            Err(e) => return Err(format!("send_body chunk2: {e:?}")),
                        }
                    }
                }
            }

            loop {
                match h3c.poll(&mut conn) {
                    Ok((_sid, quiche::h3::Event::Headers { list, .. })) => {
                        for h in &list {
                            if h.name() == b":status" {
                                status = String::from_utf8_lossy(h.value()).to_string();
                            }
                        }
                    }
                    Ok((sid, quiche::h3::Event::Data)) => loop {
                        match h3c.recv_body(&mut conn, sid, &mut buf) {
                            Ok(read) => response_body.extend_from_slice(&buf[..read]),
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => return Err(format!("recv_body: {e:?}")),
                        }
                    },
                    Ok((_sid, quiche::h3::Event::Finished)) => {
                        return Ok((status, response_body, false));
                    }
                    Ok((_sid, quiche::h3::Event::Reset(_))) => {
                        return Ok((status, response_body, true));
                    }
                    Ok((_sid, quiche::h3::Event::PriorityUpdate)) => {}
                    Ok((_sid, quiche::h3::Event::GoAway)) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => return Err(format!("poll: {e:?}")),
                }
            }
        }

        if start.elapsed() > timeout {
            return Err(format!(
                "timeout waiting for response (status='{}', body_len={})",
                status,
                response_body.len()
            ));
        }
    }
}

type TestBody = BoxBody<Bytes, Infallible>;

struct DelayedChunkBody {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
}

impl DelayedChunkBody {
    fn channel(buffer: usize) -> (tokio::sync::mpsc::Sender<Bytes>, Self) {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        (tx, Self { rx })
    }
}

impl Body for DelayedChunkBody {
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

#[derive(Debug, Clone)]
struct StreamObservation {
    path: String,
    status: Option<String>,
    body: Vec<u8>,
    data_events: Vec<Duration>,
    finished_at: Option<Duration>,
}

fn observation_for<'a>(observations: &'a [StreamObservation], path: &str) -> &'a StreamObservation {
    observations
        .iter()
        .find(|o| o.path == path)
        .unwrap_or_else(|| panic!("missing observation for path {path}"))
}

fn find_free_tcp_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().expect("local addr").port()
}

async fn scrape_metrics(port: u16, path: &str, timeout: Duration) -> Result<String, String> {
    let start = Instant::now();
    let client: Client<HttpConnector, Empty<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let target: Uri = format!("http://127.0.0.1:{port}{path}")
        .parse()
        .map_err(|err| format!("invalid metrics uri: {err}"))?;
    let mut last_error = String::new();

    while start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(500), client.get(target.clone())).await {
            Ok(Ok(response)) => {
                let status = response.status();
                if !status.is_success() {
                    last_error = format!("unexpected status: {status}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }

                let collected = match response.into_body().collect().await {
                    Ok(body) => body.to_bytes(),
                    Err(err) => {
                        last_error = format!("read body: {err}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                if collected.is_empty() {
                    last_error = "empty response body".to_string();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }

                match String::from_utf8(collected.to_vec()) {
                    Ok(text) => return Ok(text),
                    Err(err) => {
                        last_error = format!("metrics payload not utf8: {err}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                }
            }
            Ok(Err(err)) => {
                last_error = format!("http request failed: {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(_) => {
                last_error = "request timeout".to_string();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    Err(format!(
        "metrics endpoint not reachable within {:?} ({})",
        timeout, last_error
    ))
}

fn run_h3_client_concurrent_get(
    addr: SocketAddr,
    paths: &[&str],
    timeout: Duration,
) -> Result<Vec<StreamObservation>, String> {
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
    let mut requests_sent = false;
    let mut finished = 0usize;
    let start = Instant::now();

    let mut stream_to_observation: HashMap<u64, usize> = HashMap::new();
    let mut observations: Vec<StreamObservation> = paths
        .iter()
        .map(|path| StreamObservation {
            path: (*path).to_string(),
            status: None,
            body: Vec::new(),
            data_events: Vec::new(),
            finished_at: None,
        })
        .collect();

    let (write, send_info) = conn.send(&mut out).map_err(|e| format!("send: {e:?}"))?;
    socket
        .send_to(&out[..write], send_info.to)
        .map_err(|e| format!("send_to: {e:?}"))?;

    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "timeout waiting for responses (done={finished}, expected={})",
                paths.len()
            ));
        }

        loop {
            match conn.send(&mut out) {
                Ok((write, send_info)) => {
                    let _ = socket.send_to(&out[..write], send_info.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(format!("send loop: {e:?}")),
            }
        }

        let remaining = timeout.saturating_sub(start.elapsed());
        let read_timeout = quic_read_timeout(&conn)
            .min(remaining)
            .min(Duration::from_millis(50));
        let read_timeout = if read_timeout.is_zero() {
            Duration::from_millis(1)
        } else {
            read_timeout
        };
        socket
            .set_read_timeout(Some(read_timeout))
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
            if conn.is_established() && !requests_sent {
                for (idx, path) in paths.iter().enumerate() {
                    let req = vec![
                        quiche::h3::Header::new(b":method", b"GET"),
                        quiche::h3::Header::new(b":scheme", b"https"),
                        quiche::h3::Header::new(b":authority", b"localhost"),
                        quiche::h3::Header::new(b":path", path.as_bytes()),
                        quiche::h3::Header::new(b"user-agent", b"spooky-regression-test"),
                    ];
                    let stream_id = h3
                        .send_request(&mut conn, &req, true)
                        .map_err(|e| format!("send_request: {e:?}"))?;
                    stream_to_observation.insert(stream_id, idx);
                }
                requests_sent = true;
            }

            loop {
                match h3.poll(&mut conn) {
                    Ok((stream_id, quiche::h3::Event::Headers { list, .. })) => {
                        if let Some(idx) = stream_to_observation.get(&stream_id).copied() {
                            for header in &list {
                                if header.name() == b":status" {
                                    observations[idx].status =
                                        Some(String::from_utf8_lossy(header.value()).to_string());
                                }
                            }
                        }
                    }
                    Ok((stream_id, quiche::h3::Event::Data)) => {
                        let Some(idx) = stream_to_observation.get(&stream_id).copied() else {
                            break;
                        };
                        loop {
                            match h3.recv_body(&mut conn, stream_id, &mut buf) {
                                Ok(read) => {
                                    observations[idx].body.extend_from_slice(&buf[..read]);
                                    observations[idx].data_events.push(start.elapsed());
                                }
                                Err(quiche::h3::Error::Done) => break,
                                Err(e) => return Err(format!("recv_body: {e:?}")),
                            }
                        }
                    }
                    Ok((stream_id, quiche::h3::Event::Finished)) => {
                        if let Some(idx) = stream_to_observation.get(&stream_id).copied()
                            && observations[idx].finished_at.is_none()
                        {
                            observations[idx].finished_at = Some(start.elapsed());
                            finished += 1;
                        }
                    }
                    Ok((_stream_id, quiche::h3::Event::PriorityUpdate)) => {}
                    Ok((_stream_id, quiche::h3::Event::GoAway)) => {}
                    Ok((stream_id, quiche::h3::Event::Reset(_))) => {
                        return Err(format!("stream reset on id {stream_id}"));
                    }
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => return Err(format!("poll: {e:?}")),
                }
            }
        }

        if finished == paths.len() {
            return Ok(observations);
        }

    }
}

async fn start_h2_backend_with_regression_routes() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };

            let io = TokioIo::new(stream);
            let service = service_fn(|req: Request<Incoming>| async move {
                let path = req.uri().path().to_string();
                let response: Response<TestBody> = match path.as_str() {
                    "/fast" => Response::new(Full::new(Bytes::from_static(b"fast\n")).boxed()),
                    "/slow" => {
                        tokio::time::sleep(Duration::from_millis(700)).await;
                        Response::new(Full::new(Bytes::from_static(b"slow\n")).boxed())
                    }
                    "/status500" => Response::builder()
                        .status(500)
                        .body(Full::new(Bytes::from_static(b"backend 500\n")).boxed())
                        .unwrap(),
                    "/timeout" => {
                        tokio::time::sleep(Duration::from_secs(BACKEND_TIMEOUT_SECS + 1)).await;
                        Response::new(Full::new(Bytes::from_static(b"late\n")).boxed())
                    }
                    "/stream" => {
                        let (tx, body) = DelayedChunkBody::channel(8);
                        tokio::spawn(async move {
                            let _ = tx.send(Bytes::from_static(b"chunk-1")).await;
                            tokio::time::sleep(Duration::from_millis(140)).await;
                            let _ = tx.send(Bytes::from_static(b"chunk-2")).await;
                            tokio::time::sleep(Duration::from_millis(140)).await;
                            let _ = tx.send(Bytes::from_static(b"chunk-3")).await;
                        });
                        let mut response = Response::new(body.boxed());
                        response
                            .headers_mut()
                            .insert(CONTENT_LENGTH, HeaderValue::from_static("21"));
                        response
                    }
                    "/long-stream" => {
                        let (tx, body) = DelayedChunkBody::channel(8);
                        tokio::spawn(async move {
                            let _ = tx.send(Bytes::from_static(b"part-1")).await;
                            tokio::time::sleep(Duration::from_millis(220)).await;
                            let _ = tx.send(Bytes::from_static(b"part-2")).await;
                            tokio::time::sleep(Duration::from_millis(220)).await;
                            let _ = tx.send(Bytes::from_static(b"part-3")).await;
                            tokio::time::sleep(Duration::from_millis(220)).await;
                            let _ = tx.send(Bytes::from_static(b"part-4")).await;
                        });
                        Response::new(body.boxed())
                    }
                    _ => Response::new(Full::new(Bytes::from_static(b"default\n")).boxed()),
                };
                Ok::<_, hyper::Error>(response)
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

async fn start_h2_backend_with_drain_probe(inflight_seen: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };

            let io = TokioIo::new(stream);
            let inflight_seen = Arc::clone(&inflight_seen);
            let service = service_fn(move |req: Request<Incoming>| {
                let path = req.uri().path().to_string();
                let inflight_seen = Arc::clone(&inflight_seen);
                async move {
                    let response: Response<TestBody> = match path.as_str() {
                        "/drain-slow" => {
                            inflight_seen.store(true, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(700)).await;
                            Response::new(Full::new(Bytes::from_static(b"drain-ok\n")).boxed())
                        }
                        _ => Response::new(Full::new(Bytes::from_static(b"default\n")).boxed()),
                    };
                    Ok::<_, hyper::Error>(response)
                }
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

async fn start_h2_backend_with_chaos_routes() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let jitter_counter = Arc::new(AtomicUsize::new(0));
    let flap_counter = Arc::new(AtomicUsize::new(0));

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };

            let io = TokioIo::new(stream);
            let jitter_counter = Arc::clone(&jitter_counter);
            let flap_counter = Arc::clone(&flap_counter);
            let service = service_fn(move |req: Request<Incoming>| {
                let path = req.uri().path().to_string();
                let jitter_counter = Arc::clone(&jitter_counter);
                let flap_counter = Arc::clone(&flap_counter);
                async move {
                    let response: Response<TestBody> = match path.as_str() {
                        "/jitter" => {
                            let jitter =
                                15 + (jitter_counter.fetch_add(1, Ordering::Relaxed) % 120) as u64;
                            tokio::time::sleep(Duration::from_millis(jitter)).await;
                            Response::new(Full::new(Bytes::from_static(b"jitter-ok\n")).boxed())
                        }
                        "/loss" => {
                            tokio::time::sleep(Duration::from_secs(BACKEND_TIMEOUT_SECS + 2)).await;
                            Response::new(Full::new(Bytes::from_static(b"late-loss\n")).boxed())
                        }
                        "/flap" => {
                            if flap_counter
                                .fetch_add(1, Ordering::Relaxed)
                                .is_multiple_of(2)
                            {
                                Response::new(Full::new(Bytes::from_static(b"flap-ok\n")).boxed())
                            } else {
                                Response::builder()
                                    .status(503)
                                    .body(Full::new(Bytes::from_static(b"flap-fail\n")).boxed())
                                    .unwrap()
                            }
                        }
                        "/health" => Response::new(Full::new(Bytes::from_static(b"ok\n")).boxed()),
                        _ => Response::new(Full::new(Bytes::from_static(b"fast-ok\n")).boxed()),
                    };
                    Ok::<_, hyper::Error>(response)
                }
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

fn make_config_with_backends(
    port: u32,
    backends: Vec<Backend>,
    lb_type: &str,
    cert: String,
    key: String,
) -> Config {
    let mut config = make_config(
        port,
        backends
            .first()
            .map(|backend| backend.address.clone())
            .unwrap_or_else(|| "127.0.0.1:1".to_string()),
        cert,
        key,
    );
    if let Some(upstream) = config.upstream.get_mut("test_pool") {
        upstream.backends = backends
            .into_iter()
            .map(|mut backend| {
                backend.address = normalize_backend_address(backend.address);
                backend
            })
            .collect();
        upstream.load_balancing.lb_type = lb_type.to_string();
    }
    config
}

#[test]
fn http3_to_http2_roundtrip() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let body = run_h3_client(listen_addr).expect("client request failed");

    assert!(!body.is_empty(), "expected non-empty response from backend");
}

#[test]
fn in_flight_request_completes_during_drain_window() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let inflight_seen = Arc::new(AtomicBool::new(false));
    let backend_addr = rt.block_on(start_h2_backend_with_drain_probe(Arc::clone(
        &inflight_seen,
    )));
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();

    let listener = Arc::new(Mutex::new(listener));
    let stop = Arc::new(AtomicBool::new(false));
    let listener_thread = {
        let listener = Arc::clone(&listener);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(mut guard) = listener.lock() {
                    guard.poll();
                } else {
                    break;
                }
            }
        })
    };

    let client_thread = thread::spawn(move || {
        run_h3_client_concurrent_get(
            listen_addr,
            &["/drain-slow"],
            Duration::from_secs(REQUEST_TIMEOUT_SECS + 6),
        )
    });

    let wait_start = Instant::now();
    while !inflight_seen.load(Ordering::Relaxed) {
        assert!(
            wait_start.elapsed() < Duration::from_secs(3),
            "backend never observed the in-flight request before drain"
        );
        thread::sleep(Duration::from_millis(10));
    }

    if let Ok(mut guard) = listener.lock() {
        guard.start_draining();
    }

    let observations = client_thread
        .join()
        .expect("client thread join failed")
        .expect("request should complete while draining");
    let drained = observation_for(&observations, "/drain-slow");
    assert_eq!(drained.status.as_deref(), Some("200"));
    assert_eq!(drained.body, b"drain-ok\n");

    let drain_wait_start = Instant::now();
    loop {
        let done = if let Ok(mut guard) = listener.lock() {
            guard.drain_complete()
        } else {
            false
        };
        if done {
            break;
        }
        assert!(
            drain_wait_start.elapsed() < Duration::from_secs(2),
            "drain should complete promptly once in-flight streams finish"
        );
        thread::sleep(Duration::from_millis(10));
    }

    stop.store(true, Ordering::Relaxed);
    let _ = listener_thread.join();
}

/// While draining, new QUIC Initial packets must be silently dropped so no new
/// connection state is allocated.  The test starts draining before any client
/// connects, then confirms a fresh QUIC connection attempt never reaches the
/// Established state within a short window (the handshake packets are dropped).
#[test]
fn draining_rejects_new_connections() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();

    let listener = Arc::new(Mutex::new(listener));

    // Start draining immediately — before any client connects.
    listener.lock().unwrap().start_draining();

    let stop = Arc::new(AtomicBool::new(false));
    let listener_thread = {
        let listener = Arc::clone(&listener);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(mut guard) = listener.lock() {
                    guard.poll();
                }
            }
        })
    };

    // Attempt a fresh QUIC connection.  Initial packets are silently dropped,
    // so the handshake never completes; the connection stays in Initial state.
    let established = {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
        let local_addr = socket.local_addr().unwrap();

        let mut qconfig = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
        qconfig.verify_peer(false);
        qconfig
            .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
            .unwrap();
        qconfig.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
        qconfig.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
        qconfig.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
        qconfig.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
        qconfig.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
        qconfig.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
        qconfig.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
        qconfig.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
        qconfig.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
        qconfig.set_disable_active_migration(true);

        let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
        rand::thread_rng().fill_bytes(&mut scid_bytes);
        let scid = quiche::ConnectionId::from_ref(&scid_bytes);
        let mut conn = quiche::connect(
            Some("localhost"),
            &scid,
            local_addr,
            listen_addr,
            &mut qconfig,
        )
        .unwrap();

        let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
        let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

        // Send the Initial packet.
        let (w, si) = conn.send(&mut out).unwrap();
        socket.send_to(&out[..w], si.to).unwrap();

        // Poll for up to 500 ms; the handshake should never complete.
        let deadline = Instant::now() + Duration::from_millis(500);
        let mut established = false;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            socket
                .set_read_timeout(Some(remaining.min(Duration::from_millis(50))))
                .unwrap();
            match socket.recv_from(&mut buf) {
                Ok((len, from)) => {
                    let _ = conn.recv(
                        &mut buf[..len],
                        quiche::RecvInfo {
                            from,
                            to: local_addr,
                        },
                    );
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    conn.on_timeout();
                }
                Err(_) => break,
            }
            // Flush any retransmits.
            loop {
                match conn.send(&mut out) {
                    Ok((w, si)) => {
                        let _ = socket.send_to(&out[..w], si.to);
                    }
                    Err(quiche::Error::Done) => break,
                    Err(_) => break,
                }
            }
            if conn.is_established() {
                established = true;
                break;
            }
        }
        established
    };

    stop.store(true, Ordering::Relaxed);
    let _ = listener_thread.join();

    assert!(
        !established,
        "new QUIC connection must not be established while listener is draining"
    );
}

#[test]
fn draining_forces_close_after_configured_timeout() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let inflight_seen = Arc::new(AtomicBool::new(false));
    let backend_addr = rt.block_on(start_h2_backend_with_drain_probe(Arc::clone(
        &inflight_seen,
    )));
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.shutdown_drain_timeout_ms = 120;
    let mut listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();

    let client_thread = thread::spawn(move || {
        run_h3_client_concurrent_get(
            listen_addr,
            &["/drain-slow"],
            Duration::from_secs(REQUEST_TIMEOUT_SECS + 6),
        )
    });

    let mut drain_started: Option<Instant> = None;
    let drive_start = Instant::now();
    loop {
        listener.poll();

        if drain_started.is_none() && inflight_seen.load(Ordering::Relaxed) {
            listener.start_draining();
            drain_started = Some(Instant::now());
        }

        if drain_started.is_some() && listener.drain_complete() {
            break;
        }

        assert!(
            drive_start.elapsed() < Duration::from_secs(3),
            "drain test did not converge in expected time"
        );
    }
    let drain_elapsed = drain_started
        .expect("drain should start once in-flight request is observed")
        .elapsed();
    assert!(
        drain_elapsed < Duration::from_millis(600),
        "forced close should complete before slow backend success path (elapsed={drain_elapsed:?})"
    );

    let client_result = client_thread.join().expect("client thread join failed");
    if let Ok(observations) = client_result {
        let drained = observation_for(&observations, "/drain-slow");
        let got_full_success =
            drained.status.as_deref() == Some("200") && drained.body == b"drain-ok\n";
        assert!(
            !got_full_success,
            "forced close should prevent full slow-backend success before timeout"
        );
    }
}

/// A request body exceeding MAX_REQUEST_BODY_BYTES must be rejected with 413
/// before the backend is contacted.
///
/// The body is sent in two chunks so the client interleaves send/recv between
/// them, allowing the server to reply with 413 after the first chunk crosses
/// the limit without the client blocking on a single large send_body call.
#[test]
fn oversized_request_body_returns_413() {
    use rand::RngCore;

    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    // Two chunks that together exceed the limit; each fits within quiche flow control.
    let chunk_size = MAX_REQUEST_BODY_BYTES / 2 + 1;
    let chunk1 = vec![0u8; chunk_size];
    let chunk2 = vec![0u8; chunk_size];
    let total_len = chunk1.len() + chunk2.len();

    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let local_addr = socket.local_addr().unwrap();

    let mut qconfig = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    qconfig.verify_peer(false);
    qconfig
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();
    qconfig.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    qconfig.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    // Window large enough to transmit both chunks.
    let window = (total_len as u64 + 1) * 2;
    qconfig.set_initial_max_data(window * 4);
    qconfig.set_initial_max_stream_data_bidi_local(window);
    qconfig.set_initial_max_stream_data_bidi_remote(window);
    qconfig.set_initial_max_stream_data_uni(window);
    qconfig.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    qconfig.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    qconfig.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);
    let mut conn = quiche::connect(
        Some("localhost"),
        &scid,
        local_addr,
        listen_addr,
        &mut qconfig,
    )
    .unwrap();
    let h3_config = quiche::h3::Config::new().unwrap();

    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    // Send initial QUIC packet.
    let (w, si) = conn.send(&mut out).unwrap();
    socket.send_to(&out[..w], si.to).unwrap();

    let start = Instant::now();
    let mut h3: Option<quiche::h3::Connection> = None;
    let mut stream_id: Option<u64> = None;
    let mut chunk1_written = 0usize;
    let mut chunk2_written = 0usize;
    let mut response_status = String::new();
    let mut response_body = Vec::new();

    let status = 'outer: loop {
        // Flush send.
        loop {
            match conn.send(&mut out) {
                Ok((w, si)) => {
                    let _ = socket.send_to(&out[..w], si.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => panic!("send: {e:?}"),
            }
        }

        let timeout = quic_read_timeout(&conn);
        socket.set_read_timeout(Some(timeout)).unwrap();

        match socket.recv_from(&mut buf) {
            Ok((len, from)) => {
                conn.recv(
                    &mut buf[..len],
                    quiche::RecvInfo {
                        from,
                        to: local_addr,
                    },
                )
                .unwrap();
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                conn.on_timeout();
            }
            Err(e) => panic!("recv: {e:?}"),
        }

        if conn.is_established() && h3.is_none() {
            h3 = Some(quiche::h3::Connection::with_transport(&mut conn, &h3_config).unwrap());
        }

        if let Some(h3c) = h3.as_mut() {
            // Send headers + first chunk.
            if stream_id.is_none() && conn.is_established() {
                let content_length = total_len.to_string();
                let headers = vec![
                    quiche::h3::Header::new(b":method", b"POST"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", b"localhost"),
                    quiche::h3::Header::new(b":path", b"/"),
                    quiche::h3::Header::new(b"content-length", content_length.as_bytes()),
                ];
                let sid = h3c.send_request(&mut conn, &headers, false).unwrap();
                stream_id = Some(sid);
            }

            if let Some(sid) = stream_id {
                // send_body() can write partial buffers; keep retrying until both chunks are
                // fully queued so the server always receives > MAX_REQUEST_BODY_BYTES.
                if chunk1_written < chunk1.len() {
                    match h3c.send_body(&mut conn, sid, &chunk1[chunk1_written..], false) {
                        Ok(written) => chunk1_written += written,
                        Err(quiche::h3::Error::Done | quiche::h3::Error::StreamBlocked) => {}
                        Err(e) => panic!("send_body chunk1: {e:?}"),
                    }
                } else if chunk2_written < chunk2.len() {
                    match h3c.send_body(&mut conn, sid, &chunk2[chunk2_written..], true) {
                        Ok(written) => chunk2_written += written,
                        Err(quiche::h3::Error::Done | quiche::h3::Error::StreamBlocked) => {}
                        Err(e) => panic!("send_body chunk2: {e:?}"),
                    }
                }
            }

            // Poll for server response.
            loop {
                match h3c.poll(&mut conn) {
                    Ok((_sid, quiche::h3::Event::Headers { list, .. })) => {
                        for h in &list {
                            if h.name() == b":status" {
                                response_status = String::from_utf8_lossy(h.value()).to_string();
                            }
                        }
                    }
                    Ok((sid, quiche::h3::Event::Data)) => loop {
                        match h3c.recv_body(&mut conn, sid, &mut buf) {
                            Ok(r) => response_body.extend_from_slice(&buf[..r]),
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => panic!("recv_body: {e:?}"),
                        }
                    },
                    Ok((_sid, quiche::h3::Event::Finished)) => {
                        break 'outer response_status.clone();
                    }
                    Ok((_sid, quiche::h3::Event::Reset(_))) => {
                        break 'outer response_status.clone();
                    }
                    Ok(_) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => panic!("poll: {e:?}"),
                }
            }
        }

        if start.elapsed() > Duration::from_secs(REQUEST_TIMEOUT_SECS) {
            panic!("timeout waiting for 413 response");
        }
    };

    assert_eq!(
        status, "413",
        "expected 413 Payload Too Large, got {status}"
    );
}

#[test]
fn request_body_at_cap_is_accepted() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    // Use a draining backend so H2 flow control does not stall when the full
    // 1 MiB request body fills the stream window before the backend responds.
    let backend_addr = rt.block_on(start_h2_backend_draining());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let chunk1 = vec![0u8; MAX_REQUEST_BODY_BYTES / 2];
    let chunk2 = vec![0u8; MAX_REQUEST_BODY_BYTES - chunk1.len()];
    let (status, _body, got_reset) = run_h3_client_two_chunk_post(
        listen_addr,
        "/",
        chunk1,
        chunk2,
        Duration::from_millis(0),
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 45),
    )
    .expect("at-cap request should complete");

    assert_eq!(status, "200", "request at cap should be accepted");
    assert!(!got_reset, "request at cap should not reset stream");
}

#[test]
fn slow_request_producer_over_cap_returns_413() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let chunk1 = vec![0u8; MAX_REQUEST_BODY_BYTES / 2 + 1];
    let chunk2 = vec![0u8; MAX_REQUEST_BODY_BYTES / 2 + 1];
    let (status, _body, got_reset) = run_h3_client_two_chunk_post(
        listen_addr,
        "/",
        chunk1,
        chunk2,
        Duration::from_millis(120),
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 12),
    )
    .expect("slow over-cap request producer should complete");

    assert_eq!(
        status, "413",
        "slow over-cap producer should get bounded failure"
    );
    assert!(!got_reset, "slow request producer should not reset stream");
}

#[test]
fn request_body_idle_timeout_returns_408() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.client_body_idle_timeout_ms = 120;
    config.performance.backend_total_request_timeout_ms = 10_000;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let (status, body, got_reset) = run_h3_client_two_chunk_post(
        listen_addr,
        "/",
        vec![0u8; 512],
        vec![0u8; 512],
        Duration::from_millis(250),
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 8),
    )
    .expect("slow request producer should complete");

    assert_eq!(
        status, "408",
        "slow body producer should hit request-body idle timeout"
    );
    assert!(
        String::from_utf8_lossy(&body).contains("request body idle timeout"),
        "idle-timeout response body should explain failure"
    );
    assert!(
        !got_reset,
        "idle timeout should return HTTP response, not reset"
    );
}

#[test]
fn unknown_length_response_prebuffer_cap_returns_503() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.max_response_body_bytes = 64 * 1024;
    config.performance.unknown_length_response_prebuffer_bytes = 8;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/long-stream"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 8),
    )
    .expect("unknown-length prebuffer test should complete");
    let stream = observation_for(&observations, "/long-stream");
    assert_eq!(
        stream.status.as_deref(),
        Some("503"),
        "unknown-length prebuffer cap should return 503"
    );
    assert_eq!(
        stream.body, b"upstream response body too large\n",
        "unknown-length prebuffer cap should return deterministic body"
    );
}

#[test]
fn slow_response_consumer_does_not_hang() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let (status, body, got_reset) = run_h3_client_chunked_post(
        listen_addr,
        "/stream",
        0,
        1,
        Duration::from_millis(0),
        Duration::from_millis(40),
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 8),
    )
    .expect("slow response consumer should complete");

    assert_eq!(status, "200");
    assert_eq!(String::from_utf8_lossy(&body), "chunk-1chunk-2chunk-3");
    assert!(!got_reset, "slow response consumer should not reset stream");
}

#[test]
fn concurrent_large_body_pressure_is_bounded() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.global_inflight_limit = 1;
    config.performance.per_upstream_inflight_limit = 1;
    config.performance.per_backend_inflight_limit = 1;
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    const CLIENTS: usize = 6;
    let barrier = Arc::new(std::sync::Barrier::new(CLIENTS));
    let mut handles = Vec::with_capacity(CLIENTS);
    for _ in 0..CLIENTS {
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let chunk1 = vec![0u8; (MAX_REQUEST_BODY_BYTES - 8 * 1024) / 2];
            let chunk2 = vec![0u8; (MAX_REQUEST_BODY_BYTES - 8 * 1024) - chunk1.len()];
            run_h3_client_two_chunk_post(
                listen_addr,
                "/slow",
                chunk1,
                chunk2,
                Duration::from_millis(20),
                Duration::from_secs(REQUEST_TIMEOUT_SECS + 12),
            )
        }));
    }

    let mut count_200 = 0usize;
    let mut count_503 = 0usize;
    for handle in handles {
        let (status, _body, got_reset) = handle
            .join()
            .expect("client thread panicked")
            .expect("client request should terminate");
        assert!(!got_reset, "pressure requests should terminate cleanly");
        match status.as_str() {
            "200" => count_200 += 1,
            "503" => count_503 += 1,
            other => panic!("unexpected status under pressure: {other}"),
        }
    }

    assert!(count_200 >= 1, "expected at least one admitted request");
    assert!(count_503 >= 1, "expected bounded overload shedding");
    assert_eq!(count_200 + count_503, CLIENTS);
}

#[test]
fn slow_stream_does_not_block_fast_stream() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/slow", "/fast"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("client requests failed");

    let slow = observation_for(&observations, "/slow");
    let fast = observation_for(&observations, "/fast");
    assert_eq!(slow.status.as_deref(), Some("200"));
    assert_eq!(fast.status.as_deref(), Some("200"));

    let slow_done = slow.finished_at.expect("slow stream should finish");
    let fast_done = fast.finished_at.expect("fast stream should finish");
    assert!(
        fast_done < slow_done,
        "fast stream should complete before slow stream (fast={fast_done:?}, slow={slow_done:?})"
    );
}

#[test]
fn finished_stream_does_not_block_other_stream_progress() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/slow", "/stream"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("client requests failed");

    let slow = observation_for(&observations, "/slow");
    let stream = observation_for(&observations, "/stream");
    let slow_done = slow.finished_at.expect("slow stream should finish");
    let first_stream_data = stream
        .data_events
        .first()
        .copied()
        .expect("stream path should produce at least one data chunk");

    assert!(
        first_stream_data < slow_done,
        "stream data should arrive while slow stream is still pending (first_stream_data={first_stream_data:?}, slow_done={slow_done:?})"
    );
}

#[test]
fn response_body_is_streamed_progressively() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/stream"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("stream request failed");

    let stream = observation_for(&observations, "/stream");
    assert_eq!(stream.status.as_deref(), Some("200"));

    let body = String::from_utf8_lossy(&stream.body);
    assert_eq!(body, "chunk-1chunk-2chunk-3");
    assert!(
        stream.data_events.len() >= 2,
        "expected at least two data events, got {}",
        stream.data_events.len()
    );
    let first = stream.data_events.first().copied().unwrap();
    let last = stream.data_events.last().copied().unwrap();
    let span = last.saturating_sub(first);
    assert!(
        span >= Duration::from_millis(200),
        "expected delayed progressive delivery across chunks, got span {span:?}"
    );
}

#[test]
fn long_stream_survives_body_total_timeout_after_progress() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.backend_body_total_timeout_ms = 250;
    config.performance.backend_body_idle_timeout_ms = 500;
    config.performance.backend_total_request_timeout_ms = 5_000;
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/long-stream"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 6),
    )
    .expect("long stream request failed");
    let stream = observation_for(&observations, "/long-stream");
    assert_eq!(stream.status.as_deref(), Some("200"));
    assert_eq!(
        String::from_utf8_lossy(&stream.body),
        "part-1part-2part-3part-4"
    );
}

#[test]
fn error_status_mapping_parity_is_preserved() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let config = make_config(0, backend_addr.to_string(), cert.clone(), key.clone());
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let status_500 = run_h3_client_concurrent_get(
        listen_addr,
        &["/status500"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("status500 request failed");
    let status_500_obs = observation_for(&status_500, "/status500");
    assert_eq!(status_500_obs.status.as_deref(), Some("500"));

    let timeout = run_h3_client_concurrent_get(
        listen_addr,
        &["/timeout"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 6),
    )
    .expect("timeout request failed");
    let timeout_obs = observation_for(&timeout, "/timeout");
    assert_eq!(timeout_obs.status.as_deref(), Some("503"));
    assert!(
        String::from_utf8_lossy(&timeout_obs.body).contains("upstream timeout"),
        "timeout body should indicate upstream timeout"
    );

    // Separate listener with unreachable backend to validate transport mapping.
    let transport_config = make_config(0, "127.0.0.1:1".to_string(), cert, key);
    let transport_listener =
        QUICListener::new(transport_config).expect("failed to create transport listener");
    let transport_addr = transport_listener.socket.local_addr().unwrap();
    let _transport_task = ListenerTaskGuard::spawn(&rt, transport_listener);

    let transport = run_h3_client_concurrent_get(
        transport_addr,
        &["/"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("transport error request failed");
    let transport_obs = observation_for(&transport, "/");
    assert_eq!(transport_obs.status.as_deref(), Some("502"));
    assert!(
        String::from_utf8_lossy(&transport_obs.body).contains("upstream error"),
        "transport error body should indicate upstream error"
    );
}

#[test]
#[serial_test::serial]
fn metrics_endpoint_exposes_route_slo_metrics() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let metrics_port = find_free_tcp_port();
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.observability.metrics.enabled = true;
    config.observability.metrics.address = "127.0.0.1".to_string();
    config.observability.metrics.port = metrics_port;
    config.observability.metrics.path = "/metrics".to_string();

    let _enter = rt.enter();
    let listener = QUICListener::new(config).expect("failed to create listener");
    drop(_enter);
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = rt
        .block_on(async move {
            tokio::task::spawn_blocking(move || {
                run_h3_client_concurrent_get(
                    listen_addr,
                    &["/fast", "/slow"],
                    Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
                )
            })
            .await
            .expect("join concurrent client task")
        })
        .expect("requests should succeed");
    assert_eq!(observations.len(), 2);

    let metrics = rt
        .block_on(scrape_metrics(
            metrics_port,
            "/metrics",
            Duration::from_secs(20),
        ))
        .expect("metrics endpoint should become reachable");
    assert!(
        metrics.contains("spooky_requests_total"),
        "unexpected metrics payload: {metrics}"
    );
    assert!(metrics.contains("spooky_route_requests_total{route=\"test_pool\"}"));
    assert!(metrics.contains("spooky_route_latency_ms_p50{route=\"test_pool\"}"));
    assert!(metrics.contains("spooky_route_latency_ms_p95{route=\"test_pool\"}"));
    assert!(metrics.contains("spooky_route_latency_ms_p99{route=\"test_pool\"}"));
    assert!(metrics.contains("spooky_overload_shed_by_reason_total"));
    assert!(metrics.contains("spooky_active_connections"));
    assert!(metrics.contains("spooky_connection_cap_rejects"));
    assert!(metrics.contains("spooky_hedge_triggered_total"));
}

#[test]
fn global_inflight_limit_sheds_excess_requests() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.global_inflight_limit = 1;
    config.performance.per_upstream_inflight_limit = 64;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/slow", "/slow"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("concurrent requests should complete");

    let mut status_200 = 0usize;
    let mut status_503 = 0usize;
    let mut shed_body = String::new();
    for obs in &observations {
        match obs.status.as_deref() {
            Some("200") => status_200 += 1,
            Some("503") => {
                status_503 += 1;
                shed_body = String::from_utf8_lossy(&obs.body).to_string();
            }
            other => panic!("unexpected status: {:?}", other),
        }
    }

    assert_eq!(status_200, 1, "expected one successful request");
    assert_eq!(status_503, 1, "expected one shed request");
    assert!(
        shed_body.contains("overloaded"),
        "shed body should mention overload, got: {shed_body}"
    );
}

#[test]
fn route_queue_global_cap_sheds_excess_requests() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.global_inflight_limit = 64;
    config.performance.per_upstream_inflight_limit = 64;
    config.resilience.route_queue.default_cap = 64;
    config.resilience.route_queue.global_cap = 1;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/slow", "/slow"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("concurrent requests should complete");

    let mut status_200 = 0usize;
    let mut status_503 = 0usize;
    let mut shed_body = String::new();
    for obs in &observations {
        match obs.status.as_deref() {
            Some("200") => status_200 += 1,
            Some("503") => {
                status_503 += 1;
                shed_body = String::from_utf8_lossy(&obs.body).to_string();
            }
            other => panic!("unexpected status: {:?}", other),
        }
    }

    assert_eq!(status_200, 1, "expected one successful request");
    assert_eq!(status_503, 1, "expected one shed request");
    assert!(
        shed_body.contains("global queue cap exceeded"),
        "shed body should mention global queue cap, got: {shed_body}"
    );
}

#[test]
fn protocol_policy_denies_disallowed_method() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.resilience.protocol.allowed_methods = vec!["GET".to_string()];

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let (status, body, _got_reset) = run_h3_client_chunked_post(
        listen_addr,
        "/fast",
        0,
        1,
        Duration::ZERO,
        Duration::ZERO,
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("method policy request should complete");
    assert_eq!(status, "405");
    assert!(
        String::from_utf8_lossy(&body).contains("method blocked"),
        "expected policy body in response"
    );
}

#[test]
fn protocol_policy_denies_blocked_path_prefix() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.resilience.protocol.denied_path_prefixes = vec!["/admin".to_string()];

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/admin/secrets"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("path policy request should complete");
    let denied = observation_for(&observations, "/admin/secrets");
    assert_eq!(denied.status.as_deref(), Some("403"));
    assert!(
        String::from_utf8_lossy(&denied.body).contains("path blocked"),
        "expected policy body in response"
    );
}

#[test]
fn upstream_inflight_limit_sheds_excess_requests() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.global_inflight_limit = 64;
    config.performance.per_upstream_inflight_limit = 1;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/slow", "/slow"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("concurrent requests should complete");

    let mut status_200 = 0usize;
    let mut status_503 = 0usize;
    let mut shed_body = String::new();
    for obs in &observations {
        match obs.status.as_deref() {
            Some("200") => status_200 += 1,
            Some("503") => {
                status_503 += 1;
                shed_body = String::from_utf8_lossy(&obs.body).to_string();
            }
            other => panic!("unexpected status: {:?}", other),
        }
    }

    assert_eq!(status_200, 1, "expected one successful request");
    assert_eq!(status_503, 1, "expected one shed request");
    assert!(
        shed_body.contains("upstream overloaded"),
        "shed body should mention upstream overload, got: {shed_body}"
    );
}

#[test]
fn backend_pool_inflight_limit_sheds_excess_requests() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.global_inflight_limit = 64;
    config.performance.per_upstream_inflight_limit = 64;
    config.performance.per_backend_inflight_limit = 1;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/slow", "/slow"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("concurrent requests should complete");

    let mut status_200 = 0usize;
    let mut status_503 = 0usize;
    let mut shed_body = String::new();
    for obs in &observations {
        match obs.status.as_deref() {
            Some("200") => status_200 += 1,
            Some("503") => {
                status_503 += 1;
                shed_body = String::from_utf8_lossy(&obs.body).to_string();
            }
            other => panic!("unexpected status: {:?}", other),
        }
    }

    assert_eq!(status_200, 1, "expected one successful request");
    assert_eq!(status_503, 1, "expected one shed request");
    assert!(
        shed_body.contains("backend overloaded"),
        "shed body should mention backend overload, got: {shed_body}"
    );
}

#[test]
fn backend_timeout_respects_configured_performance_value() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.backend_timeout_ms = 150;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let start = Instant::now();
    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/timeout"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("timeout request should complete");
    let elapsed = start.elapsed();

    let timeout_obs = observation_for(&observations, "/timeout");
    assert_eq!(timeout_obs.status.as_deref(), Some("503"));
    assert!(
        String::from_utf8_lossy(&timeout_obs.body).contains("upstream timeout"),
        "timeout body should indicate upstream timeout"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "configured backend timeout should fail fast, observed elapsed={elapsed:?}"
    );
}

#[test]
#[serial_test::serial]
fn chaos_high_jitter_remains_responsive() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_chaos_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.backend_timeout_ms = 1_500;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let paths: Vec<&str> = vec!["/jitter"; 24];
    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &paths,
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 6),
    )
    .expect("jitter workload should complete");

    let success = observations
        .iter()
        .filter(|obs| obs.status.as_deref() == Some("200"))
        .count();
    assert_eq!(
        success,
        observations.len(),
        "all jitter requests should succeed"
    );
}

#[test]
#[serial_test::serial]
fn chaos_packet_loss_like_timeout_maps_to_recoverable_response() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_chaos_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.backend_timeout_ms = 120;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &["/fast", "/loss", "/fast", "/loss"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 6),
    )
    .expect("loss-like workload should complete");

    let fast_ok = observations
        .iter()
        .filter(|obs| obs.path == "/fast" && obs.status.as_deref() == Some("200"))
        .count();
    let loss_timeout = observations
        .iter()
        .filter(|obs| obs.path == "/loss" && obs.status.as_deref() == Some("503"))
        .count();
    assert_eq!(fast_ok, 2, "fast paths must remain healthy");
    assert_eq!(
        loss_timeout, 2,
        "loss paths must map to recoverable timeout response"
    );
}

#[test]
#[serial_test::serial]
fn chaos_backend_flapping_is_stable_under_concurrency() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_chaos_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.backend_timeout_ms = 500;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let paths: Vec<&str> = vec!["/flap"; 20];
    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &paths,
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 6),
    )
    .expect("flapping workload should complete");

    let success = observations
        .iter()
        .filter(|obs| obs.status.as_deref() == Some("200"))
        .count();
    let service_unavailable = observations
        .iter()
        .filter(|obs| obs.status.as_deref() == Some("503"))
        .count();
    assert!(
        success > 0,
        "flapping workload should still serve successes"
    );
    assert!(
        service_unavailable > 0,
        "flapping workload should surface recoverable failures without collapse"
    );
}

#[test]
#[serial_test::serial]
fn chaos_partial_outage_preserves_some_availability() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let healthy_backend = rt.block_on(start_h2_backend_with_chaos_routes());
    let backends = vec![
        Backend {
            id: "dead-backend".to_string(),
            address: "127.0.0.1:1".to_string(),
            weight: 1,
            health_check: HealthCheck {
                path: "/health".to_string(),
                interval: 1000,
                timeout_ms: 100,
                failure_threshold: 1,
                success_threshold: 1,
                cooldown_ms: 0,
            },
        },
        Backend {
            id: "healthy-backend".to_string(),
            address: healthy_backend.to_string(),
            weight: 1,
            health_check: HealthCheck {
                path: "/health".to_string(),
                interval: 1000,
                timeout_ms: 1000,
                failure_threshold: 1,
                success_threshold: 1,
                cooldown_ms: 0,
            },
        },
    ];
    let mut config = make_config_with_backends(0, backends, "round-robin", cert, key);
    config.performance.backend_timeout_ms = 250;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let paths: Vec<&str> = vec!["/fast"; 20];
    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &paths,
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 6),
    )
    .expect("partial outage workload should complete");

    let success = observations
        .iter()
        .filter(|obs| obs.status.as_deref() == Some("200"))
        .count();
    let failures = observations
        .iter()
        .filter(|obs| obs.status.as_deref() != Some("200"))
        .count();
    assert!(success >= 5, "healthy backend should preserve availability");
    assert!(
        failures > 0,
        "partial outage should still surface some failures"
    );
}

/// When more than MAX_STREAMS_PER_CONNECTION concurrent streams are opened on a
/// single connection the proxy must reject the excess stream with 503.
#[test]
fn stream_cap_rejects_excess_concurrent_streams() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    // Slow backend so all N+1 streams are in-flight simultaneously.
    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    // Raise the QUIC transport stream limit above the app-level cap so the N+1th
    // stream reaches the application guard (rather than being dropped at the
    // QUIC layer with StreamLimit).
    config.performance.quic_initial_max_streams_bidi = (MAX_STREAMS_PER_CONNECTION as u64) + 10;
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    // Send MAX_STREAMS_PER_CONNECTION + 1 slow requests concurrently.
    // The first N must all succeed (200); the N+1th must be shed (503).
    let n = MAX_STREAMS_PER_CONNECTION + 1;
    let paths: Vec<&str> = vec!["/slow"; n];
    let observations = run_h3_client_concurrent_get(
        listen_addr,
        &paths,
        // slow path takes ~700 ms; give plenty of room
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 10),
    )
    .expect("concurrent requests should complete");

    let count_503 = observations
        .iter()
        .filter(|o| o.status.as_deref() == Some("503"))
        .count();
    let count_200 = observations
        .iter()
        .filter(|o| o.status.as_deref() == Some("200"))
        .count();

    assert!(
        count_503 >= 1,
        "expected at least one 503 when stream cap is exceeded, got statuses: {:?}",
        observations.iter().map(|o| &o.status).collect::<Vec<_>>()
    );
    assert!(
        count_200 >= 1,
        "expected at least one successful stream through the cap, got statuses: {:?}",
        observations.iter().map(|o| &o.status).collect::<Vec<_>>()
    );
    assert_eq!(
        count_200 + count_503,
        n,
        "every stream must get a terminal response"
    );
}

/// When the upstream response advertises a `content-length` above
/// `max_response_body_bytes`, the proxy must fail fast with 503 before sending
/// any upstream 200 headers/body to the client.
#[test]
fn response_body_cap_returns_503_on_declared_length_breach() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    // Backend that serves exactly 8 KiB.
    let large_body = Bytes::from(vec![b'x'; 8 * 1024]);
    let listener_tcp = rt.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let backend_addr = listener_tcp.local_addr().unwrap();
    rt.spawn(async move {
        loop {
            let (stream, _) = match listener_tcp.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let body_clone = large_body.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |_req: Request<Incoming>| {
                    let body = body_clone.clone();
                    async move {
                        let mut response = Response::new(Full::new(body.clone()).boxed());
                        response.headers_mut().insert(
                            CONTENT_LENGTH,
                            HeaderValue::from_str(&body.len().to_string())
                                .expect("valid content-length"),
                        );
                        Ok::<_, hyper::Error>(response)
                    }
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });

    // Cap at 1 KiB — far below the 8 KiB the backend will send.
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.max_response_body_bytes = 1024;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    // Bespoke client loop: expect terminal 503 without reset.
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let local_addr = socket.local_addr().unwrap();

    let mut qconfig = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    qconfig.verify_peer(false);
    qconfig
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();
    qconfig.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    qconfig.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    qconfig.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    qconfig.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    qconfig.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);
    let mut conn = quiche::connect(
        Some("localhost"),
        &scid,
        local_addr,
        listen_addr,
        &mut qconfig,
    )
    .unwrap();
    let h3_config = quiche::h3::Config::new().unwrap();

    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    let (w, si) = conn.send(&mut out).unwrap();
    socket.send_to(&out[..w], si.to).unwrap();

    let start = Instant::now();
    let mut h3: Option<quiche::h3::Connection> = None;
    let mut req_sent = false;
    let mut got_reset = false;
    let mut status = String::new();
    let mut response_body = Vec::new();

    'outer: loop {
        loop {
            match conn.send(&mut out) {
                Ok((w, si)) => {
                    let _ = socket.send_to(&out[..w], si.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => panic!("send: {e:?}"),
            }
        }

        let timeout = quic_read_timeout(&conn);
        socket.set_read_timeout(Some(timeout)).unwrap();

        match socket.recv_from(&mut buf) {
            Ok((len, from)) => {
                conn.recv(
                    &mut buf[..len],
                    quiche::RecvInfo {
                        from,
                        to: local_addr,
                    },
                )
                .unwrap();
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                conn.on_timeout();
            }
            Err(e) => panic!("recv: {e:?}"),
        }

        if conn.is_established() && h3.is_none() {
            h3 = Some(quiche::h3::Connection::with_transport(&mut conn, &h3_config).unwrap());
        }

        if let Some(h3c) = h3.as_mut() {
            if !req_sent && conn.is_established() {
                let req = vec![
                    quiche::h3::Header::new(b":method", b"GET"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", b"localhost"),
                    quiche::h3::Header::new(b":path", b"/"),
                    quiche::h3::Header::new(b"user-agent", b"spooky-cap-test"),
                ];
                h3c.send_request(&mut conn, &req, true).unwrap();
                req_sent = true;
            }

            loop {
                match h3c.poll(&mut conn) {
                    Ok((_sid, quiche::h3::Event::Headers { list, .. })) => {
                        for h in &list {
                            if h.name() == b":status" {
                                status = String::from_utf8_lossy(h.value()).to_string();
                            }
                        }
                    }
                    Ok((sid, quiche::h3::Event::Data)) => loop {
                        match h3c.recv_body(&mut conn, sid, &mut buf) {
                            Ok(read) => response_body.extend_from_slice(&buf[..read]),
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => panic!("recv_body: {e:?}"),
                        }
                    },
                    Ok((_sid, quiche::h3::Event::Finished)) => {
                        break 'outer;
                    }
                    Ok((_sid, quiche::h3::Event::Reset(_))) => {
                        got_reset = true;
                        break 'outer;
                    }
                    Ok(_) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => panic!("poll: {e:?}"),
                }
            }
        }

        if start.elapsed() > Duration::from_secs(REQUEST_TIMEOUT_SECS + 4) {
            panic!("timeout waiting for 503 response");
        }
    }

    assert_eq!(status, "503", "expected 503 for response cap breach");
    assert_eq!(
        String::from_utf8_lossy(&response_body),
        "upstream response body too large\n"
    );
    assert!(
        !got_reset,
        "response cap breach should terminate with HTTP error, not stream reset"
    );
}

/// Unknown-length upstream bodies are validated against the cap before the
/// proxy emits downstream headers; breaches must terminate as 503 (no reset).
#[test]
fn response_body_cap_returns_503_on_unknown_length_breach() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let listener_tcp = rt.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let backend_addr = listener_tcp.local_addr().unwrap();
    rt.spawn(async move {
        loop {
            let (stream, _) = match listener_tcp.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |_req: Request<Incoming>| async move {
                    let (tx, body) = DelayedChunkBody::channel(8);
                    tokio::spawn(async move {
                        let chunk = Bytes::from(vec![b'x'; 1024]);
                        for _ in 0..8 {
                            let _ = tx.send(chunk.clone()).await;
                        }
                    });
                    Ok::<_, hyper::Error>(Response::new(body.boxed()))
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });

    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    config.performance.max_response_body_bytes = 1024;

    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let local_addr = socket.local_addr().unwrap();

    let mut qconfig = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    qconfig.verify_peer(false);
    qconfig
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();
    qconfig.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    qconfig.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    qconfig.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
    qconfig.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    qconfig.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    qconfig.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);
    let mut conn = quiche::connect(
        Some("localhost"),
        &scid,
        local_addr,
        listen_addr,
        &mut qconfig,
    )
    .unwrap();
    let h3_config = quiche::h3::Config::new().unwrap();

    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    let (w, si) = conn.send(&mut out).unwrap();
    socket.send_to(&out[..w], si.to).unwrap();

    let start = Instant::now();
    let mut h3: Option<quiche::h3::Connection> = None;
    let mut req_sent = false;
    let mut got_reset = false;
    let mut status = String::new();
    let mut response_body = Vec::new();

    'outer: loop {
        loop {
            match conn.send(&mut out) {
                Ok((w, si)) => {
                    let _ = socket.send_to(&out[..w], si.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => panic!("send: {e:?}"),
            }
        }

        let timeout = quic_read_timeout(&conn);
        socket.set_read_timeout(Some(timeout)).unwrap();

        match socket.recv_from(&mut buf) {
            Ok((len, from)) => {
                conn.recv(
                    &mut buf[..len],
                    quiche::RecvInfo {
                        from,
                        to: local_addr,
                    },
                )
                .unwrap();
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                conn.on_timeout();
            }
            Err(e) => panic!("recv: {e:?}"),
        }

        if conn.is_established() && h3.is_none() {
            h3 = Some(quiche::h3::Connection::with_transport(&mut conn, &h3_config).unwrap());
        }

        if let Some(h3c) = h3.as_mut() {
            if !req_sent && conn.is_established() {
                let req = vec![
                    quiche::h3::Header::new(b":method", b"GET"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", b"localhost"),
                    quiche::h3::Header::new(b":path", b"/"),
                    quiche::h3::Header::new(b"user-agent", b"spooky-cap-test"),
                ];
                h3c.send_request(&mut conn, &req, true).unwrap();
                req_sent = true;
            }

            loop {
                match h3c.poll(&mut conn) {
                    Ok((_sid, quiche::h3::Event::Headers { list, .. })) => {
                        for h in &list {
                            if h.name() == b":status" {
                                status = String::from_utf8_lossy(h.value()).to_string();
                            }
                        }
                    }
                    Ok((sid, quiche::h3::Event::Data)) => loop {
                        match h3c.recv_body(&mut conn, sid, &mut buf) {
                            Ok(read) => response_body.extend_from_slice(&buf[..read]),
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => panic!("recv_body: {e:?}"),
                        }
                    },
                    Ok((_sid, quiche::h3::Event::Finished)) => break 'outer,
                    Ok((_sid, quiche::h3::Event::Reset(_))) => {
                        got_reset = true;
                        break 'outer;
                    }
                    Ok(_) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => panic!("poll: {e:?}"),
                }
            }
        }

        if start.elapsed() > Duration::from_secs(REQUEST_TIMEOUT_SECS + 4) {
            panic!("timeout waiting for 503 response");
        }
    }

    assert_eq!(status, "503", "expected 503 for unknown-length cap breach");
    assert_eq!(
        String::from_utf8_lossy(&response_body),
        "upstream response body too large\n"
    );
    assert!(
        !got_reset,
        "unknown-length cap breach should terminate with HTTP error, not stream reset"
    );
}

// ── Teardown path integration tests (4.2) ───────────────────────────────────
//
// These tests exercise the three wire-level teardown paths at the real QUIC
// protocol layer.  Each test sends a real QUIC+H3 request, triggers a reset
// or error mid-stream, then verifies the server recovers cleanly by serving a
// subsequent request on the same connection without any stuck-stream symptoms.

/// Helper: build a minimal quiche QUIC client connected to `addr`.
/// Returns (socket, quic conn, h3 conn).  The h3 layer is negotiated inside.
fn make_quic_client(
    addr: SocketAddr,
) -> Result<
    (
        std::net::UdpSocket,
        std::net::SocketAddr,
        quiche::Connection,
        quiche::h3::Connection,
    ),
    String,
> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    let local_addr = socket.local_addr().map_err(|e| e.to_string())?;

    let mut qconfig =
        quiche::Config::new(quiche::PROTOCOL_VERSION).map_err(|e| format!("config: {e:?}"))?;
    qconfig.verify_peer(false);
    qconfig
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .map_err(|e| format!("alpn: {e:?}"))?;
    qconfig.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    qconfig.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    qconfig.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    let stream_win = QUIC_INITIAL_STREAM_DATA.saturating_add(128 * 1024);
    qconfig.set_initial_max_stream_data_bidi_local(stream_win);
    qconfig.set_initial_max_stream_data_bidi_remote(stream_win);
    qconfig.set_initial_max_stream_data_uni(stream_win);
    qconfig.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    qconfig.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    qconfig.set_disable_active_migration(true);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);
    let mut conn = quiche::connect(Some("localhost"), &scid, local_addr, addr, &mut qconfig)
        .map_err(|e| format!("connect: {e:?}"))?;

    let h3_config = quiche::h3::Config::new().map_err(|e| format!("h3: {e:?}"))?;
    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    // Initial packet.
    let (w, si) = conn
        .send(&mut out)
        .map_err(|e| format!("initial send: {e:?}"))?;
    socket
        .send_to(&out[..w], si.to)
        .map_err(|e| e.to_string())?;

    let deadline = Instant::now() + Duration::from_secs(REQUEST_TIMEOUT_SECS);
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
        socket.set_read_timeout(Some(quic_read_timeout(&conn))).ok();
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
            Err(e) => return Err(e.to_string()),
        }
        if Instant::now() > deadline {
            return Err("handshake timeout".into());
        }
    }

    let h3 = quiche::h3::Connection::with_transport(&mut conn, &h3_config)
        .map_err(|e| format!("h3 layer: {e:?}"))?;
    Ok((socket, local_addr, conn, h3))
}

/// Flush QUIC send buffer to the socket.
fn flush_quic(conn: &mut quiche::Connection, socket: &std::net::UdpSocket, out: &mut [u8]) {
    loop {
        match conn.send(out) {
            Ok((w, si)) => {
                let _ = socket.send_to(&out[..w], si.to);
            }
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }
}

struct PumpIo<'a> {
    socket: &'a std::net::UdpSocket,
    local_addr: std::net::SocketAddr,
    out: &'a mut [u8],
    buf: &'a mut [u8],
}

/// Pump the QUIC event loop until `target_sid` finishes/resets OR `deadline`
/// passes.  Collects H3 status/finished/reset events.  Returns true if done.
fn pump_h3_until(
    conn: &mut quiche::Connection,
    h3: &mut quiche::h3::Connection,
    io: &mut PumpIo<'_>,
    target_sid: u64,
    deadline: Instant,
    events: &mut Vec<String>,
) -> bool {
    while Instant::now() < deadline {
        flush_quic(conn, io.socket, io.out);
        io.socket
            .set_read_timeout(Some(quic_read_timeout(conn)))
            .ok();
        match io.socket.recv_from(io.buf) {
            Ok((len, from)) => {
                let _ = conn.recv(
                    &mut io.buf[..len],
                    quiche::RecvInfo {
                        from,
                        to: io.local_addr,
                    },
                );
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                conn.on_timeout();
            }
            Err(_) => {}
        }
        loop {
            match h3.poll(conn) {
                Ok((sid, quiche::h3::Event::Headers { list, .. })) if sid == target_sid => {
                    for h in &list {
                        if h.name() == b":status" {
                            events.push(format!("status:{}", String::from_utf8_lossy(h.value())));
                        }
                    }
                }
                Ok((sid, quiche::h3::Event::Data)) if sid == target_sid => {
                    // consume body without storing
                    let mut tmp = [0u8; 4096];
                    while h3.recv_body(conn, sid, &mut tmp).is_ok() {}
                    events.push("data".into());
                }
                Ok((sid, quiche::h3::Event::Finished)) if sid == target_sid => {
                    events.push("finished".into());
                    return true;
                }
                Ok((sid, quiche::h3::Event::Reset(_))) if sid == target_sid => {
                    events.push("reset".into());
                    return true;
                }
                Ok(_) => {}
                Err(quiche::h3::Error::Done) => break,
                Err(_) => break,
            }
        }
    }
    false
}

/// Teardown path A: client sends RESET_STREAM before the upstream has responded.
///
/// Uses `global_inflight_limit = 1` so the inflight permit is the only one
/// available.  If `abort_stream` does not release it on reset, the follow-up
/// `/fast` request on a fresh connection is rejected with 503 (inflight full)
/// instead of 200. Passing means the wire-level reset propagated and resources
/// were freed.
#[test]
fn teardown_client_reset_before_upstream_response() {
    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    // Single inflight slot: a leaked permit means the follow-up is rejected.
    config.performance.global_inflight_limit = 1;

    let listener = QUICListener::new(config).expect("listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _guard = ListenerTaskGuard::spawn(&rt, listener);

    let (socket, local_addr, mut conn, mut h3) =
        make_quic_client(listen_addr).expect("quic client");
    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    // POST to /slow with fin=false so the write side stays open.
    // Then immediately call stream_shutdown(Write) to send RESET_STREAM.
    // The server is in ReceivingRequest at reset time (before it forwards to
    // upstream), which exercises the AwaitingUpstream/ReceivingRequest teardown path.
    let slow_headers = vec![
        quiche::h3::Header::new(b":method", b"POST"),
        quiche::h3::Header::new(b":scheme", b"https"),
        quiche::h3::Header::new(b":authority", b"localhost"),
        quiche::h3::Header::new(b":path", b"/slow"),
        quiche::h3::Header::new(b"content-length", b"100"),
    ];
    let slow_sid = h3
        .send_request(&mut conn, &slow_headers, false) // fin=false: write side stays open
        .expect("send_request /slow");

    // Flush so headers reach the server, then shut down the write side.
    // This sends RESET_STREAM while the server is still in ReceivingRequest.
    flush_quic(&mut conn, &socket, &mut out);
    conn.stream_shutdown(slow_sid, quiche::Shutdown::Write, 0)
        .expect("stream_shutdown");
    flush_quic(&mut conn, &socket, &mut out);

    // Pump briefly so the listener processes the reset and calls abort_stream.
    let pump_end = Instant::now() + Duration::from_millis(200);
    let mut discard = Vec::new();
    let mut io = PumpIo {
        socket: &socket,
        local_addr,
        out: &mut out,
        buf: &mut buf,
    };
    pump_h3_until(
        &mut conn,
        &mut h3,
        &mut io,
        slow_sid,
        pump_end,
        &mut discard,
    );

    // Follow-up /fast on a fresh connection. This avoids coupling the assertion
    // to the original stream state machine on the same connection while still
    // proving the inflight permit was released.
    let followup = run_h3_client_concurrent_get(
        listen_addr,
        &["/fast"],
        Duration::from_secs(REQUEST_TIMEOUT_SECS + 4),
    )
    .expect("follow-up /fast request failed");
    let fast = observation_for(&followup, "/fast");
    assert!(
        fast.status.as_deref() == Some("200"),
        "expected 200 from /fast; 503/timeout means the inflight permit was not released on reset. \
         observed status={:?}",
        fast.status
    );
}

/// Teardown path B: connection dropped while the server is streaming the
/// response body (SendingResponse phase).
///
/// Uses `global_inflight_limit = 1` and `/long-stream` (4×220ms chunks).
/// The first QUIC connection receives response headers (server now in
/// SendingResponse) then drops abruptly — simulating the client disappearing
/// mid-stream.  The server must release the inflight permit via the timeout/
/// connection-close path so a *new* connection can immediately serve a request.
/// If the permit leaks, the second connection's /fast request gets 503.
#[test]
fn teardown_client_reset_during_response_streaming() {
    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    // Single inflight slot: a leaked permit means the follow-up is rejected.
    config.performance.global_inflight_limit = 1;
    // Short QUIC idle timeout so the server tears down the abandoned stream fast.
    config.performance.quic_max_idle_timeout_ms = 300;

    let listener = QUICListener::new(config).expect("listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _guard = ListenerTaskGuard::spawn(&rt, listener);

    // ── First connection: GET /stream, receive first body chunk, then drop ────
    {
        let (socket, local_addr, mut conn, mut h3) =
            make_quic_client(listen_addr).expect("quic client");
        let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
        let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

        let headers = vec![
            quiche::h3::Header::new(b":method", b"GET"),
            quiche::h3::Header::new(b":scheme", b"https"),
            quiche::h3::Header::new(b":authority", b"localhost"),
            quiche::h3::Header::new(b":path", b"/stream"),
        ];
        let stream_sid = h3
            .send_request(&mut conn, &headers, true)
            .expect("send_request /stream");
        flush_quic(&mut conn, &socket, &mut out);

        // Wait for first response data chunk — this guarantees the server is in
        // SendingResponse and actively streaming.
        let data_deadline = Instant::now() + Duration::from_secs(REQUEST_TIMEOUT_SECS);
        'wait: loop {
            flush_quic(&mut conn, &socket, &mut out);
            socket.set_read_timeout(Some(quic_read_timeout(&conn))).ok();
            match socket.recv_from(&mut buf) {
                Ok((len, from)) => {
                    let _ = conn.recv(
                        &mut buf[..len],
                        quiche::RecvInfo {
                            from,
                            to: local_addr,
                        },
                    );
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    conn.on_timeout();
                }
                Err(_) => {}
            }
            loop {
                match h3.poll(&mut conn) {
                    Ok((sid, quiche::h3::Event::Data)) if sid == stream_sid => {
                        let mut tmp = [0u8; 4096];
                        while h3.recv_body(&mut conn, sid, &mut tmp).is_ok() {}
                        break 'wait;
                    }
                    Ok((sid, quiche::h3::Event::Finished)) if sid == stream_sid => {
                        panic!("stream finished before teardown point");
                    }
                    Ok(_) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(_) => break,
                }
            }
            if Instant::now() > data_deadline {
                panic!("timed out waiting for /stream response data");
            }
        }
        // Connection drops here (socket + conn out of scope) — server is left
        // in SendingResponse with an orphaned body-pump task.
    }

    // Wait for the server to detect the idle timeout and tear down the stream.
    // quic_max_idle_timeout_ms=300ms + some margin for the poll loop.
    std::thread::sleep(Duration::from_millis(600));

    // ── Second connection: /fast must succeed ──────────────────────────────
    // If the inflight permit was not released, this request gets 503.
    let (socket2, local_addr2, mut conn2, mut h3_2) =
        make_quic_client(listen_addr).expect("second quic client");
    let mut out2 = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf2 = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    let fast_headers = vec![
        quiche::h3::Header::new(b":method", b"GET"),
        quiche::h3::Header::new(b":scheme", b"https"),
        quiche::h3::Header::new(b":authority", b"localhost"),
        quiche::h3::Header::new(b":path", b"/fast"),
    ];
    let fast_sid = h3_2
        .send_request(&mut conn2, &fast_headers, true)
        .expect("send_request /fast on second connection");

    let mut events = Vec::new();
    let mut io = PumpIo {
        socket: &socket2,
        local_addr: local_addr2,
        out: &mut out2,
        buf: &mut buf2,
    };
    let done = pump_h3_until(
        &mut conn2,
        &mut h3_2,
        &mut io,
        fast_sid,
        Instant::now() + Duration::from_secs(REQUEST_TIMEOUT_SECS + 2),
        &mut events,
    );

    assert!(
        done,
        "second-connection /fast must complete; timeout means inflight permit leaked \
         from the abandoned SendingResponse stream"
    );
    assert!(
        events.iter().any(|e| e.starts_with("status:200")),
        "expected 200; 503 means the global inflight permit was not released. \
         events={events:?}"
    );
}

/// Teardown path C: upstream connection times out.
///
/// The client requests `/timeout` which sleeps longer than the configured
/// backend timeout.  The server must send a 503 and clean up all stream
/// resources.  A subsequent /fast request on the same connection must succeed.
#[test]
fn teardown_upstream_timeout_cleans_up_stream() {
    let dir = tempdir().expect("tempdir");
    let (cert, key) = write_test_certs(&dir);
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let backend_addr = rt.block_on(start_h2_backend_with_regression_routes());
    let mut config = make_config(0, backend_addr.to_string(), cert, key);
    // Use a short timeout so the test doesn't wait long.
    config.performance.backend_timeout_ms = 200;

    let listener = QUICListener::new(config).expect("listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _guard = ListenerTaskGuard::spawn(&rt, listener);

    let (socket, local_addr, mut conn, mut h3) =
        make_quic_client(listen_addr).expect("quic client");
    let mut out = [0u8; MAX_UDP_PAYLOAD_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    // Step 1: send /timeout request and wait for the 503 error response.
    let timeout_headers = vec![
        quiche::h3::Header::new(b":method", b"GET"),
        quiche::h3::Header::new(b":scheme", b"https"),
        quiche::h3::Header::new(b":authority", b"localhost"),
        quiche::h3::Header::new(b":path", b"/timeout"),
    ];
    let timeout_sid = h3
        .send_request(&mut conn, &timeout_headers, true)
        .expect("send_request /timeout");

    let mut timeout_events = Vec::new();
    // backend_timeout_ms=200ms + some margin for the listener to respond
    let mut io = PumpIo {
        socket: &socket,
        local_addr,
        out: &mut out,
        buf: &mut buf,
    };
    let done = pump_h3_until(
        &mut conn,
        &mut h3,
        &mut io,
        timeout_sid,
        Instant::now() + Duration::from_secs(5),
        &mut timeout_events,
    );
    assert!(done, "timeout stream must finish (with 503)");
    assert!(
        timeout_events.iter().any(|e| e.starts_with("status:503")),
        "timeout must produce 503, got: {timeout_events:?}"
    );

    // Step 2: follow-up /fast on the same connection — proves the timeout
    // teardown released all resources and the connection is still alive.
    let fast_headers = vec![
        quiche::h3::Header::new(b":method", b"GET"),
        quiche::h3::Header::new(b":scheme", b"https"),
        quiche::h3::Header::new(b":authority", b"localhost"),
        quiche::h3::Header::new(b":path", b"/fast"),
    ];
    let fast_sid = h3
        .send_request(&mut conn, &fast_headers, true)
        .expect("send_request /fast after timeout");

    let mut fast_events = Vec::new();
    let mut io = PumpIo {
        socket: &socket,
        local_addr,
        out: &mut out,
        buf: &mut buf,
    };
    let done = pump_h3_until(
        &mut conn,
        &mut h3,
        &mut io,
        fast_sid,
        Instant::now() + Duration::from_secs(REQUEST_TIMEOUT_SECS + 2),
        &mut fast_events,
    );
    assert!(
        done,
        "follow-up /fast request must complete after timeout teardown"
    );
    assert!(
        fast_events.iter().any(|e| e.starts_with("status:200")),
        "follow-up request must receive 200, got: {fast_events:?}"
    );
}
