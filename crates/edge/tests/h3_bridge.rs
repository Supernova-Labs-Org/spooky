use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use bytes::Bytes;
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

use spooky_config::config::{Backend, Config, HealthCheck, Listen, LoadBalancing, Log, Tls};
use spooky_edge::QUICListener;
use spooky_edge::constants::{
    BACKEND_TIMEOUT_SECS, MAX_DATAGRAM_SIZE_BYTES, MAX_REQUEST_BODY_BYTES, MAX_UDP_PAYLOAD_BYTES,
    QUIC_IDLE_TIMEOUT_MS, QUIC_INITIAL_MAX_DATA, QUIC_INITIAL_MAX_STREAMS_BIDI,
    QUIC_INITIAL_MAX_STREAMS_UNI, QUIC_INITIAL_STREAM_DATA, REQUEST_TIMEOUT_SECS,
    UDP_READ_TIMEOUT_MS,
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
                address: backend_addr,
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
        performance: spooky_config::config::Performance::default(),
        observability: spooky_config::config::Observability::default(),
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

        if start.elapsed() > timeout {
            return Err(format!(
                "timeout waiting for responses (done={finished}, expected={})",
                paths.len()
            ));
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

#[test]
fn http3_to_http2_roundtrip() {
    let dir = tempdir().expect("failed to create temp dir");
    let (cert, key) = write_test_certs(&dir);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let backend_addr = rt.block_on(start_h2_backend());
    let config = make_config(0, backend_addr.to_string(), cert, key);
    let listener = QUICListener::new(config).expect("failed to create listener");
    let listen_addr = listener.socket.local_addr().unwrap();
    let _listener_task = ListenerTaskGuard::spawn(&rt, listener);

    let body = run_h3_client(listen_addr).expect("client request failed");

    assert!(body.contains("backend ok"));
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
    let backend_addr = rt.block_on(start_h2_backend());
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
