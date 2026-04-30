use std::{
    collections::HashMap,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

use clap::Parser;
use quiche::h3::NameValue;
use rand::RngCore;
use spooky_edge::constants::{
    MAX_DATAGRAM_SIZE_BYTES, MAX_UDP_PAYLOAD_BYTES, QUIC_IDLE_TIMEOUT_MS, QUIC_INITIAL_MAX_DATA,
    QUIC_INITIAL_MAX_STREAMS_BIDI, QUIC_INITIAL_MAX_STREAMS_UNI, QUIC_INITIAL_STREAM_DATA,
    REQUEST_TIMEOUT_SECS, UDP_READ_TIMEOUT_MS,
};

#[derive(Parser)]
#[command(version, about = "Minimal HTTP/3 client using quiche")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:9889")]
    connect: String,

    #[arg(long, default_value = "/")]
    path: String,

    #[arg(long, default_value = "localhost")]
    host: String,

    #[arg(long)]
    insecure: bool,

    /// Number of requests to execute in this process.
    #[arg(long, default_value_t = 1)]
    requests: usize,

    /// Maximum number of in-flight HTTP/3 request streams on a single QUIC connection.
    #[arg(long, default_value_t = 1)]
    parallel_streams: usize,

    /// Emit one line per request as: "<ok:0|1> <latency_ns>".
    /// Intended for load scripts that aggregate per-request latency.
    #[arg(long)]
    report_latency: bool,

    /// Suppress response headers/body output.
    #[arg(long)]
    quiet: bool,
}

struct RequestState {
    start: Instant,
    body: Vec<u8>,
}

fn emit_request_result(ok: bool, latency_ns: u128, report_latency: bool) {
    if report_latency {
        println!("{} {}", if ok { 1 } else { 0 }, latency_ns);
    }
}

fn run_client(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let peer_addr: SocketAddr = cli.connect.parse()?;
    let bind_addr: SocketAddr = "0.0.0.0:0".parse()?;

    let socket = UdpSocket::bind(bind_addr)?;
    let local_addr = socket.local_addr()?;

    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config.set_application_protos(quiche::h3::APPLICATION_PROTOCOL)?;
    config.set_max_idle_timeout(QUIC_IDLE_TIMEOUT_MS);
    config.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    config.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_BYTES);
    config.set_initial_max_data(QUIC_INITIAL_MAX_DATA);
    config.set_initial_max_stream_data_bidi_local(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_stream_data_bidi_remote(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_stream_data_uni(QUIC_INITIAL_STREAM_DATA);
    config.set_initial_max_streams_bidi(QUIC_INITIAL_MAX_STREAMS_BIDI);
    config.set_initial_max_streams_uni(QUIC_INITIAL_MAX_STREAMS_UNI);
    config.enable_early_data();
    config.verify_peer(!cli.insecure);

    let mut scid_bytes = [0u8; quiche::MAX_CONN_ID_LEN];
    rand::thread_rng().fill_bytes(&mut scid_bytes);
    let scid = quiche::ConnectionId::from_ref(&scid_bytes);

    let mut conn = quiche::connect(Some(&cli.host), &scid, local_addr, peer_addr, &mut config)?;
    let mut h3_conn: Option<quiche::h3::Connection> = None;

    let mut out = [0u8; MAX_DATAGRAM_SIZE_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];

    let mut next_request_idx = 0usize;
    let mut completed = 0usize;
    let mut failures = 0usize;
    let mut last_error: Option<String> = None;
    let mut inflight: HashMap<u64, RequestState> = HashMap::new();
    let per_request_timeout = Duration::from_secs(REQUEST_TIMEOUT_SECS);

    // Kick off handshake packet(s).
    let (write, send_info) = conn.send(&mut out)?;
    socket.send_to(&out[..write], send_info.to)?;

    while completed < cli.requests {
        // Flush pending egress packets.
        loop {
            match conn.send(&mut out) {
                Ok((write, send_info)) => {
                    let _ = socket.send_to(&out[..write], send_info.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => {
                    last_error = Some(format!("send failed: {e:?}"));
                    break;
                }
            }
        }

        let timeout = conn
            .timeout()
            .unwrap_or(Duration::from_millis(UDP_READ_TIMEOUT_MS));
        socket.set_read_timeout(Some(timeout))?;

        match socket.recv_from(&mut buf) {
            Ok((len, from)) => {
                let recv_info = quiche::RecvInfo {
                    from,
                    to: local_addr,
                };
                if let Err(e) = conn.recv(&mut buf[..len], recv_info) {
                    last_error = Some(format!("recv failed: {e:?}"));
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                conn.on_timeout();
            }
            Err(e) => {
                last_error = Some(e.to_string());
            }
        }

        if h3_conn.is_none() && (conn.is_established() || conn.is_in_early_data()) {
            let h3_config = quiche::h3::Config::new()?;
            h3_conn = Some(quiche::h3::Connection::with_transport(
                &mut conn, &h3_config,
            )?);
        }

        if let Some(h3) = h3_conn.as_mut() {
            // Submit more requests on the same connection up to stream concurrency cap.
            while next_request_idx < cli.requests && inflight.len() < cli.parallel_streams {
                let req = vec![
                    quiche::h3::Header::new(b":method", b"GET"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", cli.host.as_bytes()),
                    quiche::h3::Header::new(b":path", cli.path.as_bytes()),
                    quiche::h3::Header::new(b"user-agent", b"spooky-h3-client"),
                ];

                match h3.send_request(&mut conn, &req, true) {
                    Ok(stream_id) => {
                        inflight.insert(
                            stream_id,
                            RequestState {
                                start: Instant::now(),
                                body: Vec::new(),
                            },
                        );
                        next_request_idx = next_request_idx.saturating_add(1);
                    }
                    Err(quiche::h3::Error::Done) | Err(quiche::h3::Error::StreamBlocked) => {
                        break;
                    }
                    Err(e) => {
                        // Count this unsent request as failed and move on.
                        failures = failures.saturating_add(1);
                        completed = completed.saturating_add(1);
                        next_request_idx = next_request_idx.saturating_add(1);
                        last_error = Some(format!("send_request failed: {e:?}"));
                        emit_request_result(false, 0, cli.report_latency);
                    }
                }
            }

            loop {
                match h3.poll(&mut conn) {
                    Ok((_stream_id, quiche::h3::Event::Headers { list, .. })) => {
                        if !cli.quiet && !cli.report_latency && cli.requests == 1 {
                            for header in list {
                                let name = String::from_utf8_lossy(header.name());
                                let value = String::from_utf8_lossy(header.value());
                                println!("{name}: {value}");
                            }
                            println!();
                        }
                    }
                    Ok((stream_id, quiche::h3::Event::Data)) => loop {
                        match h3.recv_body(&mut conn, stream_id, &mut buf) {
                            Ok(read) => {
                                if let Some(state) = inflight.get_mut(&stream_id) {
                                    state.body.extend_from_slice(&buf[..read]);
                                }
                            }
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => {
                                if let Some(state) = inflight.remove(&stream_id) {
                                    let latency_ns = state.start.elapsed().as_nanos();
                                    failures = failures.saturating_add(1);
                                    completed = completed.saturating_add(1);
                                    emit_request_result(false, latency_ns, cli.report_latency);
                                }
                                last_error = Some(format!("recv_body failed: {e:?}"));
                                break;
                            }
                        }
                    },
                    Ok((stream_id, quiche::h3::Event::Finished)) => {
                        if let Some(state) = inflight.remove(&stream_id) {
                            let latency_ns = state.start.elapsed().as_nanos();
                            completed = completed.saturating_add(1);
                            emit_request_result(true, latency_ns, cli.report_latency);

                            if !cli.quiet && !cli.report_latency && cli.requests == 1 && !state.body.is_empty() {
                                let body = String::from_utf8_lossy(&state.body);
                                println!("{body}");
                            }
                        }
                    }
                    Ok((stream_id, quiche::h3::Event::Reset(_))) => {
                        let latency_ns = inflight
                            .remove(&stream_id)
                            .map(|state| state.start.elapsed().as_nanos())
                            .unwrap_or(0);
                        failures = failures.saturating_add(1);
                        completed = completed.saturating_add(1);
                        emit_request_result(false, latency_ns, cli.report_latency);
                    }
                    Ok((_stream_id, quiche::h3::Event::PriorityUpdate)) => {}
                    Ok((_stream_id, quiche::h3::Event::GoAway)) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => {
                        last_error = Some(format!("h3 poll failed: {e:?}"));
                        break;
                    }
                }
            }
        }

        // Per-request timeout enforcement for in-flight streams.
        let mut timed_out = Vec::new();
        for (stream_id, state) in &inflight {
            if state.start.elapsed() > per_request_timeout {
                timed_out.push(*stream_id);
            }
        }
        for stream_id in timed_out {
            if let Some(state) = inflight.remove(&stream_id) {
                let latency_ns = state.start.elapsed().as_nanos();
                failures = failures.saturating_add(1);
                completed = completed.saturating_add(1);
                emit_request_result(false, latency_ns, cli.report_latency);
            }
        }

        if conn.is_closed() {
            // Fail any remaining in-flight and unsent requests.
            for (_stream_id, state) in inflight.drain() {
                failures = failures.saturating_add(1);
                completed = completed.saturating_add(1);
                emit_request_result(false, state.start.elapsed().as_nanos(), cli.report_latency);
            }

            if completed < cli.requests {
                let remaining = cli.requests - completed;
                failures = failures.saturating_add(remaining);
                for _ in 0..remaining {
                    emit_request_result(false, 0, cli.report_latency);
                }
                completed = cli.requests;
            }
        }
    }

    if !cli.report_latency && failures > 0 {
        let suffix = last_error
            .as_deref()
            .map(|msg| format!(": {msg}"))
            .unwrap_or_default();
        return Err(format!("{} of {} request(s) failed{}", failures, cli.requests, suffix).into());
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.requests == 0 {
        return Err("--requests must be >= 1".into());
    }

    if cli.parallel_streams == 0 {
        return Err("--parallel-streams must be >= 1".into());
    }

    run_client(&cli)
}
