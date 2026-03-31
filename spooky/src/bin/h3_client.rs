use std::{
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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

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
    let mut req_sent = false;
    let mut response_done = false;
    let mut response_body = Vec::new();

    let mut out = [0u8; MAX_DATAGRAM_SIZE_BYTES];
    let mut buf = [0u8; MAX_DATAGRAM_SIZE_BYTES];
    let start = Instant::now();
    let mut last_timeout = Instant::now();

    let (write, send_info) = conn.send(&mut out)?;
    socket.send_to(&out[..write], send_info.to)?;

    while !response_done && !conn.is_closed() {
        loop {
            match conn.send(&mut out) {
                Ok((write, send_info)) => {
                    let _ = socket.send_to(&out[..write], send_info.to);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(format!("send failed: {e:?}").into()),
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
                conn.recv(&mut buf[..len], recv_info)?;
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                let now = Instant::now();
                if now >= last_timeout + timeout {
                    conn.on_timeout();
                    last_timeout = now;
                }
            }
            Err(e) => return Err(e.into()),
        }

        if conn.is_established() && h3_conn.is_none() {
            let h3_config = quiche::h3::Config::new()?;
            h3_conn = Some(quiche::h3::Connection::with_transport(
                &mut conn, &h3_config,
            )?);
        }

        if let Some(h3) = h3_conn.as_mut() {
            if conn.is_established() && !req_sent {
                let req = vec![
                    quiche::h3::Header::new(b":method", b"GET"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", cli.host.as_bytes()),
                    quiche::h3::Header::new(b":path", cli.path.as_bytes()),
                    quiche::h3::Header::new(b"user-agent", b"spooky-h3-client"),
                ];
                h3.send_request(&mut conn, &req, true)?;
                req_sent = true;
            }

            loop {
                match h3.poll(&mut conn) {
                    Ok((_stream_id, quiche::h3::Event::Headers { list, .. })) => {
                        for header in list {
                            let name = String::from_utf8_lossy(header.name());
                            let value = String::from_utf8_lossy(header.value());
                            println!("{name}: {value}");
                        }
                        println!();
                    }
                    Ok((stream_id, quiche::h3::Event::Data)) => loop {
                        match h3.recv_body(&mut conn, stream_id, &mut buf) {
                            Ok(read) => response_body.extend_from_slice(&buf[..read]),
                            Err(quiche::h3::Error::Done) => break,
                            Err(e) => return Err(format!("recv_body failed: {e:?}").into()),
                        }
                    },
                    Ok((_stream_id, quiche::h3::Event::Finished)) => {
                        response_done = true;
                        break;
                    }
                    Ok((_stream_id, quiche::h3::Event::PriorityUpdate)) => {}
                    Ok((_stream_id, quiche::h3::Event::GoAway)) => {
                        response_done = true;
                        break;
                    }
                    Ok((_stream_id, quiche::h3::Event::Reset(_))) => {
                        return Err("stream reset by peer".into());
                    }
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => return Err(format!("h3 poll failed: {e:?}").into()),
                }
            }
        }

        if start.elapsed() > Duration::from_secs(REQUEST_TIMEOUT_SECS) && !response_done {
            return Err("timeout waiting for response".into());
        }
    }

    if !response_body.is_empty() {
        let body = String::from_utf8_lossy(&response_body);
        println!("{body}");
    }

    Ok(())
}
