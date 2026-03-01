use std::{
    collections::{HashMap, HashSet},
    net::UdpSocket,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use core::net::SocketAddr;

use spooky_config::config::Config;
use spooky_lb::UpstreamPool;
use spooky_transport::h2_pool::H2Pool;

pub mod quic_listener;

pub struct QUICListener {
    pub socket: UdpSocket,
    pub config: Config,
    pub quic_config: quiche::Config,
    pub h3_config: Arc<quiche::h3::Config>,
    pub h2_pool: Arc<H2Pool>,
    pub upstream_pools: HashMap<String, Arc<Mutex<UpstreamPool>>>,
    pub metrics: Metrics,
    pub draining: bool,
    pub drain_start: Option<Instant>,

    pub recv_buf: [u8; 65535], // array initialization, let arr [<data type>, <no of elements>] = [<value of all>, <no of elements>]
    pub send_buf: [u8; 65535],

    pub connections: HashMap<Vec<u8>, QuicConnection>, // KEY: SCID(server connection id)
    pub cid_routes: HashMap<Vec<u8>, Vec<u8>>,         // KEY: alias SCID, VALUE: primary SCID
}

pub struct QuicConnection {
    pub quic: quiche::Connection,
    pub h3: Option<quiche::h3::Connection>,
    pub h3_config: Arc<quiche::h3::Config>,
    pub streams: HashMap<u64, RequestEnvelope>,

    pub peer_address: SocketAddr,
    pub last_activity: Instant,
    pub primary_scid: Vec<u8>,
    pub routing_scids: HashSet<Vec<u8>>,
    pub packets_since_rotation: u64,
    pub last_scid_rotation: Instant,
}

pub struct RequestEnvelope {
    pub method: String,
    pub path: String,
    pub authority: Option<String>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub start: Instant,
}

#[derive(Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_success: AtomicU64,
    pub requests_failure: AtomicU64,
    pub backend_timeouts: AtomicU64,
    pub backend_errors: AtomicU64,
    pub scid_rotations: AtomicU64,
}

impl Metrics {
    pub fn inc_total(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_success(&self) {
        self.requests_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failure(&self) {
        self.requests_failure.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_timeout(&self) {
        self.backend_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_backend_error(&self) {
        self.backend_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_scid_rotation(&self) {
        self.scid_rotations.fetch_add(1, Ordering::Relaxed);
    }
}
