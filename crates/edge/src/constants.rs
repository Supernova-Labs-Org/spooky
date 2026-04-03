use std::time::Duration;

pub const MAX_DATAGRAM_SIZE_BYTES: usize = 65_535;
pub const MAX_UDP_PAYLOAD_BYTES: usize = 1_350;

pub const UDP_READ_TIMEOUT_MS: u64 = 50;
pub const BACKEND_TIMEOUT_SECS: u64 = 2;
pub const DRAIN_TIMEOUT_SECS: u64 = 5;
pub const REQUEST_TIMEOUT_SECS: u64 = 5;

pub const QUIC_IDLE_TIMEOUT_MS: u64 = 5_000;
pub const QUIC_INITIAL_MAX_DATA: u64 = 10_000_000;
pub const QUIC_INITIAL_STREAM_DATA: u64 = 1_000_000;

/// Hard application-level cap on request body size per stream.
/// Requests exceeding this are rejected with 413 before forwarding to upstream.
/// Must be ≤ QUIC_INITIAL_STREAM_DATA (flow-control limit).
pub const MAX_REQUEST_BODY_BYTES: usize = 1_000_000;
pub const QUIC_INITIAL_MAX_STREAMS_BIDI: u64 = 100;
pub const QUIC_INITIAL_MAX_STREAMS_UNI: u64 = 100;

pub const MAX_INFLIGHT_PER_BACKEND: usize = 64;
pub const DEFAULT_SCID_LEN_BYTES: usize = 16;
pub const RESET_TOKEN_LEN_BYTES: usize = 16;
pub const MIN_SCID_LEN_BYTES: usize = 8;

pub const SCID_ROTATION_INTERVAL_SECS: u64 = 60;
pub const SCID_ROTATION_PACKET_THRESHOLD: u64 = 8;

pub const BENCH_CONN_PRIMARY_ID_LEN_BYTES: usize = 16;
pub const BENCH_CONN_PRIMARY_ID_PREFIX_BYTES: usize = 8;
pub const BENCH_CONN_ALIAS_SUFFIX: [u8; 4] = [0xaa, 0xbb, 0xcc, 0xdd];
pub const BENCH_CONN_MISS_ID_LEN_BYTES: usize = 24;
pub const BENCH_CONN_MISS_ID_FILL: u8 = 0xff;
pub const BENCH_CONN_PEER_BASE_PORT: u16 = 20_000;
pub const BENCH_CONN_PEER_PORT_SPAN: usize = 20_000;
pub const BENCH_CONN_MISS_PORT: u16 = u16::MAX;

pub fn backend_timeout() -> Duration {
    Duration::from_secs(BACKEND_TIMEOUT_SECS)
}

pub fn drain_timeout() -> Duration {
    Duration::from_secs(DRAIN_TIMEOUT_SECS)
}

pub fn request_timeout() -> Duration {
    Duration::from_secs(REQUEST_TIMEOUT_SECS)
}

pub fn scid_rotation_interval() -> Duration {
    Duration::from_secs(SCID_ROTATION_INTERVAL_SECS)
}
