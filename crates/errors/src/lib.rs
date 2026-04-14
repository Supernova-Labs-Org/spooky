use thiserror::Error;

/// HTTP/3 to HTTP/2 bridge error types
#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("invalid HTTP method")]
    InvalidMethod,

    #[error("invalid URI")]
    InvalidUri,

    #[error("invalid header")]
    InvalidHeader,

    #[error("failed to build request: {0}")]
    Build(#[from] http::Error),
}

/// HTTP/2 pool and transport error types
#[derive(Debug, Error)]
pub enum PoolError {
    #[error("unknown backend: {0}")]
    UnknownBackend(String),

    #[error("backend overloaded: {0}")]
    BackendOverloaded(String),

    #[error("backend circuit open: {0}")]
    CircuitOpen(String),

    #[error("send failed: {0}")]
    Send(#[source] hyper_util::client::legacy::Error),

    #[error("backend inflight limiter closed")]
    InflightLimiterClosed,
}

/// Top-level proxy error type unifying bridge and transport errors
#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("bridge error: {0}")]
    Bridge(#[from] BridgeError),

    #[error("transport error: {0}")]
    Pool(#[from] PoolError),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("backend timeout")]
    Timeout,

    #[error("TLS error: {0}")]
    Tls(String),
}
