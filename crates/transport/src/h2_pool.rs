use std::{collections::HashMap, convert::Infallible, sync::Arc, time::Duration};

use http_body_util::combinators::BoxBody;
use hyper::Request;
use hyper::body::{Bytes, Incoming};
use tokio::sync::{Semaphore, TryAcquireError};

use crate::h2_client::H2Client;
pub use spooky_errors::PoolError;

struct BackendHandle {
    client: H2Client,
    inflight: Arc<Semaphore>,
}

pub struct H2Pool {
    backends: HashMap<String, BackendHandle>,
}

impl H2Pool {
    pub fn new<I>(
        backends: I,
        max_inflight: usize,
        max_idle_per_backend: usize,
        pool_idle_timeout: Duration,
        connect_timeout: Duration,
    ) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let inflight = max_inflight.max(1);
        let max_idle_per_backend = max_idle_per_backend.max(1);
        let mut map = HashMap::new();
        for backend in backends {
            map.insert(
                backend,
                BackendHandle {
                    client: H2Client::new(max_idle_per_backend, pool_idle_timeout, connect_timeout, false),
                    inflight: Arc::new(Semaphore::new(inflight)),
                },
            );
        }
        Self { backends: map }
    }

    pub fn has_backend(&self, backend: &str) -> bool {
        self.backends.contains_key(backend)
    }

    pub fn has_capacity(&self, backend: &str) -> Result<bool, PoolError> {
        let handle = self
            .backends
            .get(backend)
            .ok_or_else(|| PoolError::UnknownBackend(backend.to_string()))?;
        Ok(handle.inflight.available_permits() > 0)
    }

    pub async fn send(
        &self,
        backend: &str,
        req: Request<BoxBody<Bytes, Infallible>>,
    ) -> Result<hyper::Response<Incoming>, PoolError> {
        let handle = self
            .backends
            .get(backend)
            .ok_or_else(|| PoolError::UnknownBackend(backend.to_string()))?;

        let _permit = match Arc::clone(&handle.inflight).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return Err(PoolError::BackendOverloaded(backend.to_string()));
            }
            Err(TryAcquireError::Closed) => return Err(PoolError::InflightLimiterClosed),
        };
        handle.client.send(req).await.map_err(PoolError::Send)
    }
}
