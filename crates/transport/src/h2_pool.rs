use std::{collections::HashMap, sync::Arc};

use http_body_util::Full;
use hyper::Request;
use hyper::body::{Bytes, Incoming};
use tokio::sync::Semaphore;

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
    pub fn new<I>(backends: I, max_inflight: usize) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let inflight = max_inflight.max(1);
        let mut map = HashMap::new();
        for backend in backends {
            map.insert(
                backend,
                BackendHandle {
                    client: H2Client::new(),
                    inflight: Arc::new(Semaphore::new(inflight)),
                },
            );
        }
        Self { backends: map }
    }

    pub fn has_backend(&self, backend: &str) -> bool {
        self.backends.contains_key(backend)
    }

    pub async fn send(
        &self,
        backend: &str,
        req: Request<Full<Bytes>>,
    ) -> Result<hyper::Response<Incoming>, PoolError> {
        let handle = self
            .backends
            .get(backend)
            .ok_or_else(|| PoolError::UnknownBackend(backend.to_string()))?;

        let _permit = handle.inflight.acquire().await.expect("semaphore closed");
        handle.client.send(req).await.map_err(PoolError::Send)
    }
}
