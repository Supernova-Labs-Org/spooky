use std::convert::Infallible;
use std::future::Future;
use std::time::Duration;

use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use hyper::{Request, rt::Executor};
use hyper_util::client::legacy::{Client, connect::HttpConnector};

pub struct H2Client {
    client: Client<HttpConnector, BoxBody<Bytes, Infallible>>,
}

#[derive(Clone, Copy)]
struct TokioExecutor;

impl<F> Executor<F> for TokioExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::spawn(fut);
    }
}

impl H2Client {
    pub fn new(max_idle_per_host: usize, pool_idle_timeout: Duration) -> Self {
        let mut http = HttpConnector::new();
        http.enforce_http(false);

        let client = Client::builder(TokioExecutor)
            .http2_only(true)
            .pool_max_idle_per_host(max_idle_per_host)
            .pool_idle_timeout(pool_idle_timeout)
            .build(http);

        Self { client }
    }

    pub async fn send(
        &self,
        req: Request<BoxBody<Bytes, Infallible>>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, hyper_util::client::legacy::Error> {
        self.client.request(req).await
    }
}

impl Default for H2Client {
    fn default() -> Self {
        Self::new(64, Duration::from_secs(30))
    }
}
