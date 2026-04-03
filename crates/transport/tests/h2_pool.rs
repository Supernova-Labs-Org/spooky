use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

use spooky_transport::h2_pool::{H2Pool, PoolError};

struct ConcurrencyTracker {
    current: AtomicUsize,
    max: AtomicUsize,
}

impl ConcurrencyTracker {
    fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            max: AtomicUsize::new(0),
        }
    }

    fn enter(&self) {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        let mut prev = self.max.load(Ordering::SeqCst);
        while now > prev {
            match self
                .max
                .compare_exchange(prev, now, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(next) => prev = next,
            }
        }
    }

    fn exit(&self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn start_h2_server(tracker: Arc<ConcurrencyTracker>) -> std::io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let tracker = tracker.clone();
            let service = service_fn(move |_req: Request<Incoming>| {
                let tracker = tracker.clone();
                async move {
                    tracker.enter();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    tracker.exit();
                    Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from("ok"))))
                }
            });

            tokio::spawn(async move {
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    Ok(port)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_limits_inflight_per_backend() {
    let tracker = Arc::new(ConcurrencyTracker::new());
    let port = start_h2_server(tracker.clone()).await.unwrap();
    let backend = format!("127.0.0.1:{port}");

    let pool = Arc::new(H2Pool::new(vec![backend.clone()], 1));
    let req1 = Request::builder()
        .method("GET")
        .uri(format!("http://{backend}/"))
        .body(Full::new(Bytes::new()).boxed())
        .unwrap();
    let req2 = Request::builder()
        .method("GET")
        .uri(format!("http://{backend}/"))
        .body(Full::new(Bytes::new()).boxed())
        .unwrap();

    let pool1 = pool.clone();
    let backend1 = backend.clone();
    let r1 = tokio::spawn(async move { pool1.send(&backend1, req1).await });

    let pool2 = pool.clone();
    let backend2 = backend.clone();
    let r2 = tokio::spawn(async move { pool2.send(&backend2, req2).await });

    let (r1, r2) = tokio::join!(r1, r2);
    assert!(r1.unwrap().is_ok());
    assert!(r2.unwrap().is_ok());

    let max = tracker.max.load(Ordering::SeqCst);
    assert_eq!(max, 1);
}

#[tokio::test]
async fn pool_rejects_unknown_backend() {
    let pool = H2Pool::new(vec!["127.0.0.1:12345".to_string()], 1);
    let req = Request::builder()
        .method("GET")
        .uri("http://127.0.0.1:12345/")
        .body(Full::new(Bytes::new()).boxed())
        .unwrap();

    let err = pool.send("127.0.0.1:9999", req).await.unwrap_err();
    match err {
        PoolError::UnknownBackend(name) => assert_eq!(name, "127.0.0.1:9999"),
        _ => panic!("unexpected error"),
    }
}
