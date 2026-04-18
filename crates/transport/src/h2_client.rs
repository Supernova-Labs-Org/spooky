use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use hyper::{Request, rt::Executor};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use rustls::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

pub struct H2Client {
    client: Client<HttpsConnector<HttpConnector>, BoxBody<Bytes, Infallible>>,
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

#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

impl H2Client {
    pub fn new(
        max_idle_per_host: usize,
        pool_idle_timeout: Duration,
        connect_timeout: Duration,
        tls_verify: bool,
    ) -> Self {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_connect_timeout(Some(connect_timeout));

        let https = if tls_verify {
            HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_http2()
                .wrap_connector(http)
        } else {
            let tls_config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth();
            HttpsConnectorBuilder::new()
                .with_tls_config(tls_config)
                .https_or_http()
                .enable_http2()
                .wrap_connector(http)
        };

        let client = Client::builder(TokioExecutor)
            .http2_only(true)
            .pool_max_idle_per_host(max_idle_per_host)
            .pool_idle_timeout(pool_idle_timeout)
            .build(https);

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
        Self::new(64, Duration::from_secs(30), Duration::from_secs(2), true)
    }
}
