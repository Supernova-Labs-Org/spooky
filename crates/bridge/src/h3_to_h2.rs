use std::convert::Infallible;

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method, Request, Uri};
use http_body_util::combinators::BoxBody;
use quiche::h3::NameValue;
use spooky_config::backend_endpoint::BackendEndpoint;

pub use spooky_errors::BridgeError;

/// Build an HTTP/2 request with a pre-boxed streaming body.
/// `content_length` is `Some(n)` only when the full length is known upfront
/// (i.e. the body was fully buffered); pass `None` for streaming bodies.
pub fn build_h2_request(
    backend: &str,
    method: &str,
    path: &str,
    headers: &[quiche::h3::Header],
    body: BoxBody<Bytes, Infallible>,
    content_length: Option<usize>,
) -> Result<Request<BoxBody<Bytes, Infallible>>, BridgeError> {
    let method = Method::from_bytes(method.as_bytes()).map_err(|_| BridgeError::InvalidMethod)?;
    let endpoint = BackendEndpoint::parse(backend).map_err(|_| BridgeError::InvalidUri)?;

    let request_path = if path.is_empty() { "/" } else { path };
    let uri = endpoint.uri_for_path(request_path);
    let uri = Uri::try_from(uri).map_err(|_| BridgeError::InvalidUri)?;

    let mut builder = Request::builder().method(method).uri(uri);

    let mut saw_host = false;
    for header in headers {
        let name = header.name();
        if name.starts_with(b":") {
            continue;
        }

        let header_name = HeaderName::from_bytes(name).map_err(|_| BridgeError::InvalidHeader)?;
        if header_name == http::header::HOST {
            saw_host = true;
        }

        if header_name == http::header::CONTENT_LENGTH {
            continue;
        }

        let header_value =
            HeaderValue::from_bytes(header.value()).map_err(|_| BridgeError::InvalidHeader)?;
        builder = builder.header(header_name, header_value);
    }

    if !saw_host {
        builder = builder.header(http::header::HOST, endpoint.authority());
    }

    if let Some(len) = content_length
        && len > 0
    {
        builder = builder.header(http::header::CONTENT_LENGTH, len);
    }

    builder.body(body).map_err(BridgeError::Build)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::header::HOST;
    use http_body_util::{BodyExt, Empty};

    use super::build_h2_request;

    #[test]
    fn defaults_to_https_origin_for_host_port_backend() {
        let req = build_h2_request(
            "backend.internal:443",
            "GET",
            "/health",
            &[],
            Empty::<Bytes>::new().boxed(),
            None,
        )
        .expect("request");

        assert_eq!(req.uri().to_string(), "https://backend.internal:443/health");
        assert_eq!(
            req.headers().get(HOST).and_then(|h| h.to_str().ok()),
            Some("backend.internal:443")
        );
    }

    #[test]
    fn keeps_explicit_http_scheme() {
        let req = build_h2_request(
            "http://127.0.0.1:8080",
            "GET",
            "/",
            &[],
            Empty::<Bytes>::new().boxed(),
            None,
        )
        .expect("request");

        assert_eq!(req.uri().to_string(), "http://127.0.0.1:8080/");
        assert_eq!(
            req.headers().get(HOST).and_then(|h| h.to_str().ok()),
            Some("127.0.0.1:8080")
        );
    }

    #[test]
    fn rejects_invalid_backend_endpoint() {
        let err = build_h2_request(
            "https://backend.internal:443/path",
            "GET",
            "/",
            &[],
            Empty::<Bytes>::new().boxed(),
            None,
        )
        .expect_err("invalid backend endpoint should fail");

        assert!(matches!(err, crate::h3_to_h2::BridgeError::InvalidUri));
    }
}
