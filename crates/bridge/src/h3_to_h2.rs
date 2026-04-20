use std::{
    collections::HashSet,
    convert::Infallible,
    net::{IpAddr, SocketAddr},
};

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method, Request, Uri};
use http_body_util::combinators::BoxBody;
use quiche::h3::NameValue;
use spooky_config::backend_endpoint::BackendEndpoint;

pub use spooky_errors::BridgeError;

pub struct ForwardedContext<'a> {
    pub client_addr: SocketAddr,
    pub request_authority: Option<&'a str>,
}

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
    forwarded_ctx: ForwardedContext<'_>,
) -> Result<Request<BoxBody<Bytes, Infallible>>, BridgeError> {
    let method = Method::from_bytes(method.as_bytes()).map_err(|_| BridgeError::InvalidMethod)?;
    let endpoint = BackendEndpoint::parse(backend).map_err(|_| BridgeError::InvalidUri)?;

    let request_path = if path.is_empty() { "/" } else { path };
    let uri = endpoint.uri_for_path(request_path);
    let uri = Uri::try_from(uri).map_err(|_| BridgeError::InvalidUri)?;

    let mut builder = Request::builder().method(method).uri(uri);
    let connection_tokens = connection_header_tokens(headers);
    let mut host_from_headers: Option<String> = None;

    for header in headers {
        let name = header.name();
        if name.starts_with(b":") {
            continue;
        }

        let header_name = HeaderName::from_bytes(name).map_err(|_| BridgeError::InvalidHeader)?;
        if should_strip_request_header(&header_name, &connection_tokens) {
            continue;
        }

        let header_value =
            HeaderValue::from_bytes(header.value()).map_err(|_| BridgeError::InvalidHeader)?;
        if header_name == http::header::HOST {
            host_from_headers = header_value.to_str().ok().map(str::to_string);
            continue;
        }
        builder = builder.header(header_name, header_value);
    }

    let host_value = forwarded_ctx
        .request_authority
        .or(host_from_headers.as_deref())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(endpoint.authority());
    builder = builder.header(http::header::HOST, host_value);

    if let Some(len) = content_length
        && len > 0
    {
        builder = builder.header(http::header::CONTENT_LENGTH, len);
    }

    let forwarded_value = format!(
        "for={};proto=https;host=\"{}\"",
        forwarded_for_value(forwarded_ctx.client_addr.ip()),
        escape_forwarded_host(host_value),
    );
    builder = builder
        .header(
            HeaderName::from_static("forwarded"),
            HeaderValue::from_str(&forwarded_value).map_err(|_| BridgeError::InvalidHeader)?,
        )
        .header(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_str(&forwarded_ctx.client_addr.ip().to_string())
                .map_err(|_| BridgeError::InvalidHeader)?,
        )
        .header(
            HeaderName::from_static("x-forwarded-proto"),
            HeaderValue::from_static("https"),
        )
        .header(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_str(host_value).map_err(|_| BridgeError::InvalidHeader)?,
        );

    builder.body(body).map_err(BridgeError::Build)
}

fn connection_header_tokens(headers: &[quiche::h3::Header]) -> HashSet<String> {
    let mut tokens = HashSet::new();
    for header in headers {
        if !header.name().eq_ignore_ascii_case(b"connection") {
            continue;
        }
        let Ok(value) = std::str::from_utf8(header.value()) else {
            continue;
        };
        for token in value.split(',') {
            let normalized = token.trim().to_ascii_lowercase();
            if !normalized.is_empty() {
                tokens.insert(normalized);
            }
        }
    }
    tokens
}

fn should_strip_request_header(name: &HeaderName, connection_tokens: &HashSet<String>) -> bool {
    if connection_tokens.contains(name.as_str()) {
        return true;
    }

    if name == http::header::CONTENT_LENGTH {
        return true;
    }

    if name == http::header::CONNECTION
        || name == http::header::PROXY_AUTHENTICATE
        || name == http::header::PROXY_AUTHORIZATION
        || name == http::header::TE
        || name == http::header::TRAILER
        || name == http::header::TRANSFER_ENCODING
        || name == http::header::UPGRADE
        || name.as_str().eq_ignore_ascii_case("keep-alive")
        || name.as_str().eq_ignore_ascii_case("proxy-connection")
        || name.as_str().eq_ignore_ascii_case("forwarded")
        || name.as_str().eq_ignore_ascii_case("x-forwarded-for")
        || name.as_str().eq_ignore_ascii_case("x-forwarded-proto")
        || name.as_str().eq_ignore_ascii_case("x-forwarded-host")
    {
        return true;
    }

    false
}

fn forwarded_for_value(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("\"[{}]\"", v6),
    }
}

fn escape_forwarded_host(host: &str) -> String {
    host.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::header::HOST;
    use http_body_util::{BodyExt, Empty};
    use quiche::h3::Header;

    use super::{ForwardedContext, build_h2_request};

    #[test]
    fn defaults_to_https_origin_for_host_port_backend() {
        let req = build_h2_request(
            "backend.internal:443",
            "GET",
            "/health",
            &[],
            Empty::<Bytes>::new().boxed(),
            None,
            ForwardedContext {
                client_addr: "203.0.113.10:44321".parse().expect("client"),
                request_authority: Some("api.example.com"),
            },
        )
        .expect("request");

        assert_eq!(req.uri().to_string(), "https://backend.internal:443/health");
        assert_eq!(
            req.headers().get(HOST).and_then(|h| h.to_str().ok()),
            Some("api.example.com")
        );
        assert_eq!(
            req.headers()
                .get("x-forwarded-proto")
                .and_then(|h| h.to_str().ok()),
            Some("https")
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
            ForwardedContext {
                client_addr: "198.51.100.3:5555".parse().expect("client"),
                request_authority: None,
            },
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
            ForwardedContext {
                client_addr: "127.0.0.1:12345".parse().expect("client"),
                request_authority: None,
            },
        )
        .expect_err("invalid backend endpoint should fail");

        assert!(matches!(err, crate::h3_to_h2::BridgeError::InvalidUri));
    }

    #[test]
    fn strips_spoofed_forwarded_headers_and_normalizes() {
        let headers = vec![
            Header::new(b"x-forwarded-for", b"1.2.3.4"),
            Header::new(b"forwarded", b"for=1.2.3.4"),
            Header::new(b"x-forwarded-host", b"evil.example"),
            Header::new(b"x-forwarded-proto", b"http"),
            Header::new(b"host", b"api.example.com"),
            Header::new(b"connection", b"keep-alive, x-secret"),
            Header::new(b"x-secret", b"drop-me"),
            Header::new(b"x-keep", b"ok"),
        ];

        let req = build_h2_request(
            "backend.internal:443",
            "GET",
            "/",
            &headers,
            Empty::<Bytes>::new().boxed(),
            None,
            ForwardedContext {
                client_addr: "203.0.113.55:43210".parse().expect("client"),
                request_authority: Some("api.example.com"),
            },
        )
        .expect("request");

        assert_eq!(
            req.headers()
                .get("x-forwarded-for")
                .and_then(|h| h.to_str().ok()),
            Some("203.0.113.55")
        );
        assert_eq!(
            req.headers()
                .get("x-forwarded-host")
                .and_then(|h| h.to_str().ok()),
            Some("api.example.com")
        );
        assert_eq!(
            req.headers().get("forwarded").and_then(|h| h.to_str().ok()),
            Some("for=203.0.113.55;proto=https;host=\"api.example.com\"")
        );
        assert!(req.headers().get("x-secret").is_none());
        assert_eq!(
            req.headers().get("x-keep").and_then(|h| h.to_str().ok()),
            Some("ok")
        );
    }

    #[test]
    fn forwarded_header_formats_ipv6_clients() {
        let req = build_h2_request(
            "backend.internal:443",
            "GET",
            "/",
            &[],
            Empty::<Bytes>::new().boxed(),
            None,
            ForwardedContext {
                client_addr: "[2001:db8::1]:4444".parse().expect("client"),
                request_authority: Some("api.example.com"),
            },
        )
        .expect("request");

        assert_eq!(
            req.headers().get("forwarded").and_then(|h| h.to_str().ok()),
            Some("for=\"[2001:db8::1]\";proto=https;host=\"api.example.com\"")
        );
    }
}
