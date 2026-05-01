#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendScheme {
    Http,
    Https,
}

impl BackendScheme {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendEndpoint {
    scheme: BackendScheme,
    authority: String,
}

impl BackendEndpoint {
    /// Parse backend endpoint policy from config.
    ///
    /// Accepted forms:
    /// - `host:port` => defaults to `https://host:port`
    /// - `https://host:port`
    /// - `http://host:port` (explicit insecure opt-out)
    pub fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err("backend address is empty".to_string());
        }

        let lower = raw.to_ascii_lowercase();
        let (scheme, authority) = if lower.starts_with("http://") {
            (BackendScheme::Http, &raw[7..])
        } else if lower.starts_with("https://") {
            (BackendScheme::Https, &raw[8..])
        } else if raw.contains("://") {
            return Err("unsupported URL scheme; use http:// or https://".to_string());
        } else {
            (BackendScheme::Https, raw)
        };

        if authority.is_empty() {
            return Err("backend authority is empty".to_string());
        }

        if authority.contains('/') || authority.contains('?') || authority.contains('#') {
            return Err("backend address must not include path, query, or fragment".to_string());
        }

        let authority = normalize_authority(authority, scheme);

        validate_authority(&authority)?;

        Ok(Self {
            scheme,
            authority,
        })
    }

    pub fn scheme(&self) -> BackendScheme {
        self.scheme
    }

    pub fn authority(&self) -> &str {
        &self.authority
    }

    pub fn origin(&self) -> String {
        format!("{}://{}", self.scheme.as_str(), self.authority)
    }

    pub fn uri_for_path(&self, path: &str) -> String {
        let normalized = if path.is_empty() {
            "/"
        } else if path.starts_with('/') {
            path
        } else {
            return format!("{}/{}", self.origin(), path);
        };
        format!("{}{}", self.origin(), normalized)
    }
}

fn normalize_authority(authority: &str, scheme: BackendScheme) -> String {
    if authority.starts_with('[') {
        // IPv6: always requires explicit port per validate_authority rules
        return authority.to_string();
    }
    if authority.rsplit_once(':').is_none() {
        // No port — append the scheme default
        let default_port = match scheme {
            BackendScheme::Https => 443,
            BackendScheme::Http => 80,
        };
        return format!("{}:{}", authority, default_port);
    }
    authority.to_string()
}

fn validate_authority(authority: &str) -> Result<(), String> {
    if authority.chars().any(|ch| ch.is_ascii_whitespace()) {
        return Err("backend authority must not contain whitespace".to_string());
    }

    let (host, port_str) = if authority.starts_with('[') {
        let bracket_end = authority
            .find(']')
            .ok_or_else(|| "invalid IPv6 authority; missing closing ']'".to_string())?;
        let host = &authority[1..bracket_end];
        if host.is_empty() {
            return Err("backend host is empty".to_string());
        }

        let suffix = &authority[bracket_end + 1..];
        if !suffix.starts_with(':') {
            return Err("backend authority must include ':port'".to_string());
        }
        (host, &suffix[1..])
    } else {
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| "backend authority must be host:port".to_string())?;
        if host.is_empty() {
            return Err("backend host is empty".to_string());
        }
        if host.contains(':') {
            return Err("IPv6 authorities must use brackets, e.g. [::1]:443".to_string());
        }
        (host, port)
    };

    if host.is_empty() {
        return Err("backend host is empty".to_string());
    }

    if port_str.is_empty() {
        return Err("backend port is empty".to_string());
    }
    if !port_str.chars().all(|c| c.is_ascii_digit()) {
        return Err("backend port must be numeric".to_string());
    }
    let port = port_str
        .parse::<u16>()
        .map_err(|_| "backend port must be in range 1-65535".to_string())?;
    if port == 0 {
        return Err("backend port must be in range 1-65535".to_string());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BackendEndpoint, BackendScheme};

    #[test]
    fn parse_host_port_defaults_to_https() {
        let endpoint = BackendEndpoint::parse("example.com:443").expect("endpoint");
        assert_eq!(endpoint.scheme(), BackendScheme::Https);
        assert_eq!(endpoint.authority(), "example.com:443");
        assert_eq!(endpoint.origin(), "https://example.com:443");
    }

    #[test]
    fn parse_explicit_http_is_supported() {
        let endpoint = BackendEndpoint::parse("http://127.0.0.1:8080").expect("endpoint");
        assert_eq!(endpoint.scheme(), BackendScheme::Http);
        assert_eq!(endpoint.authority(), "127.0.0.1:8080");
    }

    #[test]
    fn parse_bracketed_ipv6() {
        let endpoint = BackendEndpoint::parse("https://[::1]:8443").expect("endpoint");
        assert_eq!(endpoint.scheme(), BackendScheme::Https);
        assert_eq!(endpoint.authority(), "[::1]:8443");
    }

    #[test]
    fn parse_rejects_path_query_and_fragment() {
        assert!(BackendEndpoint::parse("https://example.com:443/api").is_err());
        assert!(BackendEndpoint::parse("https://example.com:443?a=1").is_err());
        assert!(BackendEndpoint::parse("https://example.com:443#frag").is_err());
    }

    #[test]
    fn parse_rejects_invalid_authority() {
        assert!(BackendEndpoint::parse("127.0.0.1:abc").is_err());
        assert!(BackendEndpoint::parse("::1:443").is_err());
        assert!(BackendEndpoint::parse("https://:443").is_err());
    }

    #[test]
    fn parse_defaults_port_from_scheme() {
        let ep = BackendEndpoint::parse("https://wearebackbenchers.info").expect("https no port");
        assert_eq!(ep.scheme(), BackendScheme::Https);
        assert_eq!(ep.authority(), "wearebackbenchers.info:443");
        assert_eq!(ep.origin(), "https://wearebackbenchers.info:443");

        let ep = BackendEndpoint::parse("http://localhost").expect("http no port");
        assert_eq!(ep.scheme(), BackendScheme::Http);
        assert_eq!(ep.authority(), "localhost:80");

        let ep = BackendEndpoint::parse("127.0.0.1").expect("bare host defaults https:443");
        assert_eq!(ep.scheme(), BackendScheme::Https);
        assert_eq!(ep.authority(), "127.0.0.1:443");
    }

    #[test]
    fn uri_for_path_normalizes_empty_and_relative_paths() {
        let endpoint = BackendEndpoint::parse("backend.local:443").expect("endpoint");
        assert_eq!(endpoint.uri_for_path(""), "https://backend.local:443/");
        assert_eq!(
            endpoint.uri_for_path("/health"),
            "https://backend.local:443/health"
        );
        assert_eq!(
            endpoint.uri_for_path("health"),
            "https://backend.local:443/health"
        );
    }
}
