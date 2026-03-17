//! Secure client IP extraction with anti-spoofing protection
//!
//! This module provides secure extraction of client IP addresses from HTTP requests,
//! with protection against header spoofing attacks that attempt to bypass localhost checks.

use actix_web::http::header::{HeaderMap, HeaderValue};
use actix_web::HttpRequest;
use ipnet::IpNet;
use kalamdb_commons::models::ConnectionInfo;
use log::warn;
use once_cell::sync::Lazy;
use std::net::IpAddr;
use std::sync::RwLock;

static TRUSTED_PROXY_RANGES: Lazy<RwLock<Vec<IpNet>>> = Lazy::new(|| RwLock::new(Vec::new()));

/// Extract client IP address with security checks against header spoofing
///
/// # Security Model
///
/// 1. **X-Forwarded-For Validation**: Rejects localhost values in X-Forwarded-For header
///    - Prevents attackers from spoofing `X-Forwarded-For: 127.0.0.1` to bypass security
///    - If localhost is detected in header, falls back to peer_addr (real IP)
///
/// 2. **Trust Hierarchy**:
///    - First: X-Forwarded-For (if present and not localhost)
///    - Fallback: peer_addr (direct connection IP)
///
/// 3. **Attack Prevention**:
///    - Blocks: `127.0.0.1`, `::1`, `127.*`, `localhost` in X-Forwarded-For
///    - Logs warning when spoofing attempt is detected
///
/// # Examples
///
/// ```rust
/// use actix_web::HttpRequest;
/// use kalamdb_auth::extract_client_ip_secure;
///
/// fn handler(req: HttpRequest) {
///     let client_ip = extract_client_ip_secure(&req);
///     println!("Client IP: {:?}", client_ip);
/// }
/// ```
pub fn init_trusted_proxy_ranges(entries: &[String]) -> anyhow::Result<()> {
    let parsed = kalamdb_configs::parse_trusted_proxy_entries(entries)?;
    *TRUSTED_PROXY_RANGES.write().expect("trusted proxy ranges lock poisoned") = parsed;
    Ok(())
}

pub fn extract_client_ip_addr_secure(
    peer_addr: Option<IpAddr>,
    headers: &HeaderMap,
) -> Option<IpAddr> {
    let trusted_proxy_ranges =
        TRUSTED_PROXY_RANGES.read().expect("trusted proxy ranges lock poisoned");
    extract_client_ip_addr_with_trusted_ranges(peer_addr, headers, &trusted_proxy_ranges)
}

pub fn extract_client_ip_secure(req: &HttpRequest) -> ConnectionInfo {
    extract_client_ip_addr_secure(req.peer_addr().map(|addr| addr.ip()), req.headers())
        .map(|ip| ConnectionInfo::new(Some(ip.to_string())))
        .unwrap_or_else(|| ConnectionInfo::new(None))
}

fn extract_client_ip_addr_with_trusted_ranges(
    peer_addr: Option<IpAddr>,
    headers: &HeaderMap,
    trusted_proxy_ranges: &[IpNet],
) -> Option<IpAddr> {
    if peer_addr.is_some_and(|ip| is_trusted_proxy_peer(ip, trusted_proxy_ranges)) {
        if let Some(ip) =
            extract_proxy_header_ip(headers.get("X-Forwarded-For"), true, "X-Forwarded-For")
        {
            return Some(ip);
        }

        if let Some(ip) = extract_proxy_header_ip(headers.get("X-Real-IP"), false, "X-Real-IP") {
            return Some(ip);
        }
    } else if headers.contains_key("X-Forwarded-For") || headers.contains_key("X-Real-IP") {
        warn!("Security: Ignoring proxy headers from untrusted peer {:?}", peer_addr);
    }

    peer_addr
}

fn is_trusted_proxy_peer(peer_addr: IpAddr, trusted_proxy_ranges: &[IpNet]) -> bool {
    peer_addr.is_loopback() || trusted_proxy_ranges.iter().any(|range| range.contains(&peer_addr))
}

fn extract_proxy_header_ip(
    header: Option<&HeaderValue>,
    first_csv_value: bool,
    header_name: &str,
) -> Option<IpAddr> {
    let header_value = header?.to_str().ok()?;
    let candidate = if first_csv_value {
        header_value.split(',').next().unwrap_or("").trim()
    } else {
        header_value.trim()
    };

    if candidate.is_empty() {
        return None;
    }

    if is_localhost_address(candidate) {
        warn!(
            "Security: Rejected localhost value in trusted {} header: '{}'. Using peer_addr instead.",
            header_name,
            candidate
        );
        return None;
    }

    candidate.parse::<IpAddr>().ok()
}

/// Check if an IP address string represents localhost
///
/// Detects all common localhost representations:
/// - IPv4: 127.0.0.1, 127.*, 127.x.x.x
/// - IPv6: ::1
/// - Hostname: localhost
///
/// # Examples
///
/// ```rust
/// use kalamdb_auth::is_localhost_address;
///
/// assert!(is_localhost_address("127.0.0.1"));
/// assert!(is_localhost_address("127.5.8.3"));
/// assert!(is_localhost_address("::1"));
/// assert!(is_localhost_address("localhost"));
/// assert!(!is_localhost_address("192.168.1.1"));
/// ```
pub fn is_localhost_address(ip: &str) -> bool {
    ip == "127.0.0.1"
        || ip == "::1"
        || ip.starts_with("127.")
        || ip.eq_ignore_ascii_case("localhost")
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};

    #[test]
    fn test_is_localhost_address() {
        // IPv4 localhost
        assert!(is_localhost_address("127.0.0.1"));
        assert!(is_localhost_address("127.1.2.3"));
        assert!(is_localhost_address("127.255.255.255"));

        // IPv6 localhost
        assert!(is_localhost_address("::1"));

        // Hostname
        assert!(is_localhost_address("localhost"));
        assert!(is_localhost_address("LOCALHOST"));
        assert!(is_localhost_address("LocalHost"));

        // Non-localhost addresses
        assert!(!is_localhost_address("192.168.1.1"));
        assert!(!is_localhost_address("10.0.0.1"));
        assert!(!is_localhost_address("8.8.8.8"));
        assert!(!is_localhost_address("::2"));
        assert!(!is_localhost_address(""));
    }

    #[test]
    fn test_localhost_bypass_attempt() {
        // These should all be detected as localhost (bypass attempts)
        let bypass_attempts = vec![
            "127.0.0.1",
            "127.1.1.1",
            "127.255.255.254",
            "::1",
            "localhost",
            "LOCALHOST",
        ];

        for attempt in bypass_attempts {
            assert!(
                is_localhost_address(attempt),
                "Failed to detect localhost bypass attempt: {}",
                attempt
            );
        }
    }

    #[test]
    fn test_loopback_proxy_headers_are_trusted() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("203.0.113.8, 10.0.0.1"),
        );

        let ip = extract_client_ip_addr_with_trusted_ranges(
            Some("127.0.0.1".parse().unwrap()),
            &headers,
            &[],
        );

        assert_eq!(ip, Some("203.0.113.8".parse().unwrap()));
    }

    #[test]
    fn test_trusted_proxy_range_allows_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("203.0.113.8"),
        );
        let trusted_ranges =
            kalamdb_configs::parse_trusted_proxy_entries(&["10.0.0.0/8".to_string()]).unwrap();

        let ip = extract_client_ip_addr_with_trusted_ranges(
            Some("10.0.1.9".parse().unwrap()),
            &headers,
            &trusted_ranges,
        );

        assert_eq!(ip, Some("203.0.113.8".parse().unwrap()));
    }

    #[test]
    fn test_untrusted_proxy_headers_are_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("203.0.113.8"),
        );

        let ip = extract_client_ip_addr_with_trusted_ranges(
            Some("10.0.1.9".parse().unwrap()),
            &headers,
            &[],
        );

        assert_eq!(ip, Some("10.0.1.9".parse().unwrap()));
    }

    #[test]
    fn test_localhost_spoofing_is_rejected_even_for_trusted_proxy() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("127.0.0.1"),
        );
        let trusted_ranges =
            kalamdb_configs::parse_trusted_proxy_entries(&["10.0.0.0/8".to_string()]).unwrap();

        let ip = extract_client_ip_addr_with_trusted_ranges(
            Some("10.0.1.9".parse().unwrap()),
            &headers,
            &trusted_ranges,
        );

        assert_eq!(ip, Some("10.0.1.9".parse().unwrap()));
    }
}
