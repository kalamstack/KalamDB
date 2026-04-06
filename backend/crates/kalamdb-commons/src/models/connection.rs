// Connection information detection for authentication

use std::sync::Arc;

/// Connection information for an authenticated request.
///
/// `remote_addr` is stored as `Arc<str>` — cheap to clone across middleware layers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionInfo {
    /// IP address of the connecting client (as string from actix-web)
    pub remote_addr: Option<Arc<str>>,
}

impl ConnectionInfo {
    /// Create a new ConnectionInfo from a remote address string.
    pub fn new(remote_addr: Option<String>) -> Self {
        Self {
            remote_addr: remote_addr.map(Arc::<str>::from),
        }
    }

    /// Check if the connection is from localhost.
    ///
    /// Handles both IPv4 (127.0.0.1) and IPv6 (::1) loopback addresses.
    pub fn is_localhost(&self) -> bool {
        match self.remote_addr.as_deref() {
            Some(addr) => {
                addr == "127.0.0.1"
                    || addr.starts_with("127.0.0.1:")
                    || addr == "localhost"
                    || addr.starts_with("localhost:")
                    || addr == "::1"
                    || addr.starts_with("[::1]")
            },
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_localhost_ipv4() {
        let conn = ConnectionInfo::new(Some("127.0.0.1".to_string()));
        assert!(conn.is_localhost());
    }

    #[test]
    fn test_localhost_ipv4_with_port() {
        let conn = ConnectionInfo::new(Some("127.0.0.1:8080".to_string()));
        assert!(conn.is_localhost());
    }

    #[test]
    fn test_localhost_name() {
        let conn = ConnectionInfo::new(Some("localhost".to_string()));
        assert!(conn.is_localhost());
    }

    #[test]
    fn test_localhost_ipv6() {
        let conn = ConnectionInfo::new(Some("::1".to_string()));
        assert!(conn.is_localhost());
    }

    #[test]
    fn test_localhost_ipv6_with_brackets() {
        let conn = ConnectionInfo::new(Some("[::1]:8080".to_string()));
        assert!(conn.is_localhost());
    }

    #[test]
    fn test_remote_ipv4() {
        let conn = ConnectionInfo::new(Some("192.168.1.100".to_string()));
        assert!(!conn.is_localhost());
    }

    #[test]
    fn test_none_address() {
        let conn = ConnectionInfo::new(None);
        assert!(!conn.is_localhost());
    }
}
