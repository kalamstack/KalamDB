use serde::{Deserialize, Serialize};

/// Remote server configuration consumed by the PostgreSQL extension.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteServerConfig {
    pub host: String,
    pub port: u16,
    /// Connection/request timeout in milliseconds (0 = no timeout).
    #[serde(default)]
    pub timeout_ms: u64,
    /// Value to send as the gRPC `authorization` metadata header, e.g. `Bearer <jwt>`.
    /// Set via `CREATE SERVER … OPTIONS (auth_header '…')` or `ALTER SERVER … OPTIONS`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_header: Option<String>,
    /// CA certificate for validating the server (inline PEM or file path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert: Option<String>,
    /// Client certificate for mTLS (inline PEM or file path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_cert: Option<String>,
    /// Client private key for mTLS (inline PEM or file path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key: Option<String>,
}

impl RemoteServerConfig {
    /// Whether TLS should be used for this connection.
    pub fn tls_enabled(&self) -> bool {
        self.ca_cert.is_some()
    }

    /// Build the endpoint URI (`http://` or `https://`).
    pub fn endpoint_uri(&self) -> String {
        let scheme = if self.tls_enabled() { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }
}

impl Default for RemoteServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 50051,
            timeout_ms: 0,
            auth_header: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
        }
    }
}
