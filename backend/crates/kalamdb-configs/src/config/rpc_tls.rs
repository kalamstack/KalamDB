use serde::{Deserialize, Serialize};

/// Unified TLS/mTLS configuration for the gRPC listener.
///
/// Serves both cluster inter-node traffic and PG extension connections on
/// the same TCP port. Each field accepts either a filesystem path or an
/// inline PEM string (detected by the presence of `-----BEGIN`).
///
/// When `enabled = true`, all three of `ca_cert`, `server_cert`, and
/// `server_key` are required.
///
/// # Example (server.toml)
///
/// ```toml
/// [rpc_tls]
/// enabled = true
/// ca_cert = "/etc/kalamdb/certs/ca.pem"
/// server_cert = "/etc/kalamdb/certs/node1.pem"
/// server_key  = "/etc/kalamdb/certs/node1.key"
/// require_client_cert = true
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RpcTlsConfig {
    /// Enable TLS/mTLS on the gRPC listener.
    #[serde(default)]
    pub enabled: bool,

    /// CA certificate used to validate incoming client certificates.
    /// Accepts a file path or an inline PEM string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert: Option<String>,

    /// Server identity certificate.
    /// Accepts a file path or an inline PEM string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_cert: Option<String>,

    /// Private key for `server_cert`.
    /// Accepts a file path or an inline PEM string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_key: Option<String>,

    /// Require clients to present a certificate signed by `ca_cert`.
    /// Default: `true` (full mTLS).
    #[serde(default = "default_require_client_cert")]
    pub require_client_cert: bool,
}

fn default_require_client_cert() -> bool {
    true
}

/// Load PEM material from either a file path or an inline PEM string.
///
/// If `value` contains `-----BEGIN`, it is treated as an inline PEM and
/// returned directly as bytes. Otherwise it is treated as a filesystem path
/// and the file contents are read.
pub fn load_pem(value: &str) -> Result<Vec<u8>, String> {
    if value.contains("-----BEGIN") {
        Ok(value.as_bytes().to_vec())
    } else {
        std::fs::read(value)
            .map_err(|e| format!("Failed reading PEM file '{}': {}", value, e))
    }
}

impl RpcTlsConfig {
    /// Validate that all required fields are present when TLS is enabled.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.ca_cert.is_none() {
            return Err("rpc_tls.enabled=true but ca_cert is not configured".to_string());
        }
        if self.server_cert.is_none() {
            return Err("rpc_tls.enabled=true but server_cert is not configured".to_string());
        }
        if self.server_key.is_none() {
            return Err("rpc_tls.enabled=true but server_key is not configured".to_string());
        }
        Ok(())
    }

    /// Load the CA certificate bytes.
    pub fn load_ca_cert(&self) -> Result<Vec<u8>, String> {
        let value = self.ca_cert.as_deref()
            .ok_or("ca_cert is not configured")?;
        load_pem(value)
    }

    /// Load the server certificate bytes.
    pub fn load_server_cert(&self) -> Result<Vec<u8>, String> {
        let value = self.server_cert.as_deref()
            .ok_or("server_cert is not configured")?;
        load_pem(value)
    }

    /// Load the server private key bytes.
    pub fn load_server_key(&self) -> Result<Vec<u8>, String> {
        let value = self.server_key.as_deref()
            .ok_or("server_key is not configured")?;
        load_pem(value)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_pem_inline() {
        let pem = "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----";
        let bytes = load_pem(pem).expect("inline PEM");
        assert_eq!(bytes, pem.as_bytes());
    }

    #[test]
    fn load_pem_file_not_found() {
        let result = load_pem("/nonexistent/path/cert.pem");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed reading PEM file"));
    }

    #[test]
    fn validate_disabled_ok() {
        let config = RpcTlsConfig::default();
        assert!(!config.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_enabled_missing_ca() {
        let config = RpcTlsConfig {
            enabled: true,
            ca_cert: None,
            server_cert: Some("cert".to_string()),
            server_key: Some("key".to_string()),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("ca_cert"));
    }

    #[test]
    fn validate_enabled_missing_server_cert() {
        let config = RpcTlsConfig {
            enabled: true,
            ca_cert: Some("ca".to_string()),
            server_cert: None,
            server_key: Some("key".to_string()),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("server_cert"));
    }

    #[test]
    fn validate_enabled_missing_server_key() {
        let config = RpcTlsConfig {
            enabled: true,
            ca_cert: Some("ca".to_string()),
            server_cert: Some("cert".to_string()),
            server_key: None,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("server_key"));
    }

    #[test]
    fn validate_enabled_all_present() {
        let config = RpcTlsConfig {
            enabled: true,
            ca_cert: Some("-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----".to_string()),
            server_cert: Some("-----BEGIN CERTIFICATE-----\nSRV\n-----END CERTIFICATE-----".to_string()),
            server_key: Some("-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----".to_string()),
            require_client_cert: true,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn load_ca_cert_inline() {
        let pem = "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----";
        let config = RpcTlsConfig {
            ca_cert: Some(pem.to_string()),
            ..Default::default()
        };
        let bytes = config.load_ca_cert().expect("load ca cert");
        assert_eq!(bytes, pem.as_bytes());
    }

    #[test]
    fn load_ca_cert_not_configured() {
        let config = RpcTlsConfig::default();
        assert!(config.load_ca_cert().is_err());
    }

    #[test]
    fn default_require_client_cert_is_true() {
        let toml_str = r#"
            enabled = true
            ca_cert = "ca.pem"
            server_cert = "srv.pem"
            server_key = "srv.key"
        "#;
        let config: RpcTlsConfig = toml::from_str(toml_str).expect("parse toml");
        assert!(config.require_client_cert);
    }

    #[test]
    fn serde_round_trip() {
        let config = RpcTlsConfig {
            enabled: true,
            ca_cert: Some("/path/to/ca.pem".to_string()),
            server_cert: Some("/path/to/node.pem".to_string()),
            server_key: Some("/path/to/node.key".to_string()),
            require_client_cert: false,
        };
        let toml_str = toml::to_string(&config).expect("serialize");
        let parsed: RpcTlsConfig = toml::from_str(&toml_str).expect("deserialize");
        assert_eq!(parsed.enabled, config.enabled);
        assert_eq!(parsed.ca_cert, config.ca_cert);
        assert_eq!(parsed.server_cert, config.server_cert);
        assert_eq!(parsed.server_key, config.server_key);
        assert_eq!(parsed.require_client_cert, config.require_client_cert);
    }
}
