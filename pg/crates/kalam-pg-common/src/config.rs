use crate::KalamPgError;
use serde::{Deserialize, Serialize};

/// Remote authentication mode for the PostgreSQL extension.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum RemoteAuthMode {
    /// No application-level auth header. Use only for unsecured local development or mTLS-only deployments.
    #[default]
    None,
    /// Send a pre-shared value in the gRPC `authorization` metadata header on `open_session`.
    StaticHeader,
    /// Send Basic credentials (login_user/login_password) in the gRPC `authorization` metadata on `open_session`.
    AccountLogin,
}

/// Remote server configuration consumed by the PostgreSQL extension.
///
/// All authentication flows use gRPC only — no separate HTTP API endpoint is needed.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RemoteServerConfig {
    pub host: String,
    pub port: u16,
    /// Connection/request timeout in milliseconds (0 = no timeout).
    #[serde(default)]
    pub timeout_ms: u64,
    /// Authentication mode for the remote PG RPC connection.
    #[serde(default)]
    pub auth_mode: RemoteAuthMode,
    /// Value to send as the gRPC `authorization` metadata header, e.g. `Bearer <jwt>`.
    /// Set via `CREATE SERVER … OPTIONS (auth_header '…')` or `ALTER SERVER … OPTIONS`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_header: Option<String>,
    /// Login user for `account_login` (sent as Basic auth on `open_session`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login_user: Option<String>,
    /// Login password for `account_login` (sent as Basic auth on `open_session`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login_password: Option<String>,
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

impl std::fmt::Debug for RemoteServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteServerConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("timeout_ms", &self.timeout_ms)
            .field("auth_mode", &self.effective_auth_mode())
            .field("has_auth_header", &self.auth_header.is_some())
            .field("login_user", &self.login_user)
            .field("has_login_password", &self.login_password.is_some())
            .field("has_ca_cert", &self.ca_cert.is_some())
            .field("has_client_cert", &self.client_cert.is_some())
            .field("has_client_key", &self.client_key.is_some())
            .finish()
    }
}

impl RemoteServerConfig {
    /// Return the normalized auth mode, preserving legacy `auth_header`-only configs.
    pub fn effective_auth_mode(&self) -> RemoteAuthMode {
        match self.auth_mode {
            RemoteAuthMode::None if self.auth_header.is_some() => RemoteAuthMode::StaticHeader,
            mode => mode,
        }
    }

    /// Whether TLS should be used for this connection.
    pub fn tls_enabled(&self) -> bool {
        self.ca_cert.is_some()
    }

    /// Build the endpoint URI (`http://` or `https://`).
    pub fn endpoint_uri(&self) -> String {
        let scheme = if self.tls_enabled() { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }

    /// Validate cross-field auth and TLS configuration.
    pub fn validate(&self) -> Result<(), KalamPgError> {
        if self.client_cert.is_some() != self.client_key.is_some() {
            return Err(KalamPgError::Validation(
                "server options 'client_cert' and 'client_key' must both be provided for mTLS"
                    .to_string(),
            ));
        }

        match self.effective_auth_mode() {
            RemoteAuthMode::None => {
                if self.login_user.is_some() || self.login_password.is_some() {
                    return Err(KalamPgError::Validation(
                        "server options 'login_user' and 'login_password' require auth_mode 'account_login'"
                            .to_string(),
                    ));
                }
            },
            RemoteAuthMode::StaticHeader => {
                if self.auth_header.as_deref().map(str::trim).unwrap_or("").is_empty() {
                    return Err(KalamPgError::Validation(
                        "server option 'auth_header' is required when auth_mode is 'static_header'"
                            .to_string(),
                    ));
                }
                if self.login_user.is_some() || self.login_password.is_some() {
                    return Err(KalamPgError::Validation(
                        "server options 'login_user' and 'login_password' are only valid with auth_mode 'account_login'"
                            .to_string(),
                    ));
                }
            },
            RemoteAuthMode::AccountLogin => {
                if self.auth_header.is_some() {
                    return Err(KalamPgError::Validation(
                        "server option 'auth_header' cannot be used when auth_mode is 'account_login'"
                            .to_string(),
                    ));
                }
                if self.login_user.as_deref().map(str::trim).unwrap_or("").is_empty() {
                    return Err(KalamPgError::Validation(
                        "server option 'login_user' is required when auth_mode is 'account_login'"
                            .to_string(),
                    ));
                }
                if self
                    .login_password
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty()
                {
                    return Err(KalamPgError::Validation(
                        "server option 'login_password' is required when auth_mode is 'account_login'"
                            .to_string(),
                    ));
                }
            },
        }

        Ok(())
    }
}

impl Default for RemoteServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 50051,
            timeout_ms: 0,
            auth_mode: RemoteAuthMode::None,
            auth_header: None,
            login_user: None,
            login_password: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
        }
    }
}
