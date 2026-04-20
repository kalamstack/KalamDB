use kalam_pg_common::{KalamPgError, RemoteAuthMode, RemoteServerConfig};
use std::collections::BTreeMap;

/// Parsed foreign-server options for the PostgreSQL extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerOptions {
    pub remote: Option<RemoteServerConfig>,
}

impl ServerOptions {
    /// Parse typed server options from raw FDW option pairs.
    ///
    /// Required options: `host`, `port`.
    /// Optional TLS options: `ca_cert`, `client_cert`, `client_key`.
    pub fn parse(options: &BTreeMap<String, String>) -> Result<Self, KalamPgError> {
        let host = options
            .get("host")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                KalamPgError::Validation("server option 'host' is required".to_string())
            })?
            .to_string();

        let port = options
            .get("port")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                KalamPgError::Validation("server option 'port' is required".to_string())
            })?
            .parse::<u16>()
            .map_err(|err| {
                KalamPgError::Validation(format!(
                    "server option 'port' must be a valid u16: {}",
                    err
                ))
            })?;

        let auth_mode = options
            .get("auth_mode")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| match value {
                "none" => Ok(RemoteAuthMode::None),
                "static_header" => Ok(RemoteAuthMode::StaticHeader),
                "account_login" => Ok(RemoteAuthMode::AccountLogin),
                _ => Err(KalamPgError::Validation(format!(
                    "server option 'auth_mode' must be one of: none, static_header, account_login (got '{}')",
                    value
                ))),
            })
            .transpose()?;

        let ca_cert = options
            .get("ca_cert")
            .map(String::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string);

        let client_cert = options
            .get("client_cert")
            .map(String::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string);

        let client_key = options
            .get("client_key")
            .map(String::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string);

        let timeout_ms = options
            .get("timeout")
            .map(String::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| {
                v.parse::<u64>().map_err(|err| {
                    KalamPgError::Validation(format!(
                        "server option 'timeout' must be a valid integer (milliseconds): {}",
                        err
                    ))
                })
            })
            .transpose()?
            .unwrap_or(0);

        let auth_header = options
            .get("auth_header")
            .map(String::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string);

        let login_user = options
            .get("login_user")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        let login_password = options
            .get("login_password")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        let auth_mode = match auth_mode {
            Some(mode) => mode,
            None if auth_header.is_some() => RemoteAuthMode::StaticHeader,
            None => RemoteAuthMode::None,
        };

        let remote = RemoteServerConfig {
            host,
            port,
            timeout_ms,
            auth_mode,
            auth_header,
            login_user,
            login_password,
            ca_cert,
            client_cert,
            client_key,
        };

        remote.validate()?;

        Ok(Self {
            remote: Some(remote),
        })
    }
}
