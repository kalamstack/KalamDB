use kalam_pg_common::{EmbeddedRuntimeConfig, KalamPgError, RemoteServerConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Parsed foreign-server options for the PostgreSQL extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerOptions {
    pub embedded_runtime: Option<EmbeddedRuntimeConfig>,
    pub remote: Option<RemoteServerConfig>,
}

impl ServerOptions {
    /// Parse typed server options from raw FDW option pairs.
    pub fn parse(options: &BTreeMap<String, String>) -> Result<Self, KalamPgError> {
        let has_remote = options.contains_key("host") || options.contains_key("port");
        let has_embedded =
            options.contains_key("storage_base_path") || options.contains_key("node_id");

        if has_remote && has_embedded {
            return Err(KalamPgError::Validation(
                "server options cannot mix remote and embedded configuration".to_string(),
            ));
        }

        if has_remote {
            let host = options
                .get("host")
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    KalamPgError::Validation(
                        "server option 'host' is required in remote mode".to_string(),
                    )
                })?
                .to_string();

            let port = options
                .get("port")
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    KalamPgError::Validation(
                        "server option 'port' is required in remote mode".to_string(),
                    )
                })?
                .parse::<u16>()
                .map_err(|err| {
                    KalamPgError::Validation(format!(
                        "server option 'port' must be a valid u16: {}",
                        err
                    ))
                })?;

            return Ok(Self {
                embedded_runtime: None,
                remote: Some(RemoteServerConfig { host, port }),
            });
        }

        let storage_base_path =
            options.get("storage_base_path").map(PathBuf::from).ok_or_else(|| {
                KalamPgError::Validation(
                    "server option 'storage_base_path' is required in embedded mode".to_string(),
                )
            })?;

        let node_id = options
            .get("node_id")
            .cloned()
            .unwrap_or_else(|| EmbeddedRuntimeConfig::default().node_id);

        Ok(Self {
            embedded_runtime: Some(EmbeddedRuntimeConfig {
                storage_base_path,
                node_id,
                http: Default::default(),
            }),
            remote: None,
        })
    }
}
