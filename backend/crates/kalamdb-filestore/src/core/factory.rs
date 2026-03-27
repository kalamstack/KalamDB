//! Unified ObjectStore factory for all storage backends.
//!
//! Uses `object_store` crate uniformly for local filesystem and cloud storage.
//! No if/else branching - all backends implement the same `ObjectStore` trait.
//!
//! # Timeout Configuration
//!
//! Timeouts are configured programmatically via `ClientOptions` from server.toml:
//! - `request_timeout_secs` - timeout for S3/GCS/Azure operations (default: 60s)
//! - `connect_timeout_secs` - timeout for connection establishment (default: 10s)
//!
//! These values are read from `server.toml` [storage.remote_timeouts] section
//! and applied to all remote storage backends (S3, GCS, Azure).

#[cfg(any(feature = "cloud-aws", feature = "cloud-gcp", feature = "cloud-azure"))]
use crate::core::paths::parse_remote_url;
use crate::error::{FilestoreError, Result};
use kalamdb_configs::config::types::RemoteStorageTimeouts;
use kalamdb_system::providers::storages::models::{
    StorageLocationConfig, StorageLocationConfigError,
};
use kalamdb_system::Storage;
#[cfg(feature = "cloud-aws")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "cloud-azure")]
use object_store::azure::MicrosoftAzureBuilder;
#[cfg(feature = "cloud-gcp")]
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::prefix::PrefixStore;
#[cfg(any(feature = "cloud-aws", feature = "cloud-gcp", feature = "cloud-azure"))]
use object_store::ClientOptions;
use object_store::ObjectStore;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(any(feature = "cloud-aws", feature = "cloud-gcp", feature = "cloud-azure"))]
use std::time::Duration;

/// Build an `ObjectStore` instance from a Storage entity.
///
/// All storage types (local, S3, GCS, Azure) are unified under `Arc<dyn ObjectStore>`.
/// For local storage, uses `LocalFileSystem` which implements the same trait.
///
/// Timeouts are applied from server config for remote storage backends.
pub fn build_object_store(
    storage: &Storage,
    timeouts: &RemoteStorageTimeouts,
) -> Result<Arc<dyn ObjectStore>> {
    let config = resolve_config(storage)?;

    // Suppress unused warning when no cloud features are enabled
    #[cfg(not(any(feature = "cloud-aws", feature = "cloud-gcp", feature = "cloud-azure")))]
    let _ = timeouts;

    match config {
        StorageLocationConfig::Local(_) => build_local(storage),
        #[cfg(feature = "cloud-aws")]
        StorageLocationConfig::S3(cfg) => build_s3(storage, &cfg, timeouts),
        #[cfg(feature = "cloud-gcp")]
        StorageLocationConfig::Gcs(cfg) => build_gcs(storage, &cfg, timeouts),
        #[cfg(feature = "cloud-azure")]
        StorageLocationConfig::Azure(cfg) => build_azure(storage, &cfg, timeouts),
        #[allow(unreachable_patterns)]
        _ => Err(FilestoreError::Config(
            "This storage backend was not compiled in. Enable the corresponding cloud-* feature.".into(),
        )),
    }
}

/// Build ClientOptions with timeouts from server configuration.
#[cfg(any(feature = "cloud-aws", feature = "cloud-gcp", feature = "cloud-azure"))]
fn build_client_options(timeouts: &RemoteStorageTimeouts) -> Option<ClientOptions> {
    let mut client_options = ClientOptions::new();
    client_options =
        client_options.with_timeout(Duration::from_secs(timeouts.request_timeout_secs));
    client_options =
        client_options.with_connect_timeout(Duration::from_secs(timeouts.connect_timeout_secs));

    Some(client_options)
}

/// Resolve the storage location config, falling back to defaults based on storage_type.
fn resolve_config(storage: &Storage) -> Result<StorageLocationConfig> {
    match storage.location_config() {
        Ok(cfg) => Ok(cfg),
        Err(StorageLocationConfigError::MissingConfigJson) => {
            // Fall back based on storage_type field
            use kalamdb_system::providers::storages::models::StorageType;
            Ok(match storage.storage_type {
                StorageType::S3 => StorageLocationConfig::S3(Default::default()),
                StorageType::Gcs => StorageLocationConfig::Gcs(Default::default()),
                StorageType::Azure => StorageLocationConfig::Azure(Default::default()),
                StorageType::Filesystem => StorageLocationConfig::Local(Default::default()),
            })
        },
        Err(e) => Err(FilestoreError::Config(e.to_string())),
    }
}

fn build_local(storage: &Storage) -> Result<Arc<dyn ObjectStore>> {
    let base = storage.base_directory.trim();
    if base.is_empty() {
        return Err(FilestoreError::Config(
            "Local storage requires non-empty base_directory".into(),
        ));
    }

    let path = PathBuf::from(base);

    // Ensure the directory exists before canonicalizing
    // LocalFileSystem::new_with_prefix requires an absolute path that exists
    if !path.exists() {
        std::fs::create_dir_all(&path).map_err(|e| {
            FilestoreError::Config(format!(
                "Failed to create storage directory '{}': {}",
                path.display(),
                e
            ))
        })?;
    }

    // Canonicalize to get absolute path (resolves ., .., symlinks)
    let absolute_path = path.canonicalize().map_err(|e| {
        FilestoreError::Config(format!(
            "Failed to resolve absolute path for '{}': {}",
            path.display(),
            e
        ))
    })?;

    // LocalFileSystem with prefix handles path resolution automatically
    LocalFileSystem::new_with_prefix(absolute_path)
        .map(|fs| Arc::new(fs) as Arc<dyn ObjectStore>)
        .map_err(|e| FilestoreError::Config(format!("LocalFileSystem: {e}")))
}

#[cfg(feature = "cloud-aws")]
fn build_s3(
    storage: &Storage,
    cfg: &kalamdb_system::providers::storages::models::S3StorageConfig,
    timeouts: &RemoteStorageTimeouts,
) -> Result<Arc<dyn ObjectStore>> {
    let (bucket, prefix) = parse_remote_url(&storage.base_directory, &["s3://"])?;

    // Start with bucket name
    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

    // Always set region (even for S3-compatible)
    let region = cfg.region.as_deref().unwrap_or("us-east-1");
    builder = builder.with_region(region);

    // Set endpoint for S3-compatible services
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_endpoint(endpoint);
        // Ensure path-style requests for custom endpoints like MinIO
        builder = builder.with_virtual_hosted_style_request(false);
    }

    // Enable HTTP if specified (needed for local/MinIO)
    if cfg.allow_http {
        builder = builder.with_allow_http(true);
    }

    // Set credentials
    if let Some(ak) = &cfg.access_key_id {
        builder = builder.with_access_key_id(ak);
    }
    if let Some(sk) = &cfg.secret_access_key {
        builder = builder.with_secret_access_key(sk);
    }
    if let Some(token) = &cfg.session_token {
        builder = builder.with_token(token);
    }

    // Skip ClientOptions for S3-compatible endpoints (to avoid conflicts)
    if cfg.endpoint.is_none() {
        if let Some(client_options) = build_client_options(timeouts) {
            builder = builder.with_client_options(client_options);
        }
    }

    let store = builder.build().map_err(|e| FilestoreError::Config(format!("S3: {}", e)))?;

    wrap_with_prefix(store, &prefix)
}

#[cfg(feature = "cloud-gcp")]
fn build_gcs(
    storage: &Storage,
    cfg: &kalamdb_system::providers::storages::models::GcsStorageConfig,
    timeouts: &RemoteStorageTimeouts,
) -> Result<Arc<dyn ObjectStore>> {
    let (bucket, prefix) = parse_remote_url(&storage.base_directory, &["gs://", "gcs://"])?;

    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);

    if let Some(ref sa) = cfg.service_account_json {
        builder = builder.with_service_account_key(sa);
    }

    // Apply timeout configuration from server config
    if let Some(client_options) = build_client_options(timeouts) {
        builder = builder.with_client_options(client_options);
    }

    let store = builder.build().map_err(|e| FilestoreError::Config(format!("GCS: {e}")))?;

    wrap_with_prefix(store, &prefix)
}

#[cfg(feature = "cloud-azure")]
fn build_azure(
    storage: &Storage,
    cfg: &kalamdb_system::providers::storages::models::AzureStorageConfig,
    timeouts: &RemoteStorageTimeouts,
) -> Result<Arc<dyn ObjectStore>> {
    let (container, prefix) = parse_remote_url(&storage.base_directory, &["az://", "azure://"])?;

    let mut builder = MicrosoftAzureBuilder::new().with_container_name(&container);

    if let Some(ref account) = cfg.account_name {
        builder = builder.with_account(account);
    }
    if let Some(ref key) = cfg.access_key {
        builder = builder.with_access_key(key);
    }
    if let Some(ref sas) = cfg.sas_token {
        // Parse SAS token query string into key-value pairs
        let query_pairs: Vec<(String, String)> = sas
            .trim_start_matches('?')
            .split('&')
            .filter_map(|pair| pair.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())))
            .collect();
        builder = builder.with_sas_authorization(query_pairs);
    }

    // Apply timeout configuration from server config
    if let Some(client_options) = build_client_options(timeouts) {
        builder = builder.with_client_options(client_options);
    }

    let store = builder.build().map_err(|e| FilestoreError::Config(format!("Azure: {e}")))?;

    wrap_with_prefix(store, &prefix)
}

/// Wrap a store with a PrefixStore if prefix is non-empty.
fn wrap_with_prefix<T: ObjectStore + 'static>(
    store: T,
    prefix: &str,
) -> Result<Arc<dyn ObjectStore>> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        Ok(Arc::new(store) as Arc<dyn ObjectStore>)
    } else {
        let prefix_path =
            ObjectStorePath::parse(prefix).map_err(|e| FilestoreError::Path(e.to_string()))?;
        Ok(Arc::new(PrefixStore::new(store, prefix_path)) as Arc<dyn ObjectStore>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_object_store_filesystem() {
        use kalamdb_commons::models::ids::StorageId;
        use kalamdb_system::providers::storages::models::StorageType;
        use kalamdb_system::Storage;
        use std::env;

        let temp_dir = env::temp_dir().join("kalamdb_test_build_store");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let now = chrono::Utc::now().timestamp_millis();
        let storage = Storage {
            storage_id: StorageId::from("test_build"),
            storage_name: "test_build".to_string(),
            description: None,
            storage_type: StorageType::Filesystem,
            base_directory: temp_dir.to_string_lossy().to_string(),
            credentials: None,
            config_json: None,
            shared_tables_template: "{namespace}/{table}".to_string(),
            user_tables_template: "{namespace}/{user}/{table}".to_string(),
            created_at: now,
            updated_at: now,
        };

        let timeouts = RemoteStorageTimeouts::default();
        let result = build_object_store(&storage, &timeouts);
        assert!(result.is_ok(), "Should build filesystem store");

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
