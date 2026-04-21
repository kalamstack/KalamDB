//! RocksDB backend implementation and supporting helpers.

mod backend;
mod cf_tuning;
mod init;
pub mod test_utils;

use std::path::Path;
use std::sync::Arc;

use crate::storage_trait::StorageBackend;

pub use backend::RocksDBBackend;
pub use init::RocksDbInit;

/// Open the RocksDB storage backend and return it through the generic storage trait.
pub fn open_storage_backend(
    db_path: &Path,
    settings: &kalamdb_configs::RocksDbSettings,
) -> anyhow::Result<(Arc<dyn StorageBackend>, usize)> {
    let db_init = RocksDbInit::new(db_path.to_string_lossy().into_owned(), settings.clone());
    let (db, cf_names) = db_init.open_with_cf_names()?;
    let backend = Arc::new(RocksDBBackend::with_options_and_settings(
        db,
        settings.sync_writes,
        settings.disable_wal,
        settings.clone(),
    ));
    backend.set_known_cf_names(cf_names.clone());
    Ok((backend, cf_names.len()))
}