//! RocksDB-backed test helpers for kalamdb-store.

use anyhow::Result;
use rocksdb::{Options, DB};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

use crate::storage_trait::StorageBackend;

use super::{RocksDBBackend, RocksDbInit};

/// Test database wrapper that automatically cleans up on drop.
pub struct TestDb {
    /// RocksDB instance
    pub db: Arc<DB>,
    /// Temporary directory (kept alive for the duration of the test)
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl TestDb {
    /// Create a new test database with the specified column families.
    pub fn new(cf_names: &[&str]) -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open_cf(&opts, temp_dir.path(), cf_names)?;

        Ok(Self {
            db: Arc::new(db),
            temp_dir,
        })
    }

    /// Create a test database initialized with the standard system partitions.
    pub fn with_system_tables() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("rocksdb");
        std::fs::create_dir_all(&db_path)?;

        let init = RocksDbInit::with_defaults(db_path.to_string_lossy());
        let db = init.open()?;

        Ok(Self { db, temp_dir })
    }

    /// Return the test workspace root path backing this database.
    pub fn path(&self) -> &Path {
        self.temp_dir.path()
    }

    /// Create and return a storage directory within the test workspace.
    pub fn storage_dir(&self) -> Result<PathBuf> {
        let storage_dir = self.temp_dir.path().join("storage");
        std::fs::create_dir_all(&storage_dir)?;
        Ok(storage_dir)
    }

    /// Create a generic storage backend for this test database.
    pub fn backend(&self) -> Arc<dyn StorageBackend> {
        Arc::new(RocksDBBackend::new(Arc::clone(&self.db)))
    }

    /// Create a test database with common user table column families.
    pub fn with_user_tables() -> Result<Self> {
        Self::new(&["user_table:app:messages", "user_table:app:events"])
    }

    /// Create a test database with a single column family.
    pub fn single_cf(cf_name: &str) -> Result<Self> {
        Self::new(&[cf_name])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_db() {
        let test_db = TestDb::new(&["user_table:app:messages"]).unwrap();

        let cf = test_db.db.cf_handle("user_table:app:messages");
        assert!(cf.is_some());
    }

    #[test]
    fn test_with_user_tables() {
        let test_db = TestDb::with_user_tables().unwrap();

        assert!(test_db.db.cf_handle("user_table:app:messages").is_some());
        assert!(test_db.db.cf_handle("user_table:app:events").is_some());
    }

    #[test]
    fn test_single_cf() {
        let test_db = TestDb::single_cf("user_table:app:messages").unwrap();

        assert!(test_db.db.cf_handle("user_table:app:messages").is_some());
    }
}