//! RocksDB initialization utilities for KalamDB.
//!
//! Provides a thin helper to open a RocksDB instance with required
//! system column families present.

use crate::cf_tuning::{apply_cf_settings, apply_db_settings};
use anyhow::Result;
use kalamdb_commons::system_tables::StoragePartition;
use kalamdb_commons::SystemTable;
use kalamdb_configs::RocksDbSettings;
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, DB};
use std::path::Path;
use std::sync::Arc;

/// RocksDB initializer for creating/opening a database with system CFs.
pub struct RocksDbInit {
    db_path: String,
    settings: RocksDbSettings,
}

impl RocksDbInit {
    /// Create a new initializer for the given path with custom settings.
    pub fn new(db_path: impl Into<String>, settings: RocksDbSettings) -> Self {
        Self {
            db_path: db_path.into(),
            settings,
        }
    }

    /// Create a new initializer with default settings.
    pub fn with_defaults(db_path: impl Into<String>) -> Self {
        Self::new(db_path, RocksDbSettings::default())
    }

    /// Open or create the RocksDB database and ensure system CFs exist.
    pub fn open(&self) -> Result<Arc<DB>> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        apply_db_settings(&mut db_opts, &self.settings);

        // Block cache: SHARED across all column families (memory efficient!)
        // Adding more CFs does NOT increase cache memory proportionally.
        // Example: 100 CFs with 4MB cache = still only 4MB total, not 400MB.
        // This is a key optimization for multi-tenant databases with many tables.
        let cache = Cache::new_lru_cache(self.settings.block_cache_size);
        let block_opts = create_block_options_with_cache(&cache);
        db_opts.set_block_based_table_factory(&block_opts);
        // NOTE: We intentionally do NOT call optimize_for_point_lookup() at the DB level.
        // That function switches memtables to a hash-based representation with 2-4x higher
        // fixed memory overhead per column family and internally creates a second block cache.
        // The bloom filters and block cache are already configured via set_block_based_table_factory()
        // which provides the read-path optimizations without the memory penalty.

        let (db, _) = self.open_internal(&db_opts, &cache)?;
        Ok(db)
    }

    /// Open and return both the DB and the list of column family names.
    ///
    /// This avoids re-listing column families from disk after opening, which
    /// can noticeably add startup time when there are many partitions.
    pub fn open_with_cf_names(&self) -> Result<(Arc<DB>, Vec<String>)> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        apply_db_settings(&mut db_opts, &self.settings);

        let cache = Cache::new_lru_cache(self.settings.block_cache_size);
        let block_opts = create_block_options_with_cache(&cache);
        db_opts.set_block_based_table_factory(&block_opts);

        self.open_internal(&db_opts, &cache)
    }

    fn open_internal(&self, db_opts: &Options, cache: &Cache) -> Result<(Arc<DB>, Vec<String>)> {
        let path = Path::new(&self.db_path);
        let cf_names = self.collect_column_family_names(db_opts, path);

        // Build CF descriptors with memory-optimized options
        let cf_descriptors: Vec<_> = cf_names
            .iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                apply_cf_settings(&mut cf_opts, &self.settings, name);
                cf_opts.set_block_based_table_factory(&create_block_options_with_cache(&cache));
                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)?;
        let db = Arc::new(db);

        // Compact all column families on startup if enabled
        // This reduces SST file count and prevents "Too many open files" errors
        if self.settings.compact_on_startup {
            log::debug!("Running startup compaction for {} column families...", cf_names.len());
            let start = std::time::Instant::now();
            for cf_name in &cf_names {
                if let Some(cf) = db.cf_handle(cf_name) {
                    db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
                }
            }
            log::info!("Startup compaction completed in {:?}", start.elapsed());
        }

        Ok((db, cf_names))
    }

    fn collect_column_family_names(&self, db_opts: &Options, path: &Path) -> Vec<String> {
        let mut existing = match DB::list_cf(db_opts, path) {
            Ok(cfs) if !cfs.is_empty() => cfs,
            _ => vec!["default".to_string()],
        };

        for table in SystemTable::all_tables().iter() {
            if let Some(name) = table.column_family_name() {
                if !existing.iter().any(|existing_name| existing_name == name) {
                    existing.push(name.to_string());
                }
            }
        }

        let extra_partitions = [
            StoragePartition::InformationSchemaTables.name(),
            StoragePartition::SystemUsersUsernameIdx.name(),
            StoragePartition::SystemUsersRoleIdx.name(),
            StoragePartition::SystemUsersDeletedAtIdx.name(),
            StoragePartition::ManifestCache.name(),
            StoragePartition::SystemJobsStatusIdx.name(),
        ];

        for name in extra_partitions {
            if !existing.iter().any(|existing_name| existing_name == name) {
                existing.push(name.to_string());
            }
        }

        existing
    }

    /// Close database handle (drop Arc)
    pub fn close(_db: Arc<DB>) {}
}

pub(crate) fn create_block_options_with_cache(cache: &Cache) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(cache);
    // Bloom + cached metadata improve point/prefix lookups used by PK checks.
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_pin_top_level_index_and_filter(true);
    block_opts.set_whole_key_filtering(true);
    block_opts
}
