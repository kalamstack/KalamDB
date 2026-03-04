//! RocksDB initialization utilities for KalamDB.
//!
//! Provides a thin helper to open a RocksDB instance with required
//! system column families present.

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
        let path = Path::new(&self.db_path);

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Memory optimization: Use configured settings instead of hardcoded values
        db_opts.set_write_buffer_size(self.settings.write_buffer_size);
        db_opts.set_max_write_buffer_number(self.settings.max_write_buffers);
        db_opts.set_max_background_jobs(self.settings.max_background_jobs);
        db_opts.increase_parallelism(self.settings.max_background_jobs);

        // Limit open files to prevent "Too many open files" errors
        // This is critical when there are many SST files
        db_opts.set_max_open_files(self.settings.max_open_files);

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

        // Determine existing CFs (or default if DB missing)
        let mut existing = match DB::list_cf(&db_opts, path) {
            Ok(cfs) if !cfs.is_empty() => cfs,
            _ => vec!["default".to_string()],
        };

        // Ensure system CFs using single source of truth (SystemTable + StoragePartition)
        // 1) All system tables' CFs (skip views which have no column family)
        for table in SystemTable::all_tables().iter() {
            if let Some(name) = table.column_family_name() {
                if !existing.iter().any(|n| n == name) {
                    existing.push(name.to_string());
                }
            }
        }

        // 2) Additional named partitions used by the system
        let extra_partitions = [
            StoragePartition::InformationSchemaTables.name(),
            StoragePartition::SystemColumns.name(),
            StoragePartition::UserTableCounters.name(),
            StoragePartition::SystemUsersUsernameIdx.name(),
            StoragePartition::SystemUsersRoleIdx.name(),
            StoragePartition::SystemUsersDeletedAtIdx.name(),
            StoragePartition::ManifestCache.name(),
            StoragePartition::SystemJobsStatusIdx.name(),
        ];

        for name in extra_partitions.iter() {
            if !existing.iter().any(|n| n == name) {
                existing.push((*name).to_string());
            }
        }

        // Build CF descriptors with memory-optimized options
        let cf_descriptors: Vec<_> = existing
            .iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                apply_cf_settings(&mut cf_opts, &self.settings);
                cf_opts.set_block_based_table_factory(&create_block_options_with_cache(&cache));
                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        let cf_names: Vec<String> = existing.clone();
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

        Ok(db)
    }

    /// Open and return both the DB and the list of column family names.
    ///
    /// This is useful for detecting orphaned column families at startup.
    pub fn open_with_cf_names(&self) -> Result<(Arc<DB>, Vec<String>)> {
        let db = self.open()?;
        // Re-list CFs from disk to get the authoritative list
        let path = std::path::Path::new(&self.db_path);
        let mut db_opts = Options::default();
        db_opts.create_if_missing(false);
        let cf_names = DB::list_cf(&db_opts, path).unwrap_or_else(|_| vec!["default".to_string()]);
        Ok((db, cf_names))
    }

    /// Close database handle (drop Arc)
    pub fn close(_db: Arc<DB>) {}
}

fn apply_cf_settings(cf_opts: &mut Options, settings: &RocksDbSettings) {
    cf_opts.set_write_buffer_size(settings.write_buffer_size);
    cf_opts.set_max_write_buffer_number(settings.max_write_buffers);
    // NOTE: We intentionally do NOT call optimize_for_point_lookup() per-CF.
    // That function switches the memtable to a hash-based representation which
    // has significantly higher fixed memory overhead per column family (~2-4x).
    // With 50-100+ CFs this wastes tens of MB. The DB-level call already sets
    // the read-path optimizations (bloom filter, block cache) via
    // set_block_based_table_factory() which is applied per-CF separately.
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
