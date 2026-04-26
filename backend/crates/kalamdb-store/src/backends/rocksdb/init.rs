//! RocksDB initialization utilities for KalamDB.
//!
//! Provides a thin helper to open a RocksDB instance with required
//! fixed physical column families present.

use std::{path::Path, sync::Arc};

use anyhow::Result;
use kalamdb_configs::RocksDbSettings;
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, DB};

use super::{
    cf_tuning::{apply_cf_settings, apply_db_settings},
    keyspace::fixed_column_families,
};

/// RocksDB initializer for creating/opening a database with fixed physical CFs.
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

    /// Open or create the RocksDB database and ensure fixed physical CFs exist.
    pub fn open(&self) -> Result<Arc<DB>> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        apply_db_settings(&mut db_opts, &self.settings);

        let cache = Cache::new_lru_cache(self.settings.block_cache_size);
        let block_opts = create_block_options_with_cache(&cache);
        db_opts.set_block_based_table_factory(&block_opts);

        let (db, _) = self.open_internal(&db_opts, &cache)?;
        Ok(db)
    }

    /// Open and return both the DB and the list of column family names.
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

        for name in fixed_column_families() {
            if !existing.iter().any(|existing_name| existing_name == name) {
                existing.push(name.to_string());
            }
        }

        existing
    }
}

pub(crate) fn create_block_options_with_cache(cache: &Cache) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(cache);
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_pin_top_level_index_and_filter(true);
    block_opts.set_whole_key_filtering(true);
    block_opts
}
