//! Manifest Management Module
//!
//! Provides manifest.json tracking and caching for Parquet batch files.
//!
//! Architecture:
//! - ManifestService: Unified service with hot cache (moka) + RocksDB persistence + cold storage
//! - FlushManifestHelper: Helper for manifest operations during flush
//! - ManifestAccessPlanner: Query planner for manifest-based segment selection
//! - flush: RocksDB-to-Parquet flush implementations
//! - manifest_helpers: Manifest helpers for providers

pub mod flush;
mod flush_helper;
pub use kalamdb_tables::manifest::{manifest_helpers, planner};
mod service;

pub use flush::{
    FlushDedupStats, FlushJobResult, FlushMetadata, SharedTableFlushJob, SharedTableFlushMetadata,
    TableFlush, UserTableFlushJob, UserTableFlushMetadata,
};
pub use flush_helper::FlushManifestHelper;
pub use kalamdb_tables::manifest::{
    ensure_manifest_ready, load_row_from_parquet_by_seq, ManifestAccessPlanner, RowGroupSelection,
};
pub use service::ManifestService;
