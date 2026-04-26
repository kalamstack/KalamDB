//! Table utilities module - unified provider support with BaseTableProvider trait
//!
//! **Phase 13: Provider Consolidation**
//!
//! This module introduces the new utils/ architecture that eliminates ~1200 lines
//! of duplicate code across User/Shared/Stream table providers.
//! Shared planning helpers now live in `kalamdb-datafusion-sources`, while this
//! module retains table-specific MVCC, Parquet, and DML behavior.

pub mod base;
pub mod core;
pub mod datafusion_dml;
pub mod dml_provider;
pub mod parquet;
pub mod pk; // Primary key utilities and existence checking
pub mod pk_utils; // Phase 13.7: Shared PK extraction and bloom filter utilities
pub mod row_utils;
pub mod unified_dml; // Phase 13.6: Moved from tables/
pub mod vector_staging;
pub mod version_resolution; // Phase 13.6: Moved from tables/

#[cfg(test)]
pub mod test_backend;

// Provider implementations live alongside table stores
pub mod users {
    pub use crate::user_tables::user_table_provider::*;
}
pub mod shared {
    pub use crate::shared_tables::shared_table_provider::*;
}
pub mod streams {
    pub use crate::stream_tables::stream_table_provider::*;
}

// Re-export key types for convenience
pub use core::TableServices;

pub use base::{BaseTableProvider, TableProviderCore};
pub use dml_provider::KalamTableProvider;
pub use shared::SharedTableProvider;
pub use streams::StreamTableProvider;
// Re-export unified DML functions
pub use unified_dml::{
    append_version, append_version_sync, extract_user_pk_value, validate_primary_key,
};
pub use users::UserTableProvider;

/// Provider consolidation summary
///
/// **Code Reduction**: ~1200 lines eliminated
/// - UserTableShared wrapper: ~200 lines
/// - Duplicate DML methods: ~800 lines
/// - Handler indirection: ~200 lines
///
/// **Architecture Benefits**:
/// - Generic trait over storage key (K) and value (V) types
/// - Shared core reduces memory overhead (Arc<TableProviderCore>)
/// - Direct DML implementation (no handler layer)
/// - Stateless providers with per-operation user_id passing
/// - SessionState extraction for DataFusion integration
pub const PROVIDER_CONSOLIDATION_VERSION: &str = "13.0.0";
