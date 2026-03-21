//! Scan and modify state structs stored as `fdw_state` in FDW callbacks.

use datafusion::arrow::record_batch::RecordBatch;
use kalam_pg_api::KalamBackendExecutor;
use kalam_pg_fdw::TableOptions;
use std::sync::Arc;

#[cfg(feature = "embedded")]
use kalam_pg_embedded::EmbeddedKalamRuntime;

/// State stored in `ForeignScanState::fdw_state` during scan lifecycle.
pub struct KalamScanState {
    /// Arrow record batches returned by the backend executor.
    pub batches: Vec<RecordBatch>,
    /// Index of the current batch being iterated.
    pub batch_index: usize,
    /// Row index within the current batch.
    pub row_index: usize,
    /// Maps PG attribute index → Arrow column index (None for virtual/missing columns).
    pub column_mapping: Vec<Option<usize>>,
    /// Effective user_id for virtual `_userid` column injection.
    pub effective_user_id: Option<String>,
}

/// State stored in `ResultRelInfo::ri_FdwState` during modify lifecycle.
pub struct KalamModifyState {
    /// Parsed foreign table options.
    pub table_options: TableOptions,
    /// Backend executor for running mutations.
    pub executor: Arc<dyn KalamBackendExecutor>,
    /// Embedded runtime providing the tokio runtime for blocking on async.
    #[cfg(feature = "embedded")]
    pub runtime: Arc<EmbeddedKalamRuntime>,
    /// Remote tokio runtime for blocking on async in remote mode.
    #[cfg(feature = "remote")]
    pub runtime: Arc<tokio::runtime::Runtime>,
    /// Column names from the PG relation, in attribute order.
    pub column_names: Vec<String>,
    /// Name of the primary key column (for UPDATE/DELETE row identification).
    pub pk_column: String,
}
