//! Domain-typed operation request/result types used by the PostgreSQL extension bridge.
//!
//! These types are transport-agnostic: they use `TableId`, `UserId`, `TableType`, `Row`,
//! and `RecordBatch` — all existing domain types. No tonic, no prost, no gRPC concern.

#[cfg(feature = "arrow")]
use arrow::record_batch::RecordBatch;

use crate::models::rows::Row;
use crate::models::UserId;
use crate::{TableId, TableType};

/// Domain-typed scan request.
pub struct ScanRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub session_id: Option<String>,
    pub columns: Vec<String>,
    pub limit: Option<usize>,
    pub user_id: Option<UserId>,
}

/// Domain-typed insert request.
pub struct InsertRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub session_id: Option<String>,
    pub user_id: Option<UserId>,
    pub rows: Vec<Row>,
}

/// Domain-typed update request.
pub struct UpdateRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub session_id: Option<String>,
    pub user_id: Option<UserId>,
    pub updates: Vec<Row>,
    pub pk_value: String,
}

/// Domain-typed delete request.
pub struct DeleteRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub session_id: Option<String>,
    pub user_id: Option<UserId>,
    pub pk_value: String,
}

/// Domain-typed scan result.
#[cfg(feature = "arrow")]
#[derive(Debug)]
pub struct ScanResult {
    pub batches: Vec<RecordBatch>,
}

/// Domain-typed mutation result.
#[derive(Debug)]
pub struct MutationResult {
    pub affected_rows: u64,
}
