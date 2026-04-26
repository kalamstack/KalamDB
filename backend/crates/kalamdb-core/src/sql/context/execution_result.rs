use arrow::{array::RecordBatch, datatypes::SchemaRef};

/// Result type for SQL execution
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    /// Generic success message (DDL operations, user management, etc.)
    Success { message: String },
    /// Query results (SELECT, SHOW, DESCRIBE)
    Rows {
        batches: Vec<RecordBatch>,
        row_count: usize,
        /// Optional schema for when batches is empty (0 rows returned)
        /// This preserves column information for the UI even when no data
        schema: Option<SchemaRef>,
    },
    /// INSERT result
    Inserted { rows_affected: usize },
    /// UPDATE result
    Updated { rows_affected: usize },
    /// DELETE result
    Deleted { rows_affected: usize },
    /// FLUSH result
    Flushed {
        tables: Vec<String>,
        bytes_written: u64,
    },
    /// Subscription metadata (for SUBSCRIBE/LIVE SELECT commands)
    Subscription {
        subscription_id: String,
        channel: String,
        select_query: String,
    },
    /// Job killed result
    JobKilled { job_id: String, status: String },
}

impl ExecutionResult {
    /// Get row_count or rows_affected for response serialization
    pub fn affected_rows(&self) -> usize {
        match self {
            ExecutionResult::Rows { row_count, .. } => *row_count,
            ExecutionResult::Inserted { rows_affected } => *rows_affected,
            ExecutionResult::Updated { rows_affected } => *rows_affected,
            ExecutionResult::Deleted { rows_affected } => *rows_affected,
            ExecutionResult::Flushed { tables, .. } => tables.len(),
            ExecutionResult::Success { .. } => 1,
            ExecutionResult::Subscription { .. } => 1,
            ExecutionResult::JobKilled { .. } => 1,
        }
    }
}
