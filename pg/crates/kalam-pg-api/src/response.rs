use arrow::record_batch::RecordBatch;

/// Scan response returned by a backend executor.
#[derive(Debug, Clone)]
pub struct ScanResponse {
    pub batches: Vec<RecordBatch>,
}

impl ScanResponse {
    /// Create a response from collected Arrow batches.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }
}

/// Generic mutation response returned by a backend executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationResponse {
    pub affected_rows: u64,
}
