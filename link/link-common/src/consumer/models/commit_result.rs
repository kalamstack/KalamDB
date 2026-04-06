use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResult {
    pub acknowledged_offset: u64,
    pub group_id: String,
    pub partition_id: u32,
}
