//! Job-node execution state for system.job_nodes table.

use kalamdb_commons::{
    datatypes::KalamDataType,
    models::ids::{JobId, JobNodeId, NodeId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

use crate::JobStatus;

#[table(name = "job_nodes", comment = "Per-node job execution state")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct JobNode {
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "Function(NOW)",
        comment = "Row creation timestamp"
    )]
    pub created_at: i64,
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "Function(NOW)",
        comment = "Row updated timestamp"
    )]
    pub updated_at: i64,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Timestamp),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Node start timestamp"
    )]
    pub started_at: Option<i64>,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Timestamp),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Node finish timestamp"
    )]
    pub finished_at: Option<i64>,
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Job identifier"
    )]
    pub job_id: JobId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Node identifier"
    )]
    pub node_id: NodeId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Per-node job status"
    )]
    pub status: JobStatus,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Error message if node failed"
    )]
    pub error_message: Option<String>,
}

impl JobNode {
    pub fn id(&self) -> JobNodeId {
        JobNodeId::new(&self.job_id, &self.node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system_row_mapper::{model_to_system_row, system_row_to_model};

    #[test]
    fn test_job_node_system_row_roundtrip_preserves_numeric_node_id() {
        let job_node = JobNode {
            created_at: 1730000000000,
            updated_at: 1730000003000,
            started_at: Some(1730000000100),
            finished_at: None,
            job_id: JobId::new("job_node_mapper_roundtrip"),
            node_id: NodeId::from(1u64),
            status: JobStatus::Queued,
            error_message: None,
        };

        let row =
            model_to_system_row(&job_node, &JobNode::definition()).expect("encode model to row");

        let node_id_scalar = row.fields.values.get("node_id").expect("node_id scalar must exist");
        assert!(matches!(node_id_scalar, datafusion::scalar::ScalarValue::Int64(Some(1))));

        let decoded: JobNode =
            system_row_to_model(&row, &JobNode::definition()).expect("decode row to model");
        assert_eq!(decoded, job_node);
    }

    #[test]
    fn test_job_node_definition_node_id_is_bigint() {
        let definition = JobNode::definition();
        let node_id_column = definition
            .columns
            .iter()
            .find(|column| column.column_name == "node_id")
            .expect("node_id column must exist");

        assert_eq!(node_id_column.data_type, KalamDataType::BigInt);
    }
}
