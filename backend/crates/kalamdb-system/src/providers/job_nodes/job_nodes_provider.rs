//! System.job_nodes table provider

use std::sync::{Arc, OnceLock};

use chrono::Utc;
use datafusion::arrow::{array::RecordBatch, datatypes::SchemaRef};
use kalamdb_commons::{
    models::{rows::SystemTableRow, JobNodeId},
    JobId, NodeId, SystemTable,
};
use kalamdb_store::{entity_store::EntityStore, IndexedEntityStore, StorageBackend};

use crate::{
    error::{SystemError, SystemResultExt},
    providers::{
        base::{system_rows_to_batch, IndexedProviderDefinition},
        job_nodes::models::JobNode,
    },
    system_row_mapper::{model_to_system_row, system_row_to_model},
    JobStatus,
};

pub type JobNodesStore = IndexedEntityStore<JobNodeId, SystemTableRow>;

#[derive(Clone)]
pub struct JobNodesTableProvider {
    store: JobNodesStore,
}

impl JobNodesTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            SystemTable::JobNodes
                .column_family_name()
                .expect("JobNodes is a table, not a view"),
            Vec::new(),
        );
        Self { store }
    }

    pub fn create_job_node(&self, job_node: JobNode) -> Result<String, SystemError> {
        let key = job_node.id();
        let row = Self::encode_job_node_row(&job_node)?;
        self.store.insert(&key, &row).into_system_error("insert job_node error")?;
        Ok(format!("Job node {} created", key))
    }

    pub fn get_job_node(
        &self,
        job_id: &kalamdb_commons::JobId,
        node_id: &NodeId,
    ) -> Result<Option<JobNode>, SystemError> {
        let key = JobNodeId::new(job_id, node_id);
        let row = self.store.get(&key)?;
        row.map(|value| Self::decode_job_node_row(&value)).transpose()
    }

    pub async fn get_job_node_async(
        &self,
        job_id: &kalamdb_commons::JobId,
        node_id: &NodeId,
    ) -> Result<Option<JobNode>, SystemError> {
        let key = JobNodeId::new(job_id, node_id);
        let row = self.store.get_async(key).await.into_system_error("get_async job_node error")?;
        row.map(|value| Self::decode_job_node_row(&value)).transpose()
    }

    pub async fn update_job_node_async(&self, job_node: JobNode) -> Result<(), SystemError> {
        let key = job_node.id();
        let row = Self::encode_job_node_row(&job_node)?;
        self.store
            .insert_async(key, row)
            .await
            .into_system_error("update_async job_node error")
    }

    pub async fn list_for_node_with_statuses_async(
        &self,
        node_id: &NodeId,
        statuses: &[JobStatus],
        limit: usize,
    ) -> Result<Vec<JobNode>, SystemError> {
        let prefix = JobNodeId::prefix_for_node(node_id);
        let scan_limit = if limit == 0 {
            10_000
        } else {
            limit.saturating_mul(10)
        };
        let rows = {
            let store = self.store.clone();
            tokio::task::spawn_blocking(move || {
                store.scan_with_raw_prefix(&prefix, None, scan_limit)
            })
            .await
            .into_system_error("scan_async job_nodes join error")?
            .into_system_error("scan_async job_nodes error")?
        };

        let mut filtered = Vec::with_capacity(rows.len());
        for (_, row) in rows {
            let node = Self::decode_job_node_row(&row)?;
            if statuses.contains(&node.status) {
                filtered.push(node);
            }
        }

        if limit > 0 && filtered.len() > limit {
            filtered.truncate(limit);
        }

        Ok(filtered)
    }

    pub async fn list_for_job_id_async(&self, job_id: &JobId) -> Result<Vec<JobNode>, SystemError> {
        let rows: Vec<(Vec<u8>, SystemTableRow)> = self
            .store
            .scan_all_async(None, None, None)
            .await
            .into_system_error("scan_async job_nodes error")?;
        let mut filtered = Vec::new();
        for (_, row) in rows {
            let node = Self::decode_job_node_row(&row)?;
            if &node.job_id == job_id {
                filtered.push(node);
            }
        }

        Ok(filtered)
    }

    fn create_batch(
        &self,
        rows: Vec<(JobNodeId, SystemTableRow)>,
    ) -> Result<RecordBatch, SystemError> {
        let rows = rows.into_iter().map(|(_, row)| row).collect();
        system_rows_to_batch(&Self::schema(), rows)
    }

    /// Delete job_nodes older than retention period (in days).
    ///
    /// Only deletes terminal statuses: Completed, Failed, Cancelled.
    /// Uses finished_at/started_at/updated_at as reference time.
    pub fn cleanup_old_job_nodes(&self, retention_days: i64) -> Result<usize, SystemError> {
        let now = Utc::now().timestamp_millis();
        let retention_ms = retention_days * 24 * 60 * 60 * 1000;
        let cutoff_time = now - retention_ms;

        let rows = self
            .store
            .scan_all_typed(None, None, None)
            .into_system_error("scan job_nodes error")?;

        let mut deleted = 0;

        for (_key, row) in rows {
            let node = Self::decode_job_node_row(&row)?;
            if !matches!(
                node.status,
                JobStatus::Completed
                    | JobStatus::Failed
                    | JobStatus::Cancelled
                    | JobStatus::Skipped
            ) {
                continue;
            }

            let reference_time = node.finished_at.or(node.started_at).unwrap_or(node.updated_at);

            if reference_time < cutoff_time {
                self.store.delete(&node.id()).into_system_error("delete job_node error")?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    fn encode_job_node_row(job_node: &JobNode) -> Result<SystemTableRow, SystemError> {
        model_to_system_row(job_node, &JobNode::definition())
    }

    fn decode_job_node_row(row: &SystemTableRow) -> Result<JobNode, SystemError> {
        system_row_to_model(row, &JobNode::definition())
    }
}

crate::impl_system_table_provider_metadata!(
    indexed,
    provider = JobNodesTableProvider,
    key = JobNodeId,
    table_name = SystemTable::JobNodes.table_name(),
    primary_key_column = "id",
    parse_key = |value| JobNodeId::from_string(value).ok(),
    schema = JobNode::definition()
        .to_arrow_schema()
        .expect("failed to build job_nodes schema")
);

crate::impl_indexed_system_table_provider!(
    provider = JobNodesTableProvider,
    key = JobNodeId,
    value = SystemTableRow,
    store = store,
    definition = provider_definition,
    build_batch = create_batch
);
