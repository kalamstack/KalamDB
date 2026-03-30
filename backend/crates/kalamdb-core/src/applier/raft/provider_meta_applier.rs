//! ProviderMetaApplier - Raft state machine applier for metadata operations
//!
//! This applier implements the MetaApplier trait and delegates to the
//! CommandExecutorImpl for actual persistence. This ensures a SINGLE
//! code path for all mutations regardless of standalone/cluster mode.
//!
//! Used by the Raft state machine to apply replicated commands on followers.

use crate::app_context::AppContext;
use crate::applier::executor::CommandExecutorImpl;
use crate::applier::ApplierError;
use async_trait::async_trait;
use kalamdb_commons::models::schemas::TableDefinition;
use kalamdb_commons::models::{JobId, NamespaceId, NodeId, StorageId, TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_raft::applier::MetaApplier;
use kalamdb_raft::RaftError;
use kalamdb_system::providers::jobs::models::Job;
use kalamdb_system::JobStatus;
use kalamdb_system::User;
use kalamdb_system::{JobNode, Storage};
use std::sync::Arc;

/// Unified applier that persists all metadata operations to system tables
///
/// This is used by the Raft state machine on follower nodes to apply
/// replicated commands locally. It delegates to CommandExecutorImpl to ensure
/// a single code path for all mutations.
///
/// ## Architecture
///
/// - DDL operations (create/alter/drop table) → DdlExecutor
/// - Namespace operations → NamespaceExecutor
/// - User operations → UserExecutor
/// - Storage operations → StorageExecutor
/// - Job operations → Local (jobs are Raft-only, no SQL handlers)
pub struct ProviderMetaApplier {
    executor: CommandExecutorImpl,
    app_context: Arc<AppContext>,
}

impl ProviderMetaApplier {
    /// Create a new ProviderMetaApplier
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self {
            executor: CommandExecutorImpl::new(Arc::clone(&app_context)),
            app_context,
        }
    }
}

#[async_trait]
impl MetaApplier for ProviderMetaApplier {
    // =========================================================================
    // Namespace Operations - Delegate to NamespaceExecutor
    // =========================================================================

    async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        created_by: Option<&UserId>,
    ) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Creating namespace {} by {:?}", namespace_id, created_by);

        let message = self
            .executor
            .namespace()
            .create_namespace(namespace_id)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))?;

        Ok(message)
    }

    async fn delete_namespace(&self, namespace_id: &NamespaceId) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Deleting namespace {}", namespace_id);

        let message = self
            .executor
            .namespace()
            .drop_namespace(namespace_id)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))?;

        Ok(message)
    }

    // =========================================================================
    // Table Operations - Delegate to DdlExecutor
    // =========================================================================

    async fn create_table(
        &self,
        table_id: &TableId,
        table_type: TableType,
        table_def: &TableDefinition,
    ) -> Result<String, RaftError> {
        log::debug!(
            "ProviderMetaApplier: Creating table {} (type: {})",
            table_id.full_name(),
            table_type
        );

        let message = self
            .executor
            .ddl()
            .create_table(table_id, table_type, table_def)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))?;

        Ok(message)
    }

    async fn alter_table(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
    ) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Altering table {}", table_id.full_name());

        // For Raft replication, we don't have the old version, so pass 0
        self.executor
            .ddl()
            .alter_table(table_id, &table_def, 0)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    async fn drop_table(&self, table_id: &TableId) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Dropping table {}", table_id.full_name());

        self.executor
            .ddl()
            .drop_table(table_id)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    // =========================================================================
    // Storage Operations - Delegate to StorageExecutor
    // =========================================================================

    async fn register_storage(
        &self,
        storage_id: &StorageId,
        storage: &Storage,
    ) -> Result<String, RaftError> {
        log::info!("ProviderMetaApplier: Registering storage {}", storage_id);

        self.executor
            .storage()
            .create_storage(storage)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    async fn unregister_storage(&self, storage_id: &StorageId) -> Result<String, RaftError> {
        log::info!("ProviderMetaApplier: Unregistering storage {}", storage_id);

        self.executor
            .storage()
            .drop_storage(storage_id)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    // =========================================================================
    // User Operations - Delegate to UserExecutor
    // =========================================================================

    async fn create_user(&self, user: &User) -> Result<String, RaftError> {
        log::info!("ProviderMetaApplier: Creating user {:?} ({})", user.user_id, user.username);

        self.executor
            .user()
            .create_user(user)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    async fn update_user(&self, user: &User) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Updating user {:?}", user.user_id);

        self.executor
            .user()
            .update_user(user)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    async fn delete_user(&self, user_id: &UserId, _deleted_at: i64) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Deleting user {:?}", user_id);

        self.executor
            .user()
            .delete_user(user_id)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    async fn record_login(&self, user_id: &UserId, logged_in_at: i64) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Recording login for {:?}", user_id);

        self.executor
            .user()
            .record_login(user_id, logged_in_at)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    async fn set_user_locked(
        &self,
        user_id: &UserId,
        locked_until: Option<i64>,
        _updated_at: i64,
    ) -> Result<String, RaftError> {
        log::debug!(
            "ProviderMetaApplier: Setting user {:?} locked until {:?}",
            user_id,
            locked_until
        );

        self.executor
            .user()
            .set_user_locked(user_id, locked_until)
            .await
            .map_err(|e: ApplierError| RaftError::Internal(e.to_string()))
    }

    // =========================================================================
    // Job Operations - Stay local (no SQL handlers for jobs)
    //
    // NEW DESIGN:
    // - create_job is replicated to ALL nodes
    // - Each node that receives create_job:
    //   1. Persists the Job row
    //   2. Checks if leader-only job → only leader creates its job_node
    //   3. Otherwise → every node creates its OWN job_node
    //   4. Awakens the job when job_node is created locally
    // - The leader no longer creates job_nodes for all nodes
    // =========================================================================

    async fn create_job(&self, job: &Job) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Creating job {} (type: {:?})", job.job_id, job.job_type);

        let app_context = self.app_context.clone();
        let job = job.clone();
        tokio::task::spawn_blocking(move || {
            app_context
                .system_tables()
                .jobs()
                .create_job(job)
                .map_err(|e| RaftError::Internal(format!("Failed to create job: {}", e)))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn create_job_node(
        &self,
        job_id: &JobId,
        node_id: NodeId,
        status: JobStatus,
        created_at: i64,
    ) -> Result<String, RaftError> {
        let job_node = JobNode {
            job_id: job_id.clone(),
            node_id,
            status,
            error_message: None,
            created_at,
            updated_at: created_at,
            started_at: None,
            finished_at: None,
        };

        let app_context = self.app_context.clone();
        tokio::task::spawn_blocking(move || {
            app_context
                .system_tables()
                .job_nodes()
                .create_job_node(job_node)
                .map_err(|e| RaftError::Internal(format!("Failed to create job_node: {}", e)))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))??;

        // Awake the job on THIS node for immediate execution
        // This provides instant dispatch rather than relying on polling
        let this_node_id = *self.app_context.node_id().as_ref();
        if node_id == this_node_id {
            self.app_context.job_waker().awake_job(job_id.clone());
        }

        Ok(format!("Job node {} created and awakened", node_id))
    }

    async fn claim_job_node(
        &self,
        job_id: &JobId,
        node_id: NodeId,
        claimed_at: i64,
    ) -> Result<String, RaftError> {
        let app_context = self.app_context.clone();
        let job_id = job_id.clone();
        let node = tokio::task::spawn_blocking(move || {
            app_context
                .system_tables()
                .job_nodes()
                .get_job_node(&job_id, &node_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get job_node: {}", e)))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))??;

        if let Some(mut node) = node {
            node.status = JobStatus::Running;
            node.started_at = Some(claimed_at);
            node.updated_at = claimed_at;

            self.app_context
                .system_tables()
                .job_nodes()
                .update_job_node_async(node)
                .await
                .map_err(|e| RaftError::Internal(format!("Failed to claim job_node: {}", e)))?;

            return Ok(format!("Job node {} claimed", node_id));
        }

        Ok(format!("Job node {} not found for claim", node_id))
    }

    async fn update_job_node_status(
        &self,
        job_id: &JobId,
        node_id: NodeId,
        status: JobStatus,
        error_message: Option<&str>,
        updated_at: i64,
    ) -> Result<String, RaftError> {
        let app_context = self.app_context.clone();
        let job_id = job_id.clone();
        let node = tokio::task::spawn_blocking(move || {
            app_context
                .system_tables()
                .job_nodes()
                .get_job_node(&job_id, &node_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get job_node: {}", e)))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))??;

        if let Some(mut node) = node {
            node.status = status;
            node.updated_at = updated_at;
            node.error_message = error_message.map(|s| s.to_string());
            if matches!(
                status,
                JobStatus::Completed
                    | JobStatus::Failed
                    | JobStatus::Cancelled
                    | JobStatus::Skipped
            ) {
                node.finished_at = Some(updated_at);
            }

            self.app_context
                .system_tables()
                .job_nodes()
                .update_job_node_async(node)
                .await
                .map_err(|e| RaftError::Internal(format!("Failed to update job_node: {}", e)))?;

            return Ok(format!("Job node {} updated", node_id));
        }

        Ok(format!("Job node {} not found for update", node_id))
    }

    async fn claim_job(
        &self,
        job_id: &JobId,
        node_id: NodeId,
        claimed_at: i64,
    ) -> Result<String, RaftError> {
        log::info!("ProviderMetaApplier: Claiming job {} by node {}", job_id, node_id);

        let app_context = self.app_context.clone();
        let job_id = job_id.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(mut job) = app_context
                .system_tables()
                .jobs()
                .get_job(&job_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get job: {}", e)))?
            {
                // Check if job is already running (claimed by another node)
                if job.status == JobStatus::Running {
                    return Err(RaftError::Internal(format!(
                        "Job {} already claimed by node {}",
                        job_id, job.node_id
                    )));
                }

                job.node_id = node_id;
                job.started_at = Some(claimed_at);
                job.status = JobStatus::Running;
                job.updated_at = claimed_at;

                app_context
                    .system_tables()
                    .jobs()
                    .update_job(job)
                    .map_err(|e| RaftError::Internal(format!("Failed to claim job: {}", e)))?;

                return Ok(format!("Job {} claimed by node {}", job_id, node_id));
            }

            Err(RaftError::Internal(format!("Job {} not found for claiming", job_id)))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn update_job_status(
        &self,
        job_id: &JobId,
        status: JobStatus,
        updated_at: i64,
    ) -> Result<String, RaftError> {
        log::debug!("ProviderMetaApplier: Updating job {} status to {:?}", job_id, status);

        let app_context = self.app_context.clone();
        let job_id = job_id.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(mut job) = app_context
                .system_tables()
                .jobs()
                .get_job(&job_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get job: {}", e)))?
            {
                let old_status = job.status;
                job.status = status;
                job.updated_at = updated_at;

                if matches!(status, JobStatus::Running | JobStatus::Retrying)
                    && job.started_at.is_none()
                {
                    job.started_at = Some(updated_at);
                }

                if matches!(status, JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled)
                    && job.finished_at.is_none()
                {
                    job.finished_at = Some(updated_at);
                }

                app_context.system_tables().jobs().update_job(job).map_err(|e| {
                    RaftError::Internal(format!("Failed to update job status: {}", e))
                })?;

                return Ok(format!(
                    "Job {} status updated from {:?} to {:?}",
                    job_id, old_status, status
                ));
            }

            Ok(format!("Job {} not found for status update", job_id))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn complete_job(
        &self,
        job_id: &JobId,
        result: Option<&str>,
        completed_at: i64,
    ) -> Result<String, RaftError> {
        log::info!("ProviderMetaApplier: Completing job {}", job_id);

        let app_context = self.app_context.clone();
        let job_id = job_id.clone();
        let result = result.map(String::from);
        tokio::task::spawn_blocking(move || {
            if let Some(mut job) = app_context
                .system_tables()
                .jobs()
                .get_job(&job_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get job: {}", e)))?
            {
                let job_type = job.job_type;
                job.status = JobStatus::Completed;
                job.message = Some(result.unwrap_or_else(|| {
                    serde_json::json!({ "message": "Job completed successfully" }).to_string()
                }));
                if job.started_at.is_none() {
                    job.started_at = Some(completed_at);
                }
                job.finished_at = Some(completed_at);
                job.updated_at = completed_at;

                app_context
                    .system_tables()
                    .jobs()
                    .update_job(job)
                    .map_err(|e| RaftError::Internal(format!("Failed to complete job: {}", e)))?;

                return Ok(format!("Job {} ({:?}) completed successfully", job_id, job_type));
            }

            Ok(format!("Job {} not found for completion", job_id))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn fail_job(
        &self,
        job_id: &JobId,
        error_message: &str,
        failed_at: i64,
    ) -> Result<String, RaftError> {
        log::warn!("ProviderMetaApplier: Failing job {}: {}", job_id, error_message);

        let app_context = self.app_context.clone();
        let job_id = job_id.clone();
        let error_message = error_message.to_string();
        tokio::task::spawn_blocking(move || {
            if let Some(mut job) = app_context
                .system_tables()
                .jobs()
                .get_job(&job_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get job: {}", e)))?
            {
                let job_type = job.job_type;
                job.status = JobStatus::Failed;
                job.message = Some(error_message.clone());
                job.finished_at = Some(failed_at);
                job.updated_at = failed_at;

                app_context
                    .system_tables()
                    .jobs()
                    .update_job(job)
                    .map_err(|e| RaftError::Internal(format!("Failed to fail job: {}", e)))?;

                return Ok(format!(
                    "Job {} ({:?}) marked as failed: {}",
                    job_id, job_type, error_message
                ));
            }

            Ok(format!("Job {} not found for failure", job_id))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn release_job(
        &self,
        job_id: &JobId,
        reason: &str,
        released_at: i64,
    ) -> Result<String, RaftError> {
        log::info!("ProviderMetaApplier: Releasing job {}: {}", job_id, reason);

        let app_context = self.app_context.clone();
        let job_id = job_id.clone();
        let reason = reason.to_string();
        tokio::task::spawn_blocking(move || {
            if let Some(mut job) = app_context
                .system_tables()
                .jobs()
                .get_job(&job_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get job: {}", e)))?
            {
                let job_type = job.job_type;
                job.status = JobStatus::New;
                job.node_id = NodeId::default();
                job.started_at = None;
                job.updated_at = released_at;

                app_context
                    .system_tables()
                    .jobs()
                    .update_job(job)
                    .map_err(|e| RaftError::Internal(format!("Failed to release job: {}", e)))?;

                return Ok(format!("Job {} ({:?}) released: {}", job_id, job_type, reason));
            }

            Ok(format!("Job {} not found for release", job_id))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn cancel_job(
        &self,
        job_id: &JobId,
        reason: &str,
        cancelled_at: i64,
    ) -> Result<String, RaftError> {
        log::info!("ProviderMetaApplier: Cancelling job {}: {}", job_id, reason);

        let app_context = self.app_context.clone();
        let job_id = job_id.clone();
        let reason = reason.to_string();
        tokio::task::spawn_blocking(move || {
            if let Some(mut job) = app_context
                .system_tables()
                .jobs()
                .get_job(&job_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get job: {}", e)))?
            {
                let job_type = job.job_type;
                job.status = JobStatus::Cancelled;
                job.finished_at = Some(cancelled_at);
                job.message = Some(reason.clone());
                job.updated_at = cancelled_at;

                app_context
                    .system_tables()
                    .jobs()
                    .update_job(job)
                    .map_err(|e| RaftError::Internal(format!("Failed to cancel job: {}", e)))?;

                return Ok(format!("Job {} ({:?}) cancelled: {}", job_id, job_type, reason));
            }

            Ok(format!("Job {} not found for cancellation", job_id))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }
}
