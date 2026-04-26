//! Adapters that implement kalamdb-live traits using kalamdb-core types.
//!
//! These bridge the boundary between the live-query crate and the core server,
//! so kalamdb-live never depends on kalamdb-core directly.

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::{arrow::record_batch::RecordBatch, prelude::SessionContext};
use kalamdb_commons::{
    models::{ReadContext, TableId, UserId},
    schemas::TableDefinition,
    Role, TableType,
};
use kalamdb_live::{
    error::LiveError,
    traits::{LiveApplyBarrier, LiveSchemaLookup, LiveSqlExecutor},
};
use kalamdb_raft::{GroupId, RaftExecutor};
use kalamdb_sharding::ShardRouter;

use crate::{
    app_context::AppContext,
    schema_registry::SchemaRegistry,
    sql::{
        context::{ExecutionContext, ExecutionResult},
        executor::SqlExecutor,
    },
};

/// Adapts [`SchemaRegistry`] to the [`LiveSchemaLookup`] trait.
pub struct SchemaRegistryLookup {
    registry: Arc<SchemaRegistry>,
}

impl SchemaRegistryLookup {
    pub fn new(registry: Arc<SchemaRegistry>) -> Self {
        Self { registry }
    }
}

impl LiveSchemaLookup for SchemaRegistryLookup {
    fn get_table_definition(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.registry.get(table_id).map(|cached| Arc::clone(&cached.table))
    }

    fn get_arrow_schema(&self, table_id: &TableId) -> Result<Arc<ArrowSchema>, LiveError> {
        self.registry
            .get_arrow_schema(table_id)
            .map_err(|e| LiveError::TableNotFound(e.to_string()))
    }
}

/// Adapts [`SqlExecutor`] to the [`LiveSqlExecutor`] trait.
pub struct SqlExecutorAdapter {
    executor: Arc<SqlExecutor>,
    base_session_context: Arc<SessionContext>,
}

impl SqlExecutorAdapter {
    pub fn new(executor: Arc<SqlExecutor>, base_session_context: Arc<SessionContext>) -> Self {
        Self {
            executor,
            base_session_context,
        }
    }
}

#[async_trait]
impl LiveSqlExecutor for SqlExecutorAdapter {
    async fn execute_for_batches(
        &self,
        sql: &str,
        user_id: UserId,
        role: Role,
        read_context: ReadContext,
    ) -> Result<Vec<RecordBatch>, LiveError> {
        let exec_ctx = ExecutionContext::new(user_id, role, Arc::clone(&self.base_session_context))
            .with_read_context(read_context);

        let result = self
            .executor
            .execute(sql, &exec_ctx, vec![])
            .await
            .map_err(|e| LiveError::ExecutionError(e.to_string()))?;

        match result {
            ExecutionResult::Rows { batches, .. } => Ok(batches),
            other => Err(LiveError::InvalidOperation(format!(
                "Expected row result from initial data query, got {:?}",
                std::mem::discriminant(&other)
            ))),
        }
    }
}

/// Adapts the cluster executor to a live snapshot apply barrier.
pub struct RaftApplyBarrierAdapter {
    app_context: Arc<AppContext>,
}

impl RaftApplyBarrierAdapter {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn table_group(&self, table_type: TableType, user_id: &UserId) -> Option<GroupId> {
        let executor = self.app_context.executor();
        let raft_executor = executor.as_any().downcast_ref::<RaftExecutor>()?;
        let manager = raft_executor.manager();
        let router = ShardRouter::new(manager.config().user_shards, manager.config().shared_shards);

        match table_type {
            TableType::User | TableType::Stream => {
                Some(GroupId::DataUserShard(router.user_shard_id(user_id)))
            },
            TableType::Shared => Some(GroupId::DataSharedShard(router.shared_shard_id())),
            TableType::System => Some(GroupId::Meta),
        }
    }
}

#[async_trait]
impl LiveApplyBarrier for RaftApplyBarrierAdapter {
    async fn wait_for_table_apply_barrier(
        &self,
        _table_id: &TableId,
        table_type: TableType,
        user_id: &UserId,
    ) -> Result<(), LiveError> {
        let Some(group_id) = self.table_group(table_type, user_id) else {
            return Ok(());
        };

        let executor = self.app_context.executor();
        let Some(raft_executor) = executor.as_any().downcast_ref::<RaftExecutor>() else {
            return Ok(());
        };

        let manager = raft_executor.manager();

        manager
            .wait_for_local_apply_barrier(group_id, manager.config().replication_timeout)
            .await
            .map(|_| ())
            .map_err(|err| LiveError::ExecutionError(err.to_string()))
    }
}
