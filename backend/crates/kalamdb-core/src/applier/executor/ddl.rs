//! DDL Executor - CREATE/ALTER/DROP TABLE operations
//!
//! This is the SINGLE place where table mutations happen.

use std::sync::Arc;

use kalamdb_commons::models::schemas::TableDefinition;
use kalamdb_commons::models::TableId;
use kalamdb_commons::schemas::TableType;

use crate::app_context::AppContext;
use crate::applier::executor::utils::{run_blocking_applier, with_plan_cache_invalidation};
use crate::applier::ApplierError;

/// Executor for DDL (Data Definition Language) operations
pub struct DdlExecutor {
    app_context: Arc<AppContext>,
}

impl DdlExecutor {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    /// Execute CREATE TABLE
    ///
    /// This performs ALL the steps for table creation:
    /// 1. Persist to system.tables (via SchemaRegistry)
    /// 2. Prime schema cache (via SchemaRegistry)
    /// 3. Invalidate plan cache
    pub async fn create_table(
        &self,
        table_id: &TableId,
        table_type: TableType,
        table_def: &TableDefinition,
    ) -> Result<String, ApplierError> {
        log::debug!("CommandExecutorImpl: Creating {} table {}", table_type, table_id.full_name());

        let app_context = self.app_context.clone();
        let table_id = table_id.clone();
        let table_def = table_def.clone();
        with_plan_cache_invalidation(app_context, move |app_context: Arc<AppContext>| async move {
            run_blocking_applier(move || {
                app_context.schema_registry().register_table(table_def).map_err(|e| {
                    ApplierError::Execution(format!("Failed to register table: {}", e))
                })?;

                Ok(format!("{} table {} created successfully", table_type, table_id.full_name(),))
            })
            .await
        })
        .await
    }

    /// Execute ALTER TABLE
    pub async fn alter_table(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
        old_version: u32,
    ) -> Result<String, ApplierError> {
        if log::log_enabled!(log::Level::Debug) {
            log::debug!(
                "CommandExecutorImpl: Altering table {} (version {} -> {}). New columns: {:?}",
                table_id.full_name(),
                old_version,
                table_def.schema_version,
                table_def.columns.iter().map(|c| c.column_name.as_str()).collect::<Vec<_>>()
            );
        }

        let app_context = self.app_context.clone();
        let table_id = table_id.clone();
        let table_def = table_def.clone();
        with_plan_cache_invalidation(app_context, move |app_context: Arc<AppContext>| async move {
            run_blocking_applier(move || {
                app_context
                    .schema_registry()
                    .register_table(table_def.clone())
                    .map_err(|e| {
                        ApplierError::Execution(format!("Failed to register altered table: {}", e))
                    })?;

                log::debug!(
                    "CommandExecutorImpl: Updated schema cache and provider for {}",
                    table_id.full_name()
                );

                if let Some(cached) = app_context.schema_registry().get(&table_id) {
                    if let Ok(schema) = cached.arrow_schema() {
                        log::debug!(
                            "CommandExecutorImpl: ALTER TABLE {} complete - Arrow schema now has {} fields: {:?}",
                            table_id.full_name(),
                            schema.fields().len(),
                            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                        );
                    }
                }

                Ok(format!(
                    "Table {} altered successfully (version {} -> {})",
                    table_id.full_name(),
                    old_version,
                    table_def.schema_version
                ))
            })
            .await
        })
        .await
    }

    /// Execute DROP TABLE
    pub async fn drop_table(&self, table_id: &TableId) -> Result<String, ApplierError> {
        log::debug!("CommandExecutorImpl: Dropping table {}", table_id.full_name());

        let app_context = self.app_context.clone();
        let table_id = table_id.clone();
        with_plan_cache_invalidation(app_context, move |app_context: Arc<AppContext>| async move {
            run_blocking_applier(move || {
                app_context
                    .schema_registry()
                    .delete_table_definition(&table_id)
                    .map_err(|e| ApplierError::Execution(format!("Failed to drop table: {}", e)))?;

                Ok(format!("Table {} dropped successfully", table_id.full_name()))
            })
            .await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::DdlExecutor;
    use crate::sql::context::ExecutionContext;
    use crate::sql::executor::SqlExecutor;
    use crate::test_helpers::test_app_context_simple;
    use kalamdb_commons::models::datatypes::KalamDataType;
    use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
    use kalamdb_commons::models::{NamespaceId, TableId, TableName};
    use kalamdb_commons::schemas::{ColumnDefault, TableType};
    use kalamdb_commons::{Role, UserId};
    use std::sync::Arc;

    #[tokio::test]
    async fn ddl_applied_via_applier_clears_plan_cache() {
        let app_ctx = test_app_context_simple();

        // Register SqlExecutor into AppContext so DdlExecutor can clear plan cache.
        let sql_executor = Arc::new(SqlExecutor::new(
            app_ctx.clone(),
            Arc::new(crate::sql::executor::handler_registry::HandlerRegistry::new()),
        ));
        app_ctx.set_sql_executor(sql_executor.clone());

        // Build a minimal STREAM table definition (avoids storage registry requirements).
        let namespace = NamespaceId::new("test_ns");
        let table_name = TableName::new("test_table");
        let table_id = TableId::new(namespace.clone(), table_name.clone());

        let id_col = ColumnDefinition::new(
            1,
            "id".to_string(),
            1,
            KalamDataType::BigInt,
            false,
            true,
            false,
            ColumnDefault::None,
            None,
        );

        let mut table_def = TableDefinition::new(
            namespace.clone(),
            table_name.clone(),
            TableType::Stream,
            vec![id_col],
            TableOptions::stream(3600),
            None,
        )
        .expect("Failed to create TableDefinition");

        // Inject system columns (_seq, _deleted) to match runtime tables.
        app_ctx
            .system_columns_service()
            .add_system_columns(&mut table_def)
            .expect("Failed to add system columns");

        // Create table via DDL executor (registers provider).
        let ddl = DdlExecutor::new(app_ctx.clone());
        ddl.create_table(&table_id, TableType::Stream, &table_def)
            .await
            .expect("CREATE TABLE failed");

        // Execute a SELECT to populate the plan cache on this node.
        let exec_ctx = ExecutionContext::new(
            UserId::from("test_user"),
            Role::User,
            app_ctx.base_session_context(),
        );

        let _ = sql_executor
            .execute("SELECT * FROM test_ns.test_table", &exec_ctx, vec![])
            .await
            .expect("SELECT failed");

        assert!(sql_executor.plan_cache_len() > 0, "Expected plan cache to be populated");

        // Now apply an ALTER TABLE via the applier path (simulates follower replication).
        let mut altered = table_def.clone();
        let new_col = ColumnDefinition::new(
            999,
            "col1".to_string(),
            (altered.columns.len() + 1) as u32,
            KalamDataType::Text,
            true,
            false,
            false,
            ColumnDefault::None,
            None,
        );
        altered.columns.push(new_col);
        altered.increment_version();

        ddl.alter_table(&table_id, &altered, 1).await.expect("ALTER TABLE failed");

        assert_eq!(
            sql_executor.plan_cache_len(),
            0,
            "Expected plan cache to be cleared after ALTER TABLE applied via applier"
        );
    }
}
