//! Typed handler for CREATE VIEW statements
//!
//! VIEWs are currently backed entirely by DataFusion. We delegate the actual
//! registration to the shared base SessionContext so subsequent per-user
//! sessions inherit the view definition.

use kalamdb_commons::models::NamespaceId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::CreateViewStatement;
use std::sync::Arc;

/// Handler for CREATE VIEW statements
pub struct CreateViewHandler {
    app_context: Arc<AppContext>,
}

impl CreateViewHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn build_create_sql(statement: &CreateViewStatement) -> String {
        let mut sql = String::from("CREATE ");
        if statement.or_replace {
            sql.push_str("OR REPLACE ");
        }
        sql.push_str("VIEW ");
        if statement.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }

        sql.push_str(statement.namespace_id.as_str());
        sql.push('.');
        sql.push_str(statement.view_name.as_str());

        if !statement.columns.is_empty() {
            sql.push('(');
            sql.push_str(&statement.columns.join(", "));
            sql.push(')');
        }

        sql.push_str(" AS ");
        sql.push_str(statement.query_sql.trim());

        sql
    }

    fn ensure_namespace_schema(&self, namespace: &NamespaceId) -> Result<(), KalamDbError> {
        let session = self.app_context.base_session_context();
        // Use constant catalog name "kalam" instead of catalog_names().first()
        // This avoids unnecessary Vec allocation and is clearer since we always use "kalam"
        let catalog = session.catalog("kalam").ok_or_else(|| {
            KalamDbError::InvalidOperation("Catalog 'kalam' not found".to_string())
        })?;

        if catalog.schema(namespace.as_str()).is_none() {
            let schema = Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new());
            catalog.register_schema(namespace.as_str(), schema).map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to register namespace schema '{}': {}",
                    namespace.as_str(),
                    e
                ))
            })?;
        }

        Ok(())
    }
}

impl TypedStatementHandler<CreateViewStatement> for CreateViewHandler {
    async fn execute(
        &self,
        statement: CreateViewStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        self.ensure_namespace_schema(&statement.namespace_id)?;
        let create_sql = Self::build_create_sql(&statement);
        let session = self.app_context.base_session_context();

        log::info!(
            "Running DataFusion CREATE VIEW for {}.{}",
            statement.namespace_id.as_str(),
            statement.view_name.as_str()
        );

        let df = session.sql(&create_sql).await.map_err(|e| {
            KalamDbError::InvalidSql(format!("Failed to parse CREATE VIEW statement: {}", e))
        })?;

        // DataFusion returns a DataFrame for DDL; collecting executes the command
        df.collect().await.into_execution_error("Failed to create view")?;

        Ok(ExecutionResult::Success {
            message: format!(
                "View {}.{} created successfully",
                statement.namespace_id.as_str(),
                statement.view_name.as_str()
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &CreateViewStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_session::can_create_view;
        if can_create_view(context.user_role()) {
            Ok(())
        } else {
            Err(KalamDbError::Unauthorized(
                "Only DBA or System roles can create views".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use kalamdb_commons::models::{NamespaceId, UserId};
    use kalamdb_core::test_helpers::test_app_context_simple;

    #[tokio::test]
    async fn create_view_registers_and_is_queryable() {
        let app_ctx = test_app_context_simple();
        let handler = CreateViewHandler::new(app_ctx.clone());

        let stmt = CreateViewStatement::parse(
            "CREATE VIEW default.test_view AS SELECT 1 AS value",
            &NamespaceId::default(),
        )
        .expect("parse view");

        let exec_ctx = ExecutionContext::new(
            UserId::new("tester"),
            kalamdb_commons::Role::Dba,
            app_ctx.base_session_context(),
        );

        handler.execute(stmt, vec![], &exec_ctx).await.expect("create view executed");

        let session = exec_ctx.create_session_with_user();
        let df = session.sql("SELECT value FROM default.test_view").await.expect("select view");
        let batches = df.collect().await.expect("collect view");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let column = batches[0].column(0);
        let values = column.as_any().downcast_ref::<Int64Array>().expect("int64 array");
        assert_eq!(values.value(0), 1);
    }

    #[tokio::test]
    async fn non_admin_cannot_create_view() {
        let app_ctx = test_app_context_simple();
        let handler = CreateViewHandler::new(app_ctx.clone());
        let stmt = CreateViewStatement::parse(
            "CREATE VIEW default.restricted_view AS SELECT 1",
            &NamespaceId::default(),
        )
        .expect("parse view");

        let exec_ctx = ExecutionContext::new(
            UserId::new("tester"),
            kalamdb_commons::Role::User,
            app_ctx.base_session_context(),
        );

        let result = handler.check_authorization(&stmt, &exec_ctx).await;
        assert!(matches!(result, Err(KalamDbError::Unauthorized(_))));
    }
}
