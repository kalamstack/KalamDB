//! Typed handler for SUBSCRIBE statement

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_handlers_support::audit;
use kalamdb_sql::ddl::SubscribeStatement;
use std::sync::Arc;
use uuid::Uuid;

/// Handler for SUBSCRIBE TO (Live Query)
pub struct SubscribeHandler {
    _app_context: Arc<AppContext>, // Reserved for future use
}

impl SubscribeHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self {
            _app_context: app_context,
        }
    }
}

impl TypedStatementHandler<SubscribeStatement> for SubscribeHandler {
    async fn execute(
        &self,
        statement: SubscribeStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Use the session's default namespace if the statement used "default"
        // This allows SUBSCRIBE TO messages to use the current USE NAMESPACE
        let effective_namespace = if statement.namespace.is_default_namespace() {
            context.default_namespace()
        } else {
            statement.namespace.clone()
        };

        // Generate a subscription id (actual registration happens over WebSocket handshake)
        let subscription_id = format!(
            "sub-{}-{}-{}",
            effective_namespace.as_str(),
            statement.table_name.as_str(),
            Uuid::new_v4().simple()
        );
        // Channel placeholder (could read from server.toml later)
        let channel = "ws://localhost:8080/ws".to_string();

        // Update select_query to use effective namespace if needed
        let select_query = if effective_namespace.as_str() != statement.namespace.as_str() {
            // Replace table reference in the query
            statement.select_query.replace(
                &format!("FROM {}", statement.table_name.as_str()),
                &format!("FROM {}.{}", effective_namespace.as_str(), statement.table_name.as_str()),
            )
        } else {
            statement.select_query.clone()
        };

        // Log query operation
        let audit_entry = audit::log_query_operation(
            context,
            "SUBSCRIBE",
            &format!("{}.{}", effective_namespace.as_str(), statement.table_name.as_str()),
            0.0, // Duration is negligible for registration
            None,
        );
        audit::persist_audit_entry(&self._app_context, &audit_entry).await?;

        // Return subscription metadata with the SELECT query
        Ok(ExecutionResult::Subscription {
            subscription_id,
            channel,
            select_query,
        })
    }

    async fn check_authorization(
        &self,
        _statement: &SubscribeStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // SUBSCRIBE allowed for all authenticated users (table-specific auth done in execution)
        Ok(())
    }
}
