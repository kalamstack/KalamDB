//! SQL executor orchestrator (Phase 9 foundations)
//!
//! Minimal dispatcher that keeps the public `SqlExecutor` API compiling
//! while provider-based handlers are migrated. Behaviour is intentionally
//! limited for now; most statements return a structured error until
//! handler implementations are in place.

pub mod default_ordering;
pub mod handler_adapter;
pub mod handler_registry;
pub mod handlers;
pub mod helpers;
pub mod parameter_binding;
pub mod request_transaction_state;
mod sql_executor;
mod transaction_batch_insert;

use crate::sql::executor::handler_registry::HandlerRegistry;
use crate::sql::plan_cache::SqlCacheRegistry;
pub use datafusion::scalar::ScalarValue;
use kalamdb_commons::models::TableId;
use kalamdb_commons::schemas::TableType;
use kalamdb_sql::classifier::SqlStatement;
use std::sync::Arc;

/// Public facade for SQL execution routing.
pub struct SqlExecutor {
    app_context: Arc<crate::app_context::AppContext>,
    handler_registry: Arc<HandlerRegistry>,
    sql_cache_registry: Arc<SqlCacheRegistry>,
}

/// Optional execution context prepared by upstream callers (e.g. API layer).
#[derive(Debug, Clone)]
pub struct PreparedExecutionStatement {
    pub sql: String,
    pub table_id: Option<TableId>,
    pub table_type: Option<TableType>,
    pub classified_statement: Option<SqlStatement>,
}

impl PreparedExecutionStatement {
    pub fn new(
        sql: String,
        table_id: Option<TableId>,
        table_type: Option<TableType>,
        classified_statement: Option<SqlStatement>,
    ) -> Self {
        Self {
            sql,
            table_id,
            table_type,
            classified_statement,
        }
    }
}
