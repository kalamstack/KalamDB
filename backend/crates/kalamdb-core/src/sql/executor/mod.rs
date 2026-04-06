//! SQL executor orchestrator (Phase 9 foundations)
//!
//! Minimal dispatcher that keeps the public `SqlExecutor` API compiling
//! while provider-based handlers are migrated. Behaviour is intentionally
//! limited for now; most statements return a structured error until
//! handler implementations are in place.

pub mod default_ordering;
mod fast_insert;
mod fast_point_dml;
pub mod handler_adapter;
pub mod handler_registry;
pub mod handlers;
pub mod helpers;
pub mod parameter_binding;
mod sql_executor;

use crate::sql::executor::handler_registry::HandlerRegistry;
use crate::sql::plan_cache::PlanCache;
pub use datafusion::scalar::ScalarValue;
use kalamdb_commons::models::TableId;
use kalamdb_commons::schemas::TableType;
use kalamdb_sql::classifier::SqlStatement;
use sqlparser::ast::Statement as SqlParserStatement;
use std::sync::Arc;

/// Public facade for SQL execution routing.
pub struct SqlExecutor {
    app_context: Arc<crate::app_context::AppContext>,
    handler_registry: Arc<HandlerRegistry>,
    plan_cache: Arc<PlanCache>,
}

/// Optional execution context prepared by upstream callers (e.g. API layer).
#[derive(Debug, Clone)]
pub struct PreparedExecutionStatement {
    pub sql: String,
    pub table_id: Option<TableId>,
    pub table_type: Option<TableType>,
    pub parsed_statement: Option<SqlParserStatement>,
    pub classified_statement: Option<SqlStatement>,
}

impl PreparedExecutionStatement {
    pub fn new(
        sql: String,
        table_id: Option<TableId>,
        table_type: Option<TableType>,
        parsed_statement: Option<SqlParserStatement>,
        classified_statement: Option<SqlStatement>,
    ) -> Self {
        Self {
            sql,
            table_id,
            table_type,
            parsed_statement,
            classified_statement,
        }
    }
}
