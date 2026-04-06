//! SQL statement handler implementations for KalamDB
//!
//! This crate contains all SQL handler implementations (CREATE TABLE, DROP NAMESPACE, etc.)
//! and shared helpers used by those handlers. Extracting these from `kalamdb-core` enables
//! parallel compilation, significantly reducing build times.
//!
//! # Architecture
//!
//! Handlers implement [`TypedStatementHandler<T>`] or [`StatementHandler`] traits defined
//! in `kalamdb-core`. Registration into the [`HandlerRegistry`] happens via
//! [`register_all_handlers`], which is called from the server lifecycle code.

pub mod subscription;

pub use kalamdb_handlers_admin::{backup, cluster, compact, export, flush, jobs, system};
pub use kalamdb_handlers_ddl::{namespace, storage, table, view};
pub use kalamdb_handlers_stream::topics;
pub use kalamdb_handlers_support::{audit, guards, table_creation};
pub use kalamdb_handlers_user::user;

use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::executor::handler_registry::HandlerRegistry;
use kalamdb_handlers_admin::register_admin_handlers;
use kalamdb_handlers_ddl::register_ddl_handlers;
use kalamdb_handlers_stream::register_stream_handlers;
use kalamdb_handlers_support::register_typed_handler;
use kalamdb_handlers_user::register_user_handlers;
use std::sync::Arc;

/// Register all SQL statement handlers into the given registry.
///
/// This function is called once during server startup from lifecycle code.
/// It populates the `HandlerRegistry` with concrete handler implementations
/// for every supported SQL statement type.
pub fn register_all_handlers(
    registry: &HandlerRegistry,
    app_context: Arc<AppContext>,
    enforce_password_complexity: bool,
) {
    use kalamdb_commons::models::{NamespaceId, TableName};
    use kalamdb_sql::classifier::SqlStatementKind;

    register_admin_handlers(registry, app_context.clone());
    register_ddl_handlers(registry, app_context.clone());
    register_stream_handlers(registry, app_context.clone());
    register_user_handlers(registry, app_context.clone(), enforce_password_complexity);

    // ============================================================================
    // SUBSCRIPTION HANDLER
    // ============================================================================
    use kalamdb_sql::ddl::{SubscribeStatement, SubscriptionOptions};

    register_typed_handler!(
        registry,
        SqlStatementKind::Subscribe(SubscribeStatement {
            select_query: "SELECT * FROM _placeholder._placeholder".to_string(),
            namespace: NamespaceId::new("_placeholder"),
            table_name: TableName::new("_placeholder"),
            options: SubscriptionOptions::default(),
        }),
        subscription::SubscribeHandler::new(app_context.clone()),
        SqlStatementKind::Subscribe,
    );
}
