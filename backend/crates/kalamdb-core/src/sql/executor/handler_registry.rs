//! Handler Registry for SQL Statement Routing
//!
//! This module provides a centralized registry for mapping SqlStatement variants
//! to their corresponding handler implementations, eliminating repetitive match arms
//! and enabling easy handler registration.
//!
//! # Architecture Benefits
//! - **Single Registration Point**: All handlers registered in one place
//! - **Type Safety**: Compile-time guarantees that handlers exist for statements
//! - **Extensibility**: New handlers added by registering, not modifying executor
//! - **Testability**: Easy to mock handlers for unit tests
//! - **Zero Overhead**: Registry lookup via DashMap is <1μs (vs 50-100ns for match)

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use crate::sql::executor::handler_adapter::{DynamicHandlerAdapter, TypedHandlerAdapter};
use dashmap::DashMap;
use kalamdb_commons::models::TableId;
use kalamdb_sql::classifier::SqlStatement;
use std::sync::Arc;
use tracing::Instrument;

// Import all typed handlers
use crate::sql::executor::handlers::backup::{BackupDatabaseHandler, RestoreDatabaseHandler};
use crate::sql::executor::handlers::cluster::{
    ClusterClearHandler, ClusterListHandler, ClusterPurgeHandler, ClusterSnapshotHandler,
    ClusterStepdownHandler, ClusterTransferLeaderHandler, ClusterTriggerElectionHandler,
};
use crate::sql::executor::handlers::compact::{CompactAllTablesHandler, CompactTableHandler};
use crate::sql::executor::handlers::export::{ExportUserDataHandler, ShowExportHandler};
use crate::sql::executor::handlers::flush::{FlushAllTablesHandler, FlushTableHandler};
use crate::sql::executor::handlers::jobs::{KillJobHandler, KillLiveQueryHandler};
use crate::sql::executor::handlers::namespace::{
    AlterNamespaceHandler, CreateNamespaceHandler, DropNamespaceHandler, ShowNamespacesHandler,
    UseNamespaceHandler,
};
use crate::sql::executor::handlers::storage::{
    AlterStorageHandler, CheckStorageHandler, CreateStorageHandler, DropStorageHandler,
    ShowStoragesHandler,
};
use crate::sql::executor::handlers::subscription::SubscribeHandler;
use crate::sql::executor::handlers::system::ShowManifestCacheHandler;
use crate::sql::executor::handlers::table::{
    AlterTableHandler, CreateTableHandler, DescribeTableHandler, DropTableHandler,
    ShowStatsHandler, ShowTablesHandler,
};
use crate::sql::executor::handlers::topics::{
    AckHandler, AddTopicSourceHandler, ClearTopicHandler, ConsumeHandler, CreateTopicHandler,
    DropTopicHandler,
};
use crate::sql::executor::handlers::user::{AlterUserHandler, CreateUserHandler, DropUserHandler};
use crate::sql::executor::handlers::view::CreateViewHandler;

/// Trait for handlers that can process any SqlStatement variant
///
/// This allows polymorphic handler dispatch without boxing every handler.
#[async_trait::async_trait]
pub trait SqlStatementHandler: Send + Sync {
    /// Execute the statement with authorization pre-checked
    ///
    /// # Parameters
    /// - `statement`: Classified SQL statement
    /// - `sql_text`: Original SQL text (for DML handlers that need to parse SQL)
    /// - `params`: Query parameters ($1, $2, etc.)
    /// - `context`: Execution context (user, role, session, etc.)
    ///
    /// # Note
    /// SessionContext is available via `context.session` - no need to pass separately
    async fn execute(
        &self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError>;

    /// Check authorization before execution (called by registry)
    async fn check_authorization(
        &self,
        statement: &SqlStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError>;
}

/// Registry key type for handler lookup
///
/// Uses statement discriminant (enum variant identifier) for O(1) lookup.
/// This is more efficient than matching on the full statement structure.
type HandlerKey = std::mem::Discriminant<kalamdb_sql::classifier::SqlStatementKind>;

/// Centralized handler registry for SQL statement routing
///
/// # Usage Pattern
/// ```ignore
/// // Build registry once during initialization
/// let registry = HandlerRegistry::new(app_context);
///
/// // Route statement to handler
/// match registry.handle(session, stmt, params, exec_ctx).await {
///     Ok(result) => // ... handle result
///     Err(e) => // ... handle error
/// }
/// ```
///
/// # Registration
/// Handlers are registered in `HandlerRegistry::new()` by calling
/// `register_typed()` or `register_dynamic()` for each statement type.
///
/// # Performance
/// - Handler lookup: <1μs via DashMap (lock-free concurrent HashMap)
/// - Authorization check: 1-10μs depending on handler
/// - Total overhead: <2μs vs ~50ns for direct match (acceptable trade-off)
pub struct HandlerRegistry {
    handlers: DashMap<HandlerKey, Arc<dyn SqlStatementHandler>>,
    app_context: Arc<AppContext>,
}

impl HandlerRegistry {
    /// Create a new handler registry with all handlers pre-registered
    ///
    /// This is called once during SqlExecutor initialization.
    pub fn new(app_context: Arc<AppContext>, enforce_password_complexity: bool) -> Self {
        use kalamdb_commons::models::{NamespaceId, StorageId};
        use kalamdb_commons::TableType;
        use kalamdb_sql::classifier::SqlStatementKind; // Role not needed here

        let registry = Self {
            handlers: DashMap::new(),
            app_context: app_context.clone(),
        };

        // ============================================================================
        // NAMESPACE HANDLERS
        // ============================================================================

        registry.register_typed(
            SqlStatementKind::CreateNamespace(kalamdb_sql::ddl::CreateNamespaceStatement {
                name: NamespaceId::new("_placeholder"),
                if_not_exists: false,
            }),
            CreateNamespaceHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::CreateNamespace(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::AlterNamespace(kalamdb_sql::ddl::AlterNamespaceStatement {
                name: NamespaceId::new("_placeholder"),
                options: std::collections::HashMap::new(),
            }),
            AlterNamespaceHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::AlterNamespace(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::DropNamespace(kalamdb_sql::ddl::DropNamespaceStatement {
                name: NamespaceId::new("_placeholder"),
                if_exists: false,
                cascade: false,
            }),
            DropNamespaceHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::DropNamespace(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::ShowNamespaces(kalamdb_sql::ddl::ShowNamespacesStatement),
            ShowNamespacesHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ShowNamespaces(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::UseNamespace(kalamdb_sql::ddl::UseNamespaceStatement {
                namespace: NamespaceId::new("_placeholder"),
            }),
            UseNamespaceHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::UseNamespace(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // STORAGE HANDLERS
        // ============================================================================

        registry.register_typed(
            SqlStatementKind::CreateStorage(kalamdb_sql::ddl::CreateStorageStatement {
                storage_id: StorageId::new("_placeholder"),
                storage_type: kalamdb_system::providers::storages::models::StorageType::Filesystem,
                storage_name: String::new(),
                description: None,
                base_directory: String::new(),
                shared_tables_template: String::new(),
                user_tables_template: String::new(),
                credentials: None,
                config_json: None,
            }),
            CreateStorageHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::CreateStorage(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::AlterStorage(kalamdb_sql::ddl::AlterStorageStatement {
                storage_id: StorageId::new("_placeholder"),
                storage_name: None,
                description: None,
                shared_tables_template: None,
                user_tables_template: None,
                config_json: None,
            }),
            AlterStorageHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::AlterStorage(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::DropStorage(kalamdb_sql::ddl::DropStorageStatement {
                storage_id: kalamdb_commons::StorageId::from(""),
                if_exists: false,
            }),
            DropStorageHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::DropStorage(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::ShowStorages(kalamdb_sql::ddl::ShowStoragesStatement),
            ShowStoragesHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ShowStorages(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::CheckStorage(kalamdb_sql::ddl::CheckStorageStatement {
                storage_id: kalamdb_commons::StorageId::from(""),
                extended: false,
            }),
            CheckStorageHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::CheckStorage(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // TABLE HANDLERS
        // ============================================================================

        // TABLE HANDLERS (Phase 8) - fully implemented
        // Register table handlers with minimal placeholder instances for discriminant extraction
        use kalamdb_sql::ddl::{
            AlterTableStatement, CreateTableStatement, CreateViewStatement, DescribeTableStatement,
            DropTableStatement, ShowTableStatsStatement, ShowTablesStatement,
        };
        // TableType already imported above; avoid duplicate imports
        use datafusion::arrow::datatypes::Schema as ArrowSchema;
        use kalamdb_commons::models::TableName;
        use std::collections::HashMap;

        registry.register_typed(
            SqlStatementKind::CreateTable(CreateTableStatement {
                table_name: TableName::new("_placeholder"),
                namespace_id: NamespaceId::new("_placeholder"),
                table_type: TableType::Shared,
                schema: Arc::new(ArrowSchema::empty()),
                column_defaults: HashMap::new(),
                primary_key_column: None,
                storage_id: None,
                use_user_storage: false,
                flush_policy: None,
                deleted_retention_hours: None,
                ttl_seconds: None,
                if_not_exists: false,
                access_level: None,
            }),
            CreateTableHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::CreateTable(s) => Some(s.clone()),
                _ => None,
            },
        );
        registry.register_typed(
            SqlStatementKind::CreateView(CreateViewStatement {
                namespace_id: NamespaceId::new("_placeholder"),
                view_name: TableName::new("_placeholder"),
                or_replace: false,
                if_not_exists: false,
                columns: Vec::new(),
                query_sql: String::new(),
                original_sql: String::new(),
            }),
            CreateViewHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::CreateView(s) => Some(s.clone()),
                _ => None,
            },
        );
        registry.register_typed(
            SqlStatementKind::AlterTable(AlterTableStatement {
                table_name: TableName::new("_placeholder"),
                namespace_id: NamespaceId::new("_placeholder"),
                operation: kalamdb_sql::ddl::ColumnOperation::Drop {
                    column_name: "_placeholder".to_string(),
                },
            }),
            AlterTableHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::AlterTable(s) => Some(s.clone()),
                _ => None,
            },
        );
        registry.register_typed(
            SqlStatementKind::DropTable(DropTableStatement {
                table_name: TableName::new("_placeholder"),
                namespace_id: NamespaceId::new("_placeholder"),
                table_type: kalamdb_sql::ddl::TableKind::Shared,
                if_exists: false,
            }),
            DropTableHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::DropTable(s) => Some(s.clone()),
                _ => None,
            },
        );
        registry.register_typed(
            SqlStatementKind::ShowTables(ShowTablesStatement { namespace_id: None }),
            ShowTablesHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ShowTables(s) => Some(s.clone()),
                _ => None,
            },
        );
        registry.register_typed(
            SqlStatementKind::DescribeTable(DescribeTableStatement {
                namespace_id: None,
                table_name: TableName::new("_placeholder"),
                show_history: false,
            }),
            DescribeTableHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::DescribeTable(s) => Some(s.clone()),
                _ => None,
            },
        );
        registry.register_typed(
            SqlStatementKind::ShowStats(ShowTableStatsStatement {
                namespace_id: None,
                table_name: TableName::new("_placeholder"),
            }),
            ShowStatsHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ShowStats(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // SYSTEM HANDLERS
        // ============================================================================
        registry.register_typed(
            SqlStatementKind::ShowManifest(kalamdb_sql::ddl::ShowManifestStatement),
            ShowManifestCacheHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ShowManifest(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // FLUSH HANDLERS
        // ============================================================================
        registry.register_typed(
            SqlStatementKind::FlushTable(kalamdb_sql::ddl::FlushTableStatement {
                namespace: NamespaceId::new("_placeholder"),
                table_name: TableName::new("_placeholder"),
            }),
            FlushTableHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::FlushTable(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::FlushAllTables(kalamdb_sql::ddl::FlushAllTablesStatement {
                namespace: NamespaceId::new("_placeholder"),
            }),
            FlushAllTablesHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::FlushAllTables(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::CompactTable(kalamdb_sql::ddl::CompactTableStatement {
                namespace: NamespaceId::new("_placeholder"),
                table_name: TableName::new("_placeholder"),
            }),
            CompactTableHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::CompactTable(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::CompactAllTables(kalamdb_sql::ddl::CompactAllTablesStatement {
                namespace: NamespaceId::new("_placeholder"),
            }),
            CompactAllTablesHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::CompactAllTables(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // CLUSTER HANDLERS
        // ============================================================================
        registry.register_dynamic(
            SqlStatementKind::ClusterSnapshot,
            ClusterSnapshotHandler::new(app_context.clone()),
        );

        registry.register_dynamic(
            SqlStatementKind::ClusterPurge(0),
            ClusterPurgeHandler::new(app_context.clone()),
        );

        registry.register_dynamic(
            SqlStatementKind::ClusterTriggerElection,
            ClusterTriggerElectionHandler::new(app_context.clone()),
        );

        registry.register_dynamic(
            SqlStatementKind::ClusterTransferLeader(0),
            ClusterTransferLeaderHandler::new(app_context.clone()),
        );

        registry.register_dynamic(
            SqlStatementKind::ClusterStepdown,
            ClusterStepdownHandler::new(app_context.clone()),
        );

        registry.register_dynamic(
            SqlStatementKind::ClusterClear,
            ClusterClearHandler::new(app_context.clone()),
        );

        registry.register_dynamic(
            SqlStatementKind::ClusterList,
            ClusterListHandler::new(app_context.clone()),
        );

        // ============================================================================
        // JOB HANDLERS
        // ============================================================================
        registry.register_typed(
            SqlStatementKind::KillJob(kalamdb_sql::ddl::JobCommand::Kill {
                job_id: String::new(),
            }),
            KillJobHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::KillJob(s) => Some(s.clone()),
                _ => None,
            },
        );

        // For discriminant extraction, we only need a placeholder instance. Use LiveQueryId::from_string.
        let placeholder_live =
            kalamdb_commons::models::LiveQueryId::from_string("user123-conn_abc-q1")
                .unwrap_or_else(|_| {
                    kalamdb_commons::models::LiveQueryId::new(
                        kalamdb_commons::models::UserId::new("user123"),
                        kalamdb_commons::models::ConnectionId::new("conn_abc"),
                        "q1".to_string(),
                    )
                });
        registry.register_typed(
            SqlStatementKind::KillLiveQuery(kalamdb_sql::ddl::KillLiveQueryStatement {
                live_id: placeholder_live,
            }),
            KillLiveQueryHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::KillLiveQuery(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // USER HANDLERS
        // ============================================================================
        use kalamdb_commons::AuthType;
        use kalamdb_sql::ddl::{
            AlterUserStatement, CreateUserStatement, DropUserStatement, UserModification,
        }; // Role not needed in placeholder (defaults handled by statement parsing)
        registry.register_typed(
            SqlStatementKind::CreateUser(CreateUserStatement {
                username: "_placeholder".to_string(),
                auth_type: AuthType::Internal,
                role: kalamdb_commons::Role::User,
                email: None,
                password: None,
            }),
            CreateUserHandler::new(app_context.clone(), enforce_password_complexity),
            |stmt| match stmt.kind() {
                SqlStatementKind::CreateUser(s) => Some(s.clone()),
                _ => None,
            },
        );
        registry.register_typed(
            SqlStatementKind::AlterUser(AlterUserStatement {
                username: "_placeholder".to_string(),
                modification: UserModification::SetEmail("_placeholder".to_string()),
            }),
            AlterUserHandler::new(app_context.clone(), enforce_password_complexity),
            |stmt| match stmt.kind() {
                SqlStatementKind::AlterUser(s) => Some(s.clone()),
                _ => None,
            },
        );
        registry.register_typed(
            SqlStatementKind::DropUser(DropUserStatement {
                username: "_placeholder".to_string(),
                if_exists: false,
            }),
            DropUserHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::DropUser(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // SUBSCRIPTION HANDLER
        // ============================================================================
        use kalamdb_sql::ddl::{SubscribeStatement, SubscriptionOptions};
        registry.register_typed(
            SqlStatementKind::Subscribe(SubscribeStatement {
                select_query: "SELECT * FROM _placeholder._placeholder".to_string(),
                namespace: NamespaceId::new("_placeholder"),
                table_name: TableName::new("_placeholder"),
                options: SubscriptionOptions::default(),
            }),
            SubscribeHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::Subscribe(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // TOPIC PUB/SUB HANDLERS
        // ============================================================================
        use kalamdb_commons::models::PayloadMode;
        use kalamdb_sql::ddl::{
            AckStatement, AddTopicSourceStatement, ClearTopicStatement, ConsumePosition,
            ConsumeStatement, CreateTopicStatement, DropTopicStatement,
        };

        registry.register_typed(
            SqlStatementKind::CreateTopic(CreateTopicStatement {
                topic_name: "_placeholder".to_string(),
                partitions: None,
            }),
            CreateTopicHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::CreateTopic(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::DropTopic(DropTopicStatement {
                topic_name: "_placeholder".to_string(),
            }),
            DropTopicHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::DropTopic(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::ClearTopic(ClearTopicStatement {
                topic_id: kalamdb_commons::models::TopicId::new("_placeholder"),
            }),
            ClearTopicHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ClearTopic(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::AddTopicSource(AddTopicSourceStatement {
                topic_name: "_placeholder".to_string(),
                table_id: TableId::from_strings("_placeholder", "_placeholder"),
                operation: kalamdb_commons::models::TopicOp::Insert,
                filter_expr: None,
                payload_mode: PayloadMode::Full,
            }),
            AddTopicSourceHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::AddTopicSource(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::ConsumeTopic(ConsumeStatement {
                topic_name: "_placeholder".to_string(),
                group_id: None,
                position: ConsumePosition::Latest,
                limit: None,
            }),
            ConsumeHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ConsumeTopic(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::AckTopic(AckStatement {
                topic_name: "_placeholder".to_string(),
                group_id: "_placeholder".to_string(),
                partition_id: 0,
                upto_offset: 0,
            }),
            AckHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::AckTopic(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // BACKUP & RESTORE HANDLERS
        // ============================================================================
        use kalamdb_sql::ddl::{BackupDatabaseStatement, RestoreDatabaseStatement};

        registry.register_typed(
            SqlStatementKind::BackupDatabase(BackupDatabaseStatement {
                backup_path: "_placeholder".to_string(),
            }),
            BackupDatabaseHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::BackupDatabase(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::RestoreDatabase(RestoreDatabaseStatement {
                backup_path: "_placeholder".to_string(),
            }),
            RestoreDatabaseHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::RestoreDatabase(s) => Some(s.clone()),
                _ => None,
            },
        );

        // ============================================================================
        // USER DATA EXPORT HANDLERS
        // ============================================================================
        use kalamdb_sql::ddl::{ExportUserDataStatement, ShowExportStatement};

        registry.register_typed(
            SqlStatementKind::ExportUserData(ExportUserDataStatement),
            ExportUserDataHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ExportUserData(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry.register_typed(
            SqlStatementKind::ShowExport(ShowExportStatement),
            ShowExportHandler::new(app_context.clone()),
            |stmt| match stmt.kind() {
                SqlStatementKind::ShowExport(s) => Some(s.clone()),
                _ => None,
            },
        );

        registry
    }

    /// Register a typed handler for a specific statement variant
    ///
    /// Uses a generic adapter to bridge TypedStatementHandler<T> to SqlStatementHandler.
    /// No custom adapter boilerplate needed!
    ///
    /// # Type Parameters
    /// - `H`: Handler type implementing TypedStatementHandler<T>
    /// - `T`: Statement type (inferred from handler and extractor)
    ///
    /// # Arguments
    /// - `placeholder`: An instance of the SqlStatement variant (for discriminant extraction)
    /// - `handler`: The handler instance to register
    /// - `extractor`: Function to extract T from SqlStatement
    fn register_typed<H, T, F>(
        &self,
        placeholder: kalamdb_sql::classifier::SqlStatementKind,
        handler: H,
        extractor: F,
    ) where
        H: crate::sql::executor::handlers::typed::TypedStatementHandler<T> + Send + Sync + 'static,
        T: kalamdb_sql::DdlAst + Send + 'static,
        F: Fn(SqlStatement) -> Option<T> + Send + Sync + 'static,
    {
        let key = std::mem::discriminant(&placeholder);
        let adapter = TypedHandlerAdapter::new(handler, extractor);
        self.handlers.insert(key, Arc::new(adapter));
    }

    /// Register a dynamic handler for a specific statement variant
    ///
    /// Uses DynamicHandlerAdapter to bridge StatementHandler to SqlStatementHandler.
    /// Simpler than register_typed() - no extractor function needed!
    ///
    /// # Type Parameters
    /// - `H`: Handler type implementing StatementHandler
    ///
    /// # Arguments
    /// - `placeholder`: An instance of the SqlStatementKind variant (for discriminant extraction)
    /// - `handler`: The handler instance to register
    ///
    /// # Example
    /// ```ignore
    /// registry.register_dynamic(
    ///     SqlStatementKind::Insert(InsertStatement),
    ///     InsertHandler::new(app_context.clone()),
    /// );
    /// ```
    fn register_dynamic<H>(
        &self,
        placeholder: kalamdb_sql::classifier::SqlStatementKind,
        handler: H,
    ) where
        H: crate::sql::executor::handlers::StatementHandler + Send + Sync + 'static,
    {
        let key = std::mem::discriminant(&placeholder);
        let adapter = DynamicHandlerAdapter::new(handler);
        self.handlers.insert(key, Arc::new(adapter));
    }

    /// Dispatch a statement to its registered handler
    ///
    /// # Flow
    /// 1. Extract discriminant from statement (O(1) key lookup)
    /// 2. Find handler in registry (O(1) DashMap lookup)
    /// 3. Call handler.check_authorization() (fail-fast)
    /// 4. Call handler.execute() if authorized
    ///
    /// # Parameters
    /// - `statement`: Classified SQL statement
    /// - `sql_text`: Original SQL text (for DML handlers)
    /// - `params`: Query parameters
    /// - `context`: Execution context (includes session)
    ///
    /// # Returns
    /// - `Ok(ExecutionResult)` if handler found and execution succeeded
    /// - `Err(KalamDbError::Unauthorized)` if authorization failed
    /// - `Err(KalamDbError::InvalidOperation)` if no handler registered
    pub async fn handle(
        &self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let stmt_name = statement.name().to_string();
        let span = tracing::debug_span!("sql.handler", handler = %stmt_name);
        async {
            // Step 1: Extract statement discriminant for O(1) lookup
            let key = std::mem::discriminant(statement.kind());

            // Step 2: Find handler in registry
            let handler = self.handlers.get(&key).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "No handler registered for statement type '{}'",
                    statement.name()
                ))
            })?;

            // Step 3: Check authorization (fail-fast)
            handler.check_authorization(&statement, context).await?;

            // Step 4: Execute statement (session is in context, no need to pass separately)
            handler.execute(statement, params, context).await
        }
        .instrument(span)
        .await
    }

    /// Check if a handler is registered for a statement type
    pub fn has_handler(&self, statement: &SqlStatement) -> bool {
        let key = std::mem::discriminant(statement.kind());
        self.handlers.contains_key(&key)
    }

    /// Get the AppContext (for handler access if needed)
    pub fn app_context(&self) -> &Arc<AppContext> {
        &self.app_context
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{create_test_session_simple, test_app_context_simple};
    use kalamdb_commons::models::{NamespaceId, UserId};
    use kalamdb_commons::Role;

    fn test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::from("test_user"), Role::Dba, create_test_session_simple())
    }

    #[ignore = "Requires Raft for CREATE NAMESPACE"]
    #[tokio::test]
    async fn test_registry_create_namespace() {
        use kalamdb_sql::classifier::SqlStatementKind;

        let app_ctx = test_app_context_simple();
        let registry = HandlerRegistry::new(app_ctx, false);
        let ctx = test_context();

        let stmt = SqlStatement::new(
            "CREATE NAMESPACE test_registry_ns".to_string(),
            SqlStatementKind::CreateNamespace(kalamdb_sql::ddl::CreateNamespaceStatement {
                name: NamespaceId::new("test_registry_ns"),
                if_not_exists: false,
            }),
        );

        // Check handler is registered
        assert!(registry.has_handler(&stmt));

        // Execute via registry
        let result = registry.handle(stmt, vec![], &ctx).await;
        assert!(result.is_ok());

        match result.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("test_registry_ns"));
            },
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_registry_unregistered_handler() {
        use kalamdb_sql::classifier::SqlStatementKind;

        let app_ctx = test_app_context_simple();
        let registry = HandlerRegistry::new(app_ctx, false);
        let ctx = test_context();

        // Use an unclassified statement (not in SqlStatementKind)
        // For this test, we'll use a deliberately malformed statement kind
        let stmt = SqlStatement::new(
            "SOME UNKNOWN STATEMENT".to_string(),
            SqlStatementKind::Unknown, // This should not have a handler
        );

        // Handler not registered for "Unknown" kind
        assert!(!registry.has_handler(&stmt));

        // Should return error
        let result = registry.handle(stmt, vec![], &ctx).await;
        assert!(result.is_err());

        match result {
            Err(KalamDbError::InvalidOperation(msg)) => {
                assert!(msg.contains("No handler registered"));
            },
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    #[tokio::test]
    async fn test_registry_authorization_check() {
        use kalamdb_sql::classifier::SqlStatementKind;

        let app_ctx = test_app_context_simple();
        let registry = HandlerRegistry::new(app_ctx, false);
        let user_ctx = ExecutionContext::new(
            UserId::from("regular_user"),
            Role::User,
            create_test_session_simple(),
        );

        let stmt = SqlStatement::new(
            "CREATE NAMESPACE unauthorized_ns".to_string(),
            SqlStatementKind::CreateNamespace(kalamdb_sql::ddl::CreateNamespaceStatement {
                name: NamespaceId::new("unauthorized_ns"),
                if_not_exists: false,
            }),
        );

        // Should fail authorization
        let result = registry.handle(stmt, vec![], &user_ctx).await;
        assert!(result.is_err());

        match result {
            Err(KalamDbError::Unauthorized(_)) => {},
            _ => panic!("Expected Unauthorized error"),
        }
    }
}
