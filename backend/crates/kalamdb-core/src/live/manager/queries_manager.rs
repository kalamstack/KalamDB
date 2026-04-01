//! Live query and consumer manager core implementation
//!
//! Orchestrates subscription lifecycle, initial data fetching, and notifications.
//! Uses SharedConnectionState pattern for efficient state access.
//!
//! Supports:
//! - WebSocket live query subscriptions
//! - Topic consumer sessions (long polling)
//!
//! Live query notifications are now handled through Raft-replicated data appliers.
//! When data is applied on any node (leader or follower), the provider's methods
//! fire local notifications - no need for separate HTTP cluster broadcast.

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::live::helpers::filter_eval::parse_where_clause;
use crate::live::helpers::initial_data::{
    InitialDataFetcher, InitialDataOptions, InitialDataResult,
};
use crate::live::helpers::query_parser::QueryParser;
use crate::live::manager::ConnectionsManager;
use crate::live::models::{RegistryStats, SharedConnectionState, SubscriptionResult};
use crate::live::subscription::SubscriptionService;
use crate::sql::executor::SqlExecutor;
use datafusion::execution::context::SessionContext;
use datafusion::sql::sqlparser::ast::Expr;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{ConnectionId, LiveQueryId, NamespaceId, TableId, TableName, UserId};
use kalamdb_commons::schemas::{SchemaField, TableDefinition};
use kalamdb_commons::websocket::SubscriptionRequest;
use kalamdb_commons::{NodeId, Role};
use kalamdb_system::providers::live_queries::models::LiveQuery as SystemLiveQuery;
use kalamdb_system::LiveQueriesTableProvider;
use std::sync::Arc;

/// Live query manager
pub struct LiveQueryManager {
    /// Unified connections manager using DashMap for lock-free concurrent access
    registry: Arc<ConnectionsManager>,
    live_queries_provider: Arc<LiveQueriesTableProvider>,
    initial_data_fetcher: Arc<InitialDataFetcher>,
    schema_registry: Arc<crate::schema_registry::SchemaRegistry>,
    node_id: NodeId,

    // Delegated services
    subscription_service: Arc<SubscriptionService>,
}

impl LiveQueryManager {
    fn validate_table_subscription_permission(
        user_role: Role,
        table_def: &TableDefinition,
        table_id: &TableId,
    ) -> Result<(), KalamDbError> {
        let is_admin = matches!(user_role, Role::Dba | Role::System);
        match table_def.table_type {
            kalamdb_commons::TableType::User => {
                // USER tables are accessible to any authenticated user
                // Row-level security filters data to only their rows during query execution
                Ok(())
            },
            kalamdb_commons::TableType::System if !is_admin => Err(KalamDbError::PermissionDenied(
                format!("Cannot subscribe to system table '{}': insufficient privileges. Only DBA and system roles can subscribe to system tables.", table_id)
            )),
            kalamdb_commons::TableType::Shared => {
                // SHARED tables require access-level check:
                // - Public: any authenticated user can subscribe
                // - Private/Restricted: only DBA/System/Service roles
                let access_level = kalamdb_session::permissions::shared_table_access_level(table_def);
                if kalamdb_session::permissions::can_access_shared_table(access_level, user_role) {
                    Ok(())
                } else {
                    Err(KalamDbError::PermissionDenied(format!(
                        "Cannot subscribe to shared table '{}': access level '{}' requires elevated privileges.",
                        table_id, access_level
                    )))
                }
            },
            _ => Ok(()),
        }
    }

    pub(crate) fn build_subscription_schema(
        table_def: &TableDefinition,
        projections: Option<&[String]>,
    ) -> Vec<SchemaField> {
        if let Some(proj_cols) = projections {
            proj_cols
                .iter()
                .enumerate()
                .filter_map(|(idx, col_name)| {
                    table_def
                        .columns
                        .iter()
                        .find(|c| c.column_name.eq_ignore_ascii_case(col_name))
                        .map(|col| SchemaField::from_column_definition(col, idx))
                })
                .collect()
        } else {
            let mut cols: Vec<_> = table_def.columns.iter().collect();
            cols.sort_by_key(|c| c.ordinal_position);
            cols.iter()
                .enumerate()
                .map(|(idx, col)| SchemaField::from_column_definition(col, idx))
                .collect()
        }
    }

    /// Create a new live query manager with an external ConnectionsManager
    ///
    /// The ConnectionsManager is shared across all WebSocket handlers for
    /// centralized connection/subscription management.
    pub fn new(
        live_queries_provider: Arc<LiveQueriesTableProvider>,
        schema_registry: Arc<crate::schema_registry::SchemaRegistry>,
        registry: Arc<ConnectionsManager>,
        base_session_context: Arc<SessionContext>,
        app_context: Arc<AppContext>,
    ) -> Self {
        let node_id = *registry.node_id();
        let initial_data_fetcher =
            Arc::new(InitialDataFetcher::new(base_session_context, schema_registry.clone()));

        let subscription_service =
            Arc::new(SubscriptionService::new(registry.clone(), node_id, app_context));

        Self {
            registry,
            live_queries_provider,
            initial_data_fetcher,
            schema_registry,
            node_id,
            subscription_service,
        }
    }

    /// Get the node_id for this manager
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Provide shared SqlExecutor so initial data fetches reuse common execution path
    pub fn set_sql_executor(&self, executor: Arc<SqlExecutor>) {
        self.initial_data_fetcher.set_sql_executor(executor);
    }

    /// Register a live query subscription
    ///
    /// Parameters:
    /// - connection_state: Shared reference to connection state
    /// - request: Client subscription request
    /// - table_id: Pre-validated table identifier
    /// - filter_expr: Optional parsed WHERE clause
    /// - projections: Optional column projections (None = SELECT *, all columns)
    /// - batch_size: Batch size for initial data loading
    /// - table_type: Type of the table (User, Shared, System, Stream)
    pub async fn register_subscription(
        &self,
        connection_state: &SharedConnectionState,
        request: &SubscriptionRequest,
        table_id: TableId,
        filter_expr: Option<Expr>,
        projections: Option<Vec<String>>,
        batch_size: usize,
        table_type: kalamdb_commons::TableType,
    ) -> Result<LiveQueryId, KalamDbError> {
        self.subscription_service
            .register_subscription(
                connection_state,
                request,
                table_id,
                filter_expr,
                projections,
                batch_size,
                table_type,
            )
            .await
    }

    /// Register subscription and fetch initial data (simplified API)
    ///
    /// This method handles all SQL parsing internally:
    /// - Extracts table name from SQL
    /// - Validates table exists
    /// - Checks user permissions
    /// - Parses WHERE clause for filtering
    /// - Extracts column projections
    ///
    /// The ws_handler only needs to validate subscription ID and rate limits.
    pub async fn register_subscription_with_initial_data(
        &self,
        connection_state: &SharedConnectionState,
        request: &SubscriptionRequest,
        initial_data_options: Option<InitialDataOptions>,
    ) -> Result<SubscriptionResult, KalamDbError> {
        // Get user_id from connection state
        let (user_id, user_role) = {
            let user_id = connection_state.user_id().cloned().ok_or_else(|| {
                KalamDbError::InvalidOperation("Connection not authenticated".to_string())
            })?;
            let user_role = connection_state.user_role().ok_or_else(|| {
                KalamDbError::InvalidOperation(
                    "Connection authenticated without role context".to_string(),
                )
            })?;
            (user_id, user_role)
        };

        let parsed_query = QueryParser::analyze_subscription_query(&request.sql)?;

        // Parse table name from SQL
        let raw_table = parsed_query.table_name.clone();
        let (namespace, table) = raw_table.split_once('.').ok_or_else(|| {
            KalamDbError::InvalidSql("Query must use namespace.table format".to_string())
        })?;

        let namespace_id = NamespaceId::from(namespace);
        let table_name = TableName::from(table);
        let table_id = TableId::new(namespace_id.clone(), table_name);

        if namespace_id.is_system_namespace() && !matches!(user_role, Role::Dba | Role::System) {
            return Err(KalamDbError::PermissionDenied(
                format!(
                    "Cannot subscribe to system table '{}': insufficient privileges. Only DBA and system roles can subscribe to system tables.",
                    table_id
                ),
            ));
        }

        // Look up table definition from in-memory cache.
        // Live queries require the table to be registered in the schema registry.
        let table_def = self
            .schema_registry
            .get(&table_id)
            .map(|cached| Arc::clone(&cached.table))
            .ok_or_else(|| KalamDbError::NotFound(format!("Table not found: {}", table_id)))?;

        // Permission check
        // - USER tables: Accessible to any authenticated user (RLS filters data to their rows)
        // - SYSTEM tables: Accessible only to DBA/System roles
        // - SHARED tables: Access-level gated (public OK, private/restricted require elevated role)
        Self::validate_table_subscription_permission(user_role, &table_def, &table_id)?;

        // Determine batch size
        let batch_size = request
            .options
            .batch_size
            .unwrap_or(kalamdb_commons::websocket::MAX_ROWS_PER_BATCH);

        // Parse filter expression from WHERE clause (if present)
        let where_clause = parsed_query.where_clause.clone();
        let filter_expr: Option<Expr> = where_clause
            .clone()
            .map(|where_clause| {
                // Resolve placeholders like CURRENT_USER() before parsing
                let resolved =
                    QueryParser::resolve_where_clause_placeholders(&where_clause, &user_id);
                parse_where_clause(&resolved)
            })
            .transpose()
            .map_err(|e| {
                log::warn!("Failed to parse WHERE clause for filter: {}", e);
                e
            })
            .ok()
            .flatten();

        // Extract column projections from SELECT clause (None = SELECT *, all columns)
        let projections = parsed_query.projections.clone();

        // if let Some(ref cols) = projections {
        //     log::info!("Subscription projections: {:?}", cols);
        // }

        // Register the subscription
        let live_id = self
            .register_subscription(
                connection_state,
                request,
                table_id.clone(),
                filter_expr,
                projections.clone(),
                batch_size,
                table_def.table_type,
            )
            .await?;

        // Fetch initial data if requested.
        // IMPORTANT: If anything fails after register_subscription succeeded,
        // we must unregister the subscription to avoid orphaned entries in
        // system.live_queries and the in-memory registry.
        let initial_data = if let Some(mut fetch_options) = initial_data_options {
            let fetch_result: Result<InitialDataResult, KalamDbError> = async {
                // Compute snapshot boundary (MAX(_seq)) before initial load unless a
                // reconnect already supplied the original boundary.
                let snapshot_seq = if let Some(snapshot_seq) = fetch_options.until_seq {
                    snapshot_seq
                } else {
                    self.initial_data_fetcher
                        .compute_snapshot_end_seq(
                            &live_id,
                            user_role,
                            &table_id,
                            table_def.table_type,
                            &fetch_options,
                            where_clause.as_deref(),
                        )
                        .await?
                        .unwrap_or_else(|| SeqId::from(0))
                };

                fetch_options.until_seq = Some(snapshot_seq);
                self.subscription_service.update_snapshot_end_seq(
                    connection_state,
                    &request.id,
                    snapshot_seq,
                );

                self.initial_data_fetcher
                    .fetch_initial_data(
                        &live_id,
                        user_role,
                        &table_id,
                        table_def.table_type,
                        fetch_options,
                        where_clause.as_deref(),
                        projections.as_deref(),
                    )
                    .await
            }
            .await;

            match fetch_result {
                Ok(result) => Some(result),
                Err(e) => {
                    // Cleanup: unregister the subscription we just registered
                    log::warn!(
                        "Initial data fetch failed for subscription {}, cleaning up: {}",
                        request.id,
                        e
                    );
                    if let Err(cleanup_err) = self
                        .subscription_service
                        .unregister_subscription(connection_state, &request.id, &live_id)
                        .await
                    {
                        log::error!(
                            "Failed to cleanup subscription {} after initial data error: {}",
                            request.id,
                            cleanup_err
                        );
                    }
                    return Err(e);
                },
            }
        } else {
            None
        };

        // Build schema from table definition, respecting projections if specified
        let schema = Self::build_subscription_schema(&table_def, projections.as_deref());

        Ok(SubscriptionResult {
            live_id,
            initial_data,
            schema,
        })
    }

    /// Fetch a batch of initial data for an existing subscription
    ///
    /// Uses subscription metadata from ConnectionState for batch fetching.
    pub async fn fetch_initial_data_batch(
        &self,
        connection_state: &SharedConnectionState,
        subscription_id: &str,
        since_seq: Option<SeqId>,
    ) -> Result<InitialDataResult, KalamDbError> {
        // Get subscription state from connection
        let sub_state = connection_state.get_subscription(subscription_id).ok_or_else(|| {
            KalamDbError::NotFound(format!("Subscription not found: {}", subscription_id))
        })?;

        let user_role = connection_state.user_role().ok_or_else(|| {
            KalamDbError::InvalidOperation(
                "Connection authenticated without role context".to_string(),
            )
        })?;

        let table_def = self
            .schema_registry
            .get(&sub_state.table_id)
            .map(|cached| Arc::clone(&cached.table))
            .ok_or_else(|| {
                KalamDbError::NotFound(format!(
                    "Table {} not found for batch fetch",
                    sub_state.table_id
                ))
            })?;

        // Extract WHERE clause from stored SQL
        let where_clause = QueryParser::extract_where_clause(&sub_state.sql);

        // Get projections from subscription state (Arc<Vec<String>> -> Option<&[String]>)
        let projections_ref = sub_state.projections.as_deref().map(|v| v.as_slice());

        // Create batch options with metadata from subscription state
        let fetch_options =
            InitialDataOptions::batch(since_seq, sub_state.snapshot_end_seq, sub_state.batch_size);

        self.initial_data_fetcher
            .fetch_initial_data(
                &sub_state.live_id,
                user_role,
                &sub_state.table_id,
                table_def.table_type,
                fetch_options,
                where_clause?.as_deref(),
                projections_ref,
            )
            .await
    }

    /// Extract table name from SQL query
    pub fn extract_table_name_from_query(&self, query: &str) -> Result<String, KalamDbError> {
        QueryParser::extract_table_name(query).map_err(|e| KalamDbError::InvalidSql(e.to_string()))
    }

    /// Unregister a WebSocket connection
    pub async fn unregister_connection(
        &self,
        user_id: &UserId,
        connection_id: &ConnectionId,
    ) -> Result<Vec<LiveQueryId>, KalamDbError> {
        self.subscription_service.unregister_connection(user_id, connection_id).await
    }

    /// Unregister a single live query subscription using SharedConnectionState
    pub async fn unregister_subscription(
        &self,
        connection_state: &SharedConnectionState,
        subscription_id: &str,
        live_id: &LiveQueryId,
    ) -> Result<(), KalamDbError> {
        self.subscription_service
            .unregister_subscription(connection_state, subscription_id, live_id)
            .await
    }

    /// Unregister a live query subscription by LiveQueryId
    ///
    /// This is a convenience method that looks up the connection state
    /// from the LiveQueryId. Used by KILL LIVE QUERY command.
    pub async fn unregister_subscription_by_id(
        &self,
        live_id: &LiveQueryId,
    ) -> Result<(), KalamDbError> {
        // Get connection from registry
        let connection_state =
            self.registry.get_connection(&live_id.connection_id).ok_or_else(|| {
                KalamDbError::NotFound(format!("Connection not found for live query: {}", live_id))
            })?;

        // Extract subscription_id from live_id
        let subscription_id = live_id.subscription_id();

        self.subscription_service
            .unregister_subscription(&connection_state, subscription_id, live_id)
            .await
    }

    /// Get all subscriptions for a user
    pub async fn get_user_subscriptions(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<SystemLiveQuery>, KalamDbError> {
        self.live_queries_provider
            .get_by_user_id_async(user_id)
            .await
            .into_kalamdb_error("Failed to get user subscriptions")
    }

    /// Get a specific live query
    pub async fn get_live_query(
        &self,
        live_id: &str,
    ) -> Result<Option<SystemLiveQuery>, KalamDbError> {
        self.live_queries_provider
            .get_live_query_async(live_id)
            .await
            .into_kalamdb_error("Failed to get live query")
    }

    /// Get registry statistics
    pub async fn get_stats(&self) -> RegistryStats {
        RegistryStats {
            total_connections: self.registry.connection_count(),
            total_subscriptions: self.registry.subscription_count(),
            node_id: self.node_id.to_string(),
        }
    }

    /// Get the connections manager
    pub fn registry(&self) -> Arc<ConnectionsManager> {
        Arc::clone(&self.registry)
    }

    // /// Notify live query subscribers of a table change
    // pub async fn notify_table_change(
    //     &self,
    //     user_id: &UserId,
    //     table_id: &TableId,
    //     change_notification: ChangeNotification,
    // ) -> Result<usize, KalamDbError> {
    //     self.notification_service
    //         .notify_table_change(user_id, table_id, change_notification)
    //         .await
    // }

    /// Handle auth expiry for a connection
    pub async fn handle_auth_expiry(
        &self,
        connection_id: &ConnectionId,
    ) -> Result<(), KalamDbError> {
        let connection_state = self.registry.get_connection(connection_id).ok_or_else(|| {
            KalamDbError::NotFound(format!("Connection not found: {}", connection_id))
        })?;

        let user_id = connection_state.user_id().cloned().ok_or_else(|| {
            KalamDbError::NotFound(format!("User not found for connection: {}", connection_id))
        })?;

        let removed_ids = self.unregister_connection(&user_id, connection_id).await?;

        for live_id in removed_ids {
            let _ = self.live_queries_provider.delete_live_query_async(&live_id).await;
        }

        Ok(())
    }

    // ==================== Backward Compatibility ====================
    // These methods support older interfaces that use connection_id instead of SharedConnectionState

    /// Get the total number of connections
    pub fn connection_count(&self) -> usize {
        self.registry.connection_count()
    }

    /// Get the total number of subscriptions
    pub fn subscription_count(&self) -> usize {
        self.registry.subscription_count()
    }
}

#[cfg(test)]
mod tests {
    use super::LiveQueryManager;
    use crate::error::KalamDbError;
    use kalamdb_commons::models::{NamespaceId, TableId, TableName};
    use kalamdb_commons::schemas::table_options::{SharedTableOptions, SystemTableOptions};
    use kalamdb_commons::schemas::{TableDefinition, TableOptions};
    use kalamdb_commons::{Role, TableAccess, TableType};

    fn table_id() -> TableId {
        TableId::new(NamespaceId::from("shared"), TableName::from("events"))
    }

    fn shared_table_def(access: TableAccess) -> TableDefinition {
        TableDefinition {
            namespace_id: NamespaceId::from("shared"),
            table_name: TableName::from("events"),
            table_type: TableType::Shared,
            columns: vec![],
            schema_version: 1,
            next_column_id: 1,
            table_options: TableOptions::Shared(SharedTableOptions {
                storage_id: kalamdb_commons::StorageId::from("default"),
                access_level: Some(access),
                flush_policy: None,
                compression: "none".to_string(),
            }),
            table_comment: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    fn system_table_def() -> TableDefinition {
        TableDefinition {
            namespace_id: NamespaceId::system(),
            table_name: TableName::from("users"),
            table_type: TableType::System,
            columns: vec![],
            schema_version: 1,
            next_column_id: 1,
            table_options: TableOptions::System(SystemTableOptions {
                read_only: true,
                enable_cache: true,
                cache_ttl_seconds: 60,
                localhost_only: false,
            }),
            table_comment: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_validate_permission_allows_public_shared_for_user_role() {
        let def = shared_table_def(TableAccess::Public);
        let result =
            LiveQueryManager::validate_table_subscription_permission(Role::User, &def, &table_id());
        assert!(
            result.is_ok(),
            "public shared table subscriptions should be allowed for regular users"
        );
    }

    #[test]
    fn test_validate_permission_denies_private_shared_for_user_role() {
        let def = shared_table_def(TableAccess::Private);
        let result =
            LiveQueryManager::validate_table_subscription_permission(Role::User, &def, &table_id());
        assert!(
            matches!(result, Err(KalamDbError::PermissionDenied(_))),
            "private shared table subscriptions should be denied for regular users"
        );
    }

    #[test]
    fn test_validate_permission_allows_private_shared_for_dba() {
        let def = shared_table_def(TableAccess::Private);
        let result =
            LiveQueryManager::validate_table_subscription_permission(Role::Dba, &def, &table_id());
        assert!(result.is_ok(), "private shared table subscriptions should be allowed for DBA");
    }

    #[test]
    fn test_validate_permission_denies_system_for_user_role() {
        let def = system_table_def();
        let result =
            LiveQueryManager::validate_table_subscription_permission(Role::User, &def, &table_id());
        assert!(matches!(result, Err(KalamDbError::PermissionDenied(_))));
    }

    #[test]
    fn test_validate_permission_allows_system_for_dba() {
        let def = system_table_def();
        let result =
            LiveQueryManager::validate_table_subscription_permission(Role::Dba, &def, &table_id());
        assert!(result.is_ok());
    }
}
