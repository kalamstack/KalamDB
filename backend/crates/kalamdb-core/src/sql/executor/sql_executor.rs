use super::{PreparedExecutionStatement, SqlExecutor};
use crate::error::KalamDbError;
use crate::sql::executor::handler_registry::HandlerRegistry;
use crate::sql::executor::request_transaction_state::RequestTransactionState;
use crate::sql::plan_cache::{PlanCacheKey, SqlCacheRegistry, SqlCacheRegistryConfig};
use crate::sql::{ExecutionContext, ExecutionResult};
use crate::transactions::CoordinatorAccessValidator;
use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::conversions::arrow_json_conversion::arrow_value_to_scalar;
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::{NamespaceId, TableId, TransactionId};
use kalamdb_commons::Role;
use kalamdb_sql::classifier::{SqlStatement, SqlStatementKind, StatementClassificationError};
use kalamdb_transactions::{TransactionQueryContext, TransactionQueryExtension};
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;

#[derive(Debug, Clone, Copy)]
enum DmlKind {
    Insert,
    Update,
    Delete,
}

impl SqlExecutor {
    async fn try_execute_embedding_literal_insert_via_applier(
        &self,
        sql: &str,
        metadata: &PreparedExecutionStatement,
        exec_ctx: &ExecutionContext,
    ) -> Result<Option<ExecutionResult>, KalamDbError> {
        let Some(table_id) = metadata.table_id.as_ref() else {
            return Ok(None);
        };

        let mut request_transaction_state =
            RequestTransactionState::from_execution_context(exec_ctx)?;
        if let Some(state) = request_transaction_state.as_mut() {
            state.sync_from_coordinator(self.app_context.as_ref());
            if state.is_active() {
                return Ok(None);
            }
        }

        let Some(cached_table) = self.app_context.schema_registry().get(table_id) else {
            return Ok(None);
        };
        let has_embedding_columns = cached_table
            .table
            .columns
            .iter()
            .any(|column| matches!(column.data_type, KalamDataType::Embedding(_)));
        if !has_embedding_columns {
            return Ok(None);
        }

        let dialect = sqlparser::dialect::GenericDialect {};
        let parsed_statements = kalamdb_sql::parser::utils::parse_sql_statements(sql, &dialect)
            .map_err(|error| KalamDbError::InvalidSql(error.to_string()))?;
        if parsed_statements.len() != 1 {
            return Ok(None);
        }

        let Some(insert_rows) = super::transaction_batch_insert::try_build_literal_insert_rows(
            &parsed_statements[0],
            self.app_context.as_ref(),
            self.sql_cache_registry.as_ref(),
            exec_ctx,
            table_id,
        )?
        else {
            return Ok(None);
        };

        let applier = self.app_context.applier();
        let rows_affected = match insert_rows.table_type {
            kalamdb_commons::schemas::TableType::Shared => applier
                .insert_shared_data(table_id.clone(), insert_rows.rows)
                .await
                .map_err(KalamDbError::from)?
                .rows_affected(),
            kalamdb_commons::schemas::TableType::User
            | kalamdb_commons::schemas::TableType::Stream => applier
                .insert_user_data(table_id.clone(), exec_ctx.user_id().clone(), insert_rows.rows)
                .await
                .map_err(KalamDbError::from)?
                .rows_affected(),
            kalamdb_commons::schemas::TableType::System => return Ok(None),
        };

        Ok(Some(ExecutionResult::Inserted {
            rows_affected: rows_affected as usize,
        }))
    }

    fn logical_plan_has_limit(plan: &datafusion::logical_expr::LogicalPlan) -> bool {
        matches!(plan, datafusion::logical_expr::LogicalPlan::Limit(_))
            || plan.inputs().iter().any(|input| Self::logical_plan_has_limit(input))
    }

    fn apply_select_limits(
        &self,
        df: datafusion::dataframe::DataFrame,
    ) -> Result<datafusion::dataframe::DataFrame, KalamDbError> {
        let max_query_limit = self.app_context.config().limits.max_query_limit;
        let default_query_limit = self.app_context.config().limits.default_query_limit;
        let has_explicit_limit = Self::logical_plan_has_limit(df.logical_plan());

        if !has_explicit_limit && default_query_limit > 0 {
            let effective_default_limit = if max_query_limit > 0 {
                default_query_limit.min(max_query_limit)
            } else {
                default_query_limit
            };

            log::debug!(
                target: "sql::exec",
                "Applying default query limit {} to unbounded SELECT",
                effective_default_limit
            );

            return df
                .limit(0, Some(effective_default_limit))
                .map_err(Self::datafusion_to_execution_error);
        }

        if max_query_limit > 0 {
            return df.limit(0, Some(max_query_limit)).map_err(Self::datafusion_to_execution_error);
        }

        Ok(df)
    }

    fn dml_operation_name(dml_kind: DmlKind) -> &'static str {
        match dml_kind {
            DmlKind::Insert => "INSERT",
            DmlKind::Update => "UPDATE",
            DmlKind::Delete => "DELETE",
        }
    }

    fn block_system_namespace_dml(
        &self,
        table_id: Option<&TableId>,
        dml_kind: DmlKind,
    ) -> Result<(), KalamDbError> {
        let Some(table_id) = table_id else {
            return Ok(());
        };

        if table_id.namespace_id().is_system_namespace() {
            let op = Self::dml_operation_name(dml_kind);
            return Err(KalamDbError::InvalidOperation(format!(
                "Cannot {} system table '{}.{}'",
                op.to_lowercase(),
                table_id.namespace_id().as_str(),
                table_id.table_name().as_str(),
            )));
        }
        Ok(())
    }

    /// Try to extract a typed `KalamDbError::NotLeader` from a `DataFusionError`.
    ///
    /// Table providers wrap [`kalamdb_commons::NotLeaderError`] inside
    /// `DataFusionError::External(...)`.  This method downcasts back to the
    /// concrete type — no string parsing required.
    fn try_not_leader_error(e: &datafusion::error::DataFusionError) -> Option<KalamDbError> {
        if let datafusion::error::DataFusionError::External(inner) = e {
            if let Some(nle) = inner.downcast_ref::<kalamdb_commons::NotLeaderError>() {
                return Some(KalamDbError::NotLeader {
                    leader_addr: nle.leader_addr.clone(),
                });
            }
        }
        None
    }

    /// Convert a `DataFusionError` into a `KalamDbError`, preserving
    /// `NotLeader` semantics when the error wraps a [`kalamdb_commons::NotLeaderError`].
    fn datafusion_to_execution_error(e: datafusion::error::DataFusionError) -> KalamDbError {
        Self::classify_datafusion_error(&e)
    }

    fn is_table_not_found_error(e: &datafusion::error::DataFusionError) -> bool {
        Self::is_table_not_found_msg(&e.to_string().to_lowercase())
    }

    fn is_table_not_found_msg(msg: &str) -> bool {
        (msg.contains("table") && msg.contains("not found"))
            || (msg.contains("relation") && msg.contains("does not exist"))
            || msg.contains("unknown table")
    }

    fn is_permission_msg(msg: &str) -> bool {
        msg.contains("access denied")
            || msg.contains("permission denied")
            || msg.contains("unauthorized")
            || msg.contains("not authorized")
            || msg.contains("forbidden")
            || msg.contains("insufficient privileges")
    }

    fn is_column_not_found_msg(msg: &str) -> bool {
        (msg.contains("column") && msg.contains("not found"))
            || (msg.contains("field") && msg.contains("not found"))
            || msg.contains("no field named")
            || msg.contains("schema error: no field named")
    }

    fn is_constraint_violation_msg(msg: &str) -> bool {
        msg.contains("primary key")
            || msg.contains("constraint violation")
            || msg.contains("already exists")
            || msg.contains("duplicate")
            || msg.contains("unique constraint")
            || msg.contains("unique index")
    }

    /// Classify a DataFusionError into a KalamDbError with a single `to_string()`
    /// call, avoiding redundant allocations on the error path.
    fn classify_datafusion_error(e: &datafusion::error::DataFusionError) -> KalamDbError {
        if let Some(not_leader) = Self::try_not_leader_error(e) {
            return not_leader;
        }

        let error_msg = e.to_string();
        let lower = error_msg.to_lowercase();

        if Self::is_table_not_found_msg(&lower) {
            return KalamDbError::TableNotFound(error_msg);
        }

        if Self::is_permission_msg(&lower) {
            return KalamDbError::PermissionDenied(error_msg);
        }

        if Self::is_column_not_found_msg(&lower) {
            return KalamDbError::InvalidOperation(error_msg);
        }

        if Self::is_constraint_violation_msg(&lower) {
            return KalamDbError::AlreadyExists(error_msg);
        }

        KalamDbError::ExecutionError(error_msg)
    }

    fn map_classification_error(
        err: kalamdb_sql::classifier::StatementClassificationError,
    ) -> KalamDbError {
        match err {
            kalamdb_sql::classifier::StatementClassificationError::Unauthorized(msg) => {
                KalamDbError::Unauthorized(msg)
            },
            kalamdb_sql::classifier::StatementClassificationError::InvalidSql {
                sql: _,
                message,
            } => KalamDbError::InvalidSql(message),
        }
    }

    fn is_ddl_statement(kind: &SqlStatementKind) -> bool {
        matches!(
            kind,
            SqlStatementKind::CreateNamespace(_)
                | SqlStatementKind::AlterNamespace(_)
                | SqlStatementKind::DropNamespace(_)
                | SqlStatementKind::CreateStorage(_)
                | SqlStatementKind::AlterStorage(_)
                | SqlStatementKind::DropStorage(_)
                | SqlStatementKind::CreateTable(_)
                | SqlStatementKind::CreateView(_)
                | SqlStatementKind::AlterTable(_)
                | SqlStatementKind::DropTable(_)
        )
    }

    fn request_transaction_state<'a>(
        &self,
        exec_ctx: &'a ExecutionContext,
    ) -> Result<Option<RequestTransactionState<'a>>, KalamDbError> {
        let mut request_state = RequestTransactionState::from_execution_context(exec_ctx)?;
        if let Some(state) = request_state.as_mut() {
            state.sync_from_coordinator(&self.app_context);
        }
        Ok(request_state)
    }

    fn active_request_transaction_id(
        &self,
        exec_ctx: &ExecutionContext,
    ) -> Result<Option<TransactionId>, KalamDbError> {
        Ok(self
            .request_transaction_state(exec_ctx)?
            .and_then(|state| state.active_transaction_id().cloned()))
    }

    fn transaction_query_context_for_request(
        &self,
        exec_ctx: &ExecutionContext,
    ) -> Result<Option<TransactionQueryContext>, KalamDbError> {
        let Some(transaction_id) = self.active_request_transaction_id(exec_ctx)? else {
            return Ok(None);
        };

        let coordinator = self.app_context.transaction_coordinator();
        let handle = coordinator.get_handle(&transaction_id).ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "active SQL transaction '{}' has no handle",
                transaction_id
            ))
        })?;

        if !handle.state.is_open() {
            return Err(KalamDbError::InvalidOperation(format!(
                "transaction '{}' is {}",
                transaction_id, handle.state
            )));
        }

        Ok(Some(TransactionQueryContext::new(
            transaction_id.clone(),
            handle.snapshot_commit_seq,
            Arc::new(crate::transactions::CoordinatorOverlayView::new(
                Arc::clone(&coordinator),
                transaction_id.clone(),
            )),
            Arc::new(crate::transactions::CoordinatorMutationSink::new(coordinator)),
            Arc::new(CoordinatorAccessValidator::new(self.app_context.transaction_coordinator())),
        )))
    }

    fn create_session_with_transaction_context(
        &self,
        exec_ctx: &ExecutionContext,
    ) -> Result<SessionContext, KalamDbError> {
        let Some(transaction_query_context) =
            self.transaction_query_context_for_request(exec_ctx)?
        else {
            return Ok(exec_ctx.create_session_with_user());
        };

        let mut state = exec_ctx.build_user_session_state();
        state
            .config_mut()
            .options_mut()
            .extensions
            .insert(TransactionQueryExtension::new(transaction_query_context));
        Ok(SessionContext::new_with_state(state))
    }

    async fn execute_begin_transaction(
        &self,
        exec_ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let mut request_state = RequestTransactionState::from_execution_context(exec_ctx)?
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(
                    "BEGIN requires a request-scoped execution context".to_string(),
                )
            })?;
        request_state.sync_from_coordinator(&self.app_context);
        let transaction_id = request_state.begin(&self.app_context)?;
        Ok(ExecutionResult::Success {
            message: format!("Transaction started ({})", transaction_id),
        })
    }

    async fn execute_commit_transaction(
        &self,
        exec_ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let mut request_state = RequestTransactionState::from_execution_context(exec_ctx)?
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(
                    "COMMIT requires a request-scoped execution context".to_string(),
                )
            })?;
        request_state.sync_from_coordinator(&self.app_context);
        let transaction_id = request_state.commit(&self.app_context).await?;
        Ok(ExecutionResult::Success {
            message: format!("Transaction committed ({})", transaction_id),
        })
    }

    fn execute_rollback_transaction(
        &self,
        exec_ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let mut request_state = RequestTransactionState::from_execution_context(exec_ctx)?
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(
                    "ROLLBACK requires a request-scoped execution context".to_string(),
                )
            })?;
        request_state.sync_from_coordinator(&self.app_context);
        let transaction_id = request_state.rollback(&self.app_context)?;
        Ok(ExecutionResult::Success {
            message: format!("Transaction rolled back ({})", transaction_id),
        })
    }

    fn reject_ddl_in_active_request_transaction(
        &self,
        classified: &SqlStatement,
        exec_ctx: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        if !Self::is_ddl_statement(classified.kind()) {
            return Ok(());
        }

        if let Some(transaction_id) = self.active_request_transaction_id(exec_ctx)? {
            self.app_context
                .transaction_coordinator()
                .reject_ddl_in_transaction(&transaction_id)?;
        }

        Ok(())
    }

    /// Construct a new executor with a pre-built handler registry.
    pub fn new(
        app_context: std::sync::Arc<crate::app_context::AppContext>,
        handler_registry: Arc<HandlerRegistry>,
    ) -> Self {
        let sql_cache_registry = Arc::new(SqlCacheRegistry::new(SqlCacheRegistryConfig::new(
            app_context.config().execution.sql_plan_cache_max_entries,
            Duration::from_secs(app_context.config().execution.sql_plan_cache_ttl_seconds),
        )));
        Self {
            app_context,
            handler_registry,
            sql_cache_registry,
        }
    }

    /// Clear SQL caches that may become stale after DDL operations.
    pub fn clear_plan_cache(&self) {
        self.sql_cache_registry.clear();
    }

    /// Get current plan cache size (diagnostics/testing)
    pub fn plan_cache_len(&self) -> usize {
        self.sql_cache_registry.plan_cache().len()
    }

    /// Batch-execute multiple INSERT statements targeting the same table in an
    /// active explicit transaction via the transaction batch insert path.
    ///
    /// Returns `Ok(Some(results))` with per-statement `ExecutionResult::Inserted`,
    /// `Ok(None)` if the batch path is not applicable (caller should fall back to
    /// per-statement execution), or `Err(e)` on execution failure.
    pub fn try_batch_insert_in_transaction(
        &self,
        statements: &[&PreparedExecutionStatement],
        exec_ctx: &ExecutionContext,
        transaction_id: &TransactionId,
    ) -> Result<Option<Vec<crate::sql::ExecutionResult>>, KalamDbError> {
        let batch_sql = statements
            .iter()
            .map(|statement| statement.sql.as_str())
            .collect::<Vec<_>>()
            .join("; ");
        let dialect = sqlparser::dialect::GenericDialect {};
        let parsed_stmts_storage =
            kalamdb_sql::parser::utils::parse_sql_statements(&batch_sql, &dialect)
                .map_err(|error| KalamDbError::InvalidSql(error.to_string()))?;

        if parsed_stmts_storage.len() != statements.len() {
            return Ok(None);
        }

        let parsed_stmts: Vec<&sqlparser::ast::Statement> = parsed_stmts_storage.iter().collect();

        let table_id = match statements[0].table_id.as_ref() {
            Some(id) => id,
            None => return Ok(None),
        };

        match super::transaction_batch_insert::try_batch_inserts_in_transaction(
            &parsed_stmts,
            self.app_context.as_ref(),
            self.sql_cache_registry.as_ref(),
            exec_ctx,
            table_id,
            transaction_id,
        )? {
            Some(counts) => Ok(Some(
                counts
                    .into_iter()
                    .map(|rows_affected| crate::sql::ExecutionResult::Inserted { rows_affected })
                    .collect(),
            )),
            None => Ok(None),
        }
    }

    pub fn prepare_statement_metadata(
        &self,
        sql: &str,
        exec_ctx: &ExecutionContext,
    ) -> Result<PreparedExecutionStatement, StatementClassificationError> {
        self.prepare_statement_metadata_for_role(
            sql,
            &exec_ctx.default_namespace(),
            exec_ctx.user_role(),
        )
    }

    pub fn prepare_statement_metadata_for_role(
        &self,
        sql: &str,
        default_namespace: &NamespaceId,
        role: Role,
    ) -> Result<PreparedExecutionStatement, StatementClassificationError> {
        let classified = SqlStatement::classify_and_parse(sql, default_namespace, role)?;
        let table_id = match classified.kind() {
            SqlStatementKind::Insert(_)
            | SqlStatementKind::Update(_)
            | SqlStatementKind::Delete(_) => {
                kalamdb_sql::extract_dml_table_id_fast(sql, default_namespace.as_str())
                    .or_else(|| kalamdb_sql::extract_dml_table_id(sql, default_namespace.as_str()))
            },
            _ => None,
        };
        let table_type = table_id.as_ref().and_then(|table_id| {
            self.app_context
                .schema_registry()
                .get(table_id)
                .map(|cached| cached.table_entry().table_type)
        });

        Ok(PreparedExecutionStatement::new(
            sql.to_string(),
            table_id,
            table_type,
            Some(classified),
        ))
    }

    /// Execute a statement without request metadata.
    pub async fn execute(
        &self,
        sql: &str,
        exec_ctx: &ExecutionContext,
        params: Vec<ScalarValue>,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Step 0: Check SQL query length to prevent DoS attacks
        if sql.len() > kalamdb_commons::constants::MAX_SQL_QUERY_LENGTH {
            log::warn!(
                "SQL query rejected: length {} bytes exceeds maximum {} bytes",
                sql.len(),
                kalamdb_commons::constants::MAX_SQL_QUERY_LENGTH
            );
            return Err(KalamDbError::InvalidSql(format!(
                "SQL query too long: {} bytes (maximum {} bytes)",
                sql.len(),
                kalamdb_commons::constants::MAX_SQL_QUERY_LENGTH
            )));
        }

        let metadata = self
            .prepare_statement_metadata(sql, exec_ctx)
            .map_err(Self::map_classification_error)?;

        self.execute_with_metadata(&metadata, exec_ctx, params).await
    }

    /// Execute a statement with prepared metadata.
    pub async fn execute_with_metadata(
        &self,
        metadata: &PreparedExecutionStatement,
        exec_ctx: &ExecutionContext,
        params: Vec<ScalarValue>,
    ) -> Result<ExecutionResult, KalamDbError> {
        let sql = metadata.sql.as_str();
        let span = tracing::info_span!(
            "sql.execute",
            user_id = %exec_ctx.user_id().as_str(),
            namespace = %exec_ctx.default_namespace().as_str(),
            command = tracing::field::Empty,
            rows = tracing::field::Empty,
        );
        // Enter the span for the entire execution
        async {
            let classified = metadata.classified_statement.clone().ok_or_else(|| {
                KalamDbError::InvalidSql(
                    "Missing pre-classified statement metadata for SQL execution".to_string(),
                )
            })?;

            // Record the command kind in the span
            let command_label = format!("{:?}", classified.kind());
            tracing::Span::current().record("command", &command_label.as_str());

            self.reject_ddl_in_active_request_transaction(&classified, exec_ctx)?;

            // Step 2: Route based on statement type
            let result = match classified.kind() {
                SqlStatementKind::BeginTransaction => {
                    self.execute_begin_transaction(exec_ctx).await
                },
                SqlStatementKind::CommitTransaction => {
                    self.execute_commit_transaction(exec_ctx).await
                },
                SqlStatementKind::RollbackTransaction => {
                    self.execute_rollback_transaction(exec_ctx)
                },

                // Hot path: SELECT queries use DataFusion
                // Tables are already registered in base session, we just inject user_id
                SqlStatementKind::Select => {
                    self.execute_via_datafusion(classified.as_str(), params, exec_ctx).await
                },

                // DataFusion meta commands (EXPLAIN, SET, SHOW, etc.) - admin only
                // No caching needed - these are diagnostic/config commands
                // Authorization already checked in classifier
                SqlStatementKind::DataFusionMetaCommand => {
                    self.execute_meta_command(sql, exec_ctx).await
                },

                // Native DataFusion DML path (provider insert/update/delete hooks)
                SqlStatementKind::Insert(_) => {
                    if params.is_empty() {
                        if let Some(result) = self
                            .try_execute_embedding_literal_insert_via_applier(
                                classified.as_str(),
                                metadata,
                                exec_ctx,
                            )
                            .await?
                        {
                            Ok(result)
                        } else {
                            self.execute_dml_via_datafusion(
                                classified.as_str(),
                                metadata,
                                params,
                                exec_ctx,
                                DmlKind::Insert,
                            )
                            .await
                        }
                    } else {
                        self.execute_dml_via_datafusion(
                            classified.as_str(),
                            metadata,
                            params,
                            exec_ctx,
                            DmlKind::Insert,
                        )
                        .await
                    }
                },
                SqlStatementKind::Update(_) => {
                    self.execute_dml_via_datafusion(
                        classified.as_str(),
                        metadata,
                        params,
                        exec_ctx,
                        DmlKind::Update,
                    )
                    .await
                },
                SqlStatementKind::Delete(_) => {
                    self.execute_dml_via_datafusion(
                        classified.as_str(),
                        metadata,
                        params,
                        exec_ctx,
                        DmlKind::Delete,
                    )
                    .await
                },

                // DDL operations that modify table/view structure require plan cache invalidation
                // This prevents stale cached plans from referencing dropped/altered tables
                SqlStatementKind::CreateTable(_)
                | SqlStatementKind::DropTable(_)
                | SqlStatementKind::AlterTable(_)
                | SqlStatementKind::CreateView(_)
                | SqlStatementKind::CreateNamespace(_)
                | SqlStatementKind::DropNamespace(_) => {
                    let result = self.handler_registry.handle(classified, params, exec_ctx).await;
                    // Clear plan cache after DDL to invalidate any cached plans
                    // that may reference the modified schema
                    if result.is_ok() {
                        self.sql_cache_registry.clear();
                        log::debug!("SQL caches cleared after DDL operation");
                    }
                    result
                },

                // All other statements: Delegate to handler registry (no cache invalidation needed)
                _ => self.handler_registry.handle(classified, params, exec_ctx).await,
            };

            // Record row count in the span
            if let Ok(ref res) = result {
                let rows = match res {
                    ExecutionResult::Rows { row_count, .. } => *row_count,
                    ExecutionResult::Inserted { rows_affected } => *rows_affected,
                    ExecutionResult::Updated { rows_affected } => *rows_affected,
                    ExecutionResult::Deleted { rows_affected } => *rows_affected,
                    _ => 0,
                };
                tracing::Span::current().record("rows", rows);
            }

            result
        }
        .instrument(span)
        .await
    }

    #[tracing::instrument(
        name = "sql.dml_datafusion",
        skip_all,
        fields(
            dml_kind = %Self::dml_operation_name(dml_kind),
            rows_affected = tracing::field::Empty,
        )
    )]
    async fn execute_dml_via_datafusion(
        &self,
        sql: &str,
        metadata: &PreparedExecutionStatement,
        params: Vec<ScalarValue>,
        exec_ctx: &ExecutionContext,
        dml_kind: DmlKind,
    ) -> Result<ExecutionResult, KalamDbError> {
        self.block_system_namespace_dml(metadata.table_id.as_ref(), dml_kind)?;

        let execution_sql = kalamdb_sql::rewrite_context_functions_for_datafusion(sql);
        let execution_sql: &str = &execution_sql;

        use crate::sql::executor::parameter_binding::{
            replace_placeholders_in_plan, validate_params,
        };

        if !params.is_empty() {
            validate_params(&params)?;
        }

        // Parameterized DML: reuse cached template plans and only bind placeholders per request.
        // This avoids reparsing/replanning the same INSERT/UPDATE/DELETE shape repeatedly.
        let df = if params.is_empty() {
            let session = self.create_session_with_transaction_context(exec_ctx)?;
            let plan_start = std::time::Instant::now();
            match session.sql(execution_sql).await {
                Ok(df) => {
                    tracing::debug!(plan_ms = %plan_start.elapsed().as_micros() as f64 / 1000.0, "sql.dml_plan");
                    df
                },
                Err(e) => {
                    if Self::is_table_not_found_error(&e) {
                        if let Err(load_err) = self.load_existing_tables().await {
                            log::warn!(
                                target: "sql::dml",
                                "⚠️  Failed to reload table providers after missing table in DML | sql='{}' | error='{}'",
                                sql,
                                load_err
                            );
                        }
                        let retry_session =
                            self.create_session_with_transaction_context(exec_ctx)?;
                        retry_session
                            .sql(execution_sql)
                            .await
                            .map_err(|e2| self.log_sql_error(sql, exec_ctx, e2))?
                    } else {
                        return Err(self.log_sql_error(sql, exec_ctx, e));
                    }
                },
            }
        } else {
            let cache_key = PlanCacheKey::new(
                exec_ctx.default_namespace().clone(),
                exec_ctx.user_role(),
                execution_sql,
            );
            let session = self.create_session_with_transaction_context(exec_ctx)?;

            if let Some(template_plan) = self.sql_cache_registry.plan_cache().get(&cache_key) {
                let bound_plan = replace_placeholders_in_plan((*template_plan).clone(), &params)?;
                match session.execute_logical_plan(bound_plan).await {
                    Ok(df) => df,
                    Err(e) => {
                        log::warn!(
                            target: "sql::dml",
                            "Failed to execute cached DML plan, reparsing SQL: {}",
                            e
                        );

                        match session.sql(execution_sql).await {
                            Ok(planned_df) => {
                                let template_plan = planned_df.logical_plan().clone();
                                self.sql_cache_registry
                                    .plan_cache()
                                    .insert(cache_key.clone(), template_plan.clone());
                                let rebound_plan =
                                    replace_placeholders_in_plan(template_plan, &params)?;
                                session
                                    .execute_logical_plan(rebound_plan)
                                    .await
                                    .map_err(|e2| Self::datafusion_to_execution_error(e2))?
                            },
                            Err(e) => {
                                if Self::is_table_not_found_error(&e) {
                                    if let Err(load_err) = self.load_existing_tables().await {
                                        log::warn!(
                                            target: "sql::dml",
                                            "⚠️  Failed to reload table providers after missing table in DML | sql='{}' | error='{}'",
                                            sql,
                                            load_err
                                        );
                                    }
                                    let retry_session =
                                        self.create_session_with_transaction_context(exec_ctx)?;
                                    let retry_df = retry_session
                                        .sql(execution_sql)
                                        .await
                                        .map_err(|e2| self.log_sql_error(sql, exec_ctx, e2))?;
                                    let template_plan = retry_df.logical_plan().clone();
                                    self.sql_cache_registry
                                        .plan_cache()
                                        .insert(cache_key.clone(), template_plan.clone());
                                    let rebound_plan =
                                        replace_placeholders_in_plan(template_plan, &params)?;
                                    retry_session
                                        .execute_logical_plan(rebound_plan)
                                        .await
                                        .map_err(|e3| Self::datafusion_to_execution_error(e3))?
                                } else {
                                    return Err(self.log_sql_error(sql, exec_ctx, e));
                                }
                            },
                        }
                    },
                }
            } else {
                match session.sql(execution_sql).await {
                    Ok(planned_df) => {
                        let template_plan = planned_df.logical_plan().clone();
                        self.sql_cache_registry
                            .plan_cache()
                            .insert(cache_key.clone(), template_plan.clone());
                        let bound_plan = replace_placeholders_in_plan(template_plan, &params)?;
                        session
                            .execute_logical_plan(bound_plan)
                            .await
                            .map_err(|e2| Self::datafusion_to_execution_error(e2))?
                    },
                    Err(e) => {
                        if Self::is_table_not_found_error(&e) {
                            if let Err(load_err) = self.load_existing_tables().await {
                                log::warn!(
                                    target: "sql::dml",
                                    "⚠️  Failed to reload table providers after missing table in DML | sql='{}' | error='{}'",
                                    sql,
                                    load_err
                                );
                            }
                            let retry_session =
                                self.create_session_with_transaction_context(exec_ctx)?;
                            let retry_df = retry_session
                                .sql(execution_sql)
                                .await
                                .map_err(|e2| self.log_sql_error(sql, exec_ctx, e2))?;

                            let template_plan = retry_df.logical_plan().clone();
                            self.sql_cache_registry
                                .plan_cache()
                                .insert(cache_key.clone(), template_plan.clone());
                            let bound_plan = replace_placeholders_in_plan(template_plan, &params)?;
                            retry_session
                                .execute_logical_plan(bound_plan)
                                .await
                                .map_err(|e3| Self::datafusion_to_execution_error(e3))?
                        } else {
                            return Err(self.log_sql_error(sql, exec_ctx, e));
                        }
                    },
                }
            }
        };

        let collect_start = std::time::Instant::now();
        let batches = df.collect().await.map_err(Self::datafusion_to_execution_error)?;
        tracing::debug!(collect_ms = %format!("{:.3}", collect_start.elapsed().as_micros() as f64 / 1000.0), "sql.dml_collect");

        let rows_affected = Self::extract_rows_affected(&batches)?;
        tracing::Span::current().record("rows_affected", rows_affected);

        Ok(match dml_kind {
            DmlKind::Insert => ExecutionResult::Inserted { rows_affected },
            DmlKind::Update => ExecutionResult::Updated { rows_affected },
            DmlKind::Delete => ExecutionResult::Deleted { rows_affected },
        })
    }

    /// Execute SELECT via DataFusion with per-user session
    #[tracing::instrument(
        name = "sql.select_datafusion",
        skip_all,
        fields(row_count = tracing::field::Empty)
    )]
    async fn execute_via_datafusion(
        &self,
        sql: &str,
        params: Vec<ScalarValue>,
        exec_ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let execution_sql = kalamdb_sql::rewrite_context_functions_for_datafusion(sql);
        let execution_sql: &str = &execution_sql;
        use crate::sql::executor::default_ordering::apply_default_order_by;
        use crate::sql::executor::parameter_binding::{
            replace_placeholders_in_plan, validate_params,
        };

        // Validate parameters if present
        if !params.is_empty() {
            validate_params(&params)?;
        }

        let session = self.create_session_with_transaction_context(exec_ctx)?;

        // Try cached template plan first (works for both plain and parameterized SQL).
        // Key excludes user_id because LogicalPlan is user-agnostic - filtering happens at scan time.
        let cache_key = PlanCacheKey::new(
            exec_ctx.default_namespace().clone(),
            exec_ctx.user_role(),
            execution_sql,
        );

        let df = if let Some(template_plan) = self.sql_cache_registry.plan_cache().get(&cache_key) {
            let executable_plan = if params.is_empty() {
                (*template_plan).clone()
            } else {
                replace_placeholders_in_plan((*template_plan).clone(), &params)?
            };

            match session.execute_logical_plan(executable_plan).await {
                Ok(df) => df,
                Err(e) => {
                    log::warn!("Failed to execute cached plan, reparsing SQL: {}", e);
                    let planned_df = match session.sql(execution_sql).await {
                        Ok(df) => df,
                        Err(e) => {
                            if Self::is_table_not_found_error(&e) {
                                log::warn!(
                                    target: "sql::plan",
                                    "⚠️  Table not found during planning; reloading table providers and retrying once | sql='{}'",
                                    sql
                                );
                                if let Err(e) = self.load_existing_tables().await {
                                    log::warn!(
                                        target: "sql::plan",
                                        "⚠️  Failed to reload table providers after missing table | sql='{}' | error='{}'",
                                        sql,
                                        e
                                    );
                                }
                                let retry_session =
                                    self.create_session_with_transaction_context(exec_ctx)?;
                                match retry_session.sql(execution_sql).await {
                                    Ok(df) => df,
                                    Err(e2) => {
                                        return Err(self.log_sql_error(sql, exec_ctx, e2));
                                    },
                                }
                            } else {
                                return Err(self.log_sql_error(sql, exec_ctx, e));
                            }
                        },
                    };

                    let ordered_template = apply_default_order_by(
                        planned_df.logical_plan().clone(),
                        &self.app_context,
                    )
                    .await?;
                    self.sql_cache_registry
                        .plan_cache()
                        .insert(cache_key.clone(), ordered_template.clone());

                    let executable_plan = if params.is_empty() {
                        ordered_template
                    } else {
                        replace_placeholders_in_plan(ordered_template, &params)?
                    };

                    match session.execute_logical_plan(executable_plan).await {
                        Ok(df) => df,
                        Err(e) => {
                            if let Some(not_leader_err) = Self::try_not_leader_error(&e) {
                                return Err(not_leader_err);
                            }
                            log::error!(
                                target: "sql::exec",
                                "❌ SQL execution failed after replan | sql='{}' | params={} | error='{}'",
                                sql,
                                params.len(),
                                e
                            );
                            return Err(Self::datafusion_to_execution_error(e));
                        },
                    }
                },
            }
        } else {
            let planned_df = match session.sql(execution_sql).await {
                Ok(df) => df,
                Err(e) => {
                    if Self::is_table_not_found_error(&e) {
                        log::warn!(
                            target: "sql::plan",
                            "⚠️  Table not found during planning; reloading table providers and retrying once | sql='{}'",
                            sql
                        );
                        if let Err(e) = self.load_existing_tables().await {
                            log::warn!(
                                target: "sql::plan",
                                "⚠️  Failed to reload table providers after missing table | sql='{}' | error='{}'",
                                sql,
                                e
                            );
                        }
                        let retry_session =
                            self.create_session_with_transaction_context(exec_ctx)?;
                        match retry_session.sql(execution_sql).await {
                            Ok(df) => df,
                            Err(e2) => {
                                return Err(self.log_sql_error(sql, exec_ctx, e2));
                            },
                        }
                    } else {
                        return Err(self.log_sql_error(sql, exec_ctx, e));
                    }
                },
            };

            // Apply default ORDER BY by primary key columns (or _seq as fallback)
            // and cache the ordered template plan for subsequent executions.
            let ordered_template =
                apply_default_order_by(planned_df.logical_plan().clone(), &self.app_context)
                    .await?;
            self.sql_cache_registry.plan_cache().insert(cache_key, ordered_template.clone());

            let executable_plan = if params.is_empty() {
                ordered_template
            } else {
                replace_placeholders_in_plan(ordered_template, &params)?
            };

            match session.execute_logical_plan(executable_plan).await {
                Ok(df) => df,
                Err(e) => {
                    if let Some(not_leader_err) = Self::try_not_leader_error(&e) {
                        return Err(not_leader_err);
                    }
                    log::error!(
                        target: "sql::exec",
                        "❌ SQL execution failed | sql='{}' | params={} | error='{}'",
                        sql,
                        params.len(),
                        e
                    );
                    return Err(Self::datafusion_to_execution_error(e));
                },
            }
        };

        let df = self.apply_select_limits(df)?;

        // Capture schema before collecting (needed for 0 row results)
        // DFSchema -> Arrow Schema via inner() method
        let schema: arrow::datatypes::SchemaRef =
            std::sync::Arc::new(df.schema().as_arrow().clone());

        // Execute and collect results (log execution errors)
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                // Propagate NOT_LEADER as a typed error so the HTTP layer can forward to leader.
                if let Some(not_leader_err) = Self::try_not_leader_error(&e) {
                    return Err(not_leader_err);
                }
                return Err(self.log_sql_error(sql, exec_ctx, e));
            },
        };

        // Calculate total row count
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        tracing::Span::current().record("row_count", row_count);

        // Return batches with row count and schema (schema is needed when batches is empty)
        Ok(ExecutionResult::Rows {
            batches,
            row_count,
            schema: Some(schema),
        })
    }

    /// Execute DataFusion meta commands (EXPLAIN, SET, SHOW, etc.)
    ///
    /// These commands are passed directly to DataFusion without custom parsing.
    /// No plan caching is performed since these are diagnostic/config commands.
    /// Authorization is already checked in the classifier (admin only).
    #[tracing::instrument(name = "sql.meta_command", skip_all)]
    async fn execute_meta_command(
        &self,
        sql: &str,
        exec_ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let execution_sql = kalamdb_sql::rewrite_context_functions_for_datafusion(sql);
        let execution_sql: &str = &execution_sql;
        // Create per-request SessionContext with user_id injected
        let session = self.create_session_with_transaction_context(exec_ctx)?;

        // Execute the command directly via DataFusion
        let df = match session.sql(execution_sql).await {
            Ok(df) => df,
            Err(e) => {
                log::error!(
                    target: "sql::meta",
                    "❌ Meta command failed | sql='{}' | user='{}' | role='{:?}' | error='{}'",
                    sql,
                    exec_ctx.user_id().as_str(),
                    exec_ctx.user_role(),
                    e
                );
                return Err(Self::datafusion_to_execution_error(e));
            },
        };

        // Capture schema before collecting
        let schema: arrow::datatypes::SchemaRef =
            std::sync::Arc::new(df.schema().as_arrow().clone());

        // Execute and collect results
        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                if let Some(not_leader_err) = Self::try_not_leader_error(&e) {
                    return Err(not_leader_err);
                }
                return Err(self.log_sql_error(sql, exec_ctx, e));
            },
        };

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // log::debug!(
        //     target: "sql::meta",
        //     "✅ Meta command completed | sql='{}' | rows={}",
        //     sql,
        //     row_count
        // );

        Ok(ExecutionResult::Rows {
            batches,
            row_count,
            schema: Some(schema),
        })
    }

    /// Log SQL errors with appropriate level (warn for user errors, error for system errors)
    fn log_sql_error(
        &self,
        sql: &str,
        exec_ctx: &ExecutionContext,
        e: datafusion::error::DataFusionError,
    ) -> KalamDbError {
        let mapped_error = Self::classify_datafusion_error(&e);

        match &mapped_error {
            KalamDbError::TableNotFound(_) => {
                log::warn!(
                    target: "sql::plan",
                    "⚠️  Table not found | sql='{}' | user='{}' | role='{:?}' | error='{}'",
                    sql,
                    exec_ctx.user_id().as_str(),
                    exec_ctx.user_role(),
                    e
                );
            },
            KalamDbError::PermissionDenied(_) => {
                log::warn!(
                    target: "sql::plan",
                    "⚠️  SQL permission denied | sql='{}' | user='{}' | role='{:?}' | error='{}'",
                    sql,
                    exec_ctx.user_id().as_str(),
                    exec_ctx.user_role(),
                    e
                );
            },
            KalamDbError::InvalidOperation(_) => {
                log::warn!(
                    target: "sql::plan",
                    "⚠️  SQL column validation failed | sql='{}' | user='{}' | role='{:?}' | error='{}'",
                    sql,
                    exec_ctx.user_id().as_str(),
                    exec_ctx.user_role(),
                    e
                );
            },
            KalamDbError::AlreadyExists(_) => {
                log::warn!(
                    target: "sql::plan",
                    "⚠️  SQL constraint validation failed | sql='{}' | user='{}' | role='{:?}' | error='{}'",
                    sql,
                    exec_ctx.user_id().as_str(),
                    exec_ctx.user_role(),
                    e
                );
            },
            _ => {
                log::error!(
                    target: "sql::plan",
                    "❌ SQL planning failed | sql='{}' | user='{}' | role='{:?}' | error='{}'",
                    sql,
                    exec_ctx.user_id().as_str(),
                    exec_ctx.user_role(),
                    e
                );
            },
        }

        mapped_error
    }

    fn extract_rows_affected(batches: &[RecordBatch]) -> Result<usize, KalamDbError> {
        let mut total: usize = 0;

        for batch in batches {
            if batch.num_columns() == 0 || batch.num_rows() == 0 {
                continue;
            }

            let count_column = batch.column_by_name("count").unwrap_or_else(|| batch.column(0));

            for row_idx in 0..batch.num_rows() {
                let count_value =
                    arrow_value_to_scalar(count_column.as_ref(), row_idx).map_err(|e| {
                        KalamDbError::ExecutionError(format!(
                            "Failed to decode DML count value from result batch: {}",
                            e
                        ))
                    })?;
                total += Self::scalar_count_to_usize(count_value)?;
            }
        }

        Ok(total)
    }

    fn scalar_count_to_usize(value: ScalarValue) -> Result<usize, KalamDbError> {
        let invalid = |v: ScalarValue| {
            KalamDbError::ExecutionError(format!(
                "DML result does not contain a valid count value: {:?}",
                v
            ))
        };

        match value {
            ScalarValue::UInt64(Some(v)) => usize::try_from(v).map_err(|_| {
                KalamDbError::ExecutionError(format!(
                    "DML count {} exceeds platform usize range",
                    v
                ))
            }),
            ScalarValue::UInt32(Some(v)) => Ok(v as usize),
            ScalarValue::UInt16(Some(v)) => Ok(v as usize),
            ScalarValue::UInt8(Some(v)) => Ok(v as usize),
            ScalarValue::Int64(Some(v)) if v >= 0 => usize::try_from(v as u64).map_err(|_| {
                KalamDbError::ExecutionError(format!(
                    "DML count {} exceeds platform usize range",
                    v
                ))
            }),
            ScalarValue::Int32(Some(v)) if v >= 0 => Ok(v as usize),
            ScalarValue::Int16(Some(v)) if v >= 0 => Ok(v as usize),
            ScalarValue::Int8(Some(v)) if v >= 0 => Ok(v as usize),
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                let parsed = v.parse::<u64>().map_err(|e| {
                    KalamDbError::ExecutionError(format!(
                        "Failed to parse DML count '{}' as number: {}",
                        v, e
                    ))
                })?;
                usize::try_from(parsed).map_err(|_| {
                    KalamDbError::ExecutionError(format!(
                        "DML count {} exceeds platform usize range",
                        parsed
                    ))
                })
            },
            other => Err(invalid(other)),
        }
    }

    /// Load existing tables from system.tables and register providers
    ///
    /// Called during server startup to restore table access after restart.
    /// Loads table definitions from the store and creates/registers:
    /// - UserTableShared instances for USER tables
    /// - SharedTableProvider instances for SHARED tables  
    /// - StreamTableProvider instances for STREAM tables
    ///
    /// # Returns
    /// Ok on success, error if table loading fails
    pub async fn load_existing_tables(&self) -> Result<(), KalamDbError> {
        let app_context = &self.app_context;
        // Delegate to unified SchemaRegistry initialization
        app_context.schema_registry().initialize_tables()
    }
}
