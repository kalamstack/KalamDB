//! Typed DDL handler for ALTER TABLE statements

use std::sync::Arc;

// Note: table_registration moved to unified applier commands
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::{
    models::{
        schemas::{ColumnDefinition, ScalarIndexDefinition, TableDefinition},
        ColumnId, NamespaceId, TableId, UserId,
    },
    schemas::{ColumnDefault, TableOptions, TableType},
};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::{AlterTableStatement, ColumnOperation, TablePropertyUpdates};
use kalamdb_store::Partition;
use kalamdb_system::{VectorEngine, VectorIndexState, VectorMetric};
use kalamdb_vector::{
    normalize_vector_column_name, shared_vector_ops_partition_name,
    shared_vector_pk_index_partition_name, user_vector_ops_partition_name,
    user_vector_pk_index_partition_name,
};

use crate::helpers::guards::block_system_namespace_modification;

/// Typed handler for ALTER TABLE statements
pub struct AlterTableHandler {
    app_context: Arc<AppContext>,
}

impl AlterTableHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn resolve_vector_column(
        &self,
        table_def: &TableDefinition,
        column_name: &str,
    ) -> Result<(String, u32), KalamDbError> {
        let column = table_def
            .columns
            .iter()
            .find(|col| col.column_name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(format!("Column '{}' does not exist", column_name))
            })?;

        let dimensions = match column.data_type {
            kalamdb_commons::models::datatypes::KalamDataType::Embedding(dim) if dim > 0 => dim,
            _ => {
                return Err(KalamDbError::InvalidOperation(format!(
                    "Column '{}' is not EMBEDDING",
                    column_name
                )))
            },
        };

        Ok((column.column_name.clone(), dimensions as u32))
    }

    fn manifest_scopes_for_vector_op(
        &self,
        table_id: &TableId,
        table_type: TableType,
        actor_user: &UserId,
    ) -> Result<Vec<Option<UserId>>, KalamDbError> {
        let manifest_service = self.app_context.manifest_service();
        match table_type {
            TableType::Shared => Ok(vec![None]),
            TableType::User => {
                let mut user_ids =
                    manifest_service.get_manifest_user_ids(table_id).map_err(|e| {
                        KalamDbError::ExecutionError(format!(
                            "Failed to list user manifest scopes: {}",
                            e
                        ))
                    })?;
                if !user_ids.iter().any(|u| u == actor_user) {
                    user_ids.push(actor_user.clone());
                }
                if user_ids.is_empty() {
                    user_ids.push(actor_user.clone());
                }
                Ok(user_ids.into_iter().map(Some).collect())
            },
            TableType::System | TableType::Stream => Err(KalamDbError::InvalidOperation(
                "Vector indexing is only supported for USER/SHARED tables".to_string(),
            )),
        }
    }

    fn ensure_vector_hot_partitions(
        &self,
        table_id: &TableId,
        table_type: TableType,
        column_name: &str,
    ) -> Result<(), KalamDbError> {
        let backend = self.app_context.storage_backend();

        let (ops_partition, pk_partition) = match table_type {
            TableType::User => (
                user_vector_ops_partition_name(table_id, column_name),
                user_vector_pk_index_partition_name(table_id, column_name),
            ),
            TableType::Shared => (
                shared_vector_ops_partition_name(table_id, column_name),
                shared_vector_pk_index_partition_name(table_id, column_name),
            ),
            TableType::System | TableType::Stream => {
                return Err(KalamDbError::InvalidOperation(
                    "Vector indexing is only supported for USER/SHARED tables".to_string(),
                ))
            },
        };

        backend.create_partition(&Partition::new(&ops_partition)).map_err(|e| {
            KalamDbError::ExecutionError(format!(
                "Failed to create vector ops partition '{}': {}",
                ops_partition, e
            ))
        })?;
        backend.create_partition(&Partition::new(&pk_partition)).map_err(|e| {
            KalamDbError::ExecutionError(format!(
                "Failed to create vector pk index partition '{}': {}",
                pk_partition, e
            ))
        })?;

        Ok(())
    }

    fn drop_vector_hot_partitions(
        &self,
        table_id: &TableId,
        table_type: TableType,
        column_name: &str,
    ) {
        let backend = self.app_context.storage_backend();
        let (ops_partition, pk_partition) = match table_type {
            TableType::User => (
                user_vector_ops_partition_name(table_id, column_name),
                user_vector_pk_index_partition_name(table_id, column_name),
            ),
            TableType::Shared => (
                shared_vector_ops_partition_name(table_id, column_name),
                shared_vector_pk_index_partition_name(table_id, column_name),
            ),
            TableType::System | TableType::Stream => return,
        };

        for partition_name in [ops_partition, pk_partition] {
            let partition = Partition::new(&partition_name);
            if let Err(err) = backend.drop_partition(&partition) {
                log::warn!(
                    "Failed to drop vector partition '{}' during DROP INDEX: {}",
                    partition_name,
                    err
                );
            }
        }
    }

    fn backfill_scalar_index(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
    ) -> Result<(), KalamDbError> {
        let pk_field = table_def
            .columns
            .iter()
            .find(|column| column.is_primary_key)
            .map(|column| column.column_name.as_str())
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(format!("table {} has no primary key", table_id))
            })?;
        let storage_schema = kalamdb_tables::storage_schema_for_table(table_def).map_err(|e| {
            KalamDbError::ExecutionError(format!(
                "failed to build storage schema for {}: {}",
                table_id, e
            ))
        })?;
        let index_idx = table_def.scalar_indexes.len();
        let backend = self.app_context.storage_backend();
        match table_def.table_type {
            TableType::User => {
                let store = kalamdb_tables::new_indexed_user_table_store(
                    backend,
                    table_id,
                    pk_field,
                    storage_schema,
                    &table_def.scalar_indexes,
                    &table_def.columns,
                );
                store.backfill_index(index_idx).map_err(|e| {
                    KalamDbError::ExecutionError(format!(
                        "failed to backfill index on {}: {}",
                        table_id, e
                    ))
                })?;
            },
            TableType::Shared => {
                let store = kalamdb_tables::new_indexed_shared_table_store(
                    backend,
                    table_id,
                    pk_field,
                    storage_schema,
                    &table_def.scalar_indexes,
                    &table_def.columns,
                );
                store.backfill_index(index_idx).map_err(|e| {
                    KalamDbError::ExecutionError(format!(
                        "failed to backfill index on {}: {}",
                        table_id, e
                    ))
                })?;
            },
            TableType::Stream | TableType::System => {},
        }
        Ok(())
    }

    fn drop_scalar_index_partition(
        &self,
        table_id: &TableId,
        table_type: TableType,
        index_name: &str,
    ) {
        let user_scoped = matches!(table_type, TableType::User);
        let partition = Partition::new(kalamdb_tables::common::scalar_index_partition_name(
            table_id,
            index_name,
            user_scoped,
        ));
        if let Err(err) = self.app_context.storage_backend().drop_partition(&partition) {
            if !err.to_string().to_lowercase().contains("not found") {
                log::warn!(
                    "Failed to drop scalar index partition '{}' during DROP INDEX: {}",
                    partition,
                    err
                );
            }
        }
    }

    fn delete_vector_cold_artifacts(
        &self,
        table_id: &TableId,
        table_type: TableType,
        manifest_user: Option<&UserId>,
        column_name: &str,
    ) -> Result<(), KalamDbError> {
        let registry = self.app_context.schema_registry();
        let cached_table = registry.get(table_id).ok_or_else(|| {
            KalamDbError::NotFound(format!("Table '{}' not found", table_id.full_name()))
        })?;
        let storage_cached =
            cached_table.storage_cached(&self.app_context.storage_registry()).map_err(|e| {
                KalamDbError::ExecutionError(format!("Failed to resolve storage cache: {}", e))
            })?;

        let listed =
            storage_cached.list_sync(table_type, table_id, manifest_user).map_err(|e| {
                KalamDbError::ExecutionError(format!("Failed to list table storage files: {}", e))
            })?;

        let prefix = listed.prefix.trim_end_matches('/');
        let normalized_column = normalize_vector_column_name(column_name);
        let vector_snapshot_prefix = format!("vec-{}-snapshot-", normalized_column);

        for listed_path in listed.paths {
            let relative_path = if listed_path.starts_with(prefix) {
                listed_path[prefix.len()..].trim_start_matches('/').to_string()
            } else {
                listed_path
            };

            let is_current_snapshot = relative_path.starts_with(&vector_snapshot_prefix)
                && relative_path.ends_with(".vix");
            if !is_current_snapshot {
                continue;
            }

            storage_cached
                .delete_sync(table_type, table_id, manifest_user, &relative_path)
                .map_err(|e| {
                    KalamDbError::ExecutionError(format!(
                        "Failed to delete vector index artifact '{}': {}",
                        relative_path, e
                    ))
                })?;
        }

        Ok(())
    }

    async fn execute_vector_index_operation(
        &self,
        statement: &AlterTableStatement,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        use crate::helpers::audit;

        let table_id =
            TableId::from_strings(statement.namespace_id.as_str(), statement.table_name.as_str());
        let registry = self.app_context.schema_registry();
        let table_def = registry.get_table_if_exists(&table_id)?.ok_or_else(|| {
            KalamDbError::NotFound(format!(
                "Table '{}' not found in namespace '{}'",
                statement.table_name.as_str(),
                statement.namespace_id.as_str()
            ))
        })?;

        let table_type = table_def.table_type;
        let manifest_service = self.app_context.manifest_service();
        let scopes =
            self.manifest_scopes_for_vector_op(&table_id, table_type, context.user_id())?;

        match &statement.operation {
            ColumnOperation::CreateVectorIndex {
                column_name,
                metric,
            } => {
                let (resolved_column_name, dimensions) =
                    self.resolve_vector_column(table_def.as_ref(), column_name)?;
                for scope in &scopes {
                    let mut manifest = manifest_service
                        .ensure_manifest_initialized(&table_id, scope.as_ref())
                        .map_err(|e| {
                            KalamDbError::ExecutionError(format!(
                                "Failed to initialize manifest for {}: {}",
                                table_id, e
                            ))
                        })?;
                    let entry = manifest.ensure_vector_index(
                        &resolved_column_name,
                        dimensions,
                        *metric,
                        VectorEngine::USearch,
                    );
                    entry.enabled = true;
                    entry.state = VectorIndexState::Active;
                    entry.updated_at = chrono::Utc::now().timestamp_millis();
                    manifest_service
                        .persist_manifest(&table_id, scope.as_ref(), &manifest)
                        .map_err(|e| {
                            KalamDbError::ExecutionError(format!(
                                "Failed to persist manifest for {}: {}",
                                table_id, e
                            ))
                        })?;
                }

                self.ensure_vector_hot_partitions(&table_id, table_type, &resolved_column_name)?;

                let audit_entry = audit::log_ddl_operation(
                    context,
                    "ALTER",
                    "TABLE",
                    &table_id.full_name(),
                    Some(format!("CREATE INDEX {} USING {:?}", resolved_column_name, metric)),
                    None,
                );
                audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

                Ok(ExecutionResult::Success {
                    message: format!(
                        "Vector index enabled on {}.{} ({:?})",
                        table_id.full_name(),
                        resolved_column_name,
                        metric
                    ),
                })
            },
            ColumnOperation::DropVectorIndex { column_name } => {
                let (resolved_column_name, dimensions) =
                    self.resolve_vector_column(table_def.as_ref(), column_name)?;
                for scope in &scopes {
                    self.delete_vector_cold_artifacts(
                        &table_id,
                        table_type,
                        scope.as_ref(),
                        &resolved_column_name,
                    )?;

                    let mut manifest = manifest_service
                        .ensure_manifest_initialized(&table_id, scope.as_ref())
                        .map_err(|e| {
                            KalamDbError::ExecutionError(format!(
                                "Failed to initialize manifest for {}: {}",
                                table_id, e
                            ))
                        })?;
                    let existing_metric = manifest
                        .vector_indexes
                        .get(&resolved_column_name)
                        .map(|meta| meta.metric)
                        .unwrap_or(VectorMetric::Cosine);
                    let existing_engine = manifest
                        .vector_indexes
                        .get(&resolved_column_name)
                        .map(|meta| meta.engine)
                        .unwrap_or(VectorEngine::USearch);
                    let entry = manifest.ensure_vector_index(
                        &resolved_column_name,
                        dimensions,
                        existing_metric,
                        existing_engine,
                    );
                    entry.enabled = false;
                    entry.state = VectorIndexState::Active;
                    entry.snapshot_path = None;
                    entry.updated_at = chrono::Utc::now().timestamp_millis();
                    manifest_service
                        .persist_manifest(&table_id, scope.as_ref(), &manifest)
                        .map_err(|e| {
                            KalamDbError::ExecutionError(format!(
                                "Failed to persist manifest for {}: {}",
                                table_id, e
                            ))
                        })?;
                }

                self.drop_vector_hot_partitions(&table_id, table_type, &resolved_column_name);

                let audit_entry = audit::log_ddl_operation(
                    context,
                    "ALTER",
                    "TABLE",
                    &table_id.full_name(),
                    Some(format!("DROP INDEX {}", resolved_column_name)),
                    None,
                );
                audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

                Ok(ExecutionResult::Success {
                    message: format!(
                        "Vector index disabled on {}.{}",
                        table_id.full_name(),
                        resolved_column_name
                    ),
                })
            },
            _ => Err(KalamDbError::InvalidOperation("Not a vector index operation".to_string())),
        }
    }

    /// Build the altered table definition without persisting or registering providers.
    /// This validates inputs and applies the schema mutation.
    fn build_altered_table_definition(
        &self,
        statement: &AlterTableStatement,
        context: &ExecutionContext,
    ) -> Result<(TableDefinition, String, bool), KalamDbError> {
        let namespace_id: NamespaceId = statement.namespace_id.clone();
        let table_id = TableId::from_strings(namespace_id.as_str(), statement.table_name.as_str());

        log::info!(
            "🔧 ALTER TABLE request: {}.{} (operation: {:?}, user: {}, role: {:?})",
            namespace_id.as_str(),
            statement.table_name.as_str(),
            get_operation_summary(&statement.operation),
            context.user_id().as_str(),
            context.user_role()
        );

        // Block ALTER on system tables
        block_system_namespace_modification(
            &namespace_id,
            "ALTER",
            "TABLE",
            Some(statement.table_name.as_str()),
        )?;

        let registry = self.app_context.schema_registry();
        let table_def_arc = registry.get_table_if_exists(&table_id)?.ok_or_else(|| {
            log::warn!(
                "⚠️  ALTER TABLE failed: Table '{}' not found in namespace '{}'",
                statement.table_name.as_str(),
                namespace_id.as_str()
            );
            KalamDbError::NotFound(format!(
                "Table '{}' not found in namespace '{}'",
                statement.table_name.as_str(),
                namespace_id.as_str()
            ))
        })?;

        let mut table_def: TableDefinition = (*table_def_arc).clone();

        log::debug!(
            "📋 Current table schema: type={:?}, columns={}, version={}",
            table_def.table_type,
            table_def.columns.len(),
            table_def.schema_version
        );

        // RBAC check
        let is_owner = matches!(table_def.table_type, TableType::User);

        if !kalamdb_session::can_alter_table(context.user_role(), table_def.table_type, is_owner) {
            log::error!(
                "❌ ALTER TABLE {}.{}: Insufficient privileges",
                namespace_id.as_str(),
                statement.table_name.as_str()
            );
            return Err(KalamDbError::Unauthorized(
                "Insufficient privileges to alter table".to_string(),
            ));
        }

        // Apply operation and get change description + whether anything actually changed
        let (change_desc, changed) = apply_alter_operation(
            &self.app_context,
            &mut table_def,
            &statement.operation,
            &table_id,
        )?;

        // Only increment version if actual changes were made
        if changed {
            table_def.increment_version();
            log::debug!(
                "✓ Built altered TableDefinition: version={}, columns={}",
                table_def.schema_version,
                table_def.columns.len()
            );
        } else {
            log::debug!(
                "⊙ No changes made to TableDefinition: version={} unchanged",
                table_def.schema_version
            );
        }

        Ok((table_def, change_desc, changed))
    }
}

impl TypedStatementHandler<AlterTableStatement> for AlterTableHandler {
    async fn execute(
        &self,
        statement: AlterTableStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        if matches!(
            statement.operation,
            ColumnOperation::CreateVectorIndex { .. } | ColumnOperation::DropVectorIndex { .. }
        ) {
            return self.execute_vector_index_operation(&statement, context).await;
        }

        if let ColumnOperation::DropIndex { name, .. } = &statement.operation {
            let table_id = TableId::from_strings(
                statement.namespace_id.as_str(),
                statement.table_name.as_str(),
            );
            let registry = self.app_context.schema_registry();
            if let Ok(Some(table_def)) = registry.get_table_if_exists(&table_id) {
                let is_scalar = table_def
                    .scalar_indexes
                    .iter()
                    .any(|index| index.name.eq_ignore_ascii_case(name));
                if !is_scalar {
                    let mut vector_stmt = statement.clone();
                    vector_stmt.operation = ColumnOperation::DropVectorIndex {
                        column_name: name.clone(),
                    };
                    return self.execute_vector_index_operation(&vector_stmt, context).await;
                }
            }
        }

        use crate::helpers::audit;

        let namespace_id: NamespaceId = statement.namespace_id.clone();
        let table_id = TableId::from_strings(namespace_id.as_str(), statement.table_name.as_str());

        // Build the altered table definition (validate + apply mutation)
        let (table_def, change_desc, changed) =
            self.build_altered_table_definition(&statement, context)?;

        // Only apply changes if something actually changed
        if changed {
            let dropped_index_name =
                if let ColumnOperation::DropIndex { name, .. } = &statement.operation {
                    Some(name.clone())
                } else {
                    None
                };
            let created_scalar =
                matches!(statement.operation, ColumnOperation::CreateScalarIndex { .. });

            self.app_context
                .applier()
                .alter_table(table_id.clone(), table_def.clone())
                .await
                .map_err(|e| KalamDbError::ExecutionError(format!("ALTER TABLE failed: {}", e)))?;

            if created_scalar {
                self.backfill_scalar_index(&table_id, &table_def)?;
            }
            if let Some(name) = dropped_index_name {
                self.drop_scalar_index_partition(&table_id, table_def.table_type, &name);
            }

            // Log DDL operation
            let audit_entry = audit::log_ddl_operation(
                context,
                "ALTER",
                "TABLE",
                &format!("{}.{}", namespace_id.as_str(), statement.table_name.as_str()),
                Some(format!(
                    "Operation: {}, New Version: {}",
                    change_desc, table_def.schema_version
                )),
                None,
            );
            audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

            log::info!(
                "✅ ALTER TABLE succeeded: {}.{} | operation: {} | new_version: {} | table_type: \
                 {:?}",
                namespace_id.as_str(),
                statement.table_name.as_str(),
                change_desc,
                table_def.schema_version,
                table_def.table_type
            );

            Ok(ExecutionResult::Success {
                message: format!(
                    "Table {}.{} altered successfully: {} (version {})",
                    namespace_id.as_str(),
                    statement.table_name.as_str(),
                    change_desc,
                    table_def.schema_version
                ),
            })
        } else {
            log::info!(
                "⊙ ALTER TABLE no-op: {}.{} | operation: {} | version unchanged: {}",
                namespace_id.as_str(),
                statement.table_name.as_str(),
                change_desc,
                table_def.schema_version
            );

            Ok(ExecutionResult::Success {
                message: format!(
                    "Table {}.{} unchanged: {} (version {} - no changes needed)",
                    namespace_id.as_str(),
                    statement.table_name.as_str(),
                    change_desc,
                    table_def.schema_version
                ),
            })
        }
    }

    async fn check_authorization(
        &self,
        statement: &AlterTableStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use crate::helpers::guards::block_anonymous_write;

        // Block anonymous users from DDL operations
        block_anonymous_write(context, "ALTER TABLE")?;

        let namespace_id = &statement.namespace_id;
        let table_id = TableId::from_strings(namespace_id.as_str(), statement.table_name.as_str());

        let registry = self.app_context.schema_registry();
        if let Ok(Some(def)) = registry.get_table_if_exists(&table_id) {
            let is_owner = matches!(def.table_type, TableType::User);

            if !kalamdb_session::can_alter_table(context.user_role(), def.table_type, is_owner) {
                return Err(KalamDbError::Unauthorized(
                    "Insufficient privileges to alter table".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Check if a column name is a system column that cannot be altered
fn is_system_column(column_name: &str) -> bool {
    SystemColumnNames::is_system_column(column_name)
}

/// Apply an ALTER TABLE operation to a table definition
/// Returns (description, changed) tuple where changed is false if no actual modifications were made
fn apply_alter_operation(
    app_context: &Arc<AppContext>,
    table_def: &mut TableDefinition,
    operation: &ColumnOperation,
    table_id: &TableId,
) -> Result<(String, bool), KalamDbError> {
    match operation {
        ColumnOperation::Add {
            column_name,
            data_type,
            nullable,
            default_value,
            if_not_exists,
        } => {
            // Block adding columns with system column names
            if is_system_column(column_name) {
                log::error!("❌ ALTER TABLE failed: Cannot add system column '{}'", column_name);
                return Err(KalamDbError::InvalidOperation(format!(
                    "Cannot add column '{}': reserved system column name",
                    column_name
                )));
            }
            // Perform case-insensitive check to prevent duplicates like 'col1' vs 'COL1'
            if table_def
                .columns
                .iter()
                .any(|c| c.column_name.eq_ignore_ascii_case(column_name))
            {
                if *if_not_exists {
                    log::debug!(
                        "⊙ Skipping ADD COLUMN {} on {} (IF NOT EXISTS)",
                        column_name,
                        table_id
                    );
                    return Ok((
                        format!("ADD COLUMN {} {}", column_name, data_type.sql_name()),
                        false,
                    ));
                }
                log::error!(
                    "❌ ALTER TABLE failed: Column '{}' already exists in {}",
                    column_name,
                    table_id
                );
                return Err(KalamDbError::InvalidOperation(format!(
                    "Column '{}' already exists",
                    column_name
                )));
            }
            let kalam_type = data_type.clone();
            let default = default_value.clone().unwrap_or(ColumnDefault::None);
            crate::helpers::table_creation::validate_column_default(app_context, &default)?;
            let ordinal = (table_def.columns.len() + 1) as u32;
            let column_id = table_def.next_column_id;
            table_def.columns.push(ColumnDefinition::new(
                column_id,
                column_name.clone(),
                ordinal,
                kalam_type,
                *nullable,
                false,
                false,
                default,
                None,
            ));
            table_def.next_column_id += 1;
            log::debug!(
                "✓ Added column {} (type: {}, nullable: {})",
                column_name,
                data_type.sql_name(),
                nullable
            );
            Ok((format!("ADD COLUMN {} {}", column_name, data_type.sql_name()), true))
        },
        ColumnOperation::Drop { column_name } => {
            // Block dropping system columns
            if is_system_column(column_name) {
                log::error!("❌ ALTER TABLE failed: Cannot drop system column '{}'", column_name);
                return Err(KalamDbError::InvalidOperation(format!(
                    "Cannot drop column '{}': system column cannot be modified",
                    column_name
                )));
            }
            let idx = table_def
                .columns
                .iter()
                .position(|c| c.column_name == *column_name)
                .ok_or_else(|| {
                    log::error!(
                        "❌ ALTER TABLE failed: Column '{}' does not exist in {}",
                        column_name,
                        table_id
                    );
                    KalamDbError::InvalidOperation(format!(
                        "Column '{}' does not exist",
                        column_name
                    ))
                })?;
            let column_id = table_def.columns[idx].column_id;
            if let Some(index) = table_def
                .scalar_indexes
                .iter()
                .find(|index| index.columns.iter().any(|id| id.as_u64() == column_id))
            {
                return Err(KalamDbError::InvalidOperation(format!(
                    "cannot drop column '{}': still used by index '{}'",
                    column_name, index.name
                )));
            }
            table_def.columns.remove(idx);
            for (i, c) in table_def.columns.iter_mut().enumerate() {
                c.ordinal_position = (i + 1) as u32;
            }
            log::debug!("✓ Dropped column {}", column_name);
            Ok((format!("DROP COLUMN {}", column_name), true))
        },
        ColumnOperation::Modify {
            column_name,
            new_data_type,
            nullable,
        } => {
            // Block modifying system columns
            if is_system_column(column_name) {
                log::error!("❌ ALTER TABLE failed: Cannot modify system column '{}'", column_name);
                return Err(KalamDbError::InvalidOperation(format!(
                    "Cannot modify column '{}': system column cannot be altered",
                    column_name
                )));
            }
            let col = table_def
                .columns
                .iter_mut()
                .find(|c| c.column_name == *column_name)
                .ok_or_else(|| {
                    log::error!(
                        "❌ ALTER TABLE failed: Column '{}' does not exist in {}",
                        column_name,
                        table_id
                    );
                    KalamDbError::InvalidOperation(format!(
                        "Column '{}' does not exist",
                        column_name
                    ))
                })?;

            // Track if anything actually changes
            let new_type = new_data_type.clone();
            let type_changed = col.data_type != new_type;
            let nullable_changed = nullable.is_some_and(|n| col.is_nullable != n);
            let changed = type_changed || nullable_changed;

            if type_changed {
                col.data_type = new_type;
            }
            if let Some(n) = nullable {
                col.is_nullable = *n;
            }

            if changed {
                log::debug!(
                    "✓ Modified column {} (new type: {})",
                    column_name,
                    new_data_type.sql_name()
                );
            } else {
                log::debug!(
                    "⊙ No changes to column {} (already type: {})",
                    column_name,
                    new_data_type.sql_name()
                );
            }
            Ok((format!("MODIFY COLUMN {} {}", column_name, new_data_type.sql_name()), changed))
        },
        ColumnOperation::SetNullable {
            column_name,
            nullable,
        } => {
            if is_system_column(column_name) {
                log::error!("❌ ALTER TABLE failed: Cannot modify system column '{}'", column_name);
                return Err(KalamDbError::InvalidOperation(format!(
                    "Cannot modify column '{}': system column cannot be altered",
                    column_name
                )));
            }

            let col = table_def
                .columns
                .iter_mut()
                .find(|c| c.column_name == *column_name)
                .ok_or_else(|| {
                    KalamDbError::InvalidOperation(format!(
                        "Column '{}' does not exist",
                        column_name
                    ))
                })?;

            if col.is_primary_key && *nullable {
                return Err(KalamDbError::InvalidOperation(format!(
                    "Column '{}' is a PRIMARY KEY and cannot be nullable",
                    column_name
                )));
            }

            let changed = col.is_nullable != *nullable;
            col.is_nullable = *nullable;
            Ok((
                format!(
                    "ALTER COLUMN {} {}",
                    column_name,
                    if *nullable {
                        "DROP NOT NULL"
                    } else {
                        "SET NOT NULL"
                    }
                ),
                changed,
            ))
        },
        ColumnOperation::SetDefault {
            column_name,
            default_value,
        } => {
            if is_system_column(column_name) {
                log::error!("❌ ALTER TABLE failed: Cannot modify system column '{}'", column_name);
                return Err(KalamDbError::InvalidOperation(format!(
                    "Cannot modify column '{}': system column cannot be altered",
                    column_name
                )));
            }

            let col = table_def
                .columns
                .iter_mut()
                .find(|c| c.column_name == *column_name)
                .ok_or_else(|| {
                    KalamDbError::InvalidOperation(format!(
                        "Column '{}' does not exist",
                        column_name
                    ))
                })?;

            let changed = col.default_value != *default_value;
            crate::helpers::table_creation::validate_column_default(app_context, default_value)?;
            col.default_value = default_value.clone();
            Ok((
                format!("ALTER COLUMN {} SET DEFAULT {}", column_name, default_value.to_sql()),
                changed,
            ))
        },
        ColumnOperation::DropDefault { column_name } => {
            if is_system_column(column_name) {
                log::error!("❌ ALTER TABLE failed: Cannot modify system column '{}'", column_name);
                return Err(KalamDbError::InvalidOperation(format!(
                    "Cannot modify column '{}': system column cannot be altered",
                    column_name
                )));
            }

            let col = table_def
                .columns
                .iter_mut()
                .find(|c| c.column_name == *column_name)
                .ok_or_else(|| {
                    KalamDbError::InvalidOperation(format!(
                        "Column '{}' does not exist",
                        column_name
                    ))
                })?;

            let changed = !col.default_value.is_none();
            col.default_value = ColumnDefault::None;
            Ok((format!("ALTER COLUMN {} DROP DEFAULT", column_name), changed))
        },
        ColumnOperation::Rename {
            old_column_name,
            new_column_name,
        } => {
            // Block renaming system columns (both renaming FROM or TO a system column name)
            if is_system_column(old_column_name) {
                log::error!(
                    "❌ ALTER TABLE failed: Cannot rename system column '{}'",
                    old_column_name
                );
                return Err(KalamDbError::InvalidOperation(format!(
                    "Cannot rename column '{}': system column cannot be modified",
                    old_column_name
                )));
            }
            if is_system_column(new_column_name) {
                log::error!(
                    "❌ ALTER TABLE failed: Cannot rename column to reserved system column name \
                     '{}'",
                    new_column_name
                );
                return Err(KalamDbError::InvalidOperation(format!(
                    "Cannot rename column to '{}': reserved system column name",
                    new_column_name
                )));
            }
            if !table_def.columns.iter().any(|c| c.column_name == *old_column_name) {
                log::error!(
                    "❌ ALTER TABLE failed: Column '{}' does not exist in {}",
                    old_column_name,
                    table_id
                );
                return Err(KalamDbError::InvalidOperation(format!(
                    "Column '{}' does not exist",
                    old_column_name
                )));
            }
            if table_def.columns.iter().any(|c| c.column_name == *new_column_name) {
                log::error!(
                    "❌ ALTER TABLE failed: Column '{}' already exists in {}",
                    new_column_name,
                    table_id
                );
                return Err(KalamDbError::InvalidOperation(format!(
                    "Column '{}' already exists",
                    new_column_name
                )));
            }
            if let Some(col) =
                table_def.columns.iter_mut().find(|c| c.column_name == *old_column_name)
            {
                col.column_name = new_column_name.clone();
            }
            log::debug!("✓ Renamed column {} to {}", old_column_name, new_column_name);
            Ok((format!("RENAME COLUMN {} TO {}", old_column_name, new_column_name), true))
        },
        ColumnOperation::SetTableOptions { updates } => {
            apply_table_property_updates(app_context, table_def, updates)
        },
        ColumnOperation::CreateVectorIndex {
            column_name,
            metric,
        } => Ok((format!("CREATE INDEX {} USING {:?}", column_name, metric), false)),
        ColumnOperation::DropVectorIndex { column_name } => {
            Ok((format!("DROP INDEX {}", column_name), false))
        },
        ColumnOperation::CreateScalarIndex {
            name,
            columns,
            unique,
            if_not_exists,
        } => apply_create_scalar_index(table_def, name, columns, *unique, *if_not_exists),
        ColumnOperation::DropIndex { name, if_exists } => {
            apply_drop_scalar_index(table_def, name, *if_exists)
        },
    }
}

fn apply_create_scalar_index(
    table_def: &mut TableDefinition,
    name: &str,
    columns: &[String],
    unique: bool,
    if_not_exists: bool,
) -> Result<(String, bool), KalamDbError> {
    if !matches!(table_def.table_type, TableType::User | TableType::Shared) {
        return Err(KalamDbError::InvalidOperation(
            "scalar CREATE INDEX is only supported on USER and SHARED tables".to_string(),
        ));
    }
    if let Some(existing) = table_def
        .scalar_indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case(name))
    {
        if if_not_exists {
            return Ok((format!("CREATE INDEX {}", name), false));
        }
        return Err(KalamDbError::InvalidOperation(format!(
            "index '{}' already exists",
            existing.name
        )));
    }

    let mut column_ids = Vec::with_capacity(columns.len());
    for column_name in columns {
        if is_system_column(column_name) {
            return Err(KalamDbError::InvalidOperation(format!(
                "cannot index system column '{}'",
                column_name
            )));
        }
        let column = table_def
            .columns
            .iter()
            .find(|column| column.column_name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(format!("column '{}' does not exist", column_name))
            })?;
        column_ids.push(ColumnId::new(column.column_id));
    }

    table_def
        .scalar_indexes
        .push(ScalarIndexDefinition::new(name, column_ids, unique));
    Ok((format!("CREATE INDEX {} ({})", name, columns.join(", ")), true))
}

fn apply_drop_scalar_index(
    table_def: &mut TableDefinition,
    name: &str,
    if_exists: bool,
) -> Result<(String, bool), KalamDbError> {
    let Some(idx) = table_def
        .scalar_indexes
        .iter()
        .position(|index| index.name.eq_ignore_ascii_case(name))
    else {
        if if_exists {
            return Ok((format!("DROP INDEX {}", name), false));
        }
        return Err(KalamDbError::InvalidOperation(format!("index '{}' does not exist", name)));
    };
    table_def.scalar_indexes.remove(idx);
    Ok((format!("DROP INDEX {}", name), true))
}

fn apply_table_property_updates(
    app_context: &Arc<AppContext>,
    table_def: &mut TableDefinition,
    updates: &TablePropertyUpdates,
) -> Result<(String, bool), KalamDbError> {
    if let Some(storage_id) = &updates.storage_id {
        let storages_provider = app_context.system_tables().storages();
        if storages_provider.get_storage_by_id(storage_id)?.is_none() {
            return Err(KalamDbError::InvalidOperation(format!(
                "Storage '{}' does not exist",
                storage_id.as_str()
            )));
        }
    }

    match &mut table_def.table_options {
        TableOptions::User(opts) => {
            ensure_no_stream_only_properties(updates, "USER")?;

            let mut changed = false;
            let mut changes = Vec::new();

            if let Some(storage_id) = &updates.storage_id {
                changed |= &opts.storage_id != storage_id;
                opts.storage_id = storage_id.clone();
                changes.push(format!("STORAGE_ID={}", storage_id.as_str()));
            }
            if let Some(use_user_storage) = updates.use_user_storage {
                changed |= opts.use_user_storage != use_user_storage;
                opts.use_user_storage = use_user_storage;
                changes.push(format!("USE_USER_STORAGE={}", use_user_storage));
            }
            if let Some(flush_policy) = &updates.flush_policy {
                changed |= &opts.flush_policy != flush_policy;
                opts.flush_policy = flush_policy.clone();
                changes.push("FLUSH_POLICY".to_string());
            }
            if let Some(compression) = &updates.compression {
                changed |= &opts.compression != compression;
                opts.compression = compression.clone();
                changes.push(format!("COMPRESSION={}", compression));
            }

            Ok((format_table_property_change(changes), changed))
        },
        TableOptions::Shared(opts) => {
            if updates.use_user_storage.is_some() {
                return Err(unsupported_table_property("USE_USER_STORAGE", "SHARED"));
            }
            ensure_no_stream_only_properties(updates, "SHARED")?;

            let mut changed = false;
            let mut changes = Vec::new();

            if let Some(storage_id) = &updates.storage_id {
                changed |= &opts.storage_id != storage_id;
                opts.storage_id = storage_id.clone();
                changes.push(format!("STORAGE_ID={}", storage_id.as_str()));
            }
            if let Some(flush_policy) = &updates.flush_policy {
                changed |= &opts.flush_policy != flush_policy;
                opts.flush_policy = flush_policy.clone();
                changes.push("FLUSH_POLICY".to_string());
            }
            if let Some(compression) = &updates.compression {
                changed |= &opts.compression != compression;
                opts.compression = compression.clone();
                changes.push(format!("COMPRESSION={}", compression));
            }

            Ok((format_table_property_change(changes), changed))
        },
        TableOptions::Stream(opts) => {
            if updates.storage_id.is_some() {
                return Err(unsupported_table_property("STORAGE_ID", "STREAM"));
            }
            if updates.use_user_storage.is_some() {
                return Err(unsupported_table_property("USE_USER_STORAGE", "STREAM"));
            }
            if updates.flush_policy.is_some() {
                return Err(unsupported_table_property("FLUSH_POLICY", "STREAM"));
            }
            if updates.compression.is_some() {
                return Err(unsupported_table_property("COMPRESSION", "STREAM"));
            }

            let mut changed = false;
            let mut changes = Vec::new();

            if let Some(ttl_seconds) = updates.ttl_seconds {
                changed |= opts.ttl_seconds != ttl_seconds;
                opts.ttl_seconds = ttl_seconds;
                changes.push(format!("TTL_SECONDS={}", ttl_seconds));
            }
            if let Some(eviction_strategy) = &updates.eviction_strategy {
                changed |= &opts.eviction_strategy != eviction_strategy;
                opts.eviction_strategy = eviction_strategy.clone();
                changes.push(format!("EVICTION_STRATEGY={}", eviction_strategy));
            }
            if let Some(max_stream_size_bytes) = updates.max_stream_size_bytes {
                changed |= opts.max_stream_size_bytes != max_stream_size_bytes;
                opts.max_stream_size_bytes = max_stream_size_bytes;
                changes.push(format!("MAX_STREAM_SIZE_BYTES={}", max_stream_size_bytes));
            }
            Ok((format_table_property_change(changes), changed))
        },
        TableOptions::System(_) => Err(KalamDbError::InvalidOperation(
            "SYSTEM table options cannot be altered".to_string(),
        )),
    }
}

fn ensure_no_stream_only_properties(
    updates: &TablePropertyUpdates,
    table_type: &str,
) -> Result<(), KalamDbError> {
    if updates.ttl_seconds.is_some() {
        return Err(unsupported_table_property("TTL_SECONDS", table_type));
    }
    if updates.eviction_strategy.is_some() {
        return Err(unsupported_table_property("EVICTION_STRATEGY", table_type));
    }
    if updates.max_stream_size_bytes.is_some() {
        return Err(unsupported_table_property("MAX_STREAM_SIZE_BYTES", table_type));
    }
    Ok(())
}

fn unsupported_table_property(property: &str, table_type: &str) -> KalamDbError {
    KalamDbError::InvalidOperation(format!(
        "{} is not supported for {} tables",
        property, table_type
    ))
}

fn format_table_property_change(changes: Vec<String>) -> String {
    if changes.is_empty() {
        "SET TBLPROPERTIES".to_string()
    } else {
        format!("SET TBLPROPERTIES ({})", changes.join(", "))
    }
}

/// Get a summary string for the operation for logging
fn get_operation_summary(op: &ColumnOperation) -> String {
    match op {
        ColumnOperation::Add {
            column_name,
            data_type,
            ..
        } => format!("ADD COLUMN {} {}", column_name, data_type.sql_name()),
        ColumnOperation::Drop { column_name } => format!("DROP COLUMN {}", column_name),
        ColumnOperation::Modify {
            column_name,
            new_data_type,
            ..
        } => format!("MODIFY COLUMN {} {}", column_name, new_data_type.sql_name()),
        ColumnOperation::SetNullable {
            column_name,
            nullable,
        } => format!(
            "ALTER COLUMN {} {}",
            column_name,
            if *nullable {
                "DROP NOT NULL"
            } else {
                "SET NOT NULL"
            }
        ),
        ColumnOperation::SetDefault {
            column_name,
            default_value,
        } => format!("ALTER COLUMN {} SET DEFAULT {}", column_name, default_value.to_sql()),
        ColumnOperation::DropDefault { column_name } => {
            format!("ALTER COLUMN {} DROP DEFAULT", column_name)
        },
        ColumnOperation::Rename {
            old_column_name,
            new_column_name,
        } => format!("RENAME COLUMN {} TO {}", old_column_name, new_column_name),
        ColumnOperation::SetTableOptions { .. } => "SET TBLPROPERTIES".to_string(),
        ColumnOperation::CreateVectorIndex {
            column_name,
            metric,
        } => format!("CREATE INDEX {} USING {:?}", column_name, metric),
        ColumnOperation::DropVectorIndex { column_name } => {
            format!("DROP INDEX {}", column_name)
        },
        ColumnOperation::CreateScalarIndex { name, .. } => format!("CREATE INDEX {}", name),
        ColumnOperation::DropIndex { name, .. } => format!("DROP INDEX {}", name),
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{
        models::{
            datatypes::KalamDataType,
            schemas::{ColumnDefinition, TableDefinition, TableOptions},
            NamespaceId, TableName,
        },
        schemas::TableType,
    };
    use kalamdb_core::test_helpers::test_app_context_simple;
    use kalamdb_sql::ddl::ColumnOperation;

    use super::{apply_create_scalar_index, apply_drop_scalar_index, AlterTableHandler};

    fn user_table() -> TableDefinition {
        TableDefinition::new(
            NamespaceId::new("public"),
            TableName::new("messages"),
            TableType::User,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
                ColumnDefinition::simple(2, "conversation_id", 2, KalamDataType::Text),
            ],
            TableOptions::user(),
            None,
        )
        .unwrap()
    }

    #[test]
    fn apply_create_and_drop_scalar_index_updates_catalog() {
        let mut table = user_table();
        let (desc, changed) = apply_create_scalar_index(
            &mut table,
            "idx_conv",
            &["conversation_id".to_string()],
            false,
            false,
        )
        .unwrap();
        assert!(changed);
        assert!(desc.contains("idx_conv"));
        assert_eq!(table.scalar_indexes.len(), 1);
        assert_eq!(table.scalar_indexes[0].columns[0].as_u64(), 2);

        let err = apply_create_scalar_index(
            &mut table,
            "idx_conv",
            &["conversation_id".to_string()],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"));

        let (_, skipped) = apply_create_scalar_index(
            &mut table,
            "idx_conv",
            &["conversation_id".to_string()],
            false,
            true,
        )
        .unwrap();
        assert!(!skipped);

        apply_drop_scalar_index(&mut table, "idx_conv", false).unwrap();
        assert!(table.scalar_indexes.is_empty());
    }

    #[tokio::test]
    async fn drop_column_blocked_while_index_exists() {
        let mut table = user_table();
        apply_create_scalar_index(
            &mut table,
            "idx_conv",
            &["conversation_id".to_string()],
            false,
            false,
        )
        .unwrap();
        let err = super::apply_alter_operation(
            &test_app_context_simple(),
            &mut table,
            &ColumnOperation::Drop {
                column_name: "conversation_id".to_string(),
            },
            &kalamdb_commons::models::TableId::new(
                NamespaceId::new("public"),
                TableName::new("messages"),
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("still used by index"));
    }

    #[tokio::test]
    async fn create_index_persists_and_backfills() {
        use datafusion::scalar::ScalarValue;
        use kalamdb_commons::{
            ids::SeqId,
            models::{rows::Row, UserTableRow},
            UserId,
        };

        let app_ctx = test_app_context_simple();
        let namespaces = app_ctx.system_tables().namespaces();
        let namespace_id = NamespaceId::default();
        if namespaces.get_namespace(&namespace_id).unwrap().is_none() {
            namespaces
                .create_namespace(kalamdb_system::Namespace {
                    namespace_id: namespace_id.clone(),
                    name:         "default".to_string(),
                    created_at:   chrono::Utc::now().timestamp_millis(),
                    options:      None,
                    table_count:  0,
                })
                .unwrap();
        }

        let table_name =
            TableName::new(format!("idx_bf_{}", chrono::Utc::now().timestamp_millis()));
        let mut table = TableDefinition::new(
            namespace_id.clone(),
            table_name.clone(),
            TableType::User,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
                ColumnDefinition::simple(2, "conversation_id", 2, KalamDataType::Text),
            ],
            TableOptions::user(),
            None,
        )
        .unwrap();
        table.table_options = TableOptions::user();
        app_ctx.schema_registry().register_table(table.clone()).unwrap();

        let table_id =
            kalamdb_commons::models::TableId::new(namespace_id.clone(), table_name.clone());
        let schema = kalamdb_tables::storage_schema_for_table(&table).unwrap();
        let store = kalamdb_tables::new_indexed_user_table_store(
            app_ctx.storage_backend(),
            &table_id,
            "id",
            schema,
            &[],
            &table.columns,
        );
        let user_id = UserId::new("alice");
        let seq = SeqId::new(1);
        let mut values = std::collections::BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(1)));
        values.insert("conversation_id".to_string(), ScalarValue::Utf8(Some("room-1".to_string())));
        store
            .insert(
                &kalamdb_commons::ids::UserTableRowId::new(user_id.clone(), seq),
                &UserTableRow {
                    user_id,
                    _seq: seq,
                    _commit_seq: 1,
                    _deleted: false,
                    fields: Row::new(values),
                },
            )
            .unwrap();

        apply_create_scalar_index(
            &mut table,
            "idx_conv",
            &["conversation_id".to_string()],
            false,
            false,
        )
        .unwrap();
        app_ctx.schema_registry().register_table(table.clone()).unwrap();

        let handler = AlterTableHandler::new(app_ctx.clone());
        handler.backfill_scalar_index(&table_id, &table).unwrap();

        let updated = app_ctx.schema_registry().get_table_if_exists(&table_id).unwrap().unwrap();
        assert_eq!(updated.scalar_indexes.len(), 1);
        assert_eq!(updated.scalar_indexes[0].name, "idx_conv");

        let indexed = kalamdb_tables::new_indexed_user_table_store(
            app_ctx.storage_backend(),
            &table_id,
            "id",
            kalamdb_tables::storage_schema_for_table(&updated).unwrap(),
            &updated.scalar_indexes,
            &updated.columns,
        );
        let hits = indexed.scan_by_index(1, None, None).unwrap();
        assert_eq!(hits.len(), 1);

        apply_drop_scalar_index(&mut table, "idx_conv", false).unwrap();
        app_ctx.schema_registry().register_table(table.clone()).unwrap();
        handler.drop_scalar_index_partition(&table_id, table.table_type, "idx_conv");
        let after_drop = app_ctx.schema_registry().get_table_if_exists(&table_id).unwrap().unwrap();
        assert!(after_drop.scalar_indexes.is_empty());
    }
}
