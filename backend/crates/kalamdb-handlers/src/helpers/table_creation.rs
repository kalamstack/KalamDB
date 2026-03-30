//! CREATE TABLE helper functions
//!
//! Provides unified logic for creating all table types (USER/SHARED/STREAM)

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_commons::models::{NamespaceId, StorageId, TableAccess, TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::Role;
use kalamdb_sql::ddl::CreateTableStatement;
use kalamdb_system::providers::storages::models::StorageType;
use std::sync::Arc;

/// Unified CREATE TABLE handler for all table types (USER/SHARED/STREAM)
///
/// This single function handles table creation by:
/// 1. Setting defaults (access_level for SHARED tables)
/// 2. Building the table definition with validation
/// 3. Registering through SchemaRegistry (persist + cache)
/// 4. Logging success with type-specific details
///
/// # Arguments
/// * `app_context` - Application context
/// * `stmt` - Parsed CREATE TABLE statement
/// * `user_id` - User ID from execution context
/// * `user_role` - User role from execution context
///
/// # Returns
/// Ok with success message, or error
pub fn create_table(
    app_context: Arc<AppContext>,
    mut stmt: CreateTableStatement,
    user_id: &UserId,
    user_role: Role,
) -> Result<String, KalamDbError> {
    let table_id_str = format!("{}.{}", stmt.namespace_id.as_str(), stmt.table_name.as_str());
    let table_type = stmt.table_type;

    log::info!(
        "🔨 CREATE TABLE request: {} (type: {:?}, user: {}, role: {:?})",
        table_id_str,
        table_type,
        user_id.as_str(),
        user_role
    );

    // Block CREATE on system namespaces - they are managed internally
    super::guards::block_system_namespace_modification(
        &stmt.namespace_id,
        "CREATE",
        "TABLE",
        Some(stmt.table_name.as_str()),
    )?;

    // Reject SYSTEM tables
    if stmt.table_type == TableType::System {
        log::error!(
            "❌ CREATE TABLE failed: Cannot create SYSTEM tables via SQL ({})",
            table_id_str
        );
        return Err(KalamDbError::InvalidOperation(
            "Cannot create SYSTEM tables via SQL".to_string(),
        ));
    }

    // Set default access level for SHARED tables
    if stmt.table_type == TableType::Shared && stmt.access_level.is_none() {
        stmt.access_level = Some(TableAccess::Private);
    }

    let schema_registry = app_context.schema_registry();
    let table_id = TableId::from_strings(stmt.namespace_id.as_str(), stmt.table_name.as_str());

    if stmt.if_not_exists {
        if let Some(existing_def) = schema_registry
            .get_table_if_exists(&table_id)
            .into_kalamdb_error("Failed to check table existence")?
        {
            if schema_registry.get_provider(&table_id).is_none() {
                log::info!("Table {} exists but provider missing - registering now", table_id);
                schema_registry.put((*existing_def).clone())?;
            }

            log::info!(
                "ℹ️  {:?} TABLE {} already exists (IF NOT EXISTS - skipping)",
                table_type,
                table_id
            );
            return Ok(format!("Table {} already exists (IF NOT EXISTS)", table_id));
        }
    }

    // Build definition (validates, checks existence, builds)
    let table_def = build_table_definition(app_context.clone(), &stmt, user_id, user_role)?;

    // Handle existing table (IF NOT EXISTS)
    if schema_registry.get(&table_id).is_some() {
        // Ensure provider is registered even if table exists
        if schema_registry.get_provider(&table_id).is_none() {
            log::info!("Table {} exists but provider missing - registering now", table_id);
            schema_registry.put(table_def)?;
        }

        log::info!(
            "ℹ️  {:?} TABLE {} already exists (IF NOT EXISTS - skipping)",
            table_type,
            table_id
        );
        return Ok(format!("Table {} already exists (IF NOT EXISTS)", table_id));
    }

    // Register (Persist + Cache) - single unified path
    schema_registry.register_table(table_def.clone())?;

    // Log success with type-specific details
    log_table_created(&table_def, &table_id);

    let type_name = match table_type {
        TableType::User => "User",
        TableType::Shared => "Shared",
        TableType::Stream => "Stream",
        TableType::System => "System",
    };

    Ok(format!("{} table {} created successfully", type_name, table_id))
}

/// Log table creation with type-specific details
fn log_table_created(
    table_def: &kalamdb_commons::models::schemas::TableDefinition,
    table_id: &TableId,
) {
    use kalamdb_commons::models::schemas::TableOptions;

    let pk_col = table_def
        .columns
        .iter()
        .find(|c| c.is_primary_key)
        .map(|c| c.column_name.as_str())
        .unwrap_or("none");

    match &table_def.table_options {
        TableOptions::User(opts) => {
            log::info!(
                "✅ USER TABLE created: {} | storage: {} | columns: {} | pk: {} | system_columns: [_seq, _deleted]",
                table_id,
                opts.storage_id.as_str(),
                table_def.columns.len(),
                pk_col
            );
        },
        TableOptions::Shared(opts) => {
            log::info!(
                "✅ SHARED TABLE created: {} | storage: {} | columns: {} | pk: {} | access_level: {:?} | system_columns: [_seq, _deleted]",
                table_id,
                opts.storage_id.as_str(),
                table_def.columns.len(),
                pk_col,
                opts.access_level
            );
        },
        TableOptions::Stream(opts) => {
            log::info!(
                "✅ STREAM TABLE created: {} | columns: {} | TTL: {}s | system_columns: none",
                table_id,
                table_def.columns.len(),
                opts.ttl_seconds
            );
        },
        TableOptions::System(_) => {
            log::info!(
                "✅ SYSTEM TABLE created: {} | columns: {}",
                table_id,
                table_def.columns.len()
            );
        },
    }
}

/// Build a TableDefinition by validating inputs and constructing the definition
/// WITHOUT persisting or registering providers.
///
/// This is used in cluster mode where the Raft applier handles the actual
/// persistence and provider registration on ALL nodes (including the leader).
///
/// # Arguments
/// * `app_context` - Application context
/// * `stmt` - Parsed CREATE TABLE statement
/// * `user_id` - User ID from execution context
/// * `user_role` - User role from execution context
///
/// # Returns
/// Ok with TableDefinition, or error
pub fn build_table_definition(
    app_context: Arc<AppContext>,
    stmt: &CreateTableStatement,
    user_id: &UserId,
    user_role: Role,
) -> Result<kalamdb_commons::models::schemas::TableDefinition, KalamDbError> {
    use super::tables::validate_table_name;
    use kalamdb_commons::datatypes::{FromArrowType, KalamDataType};
    use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
    use kalamdb_commons::schemas::ColumnDefault;

    let table_id_str = format!("{}.{}", stmt.namespace_id.as_str(), stmt.table_name.as_str());

    log::debug!(
        "🔨 BUILD TABLE DEFINITION: {} (type: {:?}, user: {}, role: {:?})",
        table_id_str,
        stmt.table_type,
        user_id.as_str(),
        user_role
    );

    // Block CREATE on system namespaces
    super::guards::block_system_namespace_modification(
        &stmt.namespace_id,
        "CREATE",
        "TABLE",
        Some(stmt.table_name.as_str()),
    )?;

    // RBAC check
    if !kalamdb_session::can_create_table(user_role, stmt.table_type) {
        log::error!(
            "❌ CREATE TABLE {:?} {}: Insufficient privileges (user: {}, role: {:?})",
            stmt.table_type,
            table_id_str,
            user_id.as_str(),
            user_role
        );
        return Err(KalamDbError::Unauthorized(format!(
            "Insufficient privileges to create {:?} tables",
            stmt.table_type
        )));
    }

    // Validate table name
    validate_table_name(stmt.table_name.as_str()).map_err(KalamDbError::InvalidOperation)?;

    // Validate namespace exists
    let namespaces_provider = app_context.system_tables().namespaces();
    let namespace_id = NamespaceId::new(stmt.namespace_id.as_str());
    if namespaces_provider.get_namespace(&namespace_id)?.is_none() {
        log::error!(
            "❌ CREATE TABLE failed: Namespace '{}' does not exist",
            stmt.namespace_id.as_str()
        );
        return Err(KalamDbError::InvalidOperation(format!(
            "Namespace '{}' does not exist. Create it first with CREATE NAMESPACE {}",
            stmt.namespace_id.as_str(),
            stmt.namespace_id.as_str()
        )));
    }

    // Type-specific validation
    match stmt.table_type {
        TableType::User | TableType::Shared => {
            // PRIMARY KEY validation
            if stmt.primary_key_column.is_none() {
                log::error!(
                    "❌ CREATE TABLE {:?} {}: PRIMARY KEY is required",
                    stmt.table_type,
                    table_id_str
                );
                return Err(KalamDbError::InvalidOperation(format!(
                    "{:?} tables require a PRIMARY KEY column",
                    stmt.table_type
                )));
            }
        },
        TableType::Stream => {
            // TTL validation
            if stmt.ttl_seconds.is_none() {
                log::error!("❌ CREATE TABLE STREAM {}: TTL clause is required", table_id_str);
                return Err(KalamDbError::InvalidOperation(
                    "STREAM tables require TTL clause (e.g., TTL 3600)".to_string(),
                ));
            }
        },
        TableType::System => {
            return Err(KalamDbError::InvalidOperation(
                "Cannot create SYSTEM tables via SQL".to_string(),
            ));
        },
    }

    // Check if table already exists
    let schema_registry = app_context.schema_registry();
    let table_id = TableId::from_strings(stmt.namespace_id.as_str(), stmt.table_name.as_str());
    let existing_def = schema_registry
        .get_table_if_exists(&table_id)
        .into_kalamdb_error("Failed to check table existence")?;

    if let Some(existing_def) = existing_def {
        if stmt.if_not_exists {
            log::info!("ℹ️  TABLE {} already exists (IF NOT EXISTS)", table_id_str);
            // Return the existing definition
            return Ok((*existing_def).clone());
        } else {
            log::warn!("❌ CREATE TABLE failed: {} already exists", table_id_str);
            return Err(KalamDbError::AlreadyExists(format!(
                "Table {} already exists",
                table_id_str
            )));
        }
    }

    // Resolve storage
    let (storage_id, _storage_type) = resolve_storage_info(&app_context, stmt.storage_id.as_ref())?;

    // Build columns from Arrow schema
    let columns: Vec<ColumnDefinition> = stmt
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            kalamdb_sql::validation::validate_column_name(field.name()).map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Invalid column name '{}': {}",
                    field.name(),
                    e
                ))
            })?;

            // First try to read KalamDataType from field metadata (preserves FILE, JSON, etc.)
            // Fall back to Arrow type conversion if metadata not present
            let kalam_type = kalamdb_commons::conversions::read_kalam_data_type_metadata(field)
                .unwrap_or_else(|| {
                    KalamDataType::from_arrow_type(field.data_type()).unwrap_or(KalamDataType::Text)
                });

            let is_pk =
                stmt.primary_key_column.as_ref().map(|pk| pk == field.name()).unwrap_or(false);

            let default_val =
                stmt.column_defaults.get(field.name()).cloned().unwrap_or(ColumnDefault::None);

            Ok(ColumnDefinition::new(
                (idx + 1) as u64,
                field.name().clone(),
                (idx + 1) as u32,
                kalam_type,
                field.is_nullable(),
                is_pk,
                false,
                default_val,
                None,
            ))
        })
        .collect::<Result<Vec<_>, KalamDbError>>()?;

    // Build table options
    let table_options = match stmt.table_type {
        TableType::User => TableOptions::user(),
        TableType::Shared => TableOptions::shared(),
        TableType::Stream => TableOptions::stream(stmt.ttl_seconds.unwrap_or(3600)),
        TableType::System => TableOptions::system(),
    };

    // Create TableDefinition
    let mut table_def = TableDefinition::new(
        stmt.namespace_id.clone(),
        stmt.table_name.clone(),
        stmt.table_type,
        columns,
        table_options.clone(),
        None,
    )
    .map_err(KalamDbError::SchemaError)?;

    // Apply table-level options from DDL
    match (&mut table_def.table_options, stmt.table_type) {
        (TableOptions::User(opts), TableType::User) => {
            opts.storage_id = storage_id.clone();
            opts.use_user_storage = stmt.use_user_storage;
            opts.flush_policy = stmt.flush_policy.clone();
        },
        (TableOptions::Shared(opts), TableType::Shared) => {
            opts.storage_id = storage_id.clone();
            // Only override access_level if explicitly specified in SQL; otherwise keep the default (Private)
            if let Some(access) = stmt.access_level {
                opts.access_level = Some(access);
            }
            opts.flush_policy = stmt.flush_policy.clone();
        },
        (TableOptions::Stream(opts), TableType::Stream) => {
            if let Some(ttl) = stmt.ttl_seconds {
                opts.ttl_seconds = ttl;
            }
        },
        _ => {},
    }

    // Inject system columns (_seq, _deleted)
    let sys_cols = app_context.system_columns_service();
    sys_cols.add_system_columns(&mut table_def)?;

    log::debug!(
        "✅ Built TableDefinition for {} (type: {:?}, columns: {}, version: {})",
        table_id_str,
        stmt.table_type,
        table_def.columns.len(),
        table_def.schema_version
    );

    Ok(table_def)
}

fn resolve_storage_info(
    app_context: &Arc<AppContext>,
    requested: Option<&StorageId>,
) -> Result<(StorageId, StorageType), KalamDbError> {
    let storages_provider = app_context.system_tables().storages();
    let storage_id = requested.cloned().unwrap_or_else(|| StorageId::from("local"));

    let storage = storages_provider.get_storage_by_id(&storage_id)?.ok_or_else(|| {
        log::error!("❌ CREATE TABLE failed: Storage '{}' does not exist", storage_id.as_str());
        KalamDbError::InvalidOperation(format!("Storage '{}' does not exist", storage_id.as_str()))
    })?;

    let storage_type = storage.storage_type;
    Ok((storage_id, storage_type))
}
