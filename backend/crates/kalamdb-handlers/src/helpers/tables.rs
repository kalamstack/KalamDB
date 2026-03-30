//! DDL Helper Functions
//!
//! Common utilities for DDL operations including schema transformations,
//! table validation, and metadata storage.

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use arrow::datatypes::Schema;
use kalamdb_commons::schemas::{ColumnDefault, TableType};
use kalamdb_commons::StorageId;
use kalamdb_sql::ddl::CreateTableStatement;
use std::sync::Arc;

/// Validate table name
///
/// # Rules
/// - Must start with lowercase letter or underscore
/// - Cannot be a SQL keyword
///
/// # Arguments
/// * `name` - Table name to validate
///
/// # Returns
/// Ok(()) if valid, error otherwise
pub fn validate_table_name(name: &str) -> Result<(), String> {
    kalamdb_sql::validation::validate_table_name(name).map_err(|e| e.to_string())
}

/// Save table definition to information_schema.tables
///
/// Replaces fragmented schema storage with single atomic write.
///
/// # Arguments
/// * `stmt` - CREATE TABLE statement with all metadata
/// * `schema` - Final Arrow schema (after auto-increment and system column injection)
///
/// # Returns
/// Ok(()) on success, error on failure
pub fn save_table_definition(
    app_ctx: &AppContext,
    stmt: &CreateTableStatement,
    original_arrow_schema: &Arc<Schema>,
) -> Result<(), KalamDbError> {
    use kalamdb_commons::datatypes::{FromArrowType, KalamDataType};
    use kalamdb_commons::models::TableId;
    use kalamdb_commons::schemas::{ColumnDefinition, TableDefinition, TableOptions};

    // Extract columns directly from Arrow schema (user-provided columns only)
    let columns: Vec<ColumnDefinition> = original_arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            // Validate column name
            kalamdb_sql::validation::validate_column_name(field.name()).map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Invalid column name '{}': {}",
                    field.name(),
                    e
                ))
            })?;

            let kalam_type =
                KalamDataType::from_arrow_type(field.data_type()).unwrap_or(KalamDataType::Text);

            // T060: Mark the PRIMARY KEY column with is_primary_key = true
            let is_pk =
                stmt.primary_key_column.as_ref().map(|pk| pk == field.name()).unwrap_or(false);

            let default_val =
                stmt.column_defaults.get(field.name()).cloned().unwrap_or(ColumnDefault::None);

            log::debug!(
                "save_table_definition: Column '{}' - is_pk={}, default={:?}",
                field.name(),
                is_pk,
                default_val
            );

            Ok(ColumnDefinition::new(
                (idx + 1) as u64, // column_id is 1-indexed
                field.name().clone(),
                (idx + 1) as u32, // ordinal_position is 1-indexed
                kalam_type,
                field.is_nullable(),
                is_pk, // is_primary_key (T060: determined from stmt.primary_key_column)
                false, // is_partition_key (not used yet)
                default_val,
                None, // column_comment
            ))
        })
        .collect::<Result<Vec<_>, KalamDbError>>()?;

    // Build table options based on table type
    let table_options = match stmt.table_type {
        TableType::User => TableOptions::user(),
        TableType::Shared => TableOptions::shared(),
        TableType::Stream => TableOptions::stream(stmt.ttl_seconds.unwrap_or(3600)),
        TableType::System => TableOptions::system(),
    };

    // Create NEW TableDefinition directly (WITHOUT system columns yet)
    let mut table_def = TableDefinition::new(
        stmt.namespace_id.clone(),
        stmt.table_name.clone(),
        stmt.table_type,
        columns,
        table_options,
        None, // table_comment
    )
    .map_err(KalamDbError::SchemaError)?;

    // Persist table-level options from DDL (storage, flush policy, ACL, TTL overrides)
    match (&mut table_def.table_options, stmt.table_type) {
        (TableOptions::User(opts), TableType::User) => {
            let storage = stmt.storage_id.clone().unwrap_or_else(StorageId::local);
            opts.storage_id = storage;
            opts.use_user_storage = stmt.use_user_storage;
            opts.flush_policy = stmt.flush_policy.clone();
        },
        (TableOptions::Shared(opts), TableType::Shared) => {
            let storage = stmt.storage_id.clone().unwrap_or_else(StorageId::local);
            opts.storage_id = storage;
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

    // Inject system columns via SystemColumnsService (Phase 12, US5, T022)
    // This adds _seq, _deleted to the TableDefinition (authoritative types)
    let sys_cols = app_ctx.system_columns_service();
    sys_cols.add_system_columns(&mut table_def)?;

    // Phase 16: Schema history is now stored externally using TableVersionId keys.
    // The TableDefinition starts with schema_version = 1 (set in new()).
    // No need to push to schema_history anymore - TablesStore handles this.

    // Persist to system.tables AND cache in SchemaRegistr
    let table_id = TableId::from_strings(stmt.namespace_id.as_str(), stmt.table_name.as_str());

    // Write to system.tables for persistence
    let tables_provider = app_ctx.system_tables().tables();
    tables_provider
        .create_table(&table_id, &table_def)
        .into_kalamdb_error("Failed to save table definition to system.tables")?;

    // Cache the definition and create provider (replaces put_table_definition)
    // Note: save_table_definition requires Arc<AppContext>, but we only have &AppContext
    // This function is only called from table_creation.rs which now uses register_table instead
    // For now, this is a legacy path that should be migrated
    log::warn!("save_table_definition is deprecated - use SchemaRegistry::register_table instead");
    // schema_registry.put(app_ctx, table_def)?;

    log::info!(
        "Table definition for {}.{} saved to information_schema.tables (version 1)",
        stmt.namespace_id.as_str(),
        stmt.table_name.as_str()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_table_name_valid() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("Users").is_ok()); // Capitals are allowed
        assert!(validate_table_name("user_data_v2").is_ok());
    }

    #[test]
    fn test_validate_table_name_invalid() {
        assert!(validate_table_name("").is_err()); // Empty
        assert!(validate_table_name("_internal").is_err()); // Starts with underscore
        assert!(validate_table_name("select").is_err()); // SQL keyword
        assert!(validate_table_name("table").is_err()); // SQL keyword
        assert!(validate_table_name("123table").is_err()); // Starts with number
    }
}
