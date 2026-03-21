//! DDL orchestration functions that bridge PostgreSQL SQL commands to Kalam DDL operations.
//!
//! These functions allow PostgreSQL users to create, alter, and drop Kalam user and stream
//! tables using familiar SQL function calls. Each function orchestrates both the Kalam-side
//! DDL and the corresponding PostgreSQL foreign table metadata.
//!
//! Example:
//! ```sql
//! SELECT kalam_create_user_table('myapp', 'profiles', 'id TEXT PRIMARY KEY, name TEXT, age INTEGER');
//! SELECT * FROM myapp.profiles;
//! ```

use pgrx::prelude::*;

#[cfg(feature = "embedded")]
use kalam_pg_common::KalamPgError;

// ---------------------------------------------------------------------------
// CREATE USER TABLE
// ---------------------------------------------------------------------------

/// Create a Kalam user table and the matching PostgreSQL foreign table.
///
/// Arguments:
///   - `schema_name`: PostgreSQL schema (maps to Kalam namespace).
///   - `table_name`: Table name.
///   - `columns_sql`: Column definitions in SQL syntax, e.g. `"id TEXT PRIMARY KEY, name TEXT"`.
///   - `server_name`: Foreign server name (defaults to `kalam_server`).
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_create_user_table(
    schema_name: &str,
    table_name: &str,
    columns_sql: &str,
    server_name: default!(Option<&str>, "NULL"),
) -> String {
    match create_table_impl(schema_name, table_name, columns_sql, "user", None, server_name) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("kalam_create_user_table: {}", e),
    }
}

// ---------------------------------------------------------------------------
// CREATE STREAM TABLE
// ---------------------------------------------------------------------------

/// Create a Kalam stream table and the matching PostgreSQL foreign table.
///
/// Arguments:
///   - `schema_name`: PostgreSQL schema (maps to Kalam namespace).
///   - `table_name`: Table name.
///   - `columns_sql`: Column definitions in SQL syntax.
///   - `ttl_seconds`: Time-to-live in seconds for stream events (required for stream tables).
///   - `server_name`: Foreign server name (defaults to `kalam_server`).
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_create_stream_table(
    schema_name: &str,
    table_name: &str,
    columns_sql: &str,
    ttl_seconds: default!(i64, "3600"),
    server_name: default!(Option<&str>, "NULL"),
) -> String {
    match create_table_impl(
        schema_name,
        table_name,
        columns_sql,
        "stream",
        Some(ttl_seconds),
        server_name,
    ) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("kalam_create_stream_table: {}", e),
    }
}

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

/// Forward an ALTER TABLE statement to the embedded Kalam engine.
///
/// Supports operations like ADD COLUMN, DROP COLUMN, RENAME COLUMN.
/// After the Kalam-side ALTER succeeds, the PostgreSQL foreign table is
/// dropped and recreated to reflect the new schema.
///
/// Arguments:
///   - `schema_name`: PostgreSQL schema (Kalam namespace).
///   - `table_name`: Table name.
///   - `alter_clause`: The ALTER clause, e.g. `"ADD COLUMN email TEXT"`.
///   - `server_name`: Foreign server name (defaults to `kalam_server`).
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_alter_table(
    schema_name: &str,
    table_name: &str,
    alter_clause: &str,
    server_name: default!(Option<&str>, "NULL"),
) -> String {
    match alter_table_impl(schema_name, table_name, alter_clause, server_name) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("kalam_alter_table: {}", e),
    }
}

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

/// Drop a Kalam table and the corresponding PostgreSQL foreign table.
///
/// Arguments:
///   - `schema_name`: PostgreSQL schema (Kalam namespace).
///   - `table_name`: Table name.
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_drop_table(schema_name: &str, table_name: &str) -> String {
    match drop_table_impl(schema_name, table_name) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("kalam_drop_table: {}", e),
    }
}

// ===========================================================================
// Implementation
// ===========================================================================

#[cfg(feature = "embedded")]
const DEFAULT_SERVER_NAME: &str = "kalam_server";

#[cfg(feature = "embedded")]
fn resolve_server(server_name: Option<&str>) -> &str {
    server_name
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SERVER_NAME)
}

#[cfg(feature = "embedded")]
pub(crate) fn create_table_impl(
    schema_name: &str,
    table_name: &str,
    columns_sql: &str,
    table_type: &str,
    ttl_seconds: Option<i64>,
    server_name: Option<&str>,
) -> Result<String, KalamPgError> {
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;

    let server = resolve_server(server_name);

    // 1. Ensure the embedded runtime is bootstrapped.
    let embedded_state = crate::ensure_embedded_extension_state(None)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;

    // 2. Ensure the Kalam namespace exists.
    crate::ddl_event::ensure_namespace_impl_inner(schema_name, &embedded_state)?;

    // 3. Build the CREATE TABLE SQL for the Kalam engine.
    let type_keyword = match table_type {
        "user" => "USER",
        "stream" => "STREAM",
        other => {
            return Err(KalamPgError::Validation(format!(
                "unsupported table type '{}' for DDL orchestration (use 'user' or 'stream')",
                other
            )))
        },
    };

    let kalam_sql =
        format!("CREATE {} TABLE {}.{} ({})", type_keyword, schema_name, table_name, columns_sql);

    // Append WITH clause for stream tables (TTL_SECONDS is required).
    let kalam_sql = if let Some(ttl) = ttl_seconds {
        format!("{} WITH (TTL_SECONDS={})", kalam_sql, ttl)
    } else {
        kalam_sql
    };

    // 4. Execute DDL on the embedded engine as admin.
    let sql_service = crate::EmbeddedSqlService::new(embedded_state.clone());
    let result = sql_service.execute_json(
        &kalam_sql,
        Some(UserId::new("postgres_extension")),
        Role::System,
    )?;

    // If Kalam creation failed, propagate the error.
    if let Some(kind) = result.get("kind").and_then(|k| k.as_str()) {
        if kind != "success" {
            return Err(KalamPgError::Execution(
                result
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error")
                    .to_string(),
            ));
        }
    }

    // 5. Look up the newly created table definition to generate the foreign table SQL.
    let foreign_table_sql =
        build_create_foreign_table_sql(&embedded_state, schema_name, table_name, server)?;

    // 6. Ensure the PostgreSQL schema exists.
    ensure_pg_schema(schema_name)?;

    // 7. Create the foreign table in PostgreSQL.
    execute_pg_sql(&foreign_table_sql)?;

    Ok(format!(
        "created {} table {}.{} (Kalam + PostgreSQL foreign table)",
        table_type, schema_name, table_name
    ))
}

#[cfg(feature = "embedded")]
fn alter_table_impl(
    schema_name: &str,
    table_name: &str,
    alter_clause: &str,
    server_name: Option<&str>,
) -> Result<String, KalamPgError> {
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;

    let server = resolve_server(server_name);

    let embedded_state = crate::ensure_embedded_extension_state(None)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;

    // 1. Execute ALTER TABLE on the Kalam engine.
    let kalam_sql = format!("ALTER TABLE {}.{} {}", schema_name, table_name, alter_clause);

    let sql_service = crate::EmbeddedSqlService::new(embedded_state.clone());
    let result = sql_service.execute_json(
        &kalam_sql,
        Some(UserId::new("postgres_extension")),
        Role::System,
    )?;

    if let Some(kind) = result.get("kind").and_then(|k| k.as_str()) {
        if kind != "success" {
            return Err(KalamPgError::Execution(
                result
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error")
                    .to_string(),
            ));
        }
    }

    // 2. Recreate the PostgreSQL foreign table to reflect the new schema.
    let foreign_table_sql =
        build_create_foreign_table_sql(&embedded_state, schema_name, table_name, server)?;

    // Drop and recreate the foreign table (ensure schema exists first).
    ensure_pg_schema(schema_name)?;
    execute_pg_sql(&format!(
        "DROP FOREIGN TABLE IF EXISTS \"{}\".\"{}\"",
        escape_identifier(schema_name),
        escape_identifier(table_name)
    ))?;

    execute_pg_sql(&foreign_table_sql)?;

    Ok(format!(
        "altered table {}.{} (Kalam + PostgreSQL foreign table refreshed)",
        schema_name, table_name
    ))
}

#[cfg(feature = "embedded")]
fn drop_table_impl(schema_name: &str, table_name: &str) -> Result<String, KalamPgError> {
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;

    let embedded_state = crate::ensure_embedded_extension_state(None)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;

    // 1. Drop the Kalam table.
    let kalam_sql = format!("DROP TABLE IF EXISTS {}.{}", schema_name, table_name);

    let sql_service = crate::EmbeddedSqlService::new(embedded_state);
    sql_service.execute_json(&kalam_sql, Some(UserId::new("postgres_extension")), Role::System)?;

    // 2. Drop the PostgreSQL foreign table (ensure schema exists first for IF EXISTS to work).
    ensure_pg_schema(schema_name)?;
    execute_pg_sql(&format!(
        "DROP FOREIGN TABLE IF EXISTS \"{}\".\"{}\"",
        escape_identifier(schema_name),
        escape_identifier(table_name)
    ))?;

    Ok(format!("dropped table {}.{}", schema_name, table_name))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Look up the Kalam table definition and generate CREATE FOREIGN TABLE SQL.
#[cfg(feature = "embedded")]
fn build_create_foreign_table_sql(
    embedded_state: &crate::EmbeddedExtensionState,
    schema_name: &str,
    table_name: &str,
    server_name: &str,
) -> Result<String, KalamPgError> {
    use kalamdb_commons::models::{NamespaceId, TableName};
    use kalamdb_commons::TableId;

    let app_context = embedded_state.runtime().app_context();
    let table_id = TableId::new(NamespaceId::new(schema_name), TableName::new(table_name));

    let cached = app_context.schema_registry().get(&table_id).ok_or_else(|| {
        KalamPgError::Execution(format!(
            "table definition not found for {}.{} after creation",
            schema_name, table_name
        ))
    })?;

    kalam_pg_fdw::create_foreign_table_sql(server_name, schema_name, &cached.table)
}

/// Execute a SQL statement via SPI (Server Programming Interface).
#[cfg(feature = "embedded")]
pub(crate) fn execute_pg_sql(sql: &str) -> Result<(), KalamPgError> {
    Spi::run(sql).map_err(|e| KalamPgError::Execution(format!("PostgreSQL SPI error: {}", e)))
}

/// Ensure the PostgreSQL schema exists.
#[cfg(feature = "embedded")]
fn ensure_pg_schema(schema_name: &str) -> Result<(), KalamPgError> {
    execute_pg_sql(&format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", escape_identifier(schema_name)))
}

/// Escape a SQL identifier to prevent injection.
#[cfg(feature = "embedded")]
pub(crate) fn escape_identifier(value: &str) -> String {
    value.replace('"', "\"\"")
}
