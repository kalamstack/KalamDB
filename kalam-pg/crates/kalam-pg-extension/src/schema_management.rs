//! Schema-level opt-in for Kalam management.
//!
//! Usage:
//! ```sql
//! SELECT kalam.enable_schema('app', 'user');
//! -- tables in 'app' are now Kalam-managed
//! CREATE TABLE app.profiles (id TEXT PRIMARY KEY, name TEXT);
//! ```

use pgrx::prelude::*;

#[cfg(feature = "embedded")]
use kalam_pg_common::KalamPgError;

/// Mark a PostgreSQL schema as Kalam-managed.
///
/// Creates the PG schema and Kalam namespace if needed, then registers
/// the schema so that `CREATE TABLE` in it is automatically intercepted.
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_enable_schema(
    schema_name: &str,
    default_table_type: default!(&str, "'user'"),
) -> String {
    match enable_schema_impl(schema_name, default_table_type) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("kalam_enable_schema: {}", e),
    }
}

#[cfg(feature = "embedded")]
fn enable_schema_impl(schema_name: &str, default_table_type: &str) -> Result<String, KalamPgError> {
    use crate::ddl_orchestration::{escape_identifier, execute_pg_sql};

    if !matches!(default_table_type, "user" | "stream") {
        return Err(KalamPgError::Validation(format!(
            "unsupported default_table_type '{}' (expected 'user' or 'stream')",
            default_table_type
        )));
    }

    // 1. Ensure PG schema exists.
    execute_pg_sql(&format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", escape_identifier(schema_name)))?;

    // 2. Ensure Kalam namespace exists.
    let embedded_state = crate::ensure_embedded_extension_state(None)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;
    crate::ddl_event::ensure_namespace_impl_inner(schema_name, &embedded_state)?;

    // 3. Register in kalam._managed_schemas.
    execute_pg_sql(&format!(
        "INSERT INTO kalam._managed_schemas (schema_name, default_table_type) \
         VALUES ('{}', '{}') \
         ON CONFLICT (schema_name) DO UPDATE SET default_table_type = EXCLUDED.default_table_type",
        schema_name.replace('\'', "''"),
        default_table_type.replace('\'', "''"),
    ))?;

    Ok(format!(
        "schema '{}' enabled for Kalam (default type: {})",
        schema_name, default_table_type
    ))
}

/// Remove Kalam management from a PostgreSQL schema.
///
/// Existing foreign tables remain but new `CREATE TABLE` will no longer
/// be intercepted.
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_disable_schema(schema_name: &str) -> String {
    match disable_schema_impl(schema_name) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("kalam_disable_schema: {}", e),
    }
}

#[cfg(feature = "embedded")]
fn disable_schema_impl(schema_name: &str) -> Result<String, KalamPgError> {
    crate::ddl_orchestration::execute_pg_sql(&format!(
        "DELETE FROM kalam._managed_schemas WHERE schema_name = '{}'",
        schema_name.replace('\'', "''"),
    ))?;
    Ok(format!("schema '{}' disabled for Kalam", schema_name))
}

// ---------------------------------------------------------------------------
// Extension SQL: kalam schema + managed_schemas table + convenience wrappers
// ---------------------------------------------------------------------------

#[cfg(feature = "embedded")]
pgrx::extension_sql!(
    r#"
CREATE SCHEMA IF NOT EXISTS kalam;

CREATE TABLE IF NOT EXISTS kalam._managed_schemas (
    schema_name TEXT PRIMARY KEY,
    default_table_type TEXT NOT NULL DEFAULT 'user'
        CHECK (default_table_type IN ('user', 'stream')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Convenience wrappers in the kalam schema for nicer calling syntax:
--   SELECT kalam.enable_schema('app', 'user');
CREATE OR REPLACE FUNCTION kalam.enable_schema(
    p_schema_name TEXT,
    p_default_table_type TEXT DEFAULT 'user'
)
RETURNS TEXT LANGUAGE SQL AS $$
    SELECT kalam_enable_schema(p_schema_name, p_default_table_type);
$$;

CREATE OR REPLACE FUNCTION kalam.disable_schema(
    p_schema_name TEXT
)
RETURNS TEXT LANGUAGE SQL AS $$
    SELECT kalam_disable_schema(p_schema_name);
$$;
"#,
    name = "kalam_schema_management_setup",
    requires = [kalam_enable_schema, kalam_disable_schema],
);
