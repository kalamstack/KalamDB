//! DDL event trigger that provides transparent DDL interception for Kalam-managed schemas.
//!
//! Only schemas explicitly enabled via `kalam.enable_schema('app')` are intercepted.
//! Regular PostgreSQL tables in non-enabled schemas are untouched.
//!
//! - `CREATE TABLE app.profiles (...)` → creates Kalam table + foreign table
//! - `ALTER TABLE app.profiles ADD COLUMN email TEXT` → syned to Kalam
//! - `DROP FOREIGN TABLE app.profiles` → drops Kalam table
//!
//! Per-table type override: `SET kalam.table_type = 'stream'` before CREATE TABLE.
//! Stream TTL: `SET kalam.stream_ttl_seconds = '3600'`.

use pgrx::prelude::*;

#[cfg(feature = "embedded")]
use kalam_pg_common::KalamPgError;

// ---------------------------------------------------------------------------
// Namespace management
// ---------------------------------------------------------------------------

/// Ensure the Kalam namespace exists for a given schema name.
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_ensure_namespace(schema_name: &str) -> String {
    match ensure_namespace_impl(schema_name) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("pg_kalam: {}", e),
    }
}

#[cfg(feature = "embedded")]
fn ensure_namespace_impl(schema_name: &str) -> Result<String, KalamPgError> {
    let embedded_state = crate::ensure_embedded_extension_state(None)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;
    ensure_namespace_impl_inner(schema_name, &embedded_state)
}

/// Inner implementation reusable by DDL orchestration functions.
#[cfg(feature = "embedded")]
pub(crate) fn ensure_namespace_impl_inner(
    schema_name: &str,
    embedded_state: &crate::EmbeddedExtensionState,
) -> Result<String, KalamPgError> {
    use kalamdb_commons::models::NamespaceId;
    use kalamdb_system::Namespace;
    use std::time::{SystemTime, UNIX_EPOCH};

    let app_context = embedded_state.runtime().app_context();
    let namespaces = app_context.system_tables().namespaces();
    let namespace_id = NamespaceId::new(schema_name);

    if namespaces
        .get_namespace(&namespace_id)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?
        .is_some()
    {
        return Ok(format!("namespace '{}' already exists", schema_name));
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?
        .as_millis() as i64;

    namespaces
        .create_namespace(Namespace {
            namespace_id,
            name: schema_name.to_string(),
            created_at: now,
            options: Some("{}".to_string()),
            table_count: 0,
        })
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;

    Ok(format!("namespace '{}' created", schema_name))
}

// ---------------------------------------------------------------------------
// CREATE TABLE interception
// ---------------------------------------------------------------------------

/// Intercept a native CREATE TABLE in a Kalam-managed schema: drop the regular
/// table PG just created and replace it with a Kalam engine table + foreign table.
///
/// `default_table_type` comes from `kalam._managed_schemas` (set by `kalam.enable_schema`).
/// Can be overridden per-table with `SET kalam.table_type = 'stream'`.
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_intercept_create_table(
    schema_name: &str,
    table_name: &str,
    default_table_type: default!(&str, "'user'"),
) -> String {
    match intercept_create_table_impl(schema_name, table_name, default_table_type) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("kalam_intercept_create_table: {}", e),
    }
}

#[cfg(feature = "embedded")]
fn intercept_create_table_impl(
    schema_name: &str,
    table_name: &str,
    default_table_type: &str,
) -> Result<String, KalamPgError> {
    // Read columns + primary key from the just-created regular table.
    let columns = read_pg_table_columns(schema_name, table_name)?;
    let pk_columns = read_pg_primary_key_columns(schema_name, table_name)?;

    if columns.is_empty() {
        return Err(KalamPgError::Validation("no columns found in created table".to_string()));
    }

    // Build Kalam-compatible column definition SQL.
    let columns_sql = build_kalam_columns_sql(&columns, &pk_columns);

    // Drop the regular heap table (will be replaced by a foreign table).
    crate::ddl_orchestration::execute_pg_sql(&format!(
        "DROP TABLE \"{}\".\"{}\"",
        crate::ddl_orchestration::escape_identifier(schema_name),
        crate::ddl_orchestration::escape_identifier(table_name),
    ))?;

    // Determine table type: GUC override > schema default.
    let table_type = read_guc_or_default("kalam.table_type", default_table_type);
    let ttl_seconds = if table_type == "stream" {
        let s = read_guc_or_default("kalam.stream_ttl_seconds", "3600");
        Some(s.parse::<i64>().unwrap_or(3600))
    } else {
        None
    };

    // Create the Kalam engine table + PG foreign table.
    crate::ddl_orchestration::create_table_impl(
        schema_name,
        table_name,
        &columns_sql,
        &table_type,
        ttl_seconds,
        None,
    )?;

    Ok(format!(
        "intercepted CREATE TABLE: created {} table {}.{} via Kalam",
        table_type, schema_name, table_name
    ))
}

// ---------------------------------------------------------------------------
// ALTER TABLE sync
// ---------------------------------------------------------------------------

/// Sync ALTER TABLE changes from PG foreign table definition to the Kalam engine.
///
/// Compares PG-side columns with Kalam-side columns and issues ADD COLUMN
/// for any columns that exist in PG but not in Kalam.
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_sync_alter_table(schema_name: &str, table_name: &str) -> String {
    match sync_alter_table_impl(schema_name, table_name) {
        Ok(msg) => msg,
        Err(e) => pgrx::error!("kalam_sync_alter_table: {}", e),
    }
}

#[cfg(feature = "embedded")]
fn sync_alter_table_impl(schema_name: &str, table_name: &str) -> Result<String, KalamPgError> {
    use kalamdb_commons::models::{NamespaceId, TableName, UserId};
    use kalamdb_commons::{Role, TableId};
    use std::collections::HashSet;

    let embedded_state = crate::ensure_embedded_extension_state(None)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;

    let app_context = embedded_state.runtime().app_context();
    let table_id = TableId::new(NamespaceId::new(schema_name), TableName::new(table_name));
    let cached = match app_context.schema_registry().get(&table_id) {
        Some(c) => c,
        None => return Ok("table not found in Kalam, skipping sync".to_string()),
    };

    let system_cols: &[&str] = &["_userid", "_seq", "_deleted"];
    let kalam_col_names: HashSet<String> = cached
        .table
        .columns
        .iter()
        .filter(|c| !system_cols.contains(&c.column_name.as_str()))
        .map(|c| c.column_name.clone())
        .collect();

    // Read current PG foreign table column definitions.
    let pg_columns = read_pg_table_columns(schema_name, table_name)?;

    // Find columns in PG that are absent from Kalam → ADD COLUMN.
    let sql_service = crate::EmbeddedSqlService::new(embedded_state);
    let mut added = 0u32;
    for pg_col in &pg_columns {
        if system_cols.contains(&pg_col.name.as_str()) {
            continue;
        }
        if !kalam_col_names.contains(&pg_col.name) {
            let kalam_type = map_pg_type_to_kalam(&pg_col.type_name);
            let kalam_sql = format!(
                "ALTER TABLE {}.{} ADD COLUMN {} {}",
                schema_name, table_name, pg_col.name, kalam_type
            );
            sql_service.execute_json(
                &kalam_sql,
                Some(UserId::new("postgres_extension")),
                Role::System,
            )?;
            added += 1;
        }
    }

    Ok(format!("synced {} new column(s) to Kalam", added))
}

// ---------------------------------------------------------------------------
// DROP handling (Kalam-side only, for sql_drop trigger)
// ---------------------------------------------------------------------------

/// Drop the Kalam-side table only (the PG foreign table is already gone).
/// Called by the `sql_drop` event trigger when a foreign table is dropped.
#[cfg(feature = "embedded")]
#[pg_extern]
pub fn kalam_drop_kalam_table(schema_name: &str, table_name: &str) -> String {
    match drop_kalam_table_impl(schema_name, table_name) {
        Ok(msg) => msg,
        Err(e) => {
            pgrx::warning!("kalam_drop_kalam_table: {}", e);
            format!("drop skipped: {}", e)
        },
    }
}

#[cfg(feature = "embedded")]
fn drop_kalam_table_impl(schema_name: &str, table_name: &str) -> Result<String, KalamPgError> {
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;

    let embedded_state = crate::ensure_embedded_extension_state(None)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;

    let kalam_sql = format!("DROP TABLE IF EXISTS {}.{}", schema_name, table_name);
    let sql_service = crate::EmbeddedSqlService::new(embedded_state);
    sql_service.execute_json(&kalam_sql, Some(UserId::new("postgres_extension")), Role::System)?;

    Ok(format!("dropped Kalam table {}.{}", schema_name, table_name))
}

// ---------------------------------------------------------------------------
// PG catalog helpers
// ---------------------------------------------------------------------------

#[cfg(feature = "embedded")]
struct PgColumn {
    name: String,
    type_name: String,
    #[allow(dead_code)]
    not_null: bool,
}

/// Read column definitions from a PG table (regular or foreign) via catalog.
#[cfg(feature = "embedded")]
fn read_pg_table_columns(
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<PgColumn>, KalamPgError> {
    Spi::connect(|client| {
        let query = format!(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)::text, a.attnotnull \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = '{}' AND c.relname = '{}' \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            schema_name.replace('\'', "''"),
            table_name.replace('\'', "''"),
        );
        let mut columns = Vec::new();
        let table = client.select(&query, None, &[])?;
        for row in table {
            let name: String = row.get::<String>(1)?.unwrap_or_default();
            let type_name: String = row.get::<String>(2)?.unwrap_or_default();
            let not_null: bool = row.get::<bool>(3)?.unwrap_or(false);
            columns.push(PgColumn {
                name,
                type_name,
                not_null,
            });
        }
        Ok(columns)
    })
    .map_err(|e: pgrx::spi::Error| {
        KalamPgError::Execution(format!("failed to read table columns: {}", e))
    })
}

/// Read primary key column names from a PG table via catalog.
#[cfg(feature = "embedded")]
fn read_pg_primary_key_columns(
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<String>, KalamPgError> {
    Spi::connect(|client| {
        let query = format!(
            "SELECT a.attname::text \
             FROM pg_constraint con \
             JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey) \
             JOIN pg_class c ON c.oid = con.conrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = '{}' AND c.relname = '{}' AND con.contype = 'p' \
             ORDER BY array_position(con.conkey, a.attnum)",
            schema_name.replace('\'', "''"),
            table_name.replace('\'', "''"),
        );
        let mut pk_cols = Vec::new();
        let table = client.select(&query, None, &[])?;
        for row in table {
            if let Some(name) = row.get::<String>(1)? {
                pk_cols.push(name);
            }
        }
        Ok(pk_cols)
    })
    .map_err(|e: pgrx::spi::Error| {
        KalamPgError::Execution(format!("failed to read primary key: {}", e))
    })
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Map PostgreSQL `format_type()` output to Kalam SQL type names.
#[cfg(feature = "embedded")]
fn map_pg_type_to_kalam(pg_type: &str) -> String {
    let lower = pg_type.to_lowercase();

    // Parameterized types.
    if lower.starts_with("numeric") || lower.starts_with("decimal") {
        return if let Some(idx) = lower.find('(') {
            format!("DECIMAL{}", &pg_type[idx..])
        } else {
            "DECIMAL".to_string()
        };
    }
    if lower.starts_with("character varying") || lower.starts_with("varchar") {
        return "TEXT".to_string();
    }
    if lower.starts_with("character(") || lower.starts_with("char(") {
        return "TEXT".to_string();
    }

    match lower.as_str() {
        "text" | "name" => "TEXT",
        "boolean" => "BOOLEAN",
        "smallint" => "SMALLINT",
        "integer" => "INTEGER",
        "bigint" => "BIGINT",
        "real" => "FLOAT",
        "double precision" => "DOUBLE",
        "date" => "DATE",
        "time without time zone" | "time" => "TIME",
        "timestamp without time zone" | "timestamp" => "TIMESTAMP",
        "timestamp with time zone" => "DATETIME",
        "uuid" => "UUID",
        "bytea" => "BYTES",
        "json" | "jsonb" => "JSON",
        _ => "TEXT", // fallback for unmapped types
    }
    .to_string()
}

/// Build Kalam column definition SQL from PG column metadata.
#[cfg(feature = "embedded")]
fn build_kalam_columns_sql(columns: &[PgColumn], pk_columns: &[String]) -> String {
    columns
        .iter()
        .map(|col| {
            let kalam_type = map_pg_type_to_kalam(&col.type_name);
            if pk_columns.contains(&col.name) {
                format!("{} {} PRIMARY KEY", col.name, kalam_type)
            } else {
                format!("{} {}", col.name, kalam_type)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Read a PostgreSQL GUC setting, returning a default if unset or empty.
#[cfg(feature = "embedded")]
fn read_guc_or_default(name: &str, default: &str) -> String {
    let query = format!("SELECT current_setting('{}', true)::text", name.replace('\'', "''"));
    Spi::connect(|client| {
        let table = client.select(&query, Some(1), &[])?;
        for row in table {
            if let Some(val) = row.get::<String>(1)? {
                if !val.is_empty() {
                    return Ok(val);
                }
            }
        }
        Ok(default.to_string())
    })
    .unwrap_or_else(|_: pgrx::spi::Error| default.to_string())
}

// ---------------------------------------------------------------------------
// Event trigger SQL registration
// ---------------------------------------------------------------------------

#[cfg(feature = "embedded")]
pgrx::extension_sql!(
    r#"
-- DDL event trigger: intercepts CREATE TABLE and ALTER TABLE for Kalam-managed schemas.
-- Only schemas registered in kalam._managed_schemas are affected.
CREATE OR REPLACE FUNCTION kalam_ddl_event_trigger()
RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    schema_config record;
    tbl_schema text;
    tbl_name text;
    managed_table_exists bool;
BEGIN
    -- Guard: skip if management table doesn't exist yet (during extension install)
    SELECT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'kalam' AND tablename = '_managed_schemas'
    ) INTO managed_table_exists;

    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        -- CREATE TABLE → intercept if schema is Kalam-managed
        IF managed_table_exists
           AND obj.command_tag = 'CREATE TABLE'
           AND obj.object_type = 'table'
        THEN
            tbl_schema := split_part(obj.object_identity, '.', 1);
            tbl_name   := split_part(obj.object_identity, '.', 2);

            SELECT INTO schema_config *
              FROM kalam._managed_schemas
             WHERE schema_name = tbl_schema;

            IF FOUND THEN
                PERFORM kalam_intercept_create_table(
                    tbl_schema,
                    tbl_name,
                    COALESCE(schema_config.default_table_type, 'user')
                );
            END IF;

        -- ALTER (FOREIGN) TABLE → sync column changes to Kalam
        ELSIF managed_table_exists
              AND obj.command_tag IN ('ALTER TABLE', 'ALTER FOREIGN TABLE')
        THEN
            tbl_schema := split_part(obj.object_identity, '.', 1);
            tbl_name   := split_part(obj.object_identity, '.', 2);

            IF EXISTS (SELECT 1 FROM kalam._managed_schemas WHERE schema_name = tbl_schema) THEN
                PERFORM kalam_sync_alter_table(tbl_schema, tbl_name);
            END IF;
        END IF;
    END LOOP;
END;
$$;

DROP EVENT TRIGGER IF EXISTS kalam_ddl_trigger;
CREATE EVENT TRIGGER kalam_ddl_trigger
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'ALTER FOREIGN TABLE')
  EXECUTE FUNCTION kalam_ddl_event_trigger();

-- sql_drop trigger: clean up Kalam table when a foreign table in a managed schema is dropped.
CREATE OR REPLACE FUNCTION kalam_sql_drop_trigger()
RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    managed_table_exists bool;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'kalam' AND tablename = '_managed_schemas'
    ) INTO managed_table_exists;

    IF NOT managed_table_exists THEN
        RETURN;
    END IF;

    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF obj.object_type = 'foreign table'
           AND EXISTS (SELECT 1 FROM kalam._managed_schemas WHERE schema_name = obj.schema_name)
        THEN
            PERFORM kalam_drop_kalam_table(obj.schema_name, obj.object_name);
        END IF;
    END LOOP;
END;
$$;

DROP EVENT TRIGGER IF EXISTS kalam_drop_trigger;
CREATE EVENT TRIGGER kalam_drop_trigger
  ON sql_drop
  EXECUTE FUNCTION kalam_sql_drop_trigger();
"#,
    name = "kalam_ddl_event_triggers",
    requires = [
        kalam_ensure_namespace,
        kalam_intercept_create_table,
        kalam_sync_alter_table,
        kalam_drop_kalam_table
    ],
);
