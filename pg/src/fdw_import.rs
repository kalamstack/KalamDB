//! `ImportForeignSchema` callback: generate foreign table SQL from Kalam metadata.

use pgrx::pg_guard;
use pgrx::pg_sys;

/// `ImportForeignSchema` callback: return a list of SQL statements to create foreign tables.
#[pg_guard]
pub unsafe extern "C-unwind" fn import_foreign_schema(
    stmt: *mut pg_sys::ImportForeignSchemaStmt,
    server_oid: pg_sys::Oid,
) -> *mut pg_sys::List {
    let result = import_foreign_schema_impl(stmt, server_oid);
    match result {
        Ok(list) => list,
        Err(e) => pgrx::error!("pg_kalam import: {}", e),
    }
}

fn import_foreign_schema_impl(
    _stmt: *mut pg_sys::ImportForeignSchemaStmt,
    _server_oid: pg_sys::Oid,
) -> Result<*mut pg_sys::List, kalam_pg_common::KalamPgError> {
    Err(kalam_pg_common::KalamPgError::Unsupported(
        "IMPORT FOREIGN SCHEMA is not yet supported in remote mode".to_string(),
    ))
}
