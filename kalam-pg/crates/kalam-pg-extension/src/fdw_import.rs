//! `ImportForeignSchema` callback: generate foreign table SQL from Kalam metadata.

use pgrx::pg_guard;
use pgrx::pg_sys;
use std::ffi::{CStr, CString};

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

#[cfg(feature = "embedded")]
unsafe fn import_foreign_schema_impl(
    stmt: *mut pg_sys::ImportForeignSchemaStmt,
    _server_oid: pg_sys::Oid,
) -> Result<*mut pg_sys::List, kalam_pg_common::KalamPgError> {
    use crate::ImportForeignSchemaRequest;
    use kalamdb_commons::models::NamespaceId;

    let remote_schema = CStr::from_ptr((*stmt).remote_schema).to_string_lossy().into_owned();
    let local_schema = CStr::from_ptr((*stmt).local_schema).to_string_lossy().into_owned();
    let server_name = CStr::from_ptr((*stmt).server_name).to_string_lossy().into_owned();

    let embedded_state = crate::ensure_embedded_extension_state(None)
        .map_err(|e| kalam_pg_common::KalamPgError::Execution(e.to_string()))?;

    let mut request = ImportForeignSchemaRequest::new(&server_name, &local_schema);
    request.source_namespace = Some(NamespaceId::new(remote_schema));

    let sql_statements = embedded_state.import_foreign_schema_sql(&request)?;

    let mut result_list: *mut pg_sys::List = std::ptr::null_mut();
    for sql in &sql_statements {
        let cstr = CString::new(sql.as_str())
            .map_err(|e| kalam_pg_common::KalamPgError::Execution(e.to_string()))?;
        let pg_str = pg_sys::pstrdup(cstr.as_ptr());
        let str_val = pg_sys::makeString(pg_str);
        result_list = pg_sys::lappend(result_list, str_val as *mut std::ffi::c_void);
    }

    Ok(result_list)
}

#[cfg(not(feature = "embedded"))]
unsafe fn import_foreign_schema_impl(
    _stmt: *mut pg_sys::ImportForeignSchemaStmt,
    _server_oid: pg_sys::Oid,
) -> Result<*mut pg_sys::List, kalam_pg_common::KalamPgError> {
    Err(kalam_pg_common::KalamPgError::Unsupported(
        "IMPORT FOREIGN SCHEMA requires embedded or remote feature".to_string(),
    ))
}
