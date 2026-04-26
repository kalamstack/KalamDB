use std::{collections::BTreeMap, ffi::CStr, str::FromStr};

use kalam_pg_common::KalamPgError;
use kalam_pg_fdw::TableOptions;
use kalamdb_commons::{
    models::{NamespaceId, TableName},
    TableId, TableType,
};
use pgrx::pg_sys;

pub fn resolve_table_options_for_relation(
    relation: pg_sys::Relation,
    options: &BTreeMap<String, String>,
) -> Result<TableOptions, KalamPgError> {
    if relation.is_null() {
        return Err(KalamPgError::Validation(
            "relation is required to resolve mirrored table identity".to_string(),
        ));
    }

    let rel = unsafe { (*relation).rd_rel };
    if rel.is_null() {
        return Err(KalamPgError::Validation(
            "relation metadata is required to resolve mirrored table identity".to_string(),
        ));
    }

    let namespace = {
        let namespace_ptr = unsafe { pg_sys::get_namespace_name((*rel).relnamespace) };
        cstr_to_string(namespace_ptr)
    };
    let table_name = Some(
        unsafe { CStr::from_ptr((*rel).relname.data.as_ptr()) }
            .to_string_lossy()
            .into_owned(),
    );
    let table_type = options
        .get("table_type")
        .ok_or_else(|| {
            KalamPgError::Validation("table option 'table_type' is required".to_string())
        })
        .and_then(|value| TableType::from_str(value).map_err(KalamPgError::Validation))?;

    let namespace = namespace
        .ok_or_else(|| KalamPgError::Validation("relation namespace is required".to_string()))?;
    let table_name = table_name
        .ok_or_else(|| KalamPgError::Validation("relation table name is required".to_string()))?;

    Ok(TableOptions {
        table_id: TableId::new(NamespaceId::new(namespace), TableName::new(table_name)),
        table_type,
    })
}

fn cstr_to_string(ptr: *mut std::ffi::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned())
}
