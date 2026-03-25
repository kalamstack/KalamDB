//! ProcessUtility hook that propagates DDL on foreign tables to KalamDB.
//!
//! Intercepts:
//! - `CREATE FOREIGN TABLE`  → `CREATE NAMESPACE IF NOT EXISTS` + `CREATE <type> TABLE`
//! - `ALTER FOREIGN TABLE`   → `ALTER TABLE ADD/DROP COLUMN`
//! - `DROP FOREIGN TABLE`    → `DROP <type> TABLE IF EXISTS`

use crate::fdw_options::parse_options;
use crate::remote_state::get_remote_extension_state;
use kalam_pg_common::KalamPgError;
use kalam_pg_fdw::ServerOptions;
use kalamdb_commons::TableType;
use pgrx::pg_sys;
use std::ffi::CStr;
use std::str::FromStr;

/// Previous ProcessUtility hook (may be null).
static mut PREV_PROCESS_UTILITY: pg_sys::ProcessUtility_hook_type = None;

/// Register the ProcessUtility hook. Must be called from `_PG_init`.
pub fn register_hook() {
    unsafe {
        PREV_PROCESS_UTILITY = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(pg_kalam_process_utility);
    }
}

/// The ProcessUtility hook entry point.
///
/// # Safety
/// Called by PostgreSQL for every utility (DDL) statement.
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn pg_kalam_process_utility(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    let utility_stmt = (*pstmt).utilityStmt;
    if utility_stmt.is_null() {
        call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        return;
    }

    let tag = (*utility_stmt).type_;

    match tag {
        pg_sys::NodeTag::T_CreateForeignTableStmt => {
            let statement_sql = extract_statement_sql(pstmt, query_string);
            // Let PostgreSQL create the foreign table first (so catalog entries exist).
            call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
            // Then propagate to KalamDB.
            handle_create_foreign_table(
                utility_stmt as *mut pg_sys::CreateForeignTableStmt,
                &statement_sql,
            );
        }
        pg_sys::NodeTag::T_AlterTableStmt => {
            let alter_stmt = utility_stmt as *mut pg_sys::AlterTableStmt;
            if (*alter_stmt).objtype == pg_sys::ObjectType::OBJECT_FOREIGN_TABLE {
                let statement_sql = extract_statement_sql(pstmt, query_string);
                let mirrored_clause = extract_alter_operation_clause(&statement_sql);
                // Let PostgreSQL alter the foreign table first.
                call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
                handle_alter_foreign_table(alter_stmt, mirrored_clause);
            } else {
                call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
            }
        }
        pg_sys::NodeTag::T_DropStmt => {
            let drop_stmt = utility_stmt as *mut pg_sys::DropStmt;
            if (*drop_stmt).removeType == pg_sys::ObjectType::OBJECT_FOREIGN_TABLE {
                // Read foreign table info BEFORE Postgres drops it.
                let drop_targets = collect_drop_targets(drop_stmt);
                call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
                handle_drop_foreign_tables(&drop_targets);
            } else {
                call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
            }
        }
        _ => {
            call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        }
    }
}

/// Call the previous ProcessUtility hook or `standard_ProcessUtility`.
fn call_prev(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    unsafe {
        if let Some(prev) = PREV_PROCESS_UTILITY {
            prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        } else {
            pg_sys::standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// CREATE FOREIGN TABLE → CREATE NAMESPACE IF NOT EXISTS + CREATE TABLE
// ---------------------------------------------------------------------------

unsafe fn handle_create_foreign_table(
    stmt: *mut pg_sys::CreateForeignTableStmt,
    statement_sql: &str,
) {
    // Only handle tables belonging to our FDW (check server → fdw name)
    let server_name = read_cstr((*stmt).servername);
    if !is_kalam_server(&server_name) {
        return;
    }

    let ft_options = parse_options((*stmt).options);
    let table_type = ft_options
        .get("table_type")
        .and_then(|value| TableType::from_str(value).ok())
        .unwrap_or(TableType::Shared);

    let base = &(*stmt).base;
    let rv = base.relation;
    let Some((namespace, table_name)) = resolve_relation_identity_from_range_var(rv) else {
        pgrx::warning!("pg_kalam DDL: cannot determine mirrored schema/table name");
        return;
    };

    let table_type_keyword = table_type_to_keyword(table_type);

    let column_defs = match extract_remote_column_definitions(statement_sql) {
        Ok(definitions) if !definitions.is_empty() => definitions,
        Ok(_) => {
            pgrx::warning!("pg_kalam DDL: no user columns found in CREATE FOREIGN TABLE");
            return;
        }
        Err(error) => {
            pgrx::error!("pg_kalam DDL: failed to parse CREATE FOREIGN TABLE: {}", error);
        }
    };

    let column_defs = infer_primary_key_column(column_defs, table_type);

    if column_defs.is_empty() {
        pgrx::warning!("pg_kalam DDL: no columns found in CREATE FOREIGN TABLE");
        return;
    }

    // Build SQL statements
    let create_ns_sql = format!(
        "CREATE NAMESPACE IF NOT EXISTS {}",
        quote_ident(&namespace)
    );
    let create_table_sql = format!(
        "CREATE {} TABLE IF NOT EXISTS {}.{} ({})",
        table_type_keyword,
        quote_ident(&namespace),
        quote_ident(&table_name),
        column_defs.join(", ")
    );

    // Send to KalamDB
    if let Err(e) = execute_remote_sql(&create_ns_sql, &server_name) {
        pgrx::warning!("pg_kalam DDL: failed to create namespace '{}': {}", namespace, e);
        // Don't error out — the table might already have a namespace, continue to CREATE TABLE.
    }

    if let Err(e) = execute_remote_sql(&create_table_sql, &server_name) {
        pgrx::error!("pg_kalam DDL: failed to create KalamDB table {}.{}: {}", namespace, table_name, e);
    }
}

// ---------------------------------------------------------------------------
// ALTER FOREIGN TABLE → ALTER TABLE ADD/DROP COLUMN
// ---------------------------------------------------------------------------

unsafe fn handle_alter_foreign_table(
    stmt: *mut pg_sys::AlterTableStmt,
    mirrored_clause: Result<String, KalamPgError>,
) {
    // Resolve the table OID from the RangeVar
    let rel = (*stmt).relation;
    if rel.is_null() {
        return;
    }

    let relid = pg_sys::RangeVarGetRelidExtended(
        rel,
        pg_sys::AccessShareLock as i32,
        pg_sys::RVROption::RVR_MISSING_OK as u32,
        None,
        std::ptr::null_mut(),
    );
    if relid == pg_sys::InvalidOid {
        return;
    }

    // Get the foreign table to check it's ours
    let ft = pg_sys::GetForeignTable(relid);
    if ft.is_null() {
        return;
    }

    let server = pg_sys::GetForeignServer((*ft).serverid);
    if server.is_null() {
        return;
    }

    let fdw = pg_sys::GetForeignDataWrapper((*server).fdwid);
    if fdw.is_null() || !is_kalam_fdw_name(fdw) {
        return;
    }

    let server_name = read_cstr((*server).servername);

    let Some((namespace, table_name)) = resolve_relation_identity(relid) else {
        return;
    };

    let cmds = (*stmt).cmds;
    if cmds.is_null() {
        return;
    }
    if (*cmds).length != 1 {
        pgrx::warning!("pg_kalam DDL: only single ALTER FOREIGN TABLE operations are mirrored");
        return;
    }

    let cell = (*cmds).elements.add(0);
    let cmd = (*cell).ptr_value as *mut pg_sys::AlterTableCmd;
    if cmd.is_null() {
        return;
    }

    match (*cmd).subtype {
        pg_sys::AlterTableType::AT_AddColumn
        | pg_sys::AlterTableType::AT_DropColumn
        | pg_sys::AlterTableType::AT_SetNotNull
        | pg_sys::AlterTableType::AT_DropNotNull
        | pg_sys::AlterTableType::AT_ColumnDefault => {}
        _ => return,
    }

    let clause = match mirrored_clause {
        Ok(clause) => clause,
        Err(error) => {
            pgrx::warning!(
                "pg_kalam DDL: failed to reconstruct ALTER FOREIGN TABLE clause: {}",
                error
            );
            return;
        }
    };

    let sql = format!(
        "ALTER TABLE {}.{} {}",
        quote_ident(&namespace),
        quote_ident(&table_name),
        clause
    );
    if let Err(error) = execute_remote_sql(&sql, &server_name) {
        pgrx::error!(
            "pg_kalam DDL: failed to mirror ALTER TABLE for {}.{}: {}",
            namespace,
            table_name,
            error
        );
    }
}

// ---------------------------------------------------------------------------
// DROP FOREIGN TABLE → DROP TABLE IF EXISTS
// ---------------------------------------------------------------------------

/// Info collected before PostgreSQL removes the foreign table catalog entry.
struct DropTarget {
    namespace: String,
    table_name: String,
    table_type: TableType,
    server_name: String,
}

/// Collect KalamDB table identifiers for all foreign tables being dropped,
/// BEFORE PostgreSQL removes them from the catalog.
unsafe fn collect_drop_targets(drop_stmt: *mut pg_sys::DropStmt) -> Vec<DropTarget> {
    let mut targets = Vec::new();
    let objects = (*drop_stmt).objects;
    if objects.is_null() {
        return targets;
    }

    let len = (*objects).length as usize;
    for i in 0..len {
        let cell = (*objects).elements.add(i);
        // For OBJECT_FOREIGN_TABLE, each object is a List of name parts
        let name_list = (*cell).ptr_value as *mut pg_sys::List;
        if name_list.is_null() {
            continue;
        }

        // Resolve the relation OID from the name list
        let rv = pg_sys::makeRangeVarFromNameList(name_list);
        if rv.is_null() {
            continue;
        }

        let flags = if (*drop_stmt).missing_ok { pg_sys::RVROption::RVR_MISSING_OK as u32 } else { 0 };
        let relid = pg_sys::RangeVarGetRelidExtended(
            rv,
            pg_sys::AccessShareLock as i32,
            flags,
            None,
            std::ptr::null_mut(),
        );
        if relid == pg_sys::InvalidOid {
            continue;
        }

        let ft = pg_sys::GetForeignTable(relid);
        if ft.is_null() {
            continue;
        }

        let server = pg_sys::GetForeignServer((*ft).serverid);
        if server.is_null() {
            continue;
        }

        let fdw = pg_sys::GetForeignDataWrapper((*server).fdwid);
        if fdw.is_null() || !is_kalam_fdw_name(fdw) {
            continue;
        }

        let ft_options = parse_options((*ft).options);
        let Some((namespace, table_name)) = resolve_relation_identity(relid) else {
            continue;
        };
        let table_type = ft_options
            .get("table_type")
            .and_then(|value| TableType::from_str(value).ok())
            .unwrap_or(TableType::Shared);

        let srv_name = read_cstr((*server).servername);
        targets.push(DropTarget {
            namespace,
            table_name,
            table_type,
            server_name: srv_name,
        });
    }
    targets
}

unsafe fn handle_drop_foreign_tables(targets: &[DropTarget]) {
    for target in targets {
        let table_type_keyword = table_type_to_keyword(target.table_type);
        let sql = format!(
            "DROP {} TABLE IF EXISTS {}.{}",
            table_type_keyword,
            quote_ident(&target.namespace),
            quote_ident(&target.table_name)
        );

        if let Err(e) = execute_remote_sql(&sql, &target.server_name) {
            pgrx::warning!("pg_kalam DDL: failed to drop KalamDB table {}.{}: {}",
                target.namespace, target.table_name, e);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a server name maps to a foreign server that uses our FDW.
fn is_kalam_server(server_name: &str) -> bool {
    if server_name.is_empty() {
        return false;
    }
    let c_name = std::ffi::CString::new(server_name).unwrap_or_default();
    let server = unsafe { pg_sys::GetForeignServerByName(c_name.as_ptr(), true) };
    if server.is_null() {
        return false;
    }
    let fdw = unsafe { pg_sys::GetForeignDataWrapper((*server).fdwid) };
    if fdw.is_null() {
        return false;
    }
    is_kalam_fdw_name(fdw)
}

/// Check if a ForeignDataWrapper is our `pg_kalam` FDW.
fn is_kalam_fdw_name(fdw: *mut pg_sys::ForeignDataWrapper) -> bool {
    if fdw.is_null() || unsafe { (*fdw).fdwname.is_null() } {
        return false;
    }
    let fdw_name = unsafe { CStr::from_ptr((*fdw).fdwname) }.to_string_lossy();
    fdw_name == "pg_kalam"
}

/// Read a C string pointer into a Rust String. Returns empty string for null.
fn read_cstr(ptr: *const std::ffi::c_char) -> String {
    if ptr.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned()
    }
}

fn extract_statement_sql(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
) -> String {
    let full_sql = read_cstr(query_string);
    if pstmt.is_null() || full_sql.is_empty() {
        return full_sql;
    }

    let start = unsafe { (*pstmt).stmt_location };
    let len = unsafe { (*pstmt).stmt_len };
    if start < 0 {
        return full_sql;
    }

    let start = start as usize;
    if start >= full_sql.len() {
        return full_sql;
    }

    if len > 0 {
        let end = start.saturating_add(len as usize).min(full_sql.len());
        return full_sql[start..end].trim().to_string();
    }

    full_sql[start..].trim().to_string()
}

fn extract_remote_column_definitions(statement_sql: &str) -> Result<Vec<String>, KalamPgError> {
    let (open_idx, close_idx) = find_column_list_bounds(statement_sql).ok_or_else(|| {
        KalamPgError::Validation(
            "could not locate CREATE FOREIGN TABLE column definitions".to_string(),
        )
    })?;

    let block = &statement_sql[open_idx + 1..close_idx];
    Ok(split_top_level_sql_list(block)
        .into_iter()
        .filter(|entry| !entry.trim().is_empty())
        .filter(|entry| !is_internal_column_entry(entry))
        .collect())
}

fn infer_primary_key_column(mut column_defs: Vec<String>, table_type: TableType) -> Vec<String> {
    if !matches!(table_type, TableType::User | TableType::Shared) {
        return column_defs;
    }

    let has_primary_key = column_defs.iter().any(|entry| {
        entry
            .split_whitespace()
            .collect::<Vec<_>>()
            .windows(2)
            .any(|window| {
                window[0].eq_ignore_ascii_case("PRIMARY")
                    && window[1].eq_ignore_ascii_case("KEY")
            })
    });
    if has_primary_key {
        return column_defs;
    }

    if let Some(index) = column_defs
        .iter()
        .position(|entry| first_sql_identifier(entry).as_deref() == Some("id"))
    {
        column_defs[index].push_str(" PRIMARY KEY");
    }

    column_defs
}

fn extract_alter_operation_clause(statement_sql: &str) -> Result<String, KalamPgError> {
    let sql = statement_sql.trim().trim_end_matches(';').trim();
    let mut index = 0usize;
    consume_sql_keyword(sql, &mut index, "ALTER")?;
    let _ = consume_sql_keyword_optional(sql, &mut index, "FOREIGN");
    consume_sql_keyword(sql, &mut index, "TABLE")?;
    consume_qualified_identifier(sql, &mut index)?;

    let clause = sql[index..].trim();
    if clause.is_empty() {
        Err(KalamPgError::Validation(
            "could not locate ALTER TABLE operation clause".to_string(),
        ))
    } else {
        Ok(clause.to_string())
    }
}

fn find_column_list_bounds(sql: &str) -> Option<(usize, usize)> {
    let mut start_idx = None;
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = sql.as_bytes();
    let mut idx = 0usize;

    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        match ch {
            '\'' if !in_double_quote => {
                if in_single_quote && bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 1;
                } else {
                    in_single_quote = !in_single_quote;
                }
            }
            '"' if !in_single_quote => {
                if in_double_quote && bytes.get(idx + 1) == Some(&b'"') {
                    idx += 1;
                } else {
                    in_double_quote = !in_double_quote;
                }
            }
            '(' if !in_single_quote && !in_double_quote => {
                if start_idx.is_none() {
                    start_idx = Some(idx);
                }
                depth += 1;
            }
            ')' if !in_single_quote && !in_double_quote => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return start_idx.map(|start| (start, idx));
                }
            }
            _ => {}
        }
        idx += 1;
    }

    None
}

fn split_top_level_sql_list(input: &str) -> Vec<String> {
    let mut entries = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = input.as_bytes();
    let mut idx = 0usize;

    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        match ch {
            '\'' if !in_double_quote => {
                if in_single_quote && bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 1;
                } else {
                    in_single_quote = !in_single_quote;
                }
            }
            '"' if !in_single_quote => {
                if in_double_quote && bytes.get(idx + 1) == Some(&b'"') {
                    idx += 1;
                } else {
                    in_double_quote = !in_double_quote;
                }
            }
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote && depth > 0 => depth -= 1,
            ',' if !in_single_quote && !in_double_quote && depth == 0 => {
                let entry = input[start..idx].trim();
                if !entry.is_empty() {
                    entries.push(entry.to_string());
                }
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        entries.push(tail.to_string());
    }

    entries
}

fn is_internal_column_entry(entry: &str) -> bool {
    first_sql_identifier(entry)
        .map(|identifier| matches!(identifier.as_str(), "_userid" | "_seq" | "_deleted"))
        .unwrap_or(false)
}

fn first_sql_identifier(entry: &str) -> Option<String> {
    let trimmed = entry.trim_start();
    if trimmed.is_empty() {
        return None;
    }

    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("PRIMARY ")
        || upper.starts_with("CONSTRAINT ")
        || upper.starts_with("UNIQUE ")
        || upper.starts_with("CHECK ")
        || upper.starts_with("FOREIGN ")
    {
        return None;
    }

    if let Some(rest) = trimmed.strip_prefix('"') {
        let end = rest.find('"')?;
        return Some(rest[..end].to_ascii_lowercase());
    }

    let end = trimmed
        .char_indices()
        .find_map(|(idx, ch)| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                None
            } else {
                Some(idx)
            }
        })
        .unwrap_or(trimmed.len());
    let identifier = trimmed[..end].trim();
    if identifier.is_empty() {
        None
    } else {
        Some(identifier.to_ascii_lowercase())
    }
}

fn consume_sql_keyword(sql: &str, index: &mut usize, keyword: &str) -> Result<(), KalamPgError> {
    skip_sql_whitespace(sql, index);
    let remaining = &sql[*index..];
    if remaining.len() < keyword.len() {
        return Err(KalamPgError::Validation(format!(
            "expected '{}' in SQL clause",
            keyword
        )));
    }
    let candidate = &remaining[..keyword.len()];
    if !candidate.eq_ignore_ascii_case(keyword) {
        return Err(KalamPgError::Validation(format!(
            "expected '{}' in SQL clause",
            keyword
        )));
    }
    *index += keyword.len();
    Ok(())
}

fn consume_sql_keyword_optional(sql: &str, index: &mut usize, keyword: &str) -> bool {
    let original = *index;
    if consume_sql_keyword(sql, index, keyword).is_ok() {
        true
    } else {
        *index = original;
        false
    }
}

fn consume_qualified_identifier(sql: &str, index: &mut usize) -> Result<(), KalamPgError> {
    skip_sql_whitespace(sql, index);
    consume_identifier_part(sql, index)?;
    loop {
        skip_sql_whitespace(sql, index);
        if sql[*index..].starts_with('.') {
            *index += 1;
            consume_identifier_part(sql, index)?;
        } else {
            return Ok(());
        }
    }
}

fn consume_identifier_part(sql: &str, index: &mut usize) -> Result<(), KalamPgError> {
    skip_sql_whitespace(sql, index);
    let remaining = &sql[*index..];
    if remaining.is_empty() {
        return Err(KalamPgError::Validation(
            "expected identifier in SQL clause".to_string(),
        ));
    }

    if let Some(rest) = remaining.strip_prefix('"') {
        let end = rest.find('"').ok_or_else(|| {
            KalamPgError::Validation("unterminated quoted identifier in SQL clause".to_string())
        })?;
        *index += end + 2;
        return Ok(());
    }

    let ident_len = remaining
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .count();
    if ident_len == 0 {
        return Err(KalamPgError::Validation(
            "expected identifier in SQL clause".to_string(),
        ));
    }
    *index += ident_len;
    Ok(())
}

fn skip_sql_whitespace(sql: &str, index: &mut usize) {
    *index += sql[*index..]
        .chars()
        .take_while(|ch| ch.is_whitespace())
        .map(char::len_utf8)
        .sum::<usize>();
}

unsafe fn resolve_relation_identity_from_range_var(
    rv: *mut pg_sys::RangeVar,
) -> Option<(String, String)> {
    if rv.is_null() {
        return None;
    }

    let relid = pg_sys::RangeVarGetRelidExtended(
        rv,
        pg_sys::AccessShareLock as i32,
        pg_sys::RVROption::RVR_MISSING_OK as u32,
        None,
        std::ptr::null_mut(),
    );
    if relid == pg_sys::InvalidOid {
        return None;
    }

    resolve_relation_identity(relid)
}

unsafe fn resolve_relation_identity(relid: pg_sys::Oid) -> Option<(String, String)> {
    if relid == pg_sys::InvalidOid {
        return None;
    }

    let namespace_oid = pg_sys::get_rel_namespace(relid);
    if namespace_oid == pg_sys::InvalidOid {
        return None;
    }

    let namespace = read_cstr(pg_sys::get_namespace_name(namespace_oid));
    let table_name = read_cstr(pg_sys::get_rel_name(relid));
    if namespace.is_empty() || table_name.is_empty() {
        None
    } else {
        Some((namespace, table_name))
    }
}

/// Convert a KalamDB TableType to the SQL keyword.
fn table_type_to_keyword(tt: TableType) -> &'static str {
    match tt {
        TableType::User => "USER",
        TableType::Shared => "SHARED",
        TableType::Stream => "STREAM",
        TableType::System => "SHARED", // shouldn't happen
    }
}

/// Format a SQL identifier for KalamDB.
///
/// KalamDB only accepts simple identifiers (alphanumeric + underscore).
/// We pass them through directly without quoting; double-quoting would
/// cause validation errors.
fn quote_ident(name: &str) -> String {
    // KalamDB rejects double-quoted identifiers in CREATE/ALTER/DROP.
    // Only simple alphanumeric+underscore names are valid, so pass as-is.
    name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_remote_column_definitions_preserves_constraints_and_defaults() {
        let sql = r#"
            CREATE FOREIGN TABLE app.shared_tbl_2 (
                id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
                title TEXT NOT NULL,
                value INTEGER,
                created TIMESTAMP DEFAULT NOW()
            ) SERVER kalam_server
            OPTIONS (table_type 'shared');
        "#;

        let defs = extract_remote_column_definitions(sql).expect("extract definitions");
        assert_eq!(defs.len(), 4);
        assert_eq!(defs[0], "id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID()");
        assert_eq!(defs[1], "title TEXT NOT NULL");
        assert_eq!(defs[3], "created TIMESTAMP DEFAULT NOW()");
    }

    #[test]
    fn extract_remote_column_definitions_skips_internal_columns_only() {
        let sql = r#"
            CREATE FOREIGN TABLE e2e.users (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                _userid TEXT,
                _seq BIGINT,
                _deleted BOOLEAN,
                CONSTRAINT users_name_check CHECK (char_length(name) > 0)
            ) SERVER kalam_server
            OPTIONS (table_type 'user');
        "#;

        let defs = extract_remote_column_definitions(sql).expect("extract definitions");
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0], "id TEXT PRIMARY KEY");
        assert_eq!(defs[1], "name TEXT NOT NULL");
        assert!(defs[2].starts_with("CONSTRAINT users_name_check CHECK"));
    }

    #[test]
    fn split_top_level_sql_list_handles_nested_parentheses() {
        let entries = split_top_level_sql_list(
            "id BIGINT DEFAULT SNOWFLAKE_ID(), amount NUMERIC(10, 2), created TIMESTAMP DEFAULT NOW()",
        );
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1], "amount NUMERIC(10, 2)");
    }

    #[test]
    fn extract_alter_operation_clause_preserves_default_and_not_null() {
        let clause = extract_alter_operation_clause(
            "ALTER FOREIGN TABLE app.items ALTER COLUMN title SET DEFAULT 'pending';",
        )
        .expect("extract alter clause");
        assert_eq!(clause, "ALTER COLUMN title SET DEFAULT 'pending'");

        let clause = extract_alter_operation_clause(
            "ALTER FOREIGN TABLE app.items ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';",
        )
        .expect("extract add-column clause");
        assert_eq!(clause, "ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'");
    }
}

/// Execute a SQL statement on the remote KalamDB backend.
///
/// Bootstraps the remote connection from the given server name if not already initialized.
fn execute_remote_sql(sql: &str, server_name: &str) -> Result<String, KalamPgError> {
    let state = match get_remote_extension_state() {
        Some(s) => s,
        None => {
            // Bootstrap connection from the foreign server's options.
            let c_name = std::ffi::CString::new(server_name).unwrap_or_default();
            let server = unsafe { pg_sys::GetForeignServerByName(c_name.as_ptr(), true) };
            if server.is_null() {
                return Err(KalamPgError::Execution(format!(
                    "foreign server '{}' not found",
                    server_name
                )));
            }
            let server_options = parse_options(unsafe { (*server).options });
            let parsed_server = ServerOptions::parse(&server_options)?;
            let remote_config = parsed_server.remote.ok_or_else(|| {
                KalamPgError::Validation(
                    "foreign server must have host and port options".to_string(),
                )
            })?;
            crate::remote_state::ensure_remote_extension_state(remote_config)
                .map_err(|e| KalamPgError::Execution(e.to_string()))?
        }
    };

    state.runtime().block_on(async {
        state.client().execute_sql(sql, state.session_id()).await
    })
}
