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
use kalam_pg_fdw::TableOptions;
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
            // Let PostgreSQL create the foreign table first (so catalog entries exist).
            call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
            // Then propagate to KalamDB.
            handle_create_foreign_table(utility_stmt as *mut pg_sys::CreateForeignTableStmt);
        }
        pg_sys::NodeTag::T_AlterTableStmt => {
            let alter_stmt = utility_stmt as *mut pg_sys::AlterTableStmt;
            if (*alter_stmt).objtype == pg_sys::ObjectType::OBJECT_FOREIGN_TABLE {
                // Let PostgreSQL alter the foreign table first.
                call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
                handle_alter_foreign_table(alter_stmt);
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
unsafe fn call_prev(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    if let Some(prev) = PREV_PROCESS_UTILITY {
        prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    } else {
        pg_sys::standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    }
}

// ---------------------------------------------------------------------------
// CREATE FOREIGN TABLE → CREATE NAMESPACE IF NOT EXISTS + CREATE TABLE
// ---------------------------------------------------------------------------

unsafe fn handle_create_foreign_table(stmt: *mut pg_sys::CreateForeignTableStmt) {
    // Only handle tables belonging to our FDW (check server → fdw name)
    let server_name = read_cstr((*stmt).servername);
    if !is_kalam_server(&server_name) {
        return;
    }

    // Try to parse explicit FDW OPTIONS (namespace, table, table_type).
    // If OPTIONS are missing, fall back to deriving from the schema-qualified name.
    let ft_options = parse_options((*stmt).options);
    let (namespace, table_name, table_type) = match TableOptions::parse(&ft_options) {
        Ok(opts) => (
            opts.table_id.namespace_id().as_str().to_string(),
            opts.table_id.table_name().as_str().to_string(),
            opts.table_type,
        ),
        Err(_) => {
            // Derive from schema.table name in the CREATE statement
            let base = &(*stmt).base;
            let rv = base.relation;
            if rv.is_null() {
                pgrx::warning!("pg_kalam DDL: cannot determine table name");
                return;
            }
            let schema = read_cstr((*rv).schemaname);
            let relname = read_cstr((*rv).relname);
            if schema.is_empty() || relname.is_empty() {
                pgrx::warning!(
                    "pg_kalam DDL: schema-qualified name required (e.g. CREATE FOREIGN TABLE myns.mytable ...) \
                     or provide OPTIONS (namespace '...', \"table\" '...', table_type '...')"
                );
                return;
            }
            // Default to SHARED when no explicit table_type option
            let tt = ft_options
                .get("table_type")
                .and_then(|s| TableType::from_str(s).ok())
                .unwrap_or(TableType::Shared);
            (schema, relname, tt)
        }
    };

    let table_type_keyword = table_type_to_keyword(table_type);

    // Extract columns from the CreateStmt base
    let base = &(*stmt).base;
    let columns = extract_columns_from_table_elts(base.tableElts);
    if columns.is_empty() {
        pgrx::warning!("pg_kalam DDL: no columns found in CREATE FOREIGN TABLE");
        return;
    }

    // Build the column definitions for KalamDB SQL
    let col_defs: Vec<String> = columns
        .iter()
        .map(|c| {
            if c.is_primary_key {
                format!("{} {} PRIMARY KEY", quote_ident(&c.name), c.kalam_type)
            } else {
                format!("{} {}", quote_ident(&c.name), c.kalam_type)
            }
        })
        .collect();

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
        col_defs.join(", ")
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

unsafe fn handle_alter_foreign_table(stmt: *mut pg_sys::AlterTableStmt) {
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

    // Parse table options; fall back to schema.relname from the ALTER statement
    let ft_options = parse_options((*ft).options);
    let (namespace, table_name) = match TableOptions::parse(&ft_options) {
        Ok(opts) => (
            opts.table_id.namespace_id().as_str().to_string(),
            opts.table_id.table_name().as_str().to_string(),
        ),
        Err(_) => {
            // Fall back to schema/relname from the RangeVar
            let schema = read_cstr((*rel).schemaname);
            let relname = read_cstr((*rel).relname);
            if schema.is_empty() || relname.is_empty() {
                return;
            }
            (schema, relname)
        }
    };

    // Iterate over ALTER commands
    let cmds = (*stmt).cmds;
    if cmds.is_null() {
        return;
    }
    let len = (*cmds).length as usize;
    for i in 0..len {
        let cell = (*cmds).elements.add(i);
        let cmd = (*cell).ptr_value as *mut pg_sys::AlterTableCmd;
        if cmd.is_null() {
            continue;
        }

        match (*cmd).subtype {
            pg_sys::AlterTableType::AT_AddColumn => {
                // cmd.def is a ColumnDef
                let col_def = (*cmd).def as *mut pg_sys::ColumnDef;
                if col_def.is_null() {
                    continue;
                }
                let col_name = read_cstr((*col_def).colname);
                let kalam_type = resolve_type_name((*col_def).typeName);

                let sql = format!(
                    "ALTER TABLE {}.{} ADD COLUMN {} {}",
                    quote_ident(&namespace),
                    quote_ident(&table_name),
                    quote_ident(&col_name),
                    kalam_type
                );

                if let Err(e) = execute_remote_sql(&sql, &server_name) {
                    pgrx::error!("pg_kalam DDL: failed to add column '{}': {}", col_name, e);
                }
            }
            pg_sys::AlterTableType::AT_DropColumn => {
                let col_name = read_cstr((*cmd).name);

                let sql = format!(
                    "ALTER TABLE {}.{} DROP COLUMN {}",
                    quote_ident(&namespace),
                    quote_ident(&table_name),
                    quote_ident(&col_name)
                );

                if let Err(e) = execute_remote_sql(&sql, &server_name) {
                    pgrx::error!("pg_kalam DDL: failed to drop column '{}': {}", col_name, e);
                }
            }
            _ => {
                // Other ALTER operations are not propagated to KalamDB.
            }
        }
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
        let (namespace, table_name, table_type) = match TableOptions::parse(&ft_options) {
            Ok(opts) => (
                opts.table_id.namespace_id().as_str().to_string(),
                opts.table_id.table_name().as_str().to_string(),
                opts.table_type,
            ),
            Err(_) => {
                // Fall back to schema/relname from the RangeVar
                let schema = read_cstr((*rv).schemaname);
                let relname = read_cstr((*rv).relname);
                if schema.is_empty() || relname.is_empty() {
                    continue;
                }
                let tt = ft_options
                    .get("table_type")
                    .and_then(|s| TableType::from_str(s).ok())
                    .unwrap_or(TableType::Shared);
                (schema, relname, tt)
            }
        };

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

/// Column info extracted from PostgreSQL DDL.
struct ColumnInfo {
    name: String,
    kalam_type: String,
    is_primary_key: bool,
}

/// Extract column definitions from the `tableElts` list of a CreateStmt.
///
/// The first non-internal column is assumed to be the primary key,
/// matching the convention used by the FDW modify path.
unsafe fn extract_columns_from_table_elts(table_elts: *mut pg_sys::List) -> Vec<ColumnInfo> {
    let mut columns = Vec::new();
    if table_elts.is_null() {
        return columns;
    }

    let mut found_pk = false;
    let len = (*table_elts).length as usize;
    for i in 0..len {
        let cell = (*table_elts).elements.add(i);
        let node = (*cell).ptr_value as *mut pg_sys::Node;
        if node.is_null() {
            continue;
        }

        if (*node).type_ == pg_sys::NodeTag::T_ColumnDef {
            let col = node as *mut pg_sys::ColumnDef;
            let name = read_cstr((*col).colname);

            // Skip internal columns that KalamDB adds automatically
            if name == "_userid" || name == "_seq" || name == "_deleted" {
                continue;
            }

            let kalam_type = resolve_type_name((*col).typeName);

            // First non-internal column is the primary key (matches FDW modify convention)
            let is_primary_key = !found_pk;
            if is_primary_key {
                found_pk = true;
            }

            columns.push(ColumnInfo {
                name,
                kalam_type,
                is_primary_key,
            });
        }
    }
    columns
}

/// Resolve a PostgreSQL TypeName to a KalamDB SQL type string.
unsafe fn resolve_type_name(type_name: *mut pg_sys::TypeName) -> String {
    if type_name.is_null() {
        return "TEXT".to_string();
    }

    // Use PostgreSQL's type resolution to get the OID
    let mut type_oid: pg_sys::Oid = pg_sys::InvalidOid;
    let mut type_mod: i32 = -1;
    pg_sys::typenameTypeIdAndMod(std::ptr::null_mut(), type_name, &mut type_oid, &mut type_mod);

    pg_oid_to_kalam_type(type_oid, type_mod)
}

/// Map a PostgreSQL type OID to a KalamDB SQL type name.
fn pg_oid_to_kalam_type(type_oid: pg_sys::Oid, type_mod: i32) -> String {
    match type_oid {
        pg_sys::BOOLOID => "BOOLEAN".to_string(),
        pg_sys::INT2OID => "SMALLINT".to_string(),
        pg_sys::INT4OID => "INTEGER".to_string(),
        pg_sys::INT8OID => "BIGINT".to_string(),
        pg_sys::FLOAT4OID => "REAL".to_string(),
        pg_sys::FLOAT8OID => "DOUBLE PRECISION".to_string(),
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => "TEXT".to_string(),
        pg_sys::BYTEAOID => "BYTEA".to_string(),
        pg_sys::DATEOID => "DATE".to_string(),
        pg_sys::TIMEOID => "TIME".to_string(),
        pg_sys::TIMESTAMPOID => "TIMESTAMP".to_string(),
        pg_sys::TIMESTAMPTZOID => "TIMESTAMPTZ".to_string(),
        pg_sys::UUIDOID => "UUID".to_string(),
        pg_sys::JSONBOID | pg_sys::JSONOID => "JSONB".to_string(),
        pg_sys::NUMERICOID => {
            // Extract precision/scale from typemod
            if type_mod >= 0 {
                let precision = ((type_mod - 4) >> 16) & 0xFFFF;
                let scale = (type_mod - 4) & 0xFFFF;
                format!("NUMERIC({}, {})", precision, scale)
            } else {
                "NUMERIC(38, 10)".to_string()
            }
        }
        _ => {
            // Fallback: use PostgreSQL's format_type_be for the name
            let cstr = unsafe { pg_sys::format_type_be(type_oid) };
            if !cstr.is_null() {
                let name = unsafe { CStr::from_ptr(cstr) }.to_string_lossy().into_owned();
                unsafe { pg_sys::pfree(cstr as *mut std::ffi::c_void) };
                name.to_uppercase()
            } else {
                "TEXT".to_string()
            }
        }
    }
}

/// Check if a server name maps to a foreign server that uses our FDW.
unsafe fn is_kalam_server(server_name: &str) -> bool {
    if server_name.is_empty() {
        return false;
    }
    let c_name = std::ffi::CString::new(server_name).unwrap_or_default();
    let server = pg_sys::GetForeignServerByName(c_name.as_ptr(), true);
    if server.is_null() {
        return false;
    }
    let fdw = pg_sys::GetForeignDataWrapper((*server).fdwid);
    if fdw.is_null() {
        return false;
    }
    is_kalam_fdw_name(fdw)
}

/// Check if a ForeignDataWrapper is our `pg_kalam` FDW.
unsafe fn is_kalam_fdw_name(fdw: *mut pg_sys::ForeignDataWrapper) -> bool {
    if fdw.is_null() || (*fdw).fdwname.is_null() {
        return false;
    }
    let fdw_name = CStr::from_ptr((*fdw).fdwname).to_string_lossy();
    fdw_name == "pg_kalam"
}

/// Read a C string pointer into a Rust String. Returns empty string for null.
unsafe fn read_cstr(ptr: *const std::ffi::c_char) -> String {
    if ptr.is_null() {
        String::new()
    } else {
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
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

/// Execute a SQL statement on the remote KalamDB backend.
///
/// Bootstraps the remote connection from the given server name if not already initialized.
unsafe fn execute_remote_sql(sql: &str, server_name: &str) -> Result<String, KalamPgError> {
    let state = match get_remote_extension_state() {
        Some(s) => s,
        None => {
            // Bootstrap connection from the foreign server's options.
            let c_name = std::ffi::CString::new(server_name).unwrap_or_default();
            let server = pg_sys::GetForeignServerByName(c_name.as_ptr(), true);
            if server.is_null() {
                return Err(KalamPgError::Execution(format!(
                    "foreign server '{}' not found",
                    server_name
                )));
            }
            let server_options = parse_options((*server).options);
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
