//! ProcessUtility hook that propagates DDL on KalamDB-backed PostgreSQL tables.
//!
//! Intercepts:
//! - `CREATE TABLE ... USING kalamdb` → remote `CREATE <type> TABLE` + internal `CREATE FOREIGN TABLE`
//! - `CREATE FOREIGN TABLE`           → `CREATE NAMESPACE IF NOT EXISTS` + `CREATE <type> TABLE`
//!   (auto-injects `_seq BIGINT` and `_userid TEXT` system columns into
//!   the local PG schema; rejects explicit declarations of these columns)
//! - `ALTER FOREIGN TABLE`            → `ALTER TABLE ADD/DROP COLUMN`
//! - `DROP FOREIGN TABLE`             → `DROP <type> TABLE IF EXISTS`

use crate::fdw_options::parse_options;
use kalam_pg_common::{KalamPgError, SEQ_COLUMN, USER_ID_COLUMN};
use kalam_pg_fdw::ServerOptions;
use kalamdb_commons::TableType;
use pgrx::pg_sys;
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::str::FromStr;

const DEFAULT_KALAM_SERVER: &str = "kalam_server";

// Thread-local flag: when true, suppress KalamDB propagation in DDL hooks
// because the extension itself is issuing SPI statements (e.g. injecting
// system columns via ALTER FOREIGN TABLE).
std::thread_local! {
    static SKIP_DDL_PROPAGATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Previous ProcessUtility hook (may be null).
static mut PREV_PROCESS_UTILITY: pg_sys::ProcessUtility_hook_type = None;

/// Register the ProcessUtility hook. Must be called from `_PG_init`.
pub fn register_hook() {
    unsafe {
        PREV_PROCESS_UTILITY = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(kalam_process_utility);
    }
}

/// The ProcessUtility hook entry point.
///
/// # Safety
/// Called by PostgreSQL for every utility (DDL) statement.
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn kalam_process_utility(
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
        pg_sys::NodeTag::T_CreateStmt => {
            let create_stmt = utility_stmt as *mut pg_sys::CreateStmt;
            let access_method = read_cstr((*create_stmt).accessMethod);
            if access_method.eq_ignore_ascii_case("kalamdb") {
                let statement_sql = extract_statement_sql(pstmt, query_string);
                handle_create_table_using_kalamdb(create_stmt, &statement_sql);
            } else {
                call_prev(
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
        },
        pg_sys::NodeTag::T_CreateForeignTableStmt => {
            let ft_stmt = utility_stmt as *mut pg_sys::CreateForeignTableStmt;
            let statement_sql = extract_statement_sql(pstmt, query_string);
            let is_internal = SKIP_DDL_PROPAGATION.with(|flag| flag.get());

            // Reject explicit system column declarations before PG creates the table.
            if !is_internal {
                let server_name = read_cstr((*ft_stmt).servername);
                if is_kalam_server(&server_name) {
                    if let Err(e) = validate_no_system_columns(&statement_sql) {
                        report_sql_error(&format!("pg_kalam DDL: {}", e));
                    }
                }
            }

            // Let PostgreSQL create the foreign table first (so catalog entries exist).
            call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
            // Then propagate to KalamDB — unless this is an internal SPI call.
            if !is_internal {
                handle_create_foreign_table(ft_stmt, &statement_sql);
            }
        },
        pg_sys::NodeTag::T_AlterTableStmt => {
            let alter_stmt = utility_stmt as *mut pg_sys::AlterTableStmt;
            if (*alter_stmt).objtype == pg_sys::ObjectType::OBJECT_FOREIGN_TABLE {
                let is_internal = SKIP_DDL_PROPAGATION.with(|flag| flag.get());
                if is_internal {
                    // Internal SPI (e.g. system column injection) — just run PG side.
                    call_prev(
                        pstmt,
                        query_string,
                        read_only_tree,
                        context,
                        params,
                        query_env,
                        dest,
                        qc,
                    );
                } else {
                    let statement_sql = extract_statement_sql(pstmt, query_string);
                    let mirrored_clause = extract_alter_operation_clause(&statement_sql);
                    // Let PostgreSQL alter the foreign table first.
                    call_prev(
                        pstmt,
                        query_string,
                        read_only_tree,
                        context,
                        params,
                        query_env,
                        dest,
                        qc,
                    );
                    handle_alter_foreign_table(alter_stmt, mirrored_clause);
                }
            } else {
                call_prev(
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
        },
        pg_sys::NodeTag::T_DropStmt => {
            let drop_stmt = utility_stmt as *mut pg_sys::DropStmt;
            if (*drop_stmt).removeType == pg_sys::ObjectType::OBJECT_FOREIGN_TABLE {
                // Read foreign table info BEFORE Postgres drops it.
                let drop_targets = collect_drop_targets(drop_stmt);
                call_prev(
                    pstmt,
                    query_string,
                    read_only_tree,
                    context,
                    params,
                    query_env,
                    dest,
                    qc,
                );
                handle_drop_foreign_tables(&drop_targets);
            } else {
                call_prev(
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
        },
        pg_sys::NodeTag::T_TransactionStmt => {
            let tx_stmt = utility_stmt as *mut pg_sys::TransactionStmt;
            let tx_kind = (*tx_stmt).kind;

            match tx_kind {
                pg_sys::TransactionStmtKind::TRANS_STMT_COMMIT => {
                    if let Err(error) = crate::fdw_xact::commit_explicit_transaction_block() {
                        report_sql_error(&format!("pg_kalam commit: {}", error));
                    }
                },
                pg_sys::TransactionStmtKind::TRANS_STMT_ROLLBACK => {
                    if let Err(error) = crate::fdw_xact::rollback_explicit_transaction_block() {
                        report_sql_error(&format!("pg_kalam rollback: {}", error));
                    }
                },
                _ => {},
            }

            call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);

            match tx_kind {
                pg_sys::TransactionStmtKind::TRANS_STMT_BEGIN
                | pg_sys::TransactionStmtKind::TRANS_STMT_START => {
                    crate::fdw_xact::set_explicit_transaction_block(true);
                },
                pg_sys::TransactionStmtKind::TRANS_STMT_COMMIT
                | pg_sys::TransactionStmtKind::TRANS_STMT_ROLLBACK
                | pg_sys::TransactionStmtKind::TRANS_STMT_PREPARE
                | pg_sys::TransactionStmtKind::TRANS_STMT_COMMIT_PREPARED
                | pg_sys::TransactionStmtKind::TRANS_STMT_ROLLBACK_PREPARED => {
                    crate::fdw_xact::set_explicit_transaction_block(false);
                },
                _ => {},
            }
        },
        _ => {
            call_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        },
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

unsafe fn report_sql_error(message: &str) -> ! {
    use pgrx::pg_sys::elog::PgLogLevel;
    use pgrx::pg_sys::errcodes::PgSqlErrorCode;

    const PERCENT_S: &CStr = c"%s";
    const DOMAIN: *const std::os::raw::c_char = std::ptr::null_mut();

    unsafe extern "C-unwind" {
        fn errcode(sqlerrcode: std::os::raw::c_int) -> std::os::raw::c_int;
        fn errmsg(fmt: *const std::os::raw::c_char, ...) -> std::os::raw::c_int;
        fn errstart(elevel: std::os::raw::c_int, domain: *const std::os::raw::c_char) -> bool;
        fn errfinish(
            filename: *const std::os::raw::c_char,
            lineno: std::os::raw::c_int,
            funcname: *const std::os::raw::c_char,
        );
    }

    if errstart(PgLogLevel::ERROR as _, DOMAIN) {
        errcode(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR as _);
        let c_message = CString::new(message).unwrap_or_else(|_| {
            CString::new("pg_kalam transaction error").expect("static message")
        });
        errmsg(PERCENT_S.as_ptr(), c_message.as_ptr());
        errfinish(c"pg_kalam".as_ptr(), 0, c"report_sql_error".as_ptr());
    }

    std::hint::unreachable_unchecked()
}

// ---------------------------------------------------------------------------
// CREATE TABLE ... USING kalamdb → CREATE FOREIGN TABLE + propagate
// ---------------------------------------------------------------------------

unsafe fn handle_create_table_using_kalamdb(stmt: *mut pg_sys::CreateStmt, statement_sql: &str) {
    let rv = (*stmt).relation;
    if rv.is_null() {
        pgrx::error!("pg_kalam DDL: CREATE TABLE USING kalamdb requires a table name");
    }

    let table_name = read_cstr((*rv).relname);
    if table_name.is_empty() {
        pgrx::error!("pg_kalam DDL: could not determine table name");
    }

    let namespace = resolve_create_namespace(rv).unwrap_or_else(|| {
        pgrx::error!(
            "pg_kalam DDL: could not determine target schema for CREATE TABLE USING kalamdb"
        )
    });

    let if_not_exists = create_statement_has_if_not_exists(statement_sql).unwrap_or(false);
    let relid = pg_sys::RangeVarGetRelidExtended(
        rv,
        pg_sys::AccessShareLock as i32,
        pg_sys::RVROption::RVR_MISSING_OK as u32,
        None,
        std::ptr::null_mut(),
    );
    if relid != pg_sys::InvalidOid {
        if if_not_exists {
            return;
        }
        pgrx::error!("pg_kalam DDL: relation '{}.{}' already exists", namespace, table_name);
    }

    if let Err(error) = validate_no_system_columns(statement_sql) {
        pgrx::error!("pg_kalam DDL: {}", error);
    }

    let column_defs = match extract_remote_column_definitions(statement_sql) {
        Ok(definitions) if !definitions.is_empty() => definitions,
        Ok(_) => {
            pgrx::error!("pg_kalam DDL: no user columns found in CREATE TABLE USING kalamdb");
        },
        Err(error) => {
            pgrx::error!("pg_kalam DDL: failed to parse CREATE TABLE USING kalamdb: {}", error);
        },
    };

    let with_options = extract_with_options_from_sql(statement_sql);
    let table_type = with_options
        .get("type")
        .and_then(|value| TableType::from_str(value).ok())
        .unwrap_or(TableType::Shared);
    let table_type_keyword = table_type_to_keyword(table_type);

    let column_defs = transform_serial_types(column_defs);
    let column_defs = infer_primary_key_column(column_defs, table_type);
    if column_defs.is_empty() {
        pgrx::error!("pg_kalam DDL: no columns found in CREATE TABLE USING kalamdb");
    }

    let pg_column_defs = strip_for_foreign_table(&column_defs);

    let kalam_options = match with_options
        .iter()
        .map(|(key, value)| format_kalam_option_assignment(key, value))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(options) => options,
        Err(error) => {
            pgrx::error!("pg_kalam DDL: invalid KalamDB table option: {}", error);
        },
    };
    let kalam_with_clause = if kalam_options.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", kalam_options.join(", "))
    };
    let remote_if_not_exists = if if_not_exists { "IF NOT EXISTS " } else { "" };
    let create_ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", quote_ident(&namespace));
    let create_table_sql = format!(
        "CREATE {} TABLE {}{}.{} ({}){}",
        table_type_keyword,
        remote_if_not_exists,
        quote_ident(&namespace),
        quote_ident(&table_name),
        column_defs.join(", "),
        kalam_with_clause
    );

    if let Err(error) = execute_remote_sql(&create_ns_sql, DEFAULT_KALAM_SERVER) {
        pgrx::warning!("pg_kalam DDL: failed to create namespace '{}': {}", namespace, error);
    }
    if let Err(error) = execute_remote_sql(&create_table_sql, DEFAULT_KALAM_SERVER) {
        pgrx::error!(
            "pg_kalam DDL: failed to create KalamDB table {}.{}: {}",
            namespace,
            table_name,
            error
        );
    }

    if !namespace_exists(&namespace) {
        let create_schema_sql =
            format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident_pg(&namespace));
        if let Err(error) = pgrx::Spi::run(&create_schema_sql) {
            pgrx::error!("pg_kalam DDL: failed to ensure local schema '{}': {}", namespace, error);
        }
    }

    let mut ft_options = vec![format!(
        "table_type '{}'",
        table_type_keyword.to_ascii_lowercase()
    )];
    for (key, value) in &with_options {
        if key == "type" {
            continue;
        }
        let assignment =
            format_foreign_table_option_assignment(key, value).unwrap_or_else(|error| {
                pgrx::error!("pg_kalam DDL: invalid local foreign table option: {}", error)
            });
        ft_options.push(assignment);
    }

    let local_if_not_exists = if if_not_exists { "IF NOT EXISTS " } else { "" };
    let create_ft_sql = format!(
        "CREATE FOREIGN TABLE {}{}.{} ({}) SERVER {} OPTIONS ({})",
        local_if_not_exists,
        quote_ident_pg(&namespace),
        quote_ident_pg(&table_name),
        pg_column_defs.join(", "),
        quote_ident_pg(DEFAULT_KALAM_SERVER),
        ft_options.join(", ")
    );

    SKIP_DDL_PROPAGATION.with(|flag| flag.set(true));
    let spi_result = pgrx::Spi::run(&create_ft_sql);
    SKIP_DDL_PROPAGATION.with(|flag| flag.set(false));

    if let Err(error) = spi_result {
        pgrx::error!(
            "pg_kalam DDL: failed to create foreign table {}.{}: {}",
            namespace,
            table_name,
            error
        );
    }

    inject_system_columns_spi(&namespace, &table_name, table_type);
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
        },
        Err(error) => {
            pgrx::error!("pg_kalam DDL: failed to parse CREATE FOREIGN TABLE: {}", error);
        },
    };

    // Transform SERIAL/BIGSERIAL/IDENTITY → integer type + DEFAULT SNOWFLAKE_ID()
    let column_defs = transform_serial_types(column_defs);

    let column_defs = infer_primary_key_column(column_defs, table_type);

    if column_defs.is_empty() {
        pgrx::warning!("pg_kalam DDL: no columns found in CREATE FOREIGN TABLE");
        return;
    }

    // Build SQL statements
    let create_ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", quote_ident(&namespace));

    // Collect any extra KalamDB-specific options from the FDW table options
    // (e.g. storage_id, flush_policy, access_level, etc.)
    let kalam_options = match ft_options
        .iter()
        .filter(|(k, _)| *k != "table_type" && *k != "namespace" && *k != "table")
        .map(|(k, v)| format_kalam_option_assignment(k, v))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(options) => options,
        Err(error) => {
            pgrx::error!("pg_kalam DDL: invalid KalamDB table option: {}", error);
        },
    };

    let with_clause = if kalam_options.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", kalam_options.join(", "))
    };

    let create_table_sql = format!(
        "CREATE {} TABLE IF NOT EXISTS {}.{} ({}){}",
        table_type_keyword,
        quote_ident(&namespace),
        quote_ident(&table_name),
        column_defs.join(", "),
        with_clause
    );

    // Send to KalamDB
    if let Err(e) = execute_remote_sql(&create_ns_sql, &server_name) {
        pgrx::warning!("pg_kalam DDL: failed to create namespace '{}': {}", namespace, e);
        // Don't error out — the table might already have a namespace, continue to CREATE TABLE.
    }

    if let Err(e) = execute_remote_sql(&create_table_sql, &server_name) {
        pgrx::error!(
            "pg_kalam DDL: failed to create KalamDB table {}.{}: {}",
            namespace,
            table_name,
            e
        );
    }

    // Auto-inject system columns into the local PG schema.
    inject_system_columns_spi(&namespace, &table_name, table_type);
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
        | pg_sys::AlterTableType::AT_ColumnDefault => {},
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
        },
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

        let flags = if (*drop_stmt).missing_ok {
            pg_sys::RVROption::RVR_MISSING_OK as u32
        } else {
            0
        };
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
            pgrx::warning!(
                "pg_kalam DDL: failed to drop KalamDB table {}.{}: {}",
                target.namespace,
                target.table_name,
                e
            );
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
        entry.split_whitespace().collect::<Vec<_>>().windows(2).any(|window| {
            window[0].eq_ignore_ascii_case("PRIMARY") && window[1].eq_ignore_ascii_case("KEY")
        })
    });
    if has_primary_key {
        return column_defs;
    }

    if let Some(id_identifier) = column_defs.iter().find_map(|entry| {
        if first_sql_identifier(entry).as_deref() == Some("id") {
            raw_first_sql_identifier(entry)
        } else {
            None
        }
    }) {
        column_defs.push(format!("PRIMARY KEY ({})", id_identifier));
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
            },
            '"' if !in_single_quote => {
                if in_double_quote && bytes.get(idx + 1) == Some(&b'"') {
                    idx += 1;
                } else {
                    in_double_quote = !in_double_quote;
                }
            },
            '(' if !in_single_quote && !in_double_quote => {
                if start_idx.is_none() {
                    start_idx = Some(idx);
                }
                depth += 1;
            },
            ')' if !in_single_quote && !in_double_quote => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return start_idx.map(|start| (start, idx));
                }
            },
            _ => {},
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
            },
            '"' if !in_single_quote => {
                if in_double_quote && bytes.get(idx + 1) == Some(&b'"') {
                    idx += 1;
                } else {
                    in_double_quote = !in_double_quote;
                }
            },
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote && depth > 0 => depth -= 1,
            ',' if !in_single_quote && !in_double_quote && depth == 0 => {
                let entry = input[start..idx].trim();
                if !entry.is_empty() {
                    entries.push(entry.to_string());
                }
                start = idx + 1;
            },
            _ => {},
        }
        idx += 1;
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        entries.push(tail.to_string());
    }

    entries
}

fn extract_with_options_from_sql(sql: &str) -> BTreeMap<String, String> {
    let mut options = BTreeMap::new();

    let Some((_open_idx, close_idx)) = find_column_list_bounds(sql) else {
        return options;
    };

    let after_columns = &sql[close_idx + 1..];
    let after_columns_upper = after_columns.to_ascii_uppercase();
    let Some(with_pos) = after_columns_upper.find("WITH") else {
        return options;
    };

    let after_with = after_columns[with_pos + 4..].trim_start();
    if !after_with.starts_with('(') {
        return options;
    }

    let mut depth = 0usize;
    let mut end_idx = None;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    for (idx, ch) in after_with.char_indices() {
        match ch {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    end_idx = Some(idx);
                    break;
                }
            },
            _ => {},
        }
    }

    let Some(end_idx) = end_idx else {
        return options;
    };

    let block = &after_with[1..end_idx];
    for entry in split_top_level_sql_list(block) {
        let Some((raw_key, raw_value)) = entry.split_once('=') else {
            continue;
        };
        let key = raw_key.trim().trim_matches('"').to_ascii_lowercase();
        let value = raw_value.trim().trim_matches('"');
        let value = if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
            &value[1..value.len() - 1]
        } else {
            value
        };
        options.insert(key, value.to_string());
    }

    options
}

fn format_kalam_option_assignment(key: &str, value: &str) -> Result<String, KalamPgError> {
    let key = key.trim();
    let mut bytes = key.bytes();
    let Some(first_byte) = bytes.next() else {
        return Err(KalamPgError::Validation(format!("unsupported KalamDB option name '{}'", key)));
    };

    if !(first_byte.is_ascii_alphabetic() || first_byte == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(KalamPgError::Validation(format!("unsupported KalamDB option name '{}'", key)));
    }

    Ok(format!("{} = '{}'", key.to_ascii_uppercase(), value.replace('\'', "''")))
}

fn format_foreign_table_option_assignment(key: &str, value: &str) -> Result<String, KalamPgError> {
    let key = key.trim();
    let mut bytes = key.bytes();
    let Some(first_byte) = bytes.next() else {
        return Err(KalamPgError::Validation(format!(
            "unsupported foreign table option name '{}'",
            key
        )));
    };

    if !(first_byte.is_ascii_alphabetic() || first_byte == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(KalamPgError::Validation(format!(
            "unsupported foreign table option name '{}'",
            key
        )));
    }

    Ok(format!("{} '{}'", key.to_ascii_lowercase(), value.replace('\'', "''")))
}

fn is_internal_column_entry(entry: &str) -> bool {
    first_sql_identifier(entry)
        .map(|identifier| matches!(identifier.as_str(), "_userid" | "_seq" | "_deleted"))
        .unwrap_or(false)
}

/// Reject explicit declarations of system columns (`_userid`, `_seq`, `_deleted`)
/// in a CREATE FOREIGN TABLE statement. These columns are auto-injected.
fn validate_no_system_columns(statement_sql: &str) -> Result<(), KalamPgError> {
    let Some((open_idx, close_idx)) = find_column_list_bounds(statement_sql) else {
        return Ok(());
    };
    let block = &statement_sql[open_idx + 1..close_idx];
    for entry in split_top_level_sql_list(block) {
        if let Some(ident) = first_sql_identifier(&entry) {
            if matches!(ident.as_str(), "_userid" | "_seq" | "_deleted") {
                return Err(KalamPgError::Validation(format!(
                    "system column '{}' must not be declared explicitly; \
                     it is auto-injected by pg_kalam",
                    ident
                )));
            }
        }
    }
    Ok(())
}

/// Auto-inject system columns into the local PG foreign table schema via SPI.
///
/// - All tables get `_seq BIGINT`
/// - User tables additionally get `_userid TEXT`
///
/// DDL propagation is suppressed so these ALTER statements do not get mirrored
/// to KalamDB (system columns are managed by the backend, not the PG schema).
unsafe fn inject_system_columns_spi(namespace: &str, table_name: &str, table_type: TableType) {
    SKIP_DDL_PROPAGATION.with(|flag| flag.set(true));

    let target = format!("{}.{}", quote_ident_pg(namespace), quote_ident_pg(table_name));

    // Always inject _seq BIGINT
    let add_seq = format!("ALTER FOREIGN TABLE {} ADD COLUMN {} BIGINT", target, SEQ_COLUMN);
    if let Err(e) = pgrx::Spi::run(&add_seq) {
        SKIP_DDL_PROPAGATION.with(|flag| flag.set(false));
        pgrx::error!("pg_kalam DDL: failed to inject _seq column: {}", e);
    }

    // Inject _userid TEXT for user tables
    if table_type == TableType::User {
        let add_userid =
            format!("ALTER FOREIGN TABLE {} ADD COLUMN {} TEXT", target, USER_ID_COLUMN);
        if let Err(e) = pgrx::Spi::run(&add_userid) {
            SKIP_DDL_PROPAGATION.with(|flag| flag.set(false));
            pgrx::error!("pg_kalam DDL: failed to inject _userid column: {}", e);
        }
    }

    SKIP_DDL_PROPAGATION.with(|flag| flag.set(false));
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

fn raw_first_sql_identifier(entry: &str) -> Option<String> {
    let trimmed = entry.trim_start();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(rest) = trimmed.strip_prefix('"') {
        let end = rest.find('"')?;
        return Some(format!("\"{}\"", &rest[..end]));
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
        Some(identifier.to_string())
    }
}

fn consume_sql_keyword(sql: &str, index: &mut usize, keyword: &str) -> Result<(), KalamPgError> {
    skip_sql_whitespace(sql, index);
    let remaining = &sql[*index..];
    if remaining.len() < keyword.len() {
        return Err(KalamPgError::Validation(format!("expected '{}' in SQL clause", keyword)));
    }
    let candidate = &remaining[..keyword.len()];
    if !candidate.eq_ignore_ascii_case(keyword) {
        return Err(KalamPgError::Validation(format!("expected '{}' in SQL clause", keyword)));
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
        return Err(KalamPgError::Validation("expected identifier in SQL clause".to_string()));
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
        return Err(KalamPgError::Validation("expected identifier in SQL clause".to_string()));
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

fn create_statement_has_if_not_exists(statement_sql: &str) -> Result<bool, KalamPgError> {
    let sql = statement_sql.trim().trim_end_matches(';').trim();
    let mut index = 0usize;
    consume_sql_keyword(sql, &mut index, "CREATE")?;
    consume_sql_keyword(sql, &mut index, "TABLE")?;
    if !consume_sql_keyword_optional(sql, &mut index, "IF") {
        return Ok(false);
    }
    consume_sql_keyword(sql, &mut index, "NOT")?;
    consume_sql_keyword(sql, &mut index, "EXISTS")?;
    Ok(true)
}

unsafe fn resolve_create_namespace(rv: *mut pg_sys::RangeVar) -> Option<String> {
    if rv.is_null() {
        return None;
    }

    let explicit_namespace = read_cstr((*rv).schemaname);
    if !explicit_namespace.is_empty() {
        return Some(explicit_namespace);
    }

    let namespace_oid = pg_sys::RangeVarGetCreationNamespace(rv);
    if namespace_oid == pg_sys::InvalidOid {
        return None;
    }

    let namespace = read_cstr(pg_sys::get_namespace_name(namespace_oid));
    if namespace.is_empty() {
        None
    } else {
        Some(namespace)
    }
}

fn namespace_exists(namespace: &str) -> bool {
    let namespace_name = CString::new(namespace).unwrap_or_default();
    unsafe { pg_sys::get_namespace_oid(namespace_name.as_ptr(), true) != pg_sys::InvalidOid }
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

/// Validate and format a SQL identifier for KalamDB.
///
/// KalamDB only accepts simple identifiers (alphanumeric + underscore, not
/// starting with a digit). Reject anything else to prevent SQL injection.
fn quote_ident(name: &str) -> String {
    if name.is_empty()
        || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        || name.as_bytes()[0].is_ascii_digit()
    {
        pgrx::error!(
            "pg_kalam: invalid identifier '{}' – only alphanumeric and underscore allowed",
            name
        );
    }
    name.to_string()
}

/// Quote an identifier for use in local PostgreSQL SPI queries.
///
/// Uses `pg_sys::quote_identifier` to properly double-quote identifiers that
/// need it, preventing SQL injection into the local PG backend.
fn quote_ident_pg(name: &str) -> String {
    let cstr = std::ffi::CString::new(name).unwrap_or_else(|_| {
        pgrx::error!("pg_kalam: identifier contains null byte");
    });
    unsafe {
        let quoted = pg_sys::quote_identifier(cstr.as_ptr());
        CStr::from_ptr(quoted).to_string_lossy().into_owned()
    }
}

// ---------------------------------------------------------------------------
// SERIAL type and GENERATED IDENTITY transformation
// ---------------------------------------------------------------------------

/// Transform PostgreSQL SERIAL/BIGSERIAL/SMALLSERIAL types and GENERATED AS IDENTITY
/// into explicit integer types with `DEFAULT SNOWFLAKE_ID()`.
///
/// This ensures KalamDB receives proper auto-increment semantics since it doesn't
/// understand PostgreSQL's implicit sequence creation for serial types.
fn transform_serial_types(column_defs: Vec<String>) -> Vec<String> {
    let serial_re = regex::Regex::new(r"(?i)\b(BIG|SMALL)?SERIAL\d?\b").unwrap();
    let generated_identity_re =
        regex::Regex::new(r"(?i)GENERATED\s+(ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY(\s*\([^)]*\))?")
            .unwrap();

    column_defs
        .into_iter()
        .map(|def| {
            let mut result = def.clone();
            let upper = def.to_ascii_uppercase();

            if serial_re.is_match(&result) {
                let replacement = if upper.contains("BIGSERIAL") || upper.contains("SERIAL8") {
                    "BIGINT"
                } else if upper.contains("SMALLSERIAL") || upper.contains("SERIAL2") {
                    "SMALLINT"
                } else {
                    "INTEGER"
                };

                result = serial_re.replace(&result, replacement).into_owned();

                if !upper.contains("DEFAULT") {
                    if let Some(pk_pos) = find_keyword_position(&result, "PRIMARY") {
                        result.insert_str(pk_pos, "DEFAULT SNOWFLAKE_ID() ");
                    } else {
                        result.push_str(" DEFAULT SNOWFLAKE_ID()");
                    }
                }
            }

            if generated_identity_re.is_match(&result) {
                result =
                    generated_identity_re.replace(&result, "DEFAULT SNOWFLAKE_ID()").into_owned();
            }

            result
        })
        .collect()
}

/// Find the byte position of a keyword in a SQL fragment (case-insensitive).
fn find_keyword_position(sql: &str, keyword: &str) -> Option<usize> {
    let upper = sql.to_ascii_uppercase();
    upper.find(keyword)
}

fn strip_for_foreign_table(column_defs: &[String]) -> Vec<String> {
    let pk_re = regex::Regex::new(r"(?i)\bPRIMARY\s+KEY\b").unwrap();
    let table_pk_re = regex::Regex::new(r"(?i)^\s*PRIMARY\s+KEY\s*\(").unwrap();

    column_defs
        .iter()
        .filter(|def| !table_pk_re.is_match(def))
        .map(|def| {
            let result = pk_re.replace_all(def, "").into_owned();
            let result = rewrite_local_foreign_table_column_type(&result);
            result
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .trim_end_matches(',')
                .trim()
                .to_string()
        })
        .collect()
}

fn rewrite_local_foreign_table_column_type(definition: &str) -> String {
    if first_sql_identifier(definition).is_none() {
        return definition.to_string();
    }

    let file_type_re =
        regex::Regex::new(r#"(?i)^(\s*(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+)FILE\b"#).unwrap();

    file_type_re.replace(definition, "${1}JSONB").into_owned()
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
    fn infer_primary_key_column_adds_table_constraint_for_shared_tables() {
        let defs = vec!["id BIGINT".to_string(), "title TEXT".to_string()];
        let result = super::infer_primary_key_column(defs, TableType::Shared);
        assert_eq!(result.len(), 3);
        assert_eq!(result[2], "PRIMARY KEY (id)");
    }

    #[test]
    fn infer_primary_key_column_preserves_quoted_id_identifier() {
        let defs = vec!["\"Id\" BIGINT".to_string(), "title TEXT".to_string()];
        let result = super::infer_primary_key_column(defs, TableType::Shared);
        assert_eq!(result.len(), 3);
        assert_eq!(result[2], "PRIMARY KEY (\"Id\")");
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

    #[test]
    fn transform_serial_types_replaces_serial_with_default() {
        let defs = vec![
            "id BIGSERIAL PRIMARY KEY".to_string(),
            "title TEXT NOT NULL".to_string(),
        ];
        let result = super::transform_serial_types(defs);
        assert_eq!(result[0], "id BIGINT DEFAULT SNOWFLAKE_ID() PRIMARY KEY");
        assert_eq!(result[1], "title TEXT NOT NULL");
    }

    #[test]
    fn transform_serial_types_preserves_explicit_default() {
        let defs = vec!["id BIGSERIAL PRIMARY KEY DEFAULT 100".to_string()];
        let result = super::transform_serial_types(defs);
        assert_eq!(result[0], "id BIGINT PRIMARY KEY DEFAULT 100");
    }

    #[test]
    fn transform_serial_variants() {
        let defs = vec![
            "a SERIAL".to_string(),
            "b SMALLSERIAL".to_string(),
            "c SERIAL4".to_string(),
            "d SERIAL8".to_string(),
            "e SERIAL2".to_string(),
        ];
        let result = super::transform_serial_types(defs);
        assert!(result[0].contains("INTEGER"));
        assert!(result[1].contains("SMALLINT"));
        assert!(result[2].contains("INTEGER"));
        assert!(result[3].contains("BIGINT"));
        assert!(result[4].contains("SMALLINT"));
    }

    #[test]
    fn transform_generated_identity() {
        let defs = vec!["id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY".to_string()];
        let result = super::transform_serial_types(defs);
        assert_eq!(result[0], "id INTEGER DEFAULT SNOWFLAKE_ID() PRIMARY KEY");
    }

    #[test]
    fn extract_with_options_from_sql_works_for_using_kalamdb() {
        let sql = "CREATE TABLE t (id INT) USING kalamdb WITH (type = 'user', storage_id = 'local', flush_policy = 'rows:100,interval:60');";
        let opts = super::extract_with_options_from_sql(sql);
        assert_eq!(opts.get("type").unwrap(), "user");
        assert_eq!(opts.get("storage_id").unwrap(), "local");
        assert_eq!(opts.get("flush_policy").unwrap(), "rows:100,interval:60");
    }

    #[test]
    fn strip_for_foreign_table_removes_pk_keeps_defaults() {
        let defs = vec![
            "id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID()".to_string(),
            "name TEXT NOT NULL".to_string(),
            "created_at TIMESTAMP DEFAULT NOW()".to_string(),
            "PRIMARY KEY (id)".to_string(),
        ];
        let stripped = super::strip_for_foreign_table(&defs);
        assert_eq!(stripped.len(), 3);
        assert_eq!(stripped[0], "id BIGINT DEFAULT SNOWFLAKE_ID()");
        assert_eq!(stripped[1], "name TEXT NOT NULL");
        assert_eq!(stripped[2], "created_at TIMESTAMP DEFAULT NOW()");
    }

    #[test]
    fn strip_for_foreign_table_rewrites_file_type_to_jsonb() {
        let defs = vec![
            "\"attachment\" FILE DEFAULT '{}'::jsonb NOT NULL".to_string(),
            "metadata JSONB".to_string(),
        ];
        let stripped = super::strip_for_foreign_table(&defs);
        assert_eq!(
            stripped,
            vec![
                "\"attachment\" JSONB DEFAULT '{}'::jsonb NOT NULL".to_string(),
                "metadata JSONB".to_string(),
            ]
        );
    }

    #[test]
    fn create_statement_has_if_not_exists_detects_clause() {
        let sql = "CREATE TABLE IF NOT EXISTS app.items (id BIGINT) USING kalamdb WITH (type = 'shared');";
        assert!(super::create_statement_has_if_not_exists(sql).expect("parse IF NOT EXISTS"));

        let sql = "CREATE TABLE app.items (id BIGINT) USING kalamdb WITH (type = 'shared');";
        assert!(!super::create_statement_has_if_not_exists(sql).expect("parse CREATE TABLE"));
    }

    #[test]
    fn format_kalam_option_assignment_rejects_numeric_prefix() {
        let error = format_kalam_option_assignment("9evil", "value")
            .expect_err("numeric prefix should be rejected");
        assert!(matches!(error, KalamPgError::Validation(_)));
        assert!(error.to_string().contains("unsupported KalamDB option name '9evil'"));
    }

    #[test]
    fn validate_no_system_columns_rejects_userid() {
        let sql =
            "CREATE FOREIGN TABLE t (id TEXT, _userid TEXT) SERVER s OPTIONS (table_type 'user');";
        let err = validate_no_system_columns(sql).expect_err("should reject _userid");
        assert!(err.to_string().contains("_userid"));
    }

    #[test]
    fn validate_no_system_columns_rejects_seq() {
        let sql =
            "CREATE FOREIGN TABLE t (id TEXT, _seq BIGINT) SERVER s OPTIONS (table_type 'shared');";
        let err = validate_no_system_columns(sql).expect_err("should reject _seq");
        assert!(err.to_string().contains("_seq"));
    }

    #[test]
    fn validate_no_system_columns_rejects_deleted() {
        let sql = "CREATE FOREIGN TABLE t (id TEXT, _deleted BOOLEAN) SERVER s OPTIONS (table_type 'shared');";
        let err = validate_no_system_columns(sql).expect_err("should reject _deleted");
        assert!(err.to_string().contains("_deleted"));
    }

    #[test]
    fn validate_no_system_columns_allows_clean_sql() {
        let sql =
            "CREATE FOREIGN TABLE t (id TEXT, name TEXT) SERVER s OPTIONS (table_type 'shared');";
        validate_no_system_columns(sql).expect("clean SQL should pass");
    }
}

/// Execute a SQL statement on the remote KalamDB backend.
///
/// Bootstraps the remote connection from the given server name if not already initialized.
fn execute_remote_sql(sql: &str, server_name: &str) -> Result<String, KalamPgError> {
    let c_name = std::ffi::CString::new(server_name).unwrap_or_default();
    let server = unsafe { pg_sys::GetForeignServerByName(c_name.as_ptr(), true) };
    if server.is_null() {
        return Err(KalamPgError::Execution(format!("foreign server '{}' not found", server_name)));
    }
    let server_options = parse_options(unsafe { (*server).options });
    let parsed_server = ServerOptions::parse(&server_options)?;
    let remote_config = parsed_server.remote.ok_or_else(|| {
        KalamPgError::Validation("foreign server must have host and port options".to_string())
    })?;
    let state = crate::remote_state::ensure_remote_extension_state(remote_config)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;

    state
        .runtime()
        .block_on(async { state.client().execute_sql(sql, state.session_id()).await })
}
