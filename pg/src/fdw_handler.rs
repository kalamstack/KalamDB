//! FDW handler and validator registration.
//!
//! Provides the `pg_kalam_handler` and `pg_kalam_validator` C functions
//! and registers the `pg_kalam` foreign data wrapper via extension SQL.

use pgrx::pg_guard;
use pgrx::pg_sys;

/// Build a fully populated `FdwRoutine` for the Kalam FDW.
unsafe fn create_fdw_routine() -> *mut pg_sys::FdwRoutine {
    let routine_ptr =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::FdwRoutine>()) as *mut pg_sys::FdwRoutine;
    (*routine_ptr).type_ = pg_sys::NodeTag::T_FdwRoutine;

    // Scan callbacks
    (*routine_ptr).GetForeignRelSize = Some(crate::fdw_scan::get_foreign_rel_size);
    (*routine_ptr).GetForeignPaths = Some(crate::fdw_scan::get_foreign_paths);
    (*routine_ptr).GetForeignPlan = Some(crate::fdw_scan::get_foreign_plan);
    (*routine_ptr).BeginForeignScan = Some(crate::fdw_scan::begin_foreign_scan);
    (*routine_ptr).IterateForeignScan = Some(crate::fdw_scan::iterate_foreign_scan);
    (*routine_ptr).ReScanForeignScan = Some(crate::fdw_scan::rescan_foreign_scan);
    (*routine_ptr).EndForeignScan = Some(crate::fdw_scan::end_foreign_scan);

    // Modify callbacks
    (*routine_ptr).IsForeignRelUpdatable = Some(crate::fdw_modify::is_foreign_rel_updatable);
    (*routine_ptr).AddForeignUpdateTargets = Some(crate::fdw_modify::add_foreign_update_targets);
    (*routine_ptr).PlanForeignModify = Some(crate::fdw_modify::plan_foreign_modify);
    (*routine_ptr).BeginForeignModify = Some(crate::fdw_modify::begin_foreign_modify);
    (*routine_ptr).ExecForeignInsert = Some(crate::fdw_modify::exec_foreign_insert);
    (*routine_ptr).ExecForeignBatchInsert = Some(crate::fdw_modify::exec_foreign_batch_insert);
    (*routine_ptr).GetForeignModifyBatchSize = Some(crate::fdw_modify::get_foreign_modify_batch_size);
    (*routine_ptr).ExecForeignUpdate = Some(crate::fdw_modify::exec_foreign_update);
    (*routine_ptr).ExecForeignDelete = Some(crate::fdw_modify::exec_foreign_delete);
    (*routine_ptr).EndForeignModify = Some(crate::fdw_modify::end_foreign_modify);

    // Import foreign schema
    (*routine_ptr).ImportForeignSchema = Some(crate::fdw_import::import_foreign_schema);

    routine_ptr
}

// PG_FUNCTION_INFO_V1 equivalents — PostgreSQL requires these to find the finfo record.
#[no_mangle]
pub extern "C" fn pg_finfo_pg_kalam_handler_c() -> &'static pg_sys::Pg_finfo_record {
    static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

#[no_mangle]
pub extern "C" fn pg_finfo_pg_kalam_validator_c() -> &'static pg_sys::Pg_finfo_record {
    static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

/// FDW handler entry point called by PostgreSQL. Returns a populated `FdwRoutine`.
///
/// # Safety
/// Called by PostgreSQL through the function manager interface.
#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn pg_kalam_handler_c(
    _fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    let routine_ptr = create_fdw_routine();
    pg_sys::Datum::from(routine_ptr as usize)
}

/// FDW option validator called during CREATE SERVER / CREATE FOREIGN TABLE.
///
/// # Safety
/// Called by PostgreSQL through the function manager interface.
#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn pg_kalam_validator_c(
    _fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    // Accept any options for now. Validation happens at scan/modify time
    // when we parse TableOptions and resolve the backend.
    pg_sys::Datum::from(0usize)
}

// Register the handler, validator, and foreign data wrapper via extension SQL.
// This SQL is injected into the extension's installation script by pgrx.
pgrx::extension_sql!(
    r#"
CREATE FUNCTION pg_kalam_handler()
RETURNS fdw_handler
LANGUAGE c STRICT
AS 'MODULE_PATHNAME', 'pg_kalam_handler_c';

CREATE FUNCTION pg_kalam_validator(text[], oid)
RETURNS void
LANGUAGE c STRICT
AS 'MODULE_PATHNAME', 'pg_kalam_validator_c';

CREATE FOREIGN DATA WRAPPER pg_kalam
  HANDLER pg_kalam_handler
  VALIDATOR pg_kalam_validator;
"#,
    name = "pg_kalam_registration",
);
