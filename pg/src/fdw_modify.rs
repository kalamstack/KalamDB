//! FDW modify callbacks: IsForeignRelUpdatable, AddForeignUpdateTargets,
//! PlanForeignModify, BeginForeignModify, ExecForeignInsert/Update/Delete,
//! EndForeignModify.

use crate::fdw_options::parse_options;
use crate::fdw_state::KalamModifyState;
use crate::pg_to_kalam::datum_to_scalar;
use crate::relation_table_options::resolve_table_options_for_relation;
use kalam_pg_api::{DeleteRequest, InsertRequest, TenantContext, UpdateRequest};
use kalam_pg_common::{KalamPgError, DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use pgrx::pg_guard;
use pgrx::pg_sys;
use std::collections::BTreeMap;
use std::ffi::CStr;

/// `IsForeignRelUpdatable` callback: report supported DML operations.
#[pg_guard]
pub unsafe extern "C-unwind" fn is_foreign_rel_updatable(
    _rel: pg_sys::Relation,
) -> std::ffi::c_int {
    // Support INSERT, UPDATE, DELETE
    (1 << pg_sys::CmdType::CMD_INSERT as i32)
        | (1 << pg_sys::CmdType::CMD_UPDATE as i32)
        | (1 << pg_sys::CmdType::CMD_DELETE as i32)
}

/// `AddForeignUpdateTargets` callback: add PK column as row identifier for UPDATE/DELETE.
#[pg_guard]
pub unsafe extern "C-unwind" fn add_foreign_update_targets(
    root: *mut pg_sys::PlannerInfo,
    rtindex: pg_sys::Index,
    _target_rte: *mut pg_sys::RangeTblEntry,
    target_relation: pg_sys::Relation,
) {
    // Use the first non-virtual, non-dropped column as the row identifier (PK)
    let tupdesc = (*target_relation).rd_att;
    let natts = (*tupdesc).natts as usize;

    for i in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(i);
        if (*att).attisdropped {
            continue;
        }
        let col_name = CStr::from_ptr((*att).attname.data.as_ptr()).to_string_lossy();
        if col_name == USER_ID_COLUMN || col_name == "_seq" || col_name == "_deleted" {
            continue;
        }
        // Use this column as the row identity
        let var = pg_sys::makeVar(
            rtindex as std::ffi::c_int,
            (*att).attnum,
            (*att).atttypid,
            (*att).atttypmod,
            (*att).attcollation,
            0, // sublevelsup
        );
        pg_sys::add_row_identity_var(root, var, rtindex, (*att).attname.data.as_ptr());
        return;
    }
}

/// `PlanForeignModify` callback: return an empty list (no additional planning data needed).
#[pg_guard]
pub unsafe extern "C-unwind" fn plan_foreign_modify(
    _root: *mut pg_sys::PlannerInfo,
    _plan: *mut pg_sys::ModifyTable,
    _result_relation: pg_sys::Index,
    _subplan_index: std::ffi::c_int,
) -> *mut pg_sys::List {
    std::ptr::null_mut()
}

/// `BeginForeignModify` callback: initialize modify state.
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_foreign_modify(
    _mtstate: *mut pg_sys::ModifyTableState,
    rinfo: *mut pg_sys::ResultRelInfo,
    _fdw_private: *mut pg_sys::List,
    _subplan_index: std::ffi::c_int,
    _eflags: std::ffi::c_int,
) {
    let result = begin_foreign_modify_impl(rinfo);
    if let Err(e) = result {
        pgrx::error!("pg_kalam modify: {}", e);
    }
}

/// `ExecForeignInsert` callback: insert a single row.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_foreign_insert(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    _plan_slot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    let result = exec_foreign_insert_impl(rinfo, slot);
    match result {
        Ok(()) => slot,
        Err(e) => pgrx::error!("pg_kalam insert: {}", e),
    }
}

/// Maximum batch size for `ExecForeignBatchInsert`.
/// PostgreSQL will accumulate up to this many slots before calling the batch callback.
const BATCH_INSERT_SIZE: i32 = 1000;

/// `GetForeignModifyBatchSize` callback: return maximum batch size for inserts.
#[pg_guard]
pub unsafe extern "C-unwind" fn get_foreign_modify_batch_size(
    _rinfo: *mut pg_sys::ResultRelInfo,
) -> std::ffi::c_int {
    BATCH_INSERT_SIZE
}

/// `ExecForeignBatchInsert` callback: insert multiple rows in a single gRPC call.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_foreign_batch_insert(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slots: *mut *mut pg_sys::TupleTableSlot,
    _plan_slots: *mut *mut pg_sys::TupleTableSlot,
    num_slots: *mut std::ffi::c_int,
) -> *mut *mut pg_sys::TupleTableSlot {
    let n = *num_slots as usize;
    let result = exec_foreign_batch_insert_impl(rinfo, slots, n);
    match result {
        Ok(()) => slots,
        Err(e) => pgrx::error!("pg_kalam batch insert: {}", e),
    }
}

/// `ExecForeignUpdate` callback: update a single row.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_foreign_update(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    let result = exec_foreign_update_impl(rinfo, slot, plan_slot);
    match result {
        Ok(()) => slot,
        Err(e) => pgrx::error!("pg_kalam update: {}", e),
    }
}

/// `ExecForeignDelete` callback: delete a single row.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_foreign_delete(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    let result = exec_foreign_delete_impl(rinfo, plan_slot);
    match result {
        Ok(()) => slot,
        Err(e) => pgrx::error!("pg_kalam delete: {}", e),
    }
}

/// `EndForeignModify` callback: release modify resources.
/// Buffered writes are NOT flushed here — they accumulate across statements
/// within a transaction and are flushed at PRE_COMMIT or before scans.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_foreign_modify(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
) {
    // Implicit autocommit statements need their buffered writes flushed here so
    // statement errors surface to the client instead of being deferred to the
    // transaction callback. Explicit BEGIN/COMMIT blocks keep batching until
    // PRE_COMMIT for better throughput.
    if let Some(table_id) = current_modify_table_id(rinfo) {
        if !pg_sys::IsTransactionBlock() {
            if let Err(e) = crate::write_buffer::flush_table(&table_id) {
                pgrx::error!("pg_kalam modify flush: {}", e);
            }
        }
    }

    let state_ptr = (*rinfo).ri_FdwState;
    if !state_ptr.is_null() {
        let _ = Box::from_raw(state_ptr as *mut KalamModifyState);
        (*rinfo).ri_FdwState = std::ptr::null_mut();
    }
}

// ---------------------------------------------------------------------------
// Internal implementations
// ---------------------------------------------------------------------------

unsafe fn begin_foreign_modify_impl(rinfo: *mut pg_sys::ResultRelInfo) -> Result<(), KalamPgError> {
    use kalam_pg_fdw::ServerOptions;

    let relation = (*rinfo).ri_RelationDesc;
    let relid = (*relation).rd_id;
    let ft = pg_sys::GetForeignTable(relid);
    let options = parse_options((*ft).options);
    let table_options = resolve_table_options_for_relation(relation, &options)?;

    let tupdesc = (*relation).rd_att;
    let natts = (*tupdesc).natts as usize;

    // Collect column names and find PK (first non-virtual, non-dropped column)
    let mut column_names = Vec::with_capacity(natts);
    let mut pk_column = String::new();
    for i in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(i);
        if (*att).attisdropped {
            column_names.push(String::new());
            continue;
        }
        let col_name = CStr::from_ptr((*att).attname.data.as_ptr()).to_string_lossy().into_owned();
        if pk_column.is_empty()
            && col_name != USER_ID_COLUMN
            && col_name != SEQ_COLUMN
            && col_name != DELETED_COLUMN
        {
            pk_column = col_name.clone();
        }
        column_names.push(col_name);
    }

    // Get server options (host/port) from the foreign server
    let server = pg_sys::GetForeignServer((*ft).serverid);
    let server_options = parse_options((*server).options);
    let parsed_server = ServerOptions::parse(&server_options)?;
    let remote_config = parsed_server.remote.ok_or_else(|| {
        KalamPgError::Validation(
            "foreign server must have host and port options for remote mode".to_string(),
        )
    })?;

    let remote_state =
        crate::remote_state::ensure_remote_extension_state(remote_config)
            .map_err(|e| KalamPgError::Execution(e.to_string()))?;
    let executor = remote_state.executor()?;
    let runtime = std::sync::Arc::clone(remote_state.runtime());

    // Lazily begin a KalamDB transaction for this PostgreSQL transaction
    let _ = crate::fdw_xact::ensure_transaction(remote_state.session_id())?;

    let modify_state = Box::new(KalamModifyState {
        table_options,
        executor,
        runtime,
        column_names,
        pk_column,
    });

    (*rinfo).ri_FdwState = Box::into_raw(modify_state) as *mut std::ffi::c_void;
    Ok(())
}

unsafe fn current_modify_table_id(rinfo: *mut pg_sys::ResultRelInfo) -> Option<kalamdb_commons::TableId> {
    let state_ptr = (*rinfo).ri_FdwState;
    if state_ptr.is_null() {
        return None;
    }

    let state = &*(state_ptr as *mut KalamModifyState);
    Some(state.table_options.table_id.clone())
}

unsafe fn exec_foreign_insert_impl(
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
) -> Result<(), KalamPgError> {
    let state = &*((*rinfo).ri_FdwState as *mut KalamModifyState);
    let row = slot_to_row(slot, &state.column_names)?;

    let user_id_str = crate::current_kalam_user_id();
    let user_id = user_id_str.map(UserId::new);

    crate::write_buffer::buffer_insert(
        &state.table_options.table_id,
        state.table_options.table_type,
        user_id,
        row,
        &state.executor,
        &state.runtime,
    )?;
    Ok(())
}

unsafe fn exec_foreign_batch_insert_impl(
    rinfo: *mut pg_sys::ResultRelInfo,
    slots: *mut *mut pg_sys::TupleTableSlot,
    num_slots: usize,
) -> Result<(), KalamPgError> {
    let state = &*((*rinfo).ri_FdwState as *mut KalamModifyState);

    let user_id_str = crate::current_kalam_user_id();
    let user_id = user_id_str.map(UserId::new);

    // Single-row case: buffer for cross-statement batching within transactions
    if num_slots == 1 {
        let slot = *slots.add(0);
        let row = slot_to_row(slot, &state.column_names)?;
        crate::write_buffer::buffer_insert(
            &state.table_options.table_id,
            state.table_options.table_type,
            user_id,
            row,
            &state.executor,
            &state.runtime,
        )?;
        return Ok(());
    }

    // Multi-row case: flush any pending buffer first, then send this batch directly
    crate::write_buffer::flush_table(&state.table_options.table_id)?;

    let mut rows = Vec::with_capacity(num_slots);
    for i in 0..num_slots {
        let slot = *slots.add(i);
        rows.push(slot_to_row(slot, &state.column_names)?);
    }

    let request = InsertRequest::new(
        state.table_options.table_id.clone(),
        state.table_options.table_type,
        TenantContext::new(None, user_id),
        rows,
    );

    state
        .runtime
        .block_on(async { state.executor.insert(request).await })?;
    Ok(())
}

unsafe fn exec_foreign_update_impl(
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> Result<(), KalamPgError> {
    let state = &*((*rinfo).ri_FdwState as *mut KalamModifyState);
    // Flush pending inserts so the update sees all rows
    crate::write_buffer::flush_table(&state.table_options.table_id)?;
    let pk_value = extract_pk_value(plan_slot, &state.pk_column, &state.column_names)?;
    let updates = slot_to_row(slot, &state.column_names)?;

    let user_id_str = crate::current_kalam_user_id();
    let user_id = user_id_str.map(UserId::new);

    let request = UpdateRequest::new(
        state.table_options.table_id.clone(),
        state.table_options.table_type,
        TenantContext::new(None, user_id),
        pk_value,
        updates,
    );

    state
        .runtime
        .block_on(async { state.executor.update(request).await })?;
    Ok(())
}

unsafe fn exec_foreign_delete_impl(
    rinfo: *mut pg_sys::ResultRelInfo,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> Result<(), KalamPgError> {
    let state = &*((*rinfo).ri_FdwState as *mut KalamModifyState);
    // Flush pending inserts so the delete sees all rows
    crate::write_buffer::flush_table(&state.table_options.table_id)?;
    let pk_value = extract_pk_value(plan_slot, &state.pk_column, &state.column_names)?;

    let user_id_str = crate::current_kalam_user_id();
    let user_id = user_id_str.map(UserId::new);

    let request = DeleteRequest::new(
        state.table_options.table_id.clone(),
        state.table_options.table_type,
        TenantContext::new(None, user_id),
        pk_value,
    );

    state
        .runtime
        .block_on(async { state.executor.delete(request).await })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract column values from a TupleTableSlot into a Kalam Row.
unsafe fn slot_to_row(
    slot: *mut pg_sys::TupleTableSlot,
    column_names: &[String],
) -> Result<Row, KalamPgError> {
    pg_sys::slot_getallattrs(slot);
    let tupdesc = (*slot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;
    let mut values = BTreeMap::new();

    for i in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(i);
        if (*att).attisdropped {
            continue;
        }
        let col_name = column_names.get(i).cloned().unwrap_or_default();
        if col_name.is_empty()
            || col_name == USER_ID_COLUMN
            || col_name == "_seq"
            || col_name == "_deleted"
        {
            continue;
        }

        let is_null = *(*slot).tts_isnull.add(i);
        let datum = *(*slot).tts_values.add(i);
        let scalar = datum_to_scalar(datum, (*att).atttypid, is_null);
        values.insert(col_name, scalar);
    }

    Ok(Row::new(values))
}

/// Extract the primary key value from the plan slot (junk attribute).
unsafe fn extract_pk_value(
    plan_slot: *mut pg_sys::TupleTableSlot,
    pk_column: &str,
    _column_names: &[String],
) -> Result<String, KalamPgError> {
    pg_sys::slot_getallattrs(plan_slot);
    let tupdesc = (*plan_slot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;

    for i in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(i);
        if (*att).attisdropped {
            continue;
        }
        let col_name = CStr::from_ptr((*att).attname.data.as_ptr()).to_string_lossy();
        if col_name != pk_column {
            continue;
        }

        let is_null = *(*plan_slot).tts_isnull.add(i);
        if is_null {
            return Err(KalamPgError::Validation(
                "primary key column is NULL in plan slot".to_string(),
            ));
        }
        let datum = *(*plan_slot).tts_values.add(i);
        let scalar = datum_to_scalar(datum, (*att).atttypid, false);
        return Ok(scalar.to_string());
    }

    Err(KalamPgError::Validation(format!(
        "primary key column '{}' not found in plan slot",
        pk_column
    )))
}
