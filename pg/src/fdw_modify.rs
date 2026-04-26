//! FDW modify callbacks: IsForeignRelUpdatable, AddForeignUpdateTargets,
//! PlanForeignModify, BeginForeignModify, ExecForeignInsert/Update/Delete,
//! EndForeignModify.

use std::{collections::BTreeMap, ffi::CStr};

use kalam_pg_api::{DeleteRequest, InsertRequest, TenantContext, UpdateRequest};
use kalam_pg_common::{KalamPgError, DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalamdb_commons::models::{rows::Row, UserId};
use pgrx::{pg_guard, pg_sys};

use crate::{
    fdw_options::parse_options, fdw_state::KalamModifyState, pg_to_kalam::datum_to_scalar,
    relation_table_options::resolve_table_options_for_relation,
};

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
        if col_name == USER_ID_COLUMN || col_name == SEQ_COLUMN || col_name == DELETED_COLUMN {
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
/// Buffered writes are flushed here at statement end for autocommit mode.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_foreign_modify(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
) {
    // Autocommit writes must flush at statement end so statement-level errors
    // surface immediately. Explicit BEGIN/COMMIT blocks keep buffering across
    // statements and flush in the terminal COMMIT hook to preserve batching.
    if !crate::fdw_xact::is_explicit_transaction_block_active()
        && !crate::fdw_xact::has_active_transaction()
    {
        if let Some((session_id, table_id, table_type)) = current_modify_flush_context(rinfo) {
            if let Err(e) = crate::write_buffer::flush_table(&session_id, &table_id, table_type) {
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

    let remote_state = crate::remote_server::remote_state_for_server_id((*ft).serverid)?;
    let executor = remote_state.executor()?;
    let runtime = std::sync::Arc::clone(remote_state.runtime());

    // Only explicit BEGIN/COMMIT blocks need a remote transaction. Implicit
    // autocommit statements should flush directly to the backend so errors
    // surface at statement end and no stale remote transaction state lingers.
    if crate::fdw_xact::is_explicit_transaction_block_active() {
        let _ = crate::fdw_xact::ensure_transaction(remote_state.session_id())?;
    }

    let modify_state = Box::new(KalamModifyState {
        session_id: remote_state.session_id().to_string(),
        table_options,
        executor,
        runtime,
        column_names,
        pk_column,
        flushed_for_modify: false,
    });

    (*rinfo).ri_FdwState = Box::into_raw(modify_state) as *mut std::ffi::c_void;
    Ok(())
}

unsafe fn current_modify_flush_context(
    rinfo: *mut pg_sys::ResultRelInfo,
) -> Option<(String, kalamdb_commons::TableId, kalamdb_commons::TableType)> {
    let state_ptr = (*rinfo).ri_FdwState;
    if state_ptr.is_null() {
        return None;
    }

    let state = &*(state_ptr as *mut KalamModifyState);
    Some((
        state.session_id.clone(),
        state.table_options.table_id.clone(),
        state.table_options.table_type,
    ))
}

unsafe fn exec_foreign_insert_impl(
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
) -> Result<(), KalamPgError> {
    let state = &*((*rinfo).ri_FdwState as *mut KalamModifyState);
    let (row, explicit_userid) = slot_to_row(slot, &state.column_names)?;

    let session_user_id = crate::current_kalam_user_id().map(UserId::new);
    let explicit_user_id = explicit_userid.map(UserId::new);
    let effective = explicit_user_id.or(session_user_id);

    if crate::fdw_xact::is_explicit_transaction_block_active() {
        let request = InsertRequest::new(
            state.table_options.table_id.clone(),
            state.table_options.table_type,
            TenantContext::new(None, effective),
            vec![row],
        );

        state.runtime.block_on(async { state.executor.insert(request).await })?;
        return Ok(());
    }

    crate::write_buffer::buffer_insert(
        &state.session_id,
        &state.table_options.table_id,
        state.table_options.table_type,
        effective,
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

    let session_user_id = crate::current_kalam_user_id().map(UserId::new);

    // Single-row case: buffer for cross-statement batching within transactions
    if num_slots == 1 {
        let slot = *slots.add(0);
        let (row, explicit_userid) = slot_to_row(slot, &state.column_names)?;
        let explicit_user_id = explicit_userid.map(UserId::new);
        let effective = explicit_user_id.or(session_user_id);

        if crate::fdw_xact::is_explicit_transaction_block_active() {
            let request = InsertRequest::new(
                state.table_options.table_id.clone(),
                state.table_options.table_type,
                TenantContext::new(None, effective),
                vec![row],
            );

            state.runtime.block_on(async { state.executor.insert(request).await })?;
            return Ok(());
        }

        crate::write_buffer::buffer_insert(
            &state.session_id,
            &state.table_options.table_id,
            state.table_options.table_type,
            effective,
            row,
            &state.executor,
            &state.runtime,
        )?;
        return Ok(());
    }

    // Multi-row case: flush any pending buffer first, then send this batch directly
    crate::write_buffer::flush_table(
        &state.session_id,
        &state.table_options.table_id,
        state.table_options.table_type,
    )?;

    // Extract rows and capture the first explicit _userid (all rows in one batch
    // share the same effective user).
    let mut rows = Vec::with_capacity(num_slots);
    let mut batch_explicit_userid: Option<UserId> = None;
    for i in 0..num_slots {
        let slot = *slots.add(i);
        let (row, explicit_userid) = slot_to_row(slot, &state.column_names)?;
        if let Some(uid) = explicit_userid {
            let uid = UserId::new(uid);
            if let Some(ref first) = batch_explicit_userid {
                if &uid != first {
                    return Err(KalamPgError::Validation(
                        "batch INSERT rows must use the same _userid value".to_string(),
                    ));
                }
            } else {
                batch_explicit_userid = Some(uid);
            }
        }
        rows.push(row);
    }

    let effective = batch_explicit_userid.or(session_user_id);

    let request = InsertRequest::new(
        state.table_options.table_id.clone(),
        state.table_options.table_type,
        TenantContext::new(None, effective),
        rows,
    );

    state.runtime.block_on(async { state.executor.insert(request).await })?;
    Ok(())
}

unsafe fn exec_foreign_update_impl(
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> Result<(), KalamPgError> {
    let state = &mut *((*rinfo).ri_FdwState as *mut KalamModifyState);
    // Flush pending inserts once per modify lifecycle so updates see all rows
    if !state.flushed_for_modify {
        crate::write_buffer::flush_table(
            &state.session_id,
            &state.table_options.table_id,
            state.table_options.table_type,
        )?;
        state.flushed_for_modify = true;
    }
    let pk_value = extract_pk_value(plan_slot, &state.pk_column, &state.column_names)?;
    let (updates, _explicit_userid) = slot_to_row(slot, &state.column_names)?;

    let user_id_str = crate::current_kalam_user_id();
    let user_id = user_id_str.map(UserId::new);

    let request = UpdateRequest::new(
        state.table_options.table_id.clone(),
        state.table_options.table_type,
        TenantContext::new(None, user_id),
        pk_value,
        updates,
    );

    state.runtime.block_on(async { state.executor.update(request).await })?;
    Ok(())
}

unsafe fn exec_foreign_delete_impl(
    rinfo: *mut pg_sys::ResultRelInfo,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> Result<(), KalamPgError> {
    let state = &mut *((*rinfo).ri_FdwState as *mut KalamModifyState);
    // Flush pending inserts once per modify lifecycle so deletes see all rows
    if !state.flushed_for_modify {
        crate::write_buffer::flush_table(
            &state.session_id,
            &state.table_options.table_id,
            state.table_options.table_type,
        )?;
        state.flushed_for_modify = true;
    }
    let pk_value = extract_pk_value(plan_slot, &state.pk_column, &state.column_names)?;

    let user_id_str = crate::current_kalam_user_id();
    let user_id = user_id_str.map(UserId::new);

    let request = DeleteRequest::new(
        state.table_options.table_id.clone(),
        state.table_options.table_type,
        TenantContext::new(None, user_id),
        pk_value,
    );

    state.runtime.block_on(async { state.executor.delete(request).await })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract column values from a TupleTableSlot into a Kalam Row.
///
/// Returns `(row, explicit_userid)` where `explicit_userid` is the value of
/// the `_userid` column if the user supplied one in the INSERT.
unsafe fn slot_to_row(
    slot: *mut pg_sys::TupleTableSlot,
    column_names: &[String],
) -> Result<(Row, Option<String>), KalamPgError> {
    pg_sys::slot_getallattrs(slot);
    let tupdesc = (*slot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;
    let mut values = BTreeMap::new();
    let mut explicit_userid: Option<String> = None;

    for i in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(i);
        if (*att).attisdropped {
            continue;
        }
        let col_name = column_names.get(i).cloned().unwrap_or_default();
        if col_name.is_empty() || col_name == SEQ_COLUMN || col_name == DELETED_COLUMN {
            continue;
        }

        let is_null = *(*slot).tts_isnull.add(i);

        // Capture explicit _userid from the slot but do not include it in the row.
        if col_name == USER_ID_COLUMN {
            if !is_null {
                let datum = *(*slot).tts_values.add(i);
                let scalar = datum_to_scalar(datum, (*att).atttypid, false);
                explicit_userid = Some(scalar.to_string());
            }
            continue;
        }

        let datum = *(*slot).tts_values.add(i);
        let scalar = datum_to_scalar(datum, (*att).atttypid, is_null);
        values.insert(col_name, scalar);
    }

    Ok((Row::new(values), explicit_userid))
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
