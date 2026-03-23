//! FDW scan callbacks: GetForeignRelSize, GetForeignPaths, GetForeignPlan,
//! BeginForeignScan, IterateForeignScan, ReScanForeignScan, EndForeignScan.

use crate::arrow_to_pg::arrow_value_to_datum;
use crate::fdw_options::parse_options;
use crate::fdw_state::KalamScanState;
use kalam_pg_common::{KalamPgError, DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalam_pg_fdw::TableOptions;
use pgrx::pg_guard;
use pgrx::pg_sys;
use std::ffi::CStr;

/// `GetForeignRelSize` callback: estimate relation size.
#[pg_guard]
pub unsafe extern "C-unwind" fn get_foreign_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    // Parse table options to validate early
    let ft = pg_sys::GetForeignTable(foreigntableid);
    let options = parse_options((*ft).options);
    if let Err(e) = TableOptions::parse(&options) {
        pgrx::error!("pg_kalam: {}", e);
    }

    // Provide a rough row estimate (PostgreSQL uses this for costing)
    (*baserel).rows = 1000.0;
}

/// `GetForeignPaths` callback: add access paths for the planner.
#[pg_guard]
pub unsafe extern "C-unwind" fn get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    // Add a single sequential-scan-like path.
    let path = pg_sys::create_foreignscan_path(
        root,
        baserel,
        std::ptr::null_mut(), // default pathtarget
        (*baserel).rows,
        10.0,                 // startup_cost
        100.0,                // total_cost
        std::ptr::null_mut(), // pathkeys
        std::ptr::null_mut(), // required_outer
        std::ptr::null_mut(), // fdw_outerpath
        std::ptr::null_mut(), // fdw_private
    );
    pg_sys::add_path(baserel, path as *mut pg_sys::Path);
}

/// `GetForeignPlan` callback: create the ForeignScan plan node.
#[pg_guard]
pub unsafe extern "C-unwind" fn get_foreign_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);
    let scan_relid = (*baserel).relid;

    pg_sys::make_foreignscan(
        tlist,
        scan_clauses,
        scan_relid,
        std::ptr::null_mut(), // fdw_exprs
        std::ptr::null_mut(), // fdw_private
        std::ptr::null_mut(), // fdw_scan_tlist
        std::ptr::null_mut(), // fdw_recheck_quals
        outer_plan,
    )
}

/// `BeginForeignScan` callback: execute the scan and store results.
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    eflags: std::ffi::c_int,
) {
    // If EXPLAIN without ANALYZE, skip actual execution
    if (eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as std::ffi::c_int) != 0 {
        return;
    }

    let result = begin_foreign_scan_impl(node);
    if let Err(e) = result {
        pgrx::error!("pg_kalam scan: {}", e);
    }
}

/// `IterateForeignScan` callback: return the next tuple.
#[pg_guard]
pub unsafe extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    let slot = (*node).ss.ss_ScanTupleSlot;
    pg_sys::ExecClearTuple(slot);

    if (*node).fdw_state.is_null() {
        return slot;
    }

    let state = &mut *((*node).fdw_state as *mut KalamScanState);
    let tupdesc = (*slot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;

    // Find the next non-exhausted batch
    loop {
        if state.batch_index >= state.batches.len() {
            return slot; // no more rows
        }
        let batch = &state.batches[state.batch_index];
        if state.row_index < batch.num_rows() {
            break;
        }
        state.batch_index += 1;
        state.row_index = 0;
    }

    let batch = &state.batches[state.batch_index];
    let row = state.row_index;
    state.row_index += 1;

    // Fill the slot with values from the Arrow batch
    for att_idx in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(att_idx);
        if (*att).attisdropped {
            *(*slot).tts_isnull.add(att_idx) = true;
            continue;
        }

        let col_name = CStr::from_ptr((*att).attname.data.as_ptr()).to_string_lossy();

        // Handle virtual columns
        match col_name.as_ref() {
            USER_ID_COLUMN => {
                if let Some(ref uid) = state.effective_user_id {
                    let cstr = std::ffi::CString::new(uid.as_str()).unwrap();
                    let pg_text = pg_sys::cstring_to_text(cstr.as_ptr());
                    *(*slot).tts_values.add(att_idx) = pg_sys::Datum::from(pg_text as usize);
                    *(*slot).tts_isnull.add(att_idx) = false;
                } else {
                    *(*slot).tts_isnull.add(att_idx) = true;
                }
                continue;
            },
            SEQ_COLUMN => {
                // _seq not yet available from scan response, return null
                *(*slot).tts_isnull.add(att_idx) = true;
                continue;
            },
            DELETED_COLUMN => {
                // Live rows are not deleted
                *(*slot).tts_values.add(att_idx) = pg_sys::Datum::from(false as usize);
                *(*slot).tts_isnull.add(att_idx) = false;
                continue;
            },
            _ => {},
        }

        // Map to Arrow column
        if let Some(Some(arrow_idx)) = state.column_mapping.get(att_idx) {
            let array = batch.column(*arrow_idx);
            let (datum, is_null) = arrow_value_to_datum(array.as_ref(), row);
            *(*slot).tts_values.add(att_idx) = datum;
            *(*slot).tts_isnull.add(att_idx) = is_null;
        } else {
            *(*slot).tts_isnull.add(att_idx) = true;
        }
    }

    pg_sys::ExecStoreVirtualTuple(slot);
    slot
}

/// `ReScanForeignScan` callback: restart the scan from the beginning.
#[pg_guard]
pub unsafe extern "C-unwind" fn rescan_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    if (*node).fdw_state.is_null() {
        return;
    }
    let state = &mut *((*node).fdw_state as *mut KalamScanState);
    state.batch_index = 0;
    state.row_index = 0;
}

/// `EndForeignScan` callback: release scan resources.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    if !(*node).fdw_state.is_null() {
        let _ = Box::from_raw((*node).fdw_state as *mut KalamScanState);
        (*node).fdw_state = std::ptr::null_mut();
    }
}

// ---------------------------------------------------------------------------
// Internal implementation
// ---------------------------------------------------------------------------

unsafe fn begin_foreign_scan_impl(node: *mut pg_sys::ForeignScanState) -> Result<(), KalamPgError> {
    use kalam_pg_fdw::ServerOptions;

    let relation = (*node).ss.ss_currentRelation;
    let relid = (*relation).rd_id;
    let ft = pg_sys::GetForeignTable(relid);
    let options = parse_options((*ft).options);
    let table_options = TableOptions::parse(&options)?;

    // Flush any pending writes for this table before scanning (read-your-writes)
    crate::write_buffer::flush_table(&table_options.table_id)?;

    // Get server options (host/port) from the foreign server
    let server = pg_sys::GetForeignServer((*ft).serverid);
    let server_options = parse_options((*server).options);
    let parsed_server = ServerOptions::parse(&server_options)?;
    let remote_config = parsed_server.remote.ok_or_else(|| {
        KalamPgError::Validation("foreign server must have host and port options for remote mode".to_string())
    })?;

    // Read session user_id from GUC
    let user_id_str = crate::current_kalam_user_id();
    let user_id = user_id_str.map(kalamdb_commons::models::UserId::new);

    // Ensure remote connection
    let remote_state = crate::remote_state::ensure_remote_extension_state(remote_config)
        .map_err(|e| KalamPgError::Execution(e.to_string()))?;
    let executor = remote_state.executor()?;
    let runtime = remote_state.runtime();

    // Lazily begin a KalamDB transaction for this PostgreSQL transaction
    let _ = crate::fdw_xact::ensure_transaction(remote_state.session_id())?;

    // Build column mapping from PG TupleDesc to Arrow schema
    let tupdesc = (*(*node).ss.ss_ScanTupleSlot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;

    // Collect physical column names for projection
    let mut physical_columns: Vec<String> = Vec::new();
    for i in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(i);
        if (*att).attisdropped {
            continue;
        }
        let col_name = CStr::from_ptr((*att).attname.data.as_ptr()).to_string_lossy().into_owned();
        if col_name != USER_ID_COLUMN && col_name != SEQ_COLUMN && col_name != DELETED_COLUMN {
            physical_columns.push(col_name);
        }
    }

    let projection = if physical_columns.is_empty() {
        None
    } else {
        Some(physical_columns)
    };

    let tenant_context = kalam_pg_api::TenantContext::new(None, user_id.clone());

    let request = kalam_pg_api::ScanRequest {
        table_id: table_options.table_id.clone(),
        table_type: table_options.table_type,
        tenant_context,
        remote_session: None,
        projection,
        filters: Vec::new(),
        limit: None,
    };

    let response = runtime.block_on(async { executor.scan(request).await })?;

    // Build column mapping: PG attnum -> Arrow column index
    let arrow_schema = response.batches.first().map(|b| b.schema());

    let mut column_mapping = Vec::with_capacity(natts);
    for i in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(i);
        let col_name = CStr::from_ptr((*att).attname.data.as_ptr()).to_string_lossy();

        if col_name == USER_ID_COLUMN || col_name == SEQ_COLUMN || col_name == DELETED_COLUMN {
            column_mapping.push(None);
            continue;
        }

        let arrow_idx = arrow_schema
            .as_ref()
            .and_then(|schema| schema.column_with_name(col_name.as_ref()))
            .map(|(idx, _)| idx);
        column_mapping.push(arrow_idx);
    }

    let effective_user_id = user_id.map(|u| u.as_str().to_string());

    let scan_state = Box::new(KalamScanState {
        batches: response.batches,
        batch_index: 0,
        row_index: 0,
        column_mapping,
        effective_user_id,
    });

    (*node).fdw_state = Box::into_raw(scan_state) as *mut std::ffi::c_void;
    Ok(())
}
