//! FDW scan callbacks: GetForeignRelSize, GetForeignPaths, GetForeignPlan,
//! BeginForeignScan, IterateForeignScan, ReScanForeignScan, EndForeignScan.

use std::ffi::CStr;

use datafusion_common::ScalarValue;
use kalam_pg_api::ScanFilter;
use kalam_pg_common::{KalamPgError, DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use pgrx::{pg_guard, pg_sys};

use crate::{
    arrow_to_pg::arrow_value_to_datum, fdw_options::parse_options, fdw_state::KalamScanState,
    relation_table_options::resolve_table_options_for_relation,
};

/// `GetForeignRelSize` callback: estimate relation size.
#[pg_guard]
pub unsafe extern "C-unwind" fn get_foreign_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    // Validate only the options that cannot be derived from the relation itself.
    let ft = pg_sys::GetForeignTable(foreigntableid);
    let options = parse_options((*ft).options);
    if !options.contains_key("table_type") {
        let e = KalamPgError::Validation("table option 'table_type' is required".to_string());
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
            _ => {},
        }

        // Map to Arrow column
        if let Some(Some(arrow_idx)) = state.column_mapping.get(att_idx) {
            let array = batch.column(*arrow_idx);
            let (datum, is_null) = arrow_value_to_datum(array.as_ref(), row, (*att).atttypid);
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
    let relation = (*node).ss.ss_currentRelation;
    let relid = (*relation).rd_id;
    let ft = pg_sys::GetForeignTable(relid);
    let options = parse_options((*ft).options);
    let table_options = resolve_table_options_for_relation(relation, &options)?;

    // Read session user_id from GUC
    let user_id_str = crate::current_kalam_user_id();
    let user_id = user_id_str.map(kalamdb_commons::models::UserId::new);

    // Ensure remote connection
    let remote_state = crate::remote_server::remote_state_for_server_id((*ft).serverid)?;

    // Flush any pending writes for this table before scanning (read-your-writes)
    crate::write_buffer::flush_table(
        remote_state.session_id(),
        &table_options.table_id,
        table_options.table_type,
    )?;

    let executor = remote_state.executor()?;
    let runtime = remote_state.runtime();

    // Autocommit SELECTs do not need a remote transaction. Keep remote
    // transactions only for explicit BEGIN/COMMIT blocks so transaction
    // bookkeeping stays aligned with PostgreSQL statement boundaries.
    if crate::fdw_xact::is_explicit_transaction_block_active() {
        let _ = crate::fdw_xact::ensure_transaction(remote_state.session_id())?;
    }

    // Build column mapping from PG TupleDesc to Arrow schema
    let tupdesc = (*(*node).ss.ss_ScanTupleSlot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;

    // Collect physical column names for projection (sent to KalamDB backend).
    // _userid is synthesized locally; _seq is injected by the backend automatically.
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

    // Extract pushdown-safe equality filters from plan quals.
    // PG still applies all quals locally, so this is a pure optimization.
    let pushdown_filters = extract_pushdown_filters(node);

    let request = kalam_pg_api::ScanRequest {
        table_id: table_options.table_id.clone(),
        table_type: table_options.table_type,
        tenant_context,
        remote_session: None,
        projection,
        filters: pushdown_filters,
        limit: None,
    };
    request.validate()?;

    let response = runtime.block_on(async { executor.scan(request).await })?;

    // Build column mapping: PG attnum -> Arrow column index
    let arrow_schema = response.batches.first().map(|b| b.schema());

    let mut column_mapping = Vec::with_capacity(natts);
    for i in 0..natts {
        let att = (*tupdesc).attrs.as_ptr().add(i);
        let col_name = CStr::from_ptr((*att).attname.data.as_ptr()).to_string_lossy();

        // _userid is synthesized from the session; skip Arrow mapping.
        if col_name == USER_ID_COLUMN {
            column_mapping.push(None);
            continue;
        }

        // _seq and user columns are mapped from Arrow.
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

// ---------------------------------------------------------------------------
// WHERE clause pushdown: extract simple `column = value` predicates
// ---------------------------------------------------------------------------

/// Extract simple `column = constant` filters from plan quals for remote pushdown.
///
/// Only extracts filters that are safe to push to the remote server:
/// - Equality operator (`=`)
/// - One side is a column reference (Var)
/// - Other side is a Const or resolved external Param
///
/// PG still applies all quals locally, so incomplete extraction is safe —
/// it only reduces the data transferred from the remote server.
unsafe fn extract_pushdown_filters(node: *mut pg_sys::ForeignScanState) -> Vec<ScanFilter> {
    let mut filters = Vec::new();

    let qual_list = (*(*node).ss.ps.plan).qual;
    if qual_list.is_null() {
        return filters;
    }

    let tupdesc = (*(*node).ss.ss_ScanTupleSlot).tts_tupleDescriptor;

    // Parameter list for resolving $1, $2, ... in prepared statements
    let param_list = if !(*node).ss.ps.state.is_null() {
        (*(*node).ss.ps.state).es_param_list_info
    } else {
        std::ptr::null_mut()
    };

    let length = (*qual_list).length as usize;
    for i in 0..length {
        let element = (*qual_list).elements.add(i);
        let expr_node = (*element).ptr_value as *mut pg_sys::Node;

        if let Some(filter) = try_extract_eq_filter(expr_node, tupdesc, param_list) {
            filters.push(filter);
        }
    }

    filters
}

/// Try to extract an equality filter from a single qual expression.
///
/// Returns `Some(ScanFilter::Eq { ... })` for `column = value` patterns,
/// `None` for anything else (complex expressions, non-equality ops, etc.).
unsafe fn try_extract_eq_filter(
    node: *mut pg_sys::Node,
    tupdesc: pg_sys::TupleDesc,
    params: pg_sys::ParamListInfo,
) -> Option<ScanFilter> {
    if (*node).type_ != pg_sys::NodeTag::T_OpExpr {
        return None;
    }

    let opexpr = node as *mut pg_sys::OpExpr;

    // Verify this is an equality operator by checking the operator name.
    let opname_ptr = pg_sys::get_opname((*opexpr).opno);
    if opname_ptr.is_null() {
        return None;
    }
    let opname = CStr::from_ptr(opname_ptr).to_str().ok()?;
    if opname != "=" {
        return None;
    }

    // Must have exactly 2 arguments (binary operator)
    let args = (*opexpr).args;
    if args.is_null() || (*args).length != 2 {
        return None;
    }

    let first = (*(*args).elements.add(0)).ptr_value as *mut pg_sys::Node;
    let second = (*(*args).elements.add(1)).ptr_value as *mut pg_sys::Node;

    // Try both orderings: Var = Value and Value = Var
    try_var_value_pair(first, second, tupdesc, params)
        .or_else(|| try_var_value_pair(second, first, tupdesc, params))
}

/// Try to match a (Var, Const|Param) pair and extract column name + value.
unsafe fn try_var_value_pair(
    maybe_var: *mut pg_sys::Node,
    maybe_value: *mut pg_sys::Node,
    tupdesc: pg_sys::TupleDesc,
    params: pg_sys::ParamListInfo,
) -> Option<ScanFilter> {
    if (*maybe_var).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }

    let var = maybe_var as *mut pg_sys::Var;

    // Get column name from attribute number
    let attnum = (*var).varattno;
    if attnum <= 0 || attnum as i32 > (*tupdesc).natts {
        return None;
    }
    let att = (*tupdesc).attrs.as_ptr().add((attnum - 1) as usize);
    if (*att).attisdropped {
        return None;
    }
    let col_name = CStr::from_ptr((*att).attname.data.as_ptr()).to_string_lossy().into_owned();

    // Skip virtual columns — they are handled separately by the FDW
    match col_name.as_str() {
        USER_ID_COLUMN | SEQ_COLUMN | DELETED_COLUMN => return None,
        _ => {},
    }

    // Extract scalar value from Const or external Param node
    let scalar = extract_node_value(maybe_value, params)?;

    Some(ScanFilter::eq(col_name, scalar))
}

/// Convert a Const or external Param node to a DataFusion ScalarValue.
unsafe fn extract_node_value(
    node: *mut pg_sys::Node,
    params: pg_sys::ParamListInfo,
) -> Option<ScalarValue> {
    match (*node).type_ {
        pg_sys::NodeTag::T_Const => {
            let konst = node as *mut pg_sys::Const;
            if (*konst).constisnull {
                return None;
            }
            Some(crate::pg_to_kalam::datum_to_scalar(
                (*konst).constvalue,
                (*konst).consttype,
                false,
            ))
        },
        pg_sys::NodeTag::T_Param => {
            let param = node as *mut pg_sys::Param;
            // Only external params (from prepared statements: $1, $2, ...)
            if (*param).paramkind != pg_sys::ParamKind::PARAM_EXTERN {
                return None;
            }
            if params.is_null() {
                return None;
            }
            let param_id = (*param).paramid;
            if param_id < 1 || param_id > (*params).numParams {
                return None;
            }
            let prm = &*(*params).params.as_ptr().add((param_id - 1) as usize);
            if prm.isnull {
                return None;
            }
            Some(crate::pg_to_kalam::datum_to_scalar(prm.value, (*param).paramtype, false))
        },
        _ => None,
    }
}
