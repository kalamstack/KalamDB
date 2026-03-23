//! FDW transaction lifecycle hooks.
//!
//! Registers PostgreSQL transaction callbacks (`RegisterXactCallback`) to
//! lazily begin a KalamDB transaction on first FDW operation and finalize it
//! (commit or rollback) when the PostgreSQL transaction ends.
//!
//! This module implements Phase 1 of the transactional FDW spec:
//! - Lazy transaction start on first foreign operation
//! - Commit on PostgreSQL COMMIT
//! - Rollback on PostgreSQL ABORT
//!
//! Note: True rollback of already-applied writes requires MVCC on the KalamDB
//! side (future work). Phase 1 tracks transaction state and coordinates the
//! lifecycle so the RPC contract is in place.

use std::sync::Mutex;

use pgrx::pg_sys;

/// Per-backend transaction state for the current PostgreSQL transaction.
///
/// This is stored in a process-global static because each PG backend is a
/// single-threaded process.
static CURRENT_TX: Mutex<Option<ActiveTransaction>> = Mutex::new(None);

/// Whether we have already registered the xact callback for this backend.
static XACT_CALLBACK_REGISTERED: Mutex<bool> = Mutex::new(false);

/// State for the active KalamDB transaction in this PostgreSQL backend.
struct ActiveTransaction {
    session_id: String,
    transaction_id: String,
}

/// Ensure that a KalamDB transaction is active for the current PG transaction.
///
/// Called lazily from FDW scan/modify operations. If no transaction is active,
/// begins one via the remote client. Returns the transaction ID.
///
/// # Safety
/// Must be called from within a PostgreSQL transaction context.
pub fn ensure_transaction(session_id: &str) -> Result<String, kalam_pg_common::KalamPgError> {
    // Check if we already have an active transaction
    {
        let guard = CURRENT_TX.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ref tx) = *guard {
            return Ok(tx.transaction_id.clone());
        }
    }

    // Register xact callback if not already registered
    register_xact_callback();

    // Begin a new transaction via the remote client
    let state = crate::remote_state::get_remote_extension_state()
        .ok_or_else(|| {
            kalam_pg_common::KalamPgError::Execution(
                "remote extension state not initialized".to_string(),
            )
        })?;

    let transaction_id = state.runtime().block_on(async {
        state.client().begin_transaction(session_id).await
    })?;

    let mut guard = CURRENT_TX.lock().unwrap_or_else(|e| e.into_inner());
    *guard = Some(ActiveTransaction {
        session_id: session_id.to_string(),
        transaction_id: transaction_id.clone(),
    });

    Ok(transaction_id)
}

/// Register the PostgreSQL xact callback (idempotent).
fn register_xact_callback() {
    let mut registered = XACT_CALLBACK_REGISTERED
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if *registered {
        return;
    }

    unsafe {
        pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
    }

    *registered = true;
}

/// PostgreSQL transaction callback invoked at commit/abort.
///
/// # Safety
/// Called by PostgreSQL at transaction end. Must not panic through the FFI
/// boundary, so all `block_on()` calls are wrapped in `catch_unwind`.
///
/// IMPORTANT: PostgreSQL fires multiple events per transaction:
///   PRE_COMMIT → COMMIT (normal) or PRE_PREPARE → PREPARE (2PC) or ABORT.
/// We must only consume CURRENT_TX on the final COMMIT/ABORT events.
/// Taking it on PRE_COMMIT would prevent the actual COMMIT handler from
/// seeing the transaction, leaving the server-side transaction dangling.
unsafe extern "C-unwind" fn xact_callback(event: pg_sys::XactEvent::Type, _arg: *mut std::ffi::c_void) {
    // Flush write buffer at PRE_COMMIT (before the transaction commit RPC).
    if matches!(event, pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT) {
        if let Err(e) = crate::write_buffer::flush_all() {
            eprintln!("pg_kalam: failed to flush write buffer at PRE_COMMIT: {}", e);
            // Discard unflushed rows so commit doesn't proceed with partial data
            crate::write_buffer::discard_all();
        }
        return;
    }

    // Only act on final commit/abort events.
    // Ignore PRE_PREPARE, PREPARE — they fire before the actual
    // COMMIT/ABORT and must not consume the transaction state.
    let is_commit = matches!(
        event,
        pg_sys::XactEvent::XACT_EVENT_COMMIT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT
    );
    let is_abort = matches!(
        event,
        pg_sys::XactEvent::XACT_EVENT_ABORT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
    );
    if !is_commit && !is_abort {
        return;
    }

    // On abort, discard any remaining buffered writes
    if is_abort {
        crate::write_buffer::discard_all();
    }

    let tx = {
        let mut guard = CURRENT_TX.lock().unwrap_or_else(|e| e.into_inner());
        guard.take()
    };

    let Some(tx) = tx else {
        return; // No active KalamDB transaction for this PG transaction
    };

    let state = match crate::remote_state::get_remote_extension_state() {
        Some(s) => s,
        None => return, // Remote state not available, nothing to finalize
    };

    if is_commit {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.runtime().block_on(async {
                state
                    .client()
                    .commit_transaction(&tx.session_id, &tx.transaction_id)
                    .await
            })
        }));
        match result {
            Ok(Err(e)) => {
                eprintln!(
                    "pg_kalam: failed to commit KalamDB transaction {}: {}",
                    tx.transaction_id, e
                );
            },
            Err(_panic) => {
                eprintln!(
                    "pg_kalam: panic committing KalamDB transaction {}",
                    tx.transaction_id,
                );
            },
            Ok(Ok(_)) => {},
        }
    } else {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.runtime().block_on(async {
                state
                    .client()
                    .rollback_transaction(&tx.session_id, &tx.transaction_id)
                    .await
            })
        }));
        match result {
            Ok(Err(e)) => {
                eprintln!(
                    "pg_kalam: failed to rollback KalamDB transaction {}: {}",
                    tx.transaction_id, e
                );
            },
            Err(_panic) => {
                eprintln!(
                    "pg_kalam: panic rolling back KalamDB transaction {}",
                    tx.transaction_id,
                );
            },
            Ok(Ok(_)) => {},
        }
    }
}
