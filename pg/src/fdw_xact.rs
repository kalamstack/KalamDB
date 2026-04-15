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

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use pgrx::pg_sys;

/// Per-backend transaction state for the current PostgreSQL transaction.
///
/// This is stored in a process-global static because each PG backend is a
/// single-threaded process.
static CURRENT_TX: LazyLock<Mutex<HashMap<String, ActiveTransaction>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Whether the current PostgreSQL backend is inside an explicit BEGIN/COMMIT
/// block issued by the client.
static EXPLICIT_TX_BLOCK_ACTIVE: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

/// Whether we have already registered the xact callback for this backend.
static XACT_CALLBACK_REGISTERED: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

/// State for an active KalamDB transaction in this PostgreSQL backend.
#[derive(Clone)]
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
        if let Some(tx) = guard.get(session_id) {
            return Ok(tx.transaction_id.clone());
        }
    }

    // Register xact callback if not already registered
    register_xact_callback();

    // Begin a new transaction via the remote client
    let state = crate::remote_state::get_remote_extension_state_for_session(session_id)
        .ok_or_else(|| {
            kalam_pg_common::KalamPgError::Execution(
                "remote extension state not initialized".to_string(),
            )
        })?;

    let transaction_id = state
        .runtime()
        .block_on(async { state.client().begin_transaction(session_id).await })?;

    let mut guard = CURRENT_TX.lock().unwrap_or_else(|e| e.into_inner());
    guard.insert(
        session_id.to_string(),
        ActiveTransaction {
            session_id: session_id.to_string(),
            transaction_id: transaction_id.clone(),
        },
    );

    Ok(transaction_id)
}

pub fn set_explicit_transaction_block(active: bool) {
    let mut guard = EXPLICIT_TX_BLOCK_ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
    *guard = active;
}

pub fn is_explicit_transaction_block_active() -> bool {
    (unsafe { pg_sys::IsTransactionBlock() })
        || *EXPLICIT_TX_BLOCK_ACTIVE.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn has_active_transaction() -> bool {
    !CURRENT_TX.lock().unwrap_or_else(|e| e.into_inner()).is_empty()
}

/// Register the PostgreSQL xact callback (idempotent).
fn register_xact_callback() {
    let mut registered = XACT_CALLBACK_REGISTERED.lock().unwrap_or_else(|e| e.into_inner());
    if *registered {
        return;
    }

    unsafe {
        pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
    }

    *registered = true;
}

fn take_active_transactions() -> Vec<ActiveTransaction> {
    let mut guard = CURRENT_TX.lock().unwrap_or_else(|e| e.into_inner());
    std::mem::take(&mut *guard).into_values().collect()
}

fn active_transactions_snapshot() -> Vec<ActiveTransaction> {
    CURRENT_TX.lock().unwrap_or_else(|e| e.into_inner()).values().cloned().collect()
}

fn clear_active_transactions() {
    CURRENT_TX.lock().unwrap_or_else(|e| e.into_inner()).clear();
}

fn commit_transactions(transactions: &[ActiveTransaction]) -> Result<(), String> {
    for tx in transactions {
        let state = crate::remote_state::get_remote_extension_state_for_session(&tx.session_id)
            .ok_or_else(|| {
                format!("remote extension state not initialized for session '{}'", tx.session_id)
            })?;

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.runtime().block_on(async {
                state.client().commit_transaction(&tx.session_id, &tx.transaction_id).await
            })
        }));

        match result {
            Ok(Ok(_)) => {},
            Ok(Err(error)) => {
                return Err(format!(
                    "failed to commit KalamDB transaction {}: {}",
                    tx.transaction_id, error
                ));
            },
            Err(_panic) => {
                return Err(format!("panic committing KalamDB transaction {}", tx.transaction_id));
            },
        }
    }

    Ok(())
}

fn rollback_transactions(transactions: &[ActiveTransaction]) {
    for tx in transactions {
        let Some(state) =
            crate::remote_state::get_remote_extension_state_for_session(&tx.session_id)
        else {
            continue;
        };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.runtime().block_on(async {
                state.client().rollback_transaction(&tx.session_id, &tx.transaction_id).await
            })
        }));

        match result {
            Ok(Ok(_)) => {},
            Ok(Err(error)) => {
                eprintln!(
                    "pg_kalam: failed to rollback KalamDB transaction {}: {}",
                    tx.transaction_id, error
                );
            },
            Err(_panic) => {
                eprintln!("pg_kalam: panic rolling back KalamDB transaction {}", tx.transaction_id,);
            },
        }
    }
}

fn try_rollback_transactions(transactions: &[ActiveTransaction]) -> Result<(), String> {
    for tx in transactions {
        let state = crate::remote_state::get_remote_extension_state_for_session(&tx.session_id)
            .ok_or_else(|| {
                format!("remote extension state not initialized for session '{}'", tx.session_id)
            })?;

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.runtime().block_on(async {
                state.client().rollback_transaction(&tx.session_id, &tx.transaction_id).await
            })
        }));

        match result {
            Ok(Ok(_)) => {},
            Ok(Err(error)) => {
                return Err(format!(
                    "failed to rollback KalamDB transaction {}: {}",
                    tx.transaction_id, error
                ));
            },
            Err(_panic) => {
                return Err(format!(
                    "panic rolling back KalamDB transaction {}",
                    tx.transaction_id
                ));
            },
        }
    }

    Ok(())
}

pub fn commit_explicit_transaction_block() -> Result<(), String> {
    let transactions = active_transactions_snapshot();
    if transactions.is_empty() {
        set_explicit_transaction_block(false);
        return Ok(());
    }

    crate::write_buffer::flush_all()
        .map_err(|error| format!("failed to flush writes before explicit COMMIT: {}", error))?;

    commit_transactions(&transactions)?;
    clear_active_transactions();
    set_explicit_transaction_block(false);
    Ok(())
}

pub fn rollback_explicit_transaction_block() -> Result<(), String> {
    crate::write_buffer::discard_all();

    let transactions = active_transactions_snapshot();
    if transactions.is_empty() {
        set_explicit_transaction_block(false);
        return Ok(());
    }

    try_rollback_transactions(&transactions)?;
    clear_active_transactions();
    set_explicit_transaction_block(false);
    Ok(())
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
unsafe extern "C-unwind" fn xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    // Finalize remote transactions at PRE_COMMIT so failures can still abort the
    // PostgreSQL transaction before it becomes visible to the client.
    if matches!(event, pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT) {
        let flush_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::write_buffer::flush_all()
        }));
        match flush_result {
            Ok(Ok(())) => {},
            Ok(Err(e)) => {
                let transactions = take_active_transactions();
                crate::write_buffer::discard_all();
                rollback_transactions(&transactions);
                pgrx::error!("pg_kalam: failed to flush writes, aborting transaction: {}", e);
            },
            Err(_panic) => {
                let transactions = take_active_transactions();
                crate::write_buffer::discard_all();
                rollback_transactions(&transactions);
                pgrx::error!("pg_kalam: panic during write flush, aborting transaction");
            },
        }

        let transactions = take_active_transactions();
        if transactions.is_empty() {
            return;
        }

        if let Err(error) = commit_transactions(&transactions) {
            rollback_transactions(&transactions);
            pgrx::error!("pg_kalam: {}, aborting transaction before PostgreSQL commit", error);
        }

        return;
    }

    // Ignore PRE_PREPARE, PREPARE — PRE_COMMIT already finalized any active
    // remote transaction, and ABORT handles the error path.
    let is_abort = matches!(
        event,
        pg_sys::XactEvent::XACT_EVENT_ABORT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
    );
    let is_commit = matches!(
        event,
        pg_sys::XactEvent::XACT_EVENT_COMMIT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT
    );
    if !is_commit && !is_abort {
        return;
    }

    if is_abort {
        set_explicit_transaction_block(false);
        crate::write_buffer::discard_all();
        let transactions = take_active_transactions();
        if !transactions.is_empty() {
            rollback_transactions(&transactions);
        }
        return;
    }

    set_explicit_transaction_block(false);
    let transactions = take_active_transactions();
    if transactions.is_empty() {
        return;
    }

    // PRE_COMMIT should have finalized everything already. If PostgreSQL reaches
    // COMMIT with leftover remote transactions, fall back to best-effort commit
    // to avoid leaking session state, but we can no longer surface the error.
    if let Err(error) = commit_transactions(&transactions) {
        eprintln!(
            "pg_kalam: late COMMIT fallback failed for {} transaction(s): {}",
            transactions.len(),
            error
        );
    }
}
