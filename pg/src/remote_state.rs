use std::sync::Arc;

use std::sync::OnceLock;

use kalam_pg_api::KalamBackendExecutor;
use kalam_pg_client::RemoteKalamClient;
use kalam_pg_common::{KalamPgError, RemoteServerConfig};
use pgrx::pg_sys;

use crate::remote_executor::RemoteBackendExecutor;

/// Global state holding the remote connection + tokio runtime for the PostgreSQL extension
/// in remote mode.
pub struct RemoteExtensionState {
    client: RemoteKalamClient,
    runtime: Arc<tokio::runtime::Runtime>,
    session_id: String,
}

impl RemoteExtensionState {
    pub fn executor(&self) -> Result<Arc<dyn KalamBackendExecutor>, KalamPgError> {
        Ok(Arc::new(RemoteBackendExecutor::new(
            self.client.clone(),
            self.session_id.clone(),
        )))
    }

    pub fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self.runtime
    }

    pub fn client(&self) -> &RemoteKalamClient {
        &self.client
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

/// Per-process singleton for the remote extension state.
static REMOTE_STATE: OnceLock<RemoteExtensionState> = OnceLock::new();

/// Return the remote extension state if already initialized.
pub fn get_remote_extension_state() -> Option<&'static RemoteExtensionState> {
    REMOTE_STATE.get()
}

/// Bootstrap or retrieve the remote extension state.
///
/// The `host`, `port`, and `auth_header` are parsed from the foreign server OPTIONS.
pub fn ensure_remote_extension_state(
    config: RemoteServerConfig,
) -> Result<&'static RemoteExtensionState, KalamPgError> {
    // OnceLock doesn't have get_or_try_init on stable yet, so use get_or_init
    // with an inner Result to handle errors.
    if let Some(state) = REMOTE_STATE.get() {
        return Ok(state);
    }
    let state = (|| -> Result<RemoteExtensionState, KalamPgError> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| KalamPgError::Execution(format!("failed to build tokio runtime: {}", e)))?,
        );

        let client = runtime.block_on(async {
            RemoteKalamClient::connect(config).await
        })?;

        // Generate a session id based on PG backend PID for session reuse.
        let session_id = format!("pg-{}", std::process::id());
        runtime.block_on(async {
            client.open_session(&session_id, None).await
        })?;

        // Register a PostgreSQL process-exit callback to close the session
        // when this backend shuts down (disconnect, idle timeout, etc.).
        unsafe {
            pg_sys::on_proc_exit(Some(on_proc_exit_close_session), pg_sys::Datum::from(0));
        }

        Ok(RemoteExtensionState {
            client,
            runtime,
            session_id,
        })
    })()?;
    Ok(REMOTE_STATE.get_or_init(|| state))
}

/// PostgreSQL process-exit callback that closes the KalamDB session.
///
/// # Safety
/// Called by PostgreSQL at backend process exit. Must not panic through FFI.
unsafe extern "C-unwind" fn on_proc_exit_close_session(_code: i32, _arg: pg_sys::Datum) {
    let Some(state) = REMOTE_STATE.get() else {
        return;
    };

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        state.runtime.block_on(async {
            state.client.close_session(&state.session_id).await
        })
    }));

    match result {
        Ok(Ok(())) => {
            eprintln!("pg_kalam: session {} closed", state.session_id);
        }
        Ok(Err(e)) => {
            eprintln!(
                "pg_kalam: failed to close session {}: {}",
                state.session_id, e
            );
        }
        Err(_panic) => {
            eprintln!(
                "pg_kalam: panic closing session {}",
                state.session_id,
            );
        }
    }
}
