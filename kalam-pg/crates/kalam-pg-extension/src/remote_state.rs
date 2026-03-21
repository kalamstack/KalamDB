use std::sync::Arc;

use kalam_pg_api::KalamBackendExecutor;
use kalam_pg_client::RemoteKalamClient;
use kalam_pg_common::{KalamPgError, RemoteServerConfig};
use once_cell::sync::OnceCell;

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
}

/// Per-process singleton for the remote extension state.
static REMOTE_STATE: OnceCell<RemoteExtensionState> = OnceCell::new();

/// Bootstrap or retrieve the remote extension state.
///
/// The `host`, `port`, and `auth_header` are parsed from the foreign server OPTIONS.
pub fn ensure_remote_extension_state(
    config: RemoteServerConfig,
    auth_header: Option<String>,
) -> Result<&'static RemoteExtensionState, KalamPgError> {
    REMOTE_STATE.get_or_try_init(|| {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| KalamPgError::Execution(format!("failed to build tokio runtime: {}", e)))?,
        );

        let client = runtime.block_on(async {
            RemoteKalamClient::connect(config, auth_header).await
        })?;

        // Generate a session id based on PG backend PID for session reuse.
        let session_id = format!("pg-{}", std::process::id());
        runtime.block_on(async {
            client.open_session(&session_id, None).await
        })?;

        Ok(RemoteExtensionState {
            client,
            runtime,
            session_id,
        })
    })
}
