use crate::EmbeddedExtensionState;
use kalam_pg_common::{EmbeddedRuntimeConfig, KalamPgError};
use std::sync::{Arc, OnceLock, RwLock};

fn embedded_state_cell() -> &'static RwLock<Option<Arc<EmbeddedExtensionState>>> {
    static EMBEDDED_STATE: OnceLock<RwLock<Option<Arc<EmbeddedExtensionState>>>> = OnceLock::new();
    EMBEDDED_STATE.get_or_init(|| RwLock::new(None))
}

/// Bootstrap the singleton embedded KalamDB runtime for the PostgreSQL extension.
pub fn bootstrap_embedded_extension_state(
    runtime_config: EmbeddedRuntimeConfig,
) -> Result<Arc<EmbeddedExtensionState>, KalamPgError> {
    if let Some(existing_state) = current_embedded_extension_state()? {
        return Ok(existing_state);
    }

    let embedded_state = Arc::new(EmbeddedExtensionState::bootstrap(runtime_config)?);
    let state_cell = embedded_state_cell();
    let mut state_guard =
        state_cell.write().map_err(|err| KalamPgError::Execution(err.to_string()))?;

    if let Some(existing_state) = state_guard.as_ref() {
        return Ok(Arc::clone(existing_state));
    }

    *state_guard = Some(Arc::clone(&embedded_state));
    Ok(embedded_state)
}

/// Return the currently bootstrapped embedded runtime state, if one exists.
pub fn current_embedded_extension_state(
) -> Result<Option<Arc<EmbeddedExtensionState>>, KalamPgError> {
    let state_guard = embedded_state_cell()
        .read()
        .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    Ok(state_guard.as_ref().map(Arc::clone))
}
