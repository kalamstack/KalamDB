use crate::executor_factory::ExecutorFactory;
use kalam_pg_api::KalamBackendExecutor;
use kalam_pg_common::KalamPgError;
use kalam_pg_embedded::EmbeddedKalamRuntime;
use std::sync::Arc;

/// Embedded-mode executor factory backed by an in-process Kalam runtime.
pub struct EmbeddedExecutorFactory {
    runtime: Arc<EmbeddedKalamRuntime>,
}

impl EmbeddedExecutorFactory {
    /// Create a factory from an embedded runtime instance.
    pub fn new(runtime: Arc<EmbeddedKalamRuntime>) -> Self {
        Self { runtime }
    }
}

impl ExecutorFactory for EmbeddedExecutorFactory {
    fn build(&self) -> Result<Arc<dyn KalamBackendExecutor>, KalamPgError> {
        Ok(Arc::clone(&self.runtime) as Arc<dyn KalamBackendExecutor>)
    }
}
