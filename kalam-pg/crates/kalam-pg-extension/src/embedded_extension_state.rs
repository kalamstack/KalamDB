use crate::embedded_executor_factory::EmbeddedExecutorFactory;
use crate::import_foreign_schema_service::{
    ImportForeignSchemaRequest, ImportForeignSchemaService,
};
use crate::ExecutorFactory;
use kalam_pg_api::KalamBackendExecutor;
use kalam_pg_common::{EmbeddedRuntimeConfig, KalamPgError};
use kalam_pg_embedded::EmbeddedKalamRuntime;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::executor::SqlExecutor;
use std::sync::Arc;

/// Embedded extension state shared by future `pgrx` callbacks.
pub struct EmbeddedExtensionState {
    runtime: Arc<EmbeddedKalamRuntime>,
    executor_factory: EmbeddedExecutorFactory,
    import_service: ImportForeignSchemaService,
}

impl EmbeddedExtensionState {
    /// Create embedded extension state from an existing shared AppContext.
    pub fn new(app_context: Arc<AppContext>) -> Result<Self, KalamPgError> {
        ensure_sql_executor(&app_context);
        let runtime = Arc::new(EmbeddedKalamRuntime::from_app_context(Arc::clone(&app_context))?);
        Ok(Self {
            executor_factory: EmbeddedExecutorFactory::new(Arc::clone(&runtime)),
            runtime,
            import_service: ImportForeignSchemaService::new(app_context),
        })
    }

    /// Bootstrap a fresh embedded KalamDB runtime for the PostgreSQL extension.
    pub fn bootstrap(runtime_config: EmbeddedRuntimeConfig) -> Result<Self, KalamPgError> {
        let runtime = Arc::new(EmbeddedKalamRuntime::bootstrap(runtime_config)?);
        let app_context = Arc::clone(runtime.app_context());

        Ok(Self {
            executor_factory: EmbeddedExecutorFactory::new(Arc::clone(&runtime)),
            runtime,
            import_service: ImportForeignSchemaService::new(app_context),
        })
    }

    /// Build the active backend executor for scan and modify callbacks.
    pub fn executor(&self) -> Result<Arc<dyn KalamBackendExecutor>, KalamPgError> {
        self.executor_factory.build()
    }

    /// Access the embedded runtime owned by the extension state.
    pub fn runtime(&self) -> &Arc<EmbeddedKalamRuntime> {
        &self.runtime
    }

    /// Generate foreign-table SQL statements for `IMPORT FOREIGN SCHEMA`.
    pub fn import_foreign_schema_sql(
        &self,
        request: &ImportForeignSchemaRequest,
    ) -> Result<Vec<String>, KalamPgError> {
        self.import_service.generate_sql(request)
    }
}

fn ensure_sql_executor(app_context: &Arc<AppContext>) {
    if app_context.try_sql_executor().is_none() {
        app_context.set_sql_executor(Arc::new(SqlExecutor::new(Arc::clone(app_context), false)));
    }
}
