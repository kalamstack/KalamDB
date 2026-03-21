//! PostgreSQL extension entrypoint and backend wiring.

#[cfg(all(feature = "remote", feature = "embedded"))]
compile_error!("Enable only one backend mode: 'remote' or 'embedded'.");

#[cfg(feature = "embedded")]
mod embedded_extension_state;
#[cfg(feature = "embedded")]
mod embedded_fdw_service;
#[cfg(feature = "embedded")]
mod embedded_runtime_registry;
#[cfg(feature = "embedded")]
mod embedded_sql_service;
mod executor_factory;
#[cfg(feature = "embedded")]
mod import_foreign_schema_service;
#[cfg(feature = "remote")]
mod remote_executor;
#[cfg(feature = "remote")]
mod remote_state;
mod session_settings;

#[cfg(feature = "embedded")]
mod embedded_executor_factory;

// FDW callback layer
mod arrow_to_pg;
#[cfg(feature = "embedded")]
mod ddl_event;
#[cfg(feature = "embedded")]
mod ddl_orchestration;
mod fdw_handler;
mod fdw_import;
mod fdw_modify;
mod fdw_options;
mod fdw_scan;
mod fdw_state;
mod pg_to_kalam;
#[cfg(feature = "embedded")]
mod schema_management;

pub use executor_factory::ExecutorFactory;

#[cfg(feature = "embedded")]
pub use embedded_executor_factory::EmbeddedExecutorFactory;
#[cfg(feature = "embedded")]
pub use embedded_extension_state::EmbeddedExtensionState;
#[cfg(feature = "embedded")]
pub use embedded_fdw_service::EmbeddedFdwService;
#[cfg(feature = "embedded")]
pub use embedded_runtime_registry::{
    bootstrap_embedded_extension_state, current_embedded_extension_state,
};
#[cfg(feature = "embedded")]
pub use embedded_sql_service::EmbeddedSqlService;
#[cfg(feature = "embedded")]
pub use import_foreign_schema_service::{ImportForeignSchemaRequest, ImportForeignSchemaService};
pub use session_settings::SessionSettings;

include!("pgrx_entrypoint.rs");

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
