//! PostgreSQL extension entrypoint — remote-only mode.

mod remote_executor;
mod remote_state;
mod session_settings;

// FDW callback layer
mod arrow_to_pg;
mod fdw_ddl;
mod fdw_handler;
mod fdw_import;
mod fdw_modify;
mod fdw_options;
mod fdw_scan;
mod fdw_state;
mod fdw_xact;
mod pg_to_kalam;
mod relation_table_options;
mod write_buffer;

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
