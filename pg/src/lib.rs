//! PostgreSQL extension entrypoint — remote-only mode.

#[cfg(any(not(test), feature = "pg_test"))]
mod remote_executor;
#[cfg(any(not(test), feature = "pg_test"))]
mod remote_state;
mod session_settings;

// FDW callback layer
#[cfg(any(not(test), feature = "pg_test"))]
mod arrow_to_pg;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_ddl;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_handler;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_import;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_modify;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_options;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_scan;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_state;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_xact;
#[cfg(any(not(test), feature = "pg_test"))]
mod pg_to_kalam;
#[cfg(any(not(test), feature = "pg_test"))]
mod relation_table_options;
#[cfg(any(not(test), feature = "pg_test"))]
mod write_buffer;

pub use session_settings::SessionSettings;

// Plain cargo test/nextest lib builds do not run inside a PostgreSQL backend.
// Keep that target limited to the pure-Rust surface so `--all-targets` does not
// try to link Postgres symbols. Normal builds and `cargo pgrx test` still compile
// the full extension surface.
#[cfg(any(not(test), feature = "pg_test"))]
include!("pgrx_entrypoint.rs");

#[cfg(all(test, not(feature = "pg_test")))]
pub fn kalam_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(all(test, not(feature = "pg_test")))]
pub fn kalam_compiled_mode() -> &'static str {
    "remote"
}

#[cfg(all(test, not(feature = "pg_test")))]
pub fn kalam_user_id_guc_name() -> &'static str {
    kalam_pg_common::USER_ID_GUC
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
