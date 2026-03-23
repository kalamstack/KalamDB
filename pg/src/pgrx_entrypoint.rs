use kalam_pg_common::USER_ID_GUC;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use std::ffi::{CStr, CString};

static KALAM_USER_ID_SETTING: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None::<&'static CStr>);

::pgrx::pg_module_magic!(name, version);

/// PostgreSQL extension initialization hook that registers the session GUC.
#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"kalam.user_id",
        c"KalamDB session user id",
        c"Session-scoped tenant identifier used by the KalamDB PostgreSQL extension.",
        &KALAM_USER_ID_SETTING,
        GucContext::Userset,
        GucFlags::default(),
    );

    // Register ProcessUtility hook for DDL propagation.
    crate::fdw_ddl::register_hook();
}

/// Return the extension crate version exposed through PostgreSQL.
#[pg_extern]
pub fn pg_kalam_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Return the compile-time backend mode baked into the extension.
#[pg_extern]
pub fn pg_kalam_compiled_mode() -> &'static str {
    "remote"
}

/// Return the configured `kalam.user_id` value for the current PostgreSQL session.
#[pg_extern]
pub fn pg_kalam_user_id() -> Option<String> {
    current_kalam_user_id()
}

/// Return the exact PostgreSQL GUC name used for KalamDB tenant context.
#[pg_extern]
pub fn pg_kalam_user_id_guc_name() -> &'static str {
    USER_ID_GUC
}

/// Read the active PostgreSQL GUC value for `kalam.user_id`.
pub fn current_kalam_user_id() -> Option<String> {
    KALAM_USER_ID_SETTING
        .get()
        .map(|value| value.to_string_lossy().into_owned())
}

