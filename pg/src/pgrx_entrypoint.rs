use kalam_pg_common::USER_ID_GUC;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use std::ffi::{c_void, CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "e2e")]
use arrow::array::StringArray;

#[cfg(feature = "e2e")]
#[derive(serde::Serialize)]
struct ConversionProbeResult {
    matched: bool,
    allocations: crate::test_alloc::AllocationSnapshot,
    counters: crate::conversion_test_stats::ConversionTestStats,
}

#[cfg(feature = "e2e")]
fn reset_conversion_probe_state() {
    crate::test_alloc::reset();
    crate::conversion_test_stats::reset();
}

#[cfg(feature = "e2e")]
fn conversion_probe_result(matched: bool) -> String {
    serde_json::to_string(&ConversionProbeResult {
        matched,
        allocations: crate::test_alloc::snapshot(),
        counters: crate::conversion_test_stats::snapshot(),
    })
    .expect("serialize conversion probe result")
}

#[cfg(feature = "e2e")]
fn normalize_jsonb_text(value: &str) -> String {
    let cstr = CString::new(value).expect("jsonb probe input must not contain interior nulls");
    let datum = unsafe {
        pg_sys::OidInputFunctionCall(
            pg_sys::Oid::from(pg_sys::F_JSONB_IN),
            cstr.as_ptr() as *mut _,
            pg_sys::Oid::INVALID,
            -1,
        )
    };
    let output_cstr = unsafe {
        pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(pg_sys::F_JSONB_OUT), datum)
    };
    let text = unsafe {
        CStr::from_ptr(output_cstr)
            .to_str()
            .expect("jsonb output should be utf-8")
            .to_owned()
    };
    unsafe { pg_sys::pfree(output_cstr as *mut c_void) };
    text
}

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
        GucContext::Suset,
        GucFlags::default(),
    );

    // Register ProcessUtility hook for DDL propagation.
    crate::fdw_ddl::register_hook();
}

/// Return the extension crate version exposed through PostgreSQL.
#[pg_extern]
pub fn kalam_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Return the compile-time backend mode baked into the extension.
#[pg_extern]
pub fn kalam_compiled_mode() -> &'static str {
    "remote"
}

/// Return the configured `kalam.user_id` value for the current PostgreSQL session.
#[pg_extern]
pub fn kalam_user_id() -> Option<String> {
    current_kalam_user_id()
}

/// Return the exact PostgreSQL GUC name used for KalamDB tenant context.
#[pg_extern]
pub fn kalam_user_id_guc_name() -> &'static str {
    USER_ID_GUC
}

// Monotonic counter for SNOWFLAKE_ID to guarantee uniqueness within a PG backend.
static SNOWFLAKE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Generate a KalamDB-compatible snowflake ID.
///
/// Layout (63 usable bits, always positive):
///   - Bits 22..62: milliseconds since 2020-01-01 (covers ~139 years)
///   - Bits 12..21: lower 10 bits of PG backend PID (node disambiguation)
///   - Bits  0..11: per-backend monotonic sequence (4096 IDs/ms before wrap)
#[pg_extern]
pub fn snowflake_id() -> i64 {
    // Custom epoch: 2020-01-01T00:00:00Z in milliseconds
    const EPOCH_MS: u64 = 1_577_836_800_000;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64;
    let ts = now_ms.saturating_sub(EPOCH_MS);

    let pid = std::process::id() as u64;
    let seq = SNOWFLAKE_SEQ.fetch_add(1, Ordering::Relaxed);

    let id = ((ts & 0x1FF_FFFF_FFFF) << 22) | ((pid & 0x3FF) << 12) | (seq & 0xFFF);
    id as i64
}

/// Read the active PostgreSQL GUC value for `kalam.user_id`.
pub fn current_kalam_user_id() -> Option<String> {
    KALAM_USER_ID_SETTING
        .get()
        .map(|value| value.to_string_lossy().into_owned())
}

/// Execute an arbitrary SQL statement on the connected KalamDB server and return the
/// results as a JSON array string: `[{"col": value, ...}, ...]`.
///
/// For DDL/DML statements the message is returned (e.g. `"OK (affected: 1)"`).
///
/// Requires an active `kalam_server` foreign server (as set up for the FDW).
///
/// Example:
/// ```sql
/// SELECT kalam_exec('SELECT * FROM system.users');
/// -- returns: [{"user_id":"u_admin","username":"admin",...}, ...]
/// ```
#[pg_extern]
pub fn kalam_exec(sql: &str) -> String {
    use crate::fdw_options::parse_options;
    use kalam_pg_common::KalamPgError;
    use kalam_pg_fdw::ServerOptions;

    const DEFAULT_SERVER: &str = "kalam_server";

    let c_name = std::ffi::CString::new(DEFAULT_SERVER).unwrap_or_default();
    let server = unsafe { pgrx::pg_sys::GetForeignServerByName(c_name.as_ptr(), true) };
    if server.is_null() {
        pgrx::error!(
            "kalam_exec: foreign server '{}' not found – create it first with CREATE SERVER",
            DEFAULT_SERVER
        );
    }
    let server_options = parse_options(unsafe { (*server).options });
    let parsed = match ServerOptions::parse(&server_options) {
        Ok(parsed) => parsed,
        Err(e) => pgrx::error!("kalam_exec: failed to parse server options: {}", e),
    };
    let remote_config = match parsed.remote {
        Some(config) => config,
        None => pgrx::error!("kalam_exec: foreign server must have host and port options"),
    };
    let state = match crate::remote_state::ensure_remote_extension_state(remote_config) {
        Ok(state) => state,
        Err(e) => pgrx::error!("kalam_exec: failed to connect to KalamDB: {}", e),
    };

    let result: Result<(String, Vec<String>), KalamPgError> = state.runtime().block_on(async {
        state.client().execute_query(sql, state.session_id()).await
    });

    match result {
        Ok((_message, rows)) if !rows.is_empty() => {
            format!("[{}]", rows.join(","))
        }
        Ok((message, _)) => {
            // DDL/DML or empty SELECT — return status as JSON string
            serde_json::json!(message).to_string()
        }
        Err(_e) => {
            pgrx::error!("kalam_exec: remote query failed");
        }
    }
}

// Revoke PUBLIC access to kalam_exec — only superusers / explicitly granted
// roles should be able to send arbitrary SQL to a KalamDB server.
pgrx::extension_sql!(
    r#"
REVOKE EXECUTE ON FUNCTION kalam_exec(text) FROM PUBLIC;
"#,
    name = "kalam_exec_revoke",
    finalize,
);

#[cfg(feature = "e2e")]
#[pg_extern]
pub fn kalam_test_probe_text_to_pg(value: &str) -> String {
    let array = StringArray::from(vec![value]);

    reset_conversion_probe_state();
    let (datum, is_null) = unsafe { crate::arrow_to_pg::arrow_value_to_datum(&array, 0, pg_sys::TEXTOID) };
    let matched = !is_null
        && unsafe { pgrx::text_to_rust_str_unchecked(datum.cast_mut_ptr::<pg_sys::varlena>()) == value };

    conversion_probe_result(matched)
}

#[cfg(feature = "e2e")]
#[pg_extern]
pub fn kalam_test_probe_jsonb_to_pg(value: &str) -> String {
    let array = StringArray::from(vec![value]);
    let expected = normalize_jsonb_text(value);

    reset_conversion_probe_state();
    let (datum, is_null) = unsafe { crate::arrow_to_pg::arrow_value_to_datum(&array, 0, pg_sys::JSONBOID) };
    let output_cstr = unsafe {
        pg_sys::OidOutputFunctionCall(pg_sys::Oid::from(pg_sys::F_JSONB_OUT), datum)
    };
    let matched = !is_null
        && unsafe {
            CStr::from_ptr(output_cstr)
                .to_str()
                .expect("jsonb output should be utf-8")
                == expected
        };
    unsafe { pg_sys::pfree(output_cstr as *mut c_void) };

    conversion_probe_result(matched)
}

#[cfg(feature = "e2e")]
#[pg_extern]
pub fn kalam_test_probe_json_to_scalar(value: &str) -> String {
    use pgrx::{datum::JsonString, IntoDatum};

    let datum = JsonString(value.to_owned())
        .into_datum()
        .expect("json datum should be created");

    reset_conversion_probe_state();
    let scalar = unsafe { crate::pg_to_kalam::datum_to_scalar(datum, pg_sys::JSONOID, false) };
    let matched = matches!(
        scalar,
        datafusion_common::ScalarValue::Utf8(Some(text)) if text == value
    );

    conversion_probe_result(matched)
}

#[cfg(feature = "e2e")]
#[pg_extern]
pub fn kalam_test_probe_jsonb_to_scalar(value: &str) -> String {
    use pgrx::{IntoDatum, JsonB};

    let expected = normalize_jsonb_text(value);
    let parsed = serde_json::from_str::<serde_json::Value>(value).expect("valid jsonb text");
    let datum = JsonB(parsed)
        .into_datum()
        .expect("jsonb datum should be created");

    reset_conversion_probe_state();
    let scalar = unsafe { crate::pg_to_kalam::datum_to_scalar(datum, pg_sys::JSONBOID, false) };
    let matched = matches!(
        scalar,
        datafusion_common::ScalarValue::Utf8(Some(text)) if text == expected
    );

    conversion_probe_result(matched)
}

