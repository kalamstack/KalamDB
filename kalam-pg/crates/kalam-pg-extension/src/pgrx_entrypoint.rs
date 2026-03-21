use kalam_pg_common::USER_ID_GUC;
#[cfg(feature = "embedded")]
use kalam_pg_common::{EmbeddedRuntimeConfig, KalamPgError};
#[cfg(feature = "embedded")]
use kalamdb_commons::models::UserId;
#[cfg(feature = "embedded")]
use kalamdb_commons::Role;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use pgrx::JsonB;
use std::env;
use std::ffi::{CStr, CString};
#[cfg(feature = "embedded")]
use std::path::PathBuf;
#[cfg(feature = "embedded")]
use std::sync::Arc;

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
}

/// Return the extension crate version exposed through PostgreSQL.
#[pg_extern]
pub fn pg_kalam_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Return the compile-time backend mode baked into the extension.
#[pg_extern]
pub fn pg_kalam_compiled_mode() -> &'static str {
    if cfg!(feature = "embedded") {
        "embedded"
    } else if cfg!(feature = "remote") {
        "remote"
    } else {
        "shell"
    }
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

#[cfg(feature = "embedded")]
#[pg_extern]
pub fn pg_kalam_embedded_start(data_dir: Option<&str>) -> String {
    let embedded_state = ensure_embedded_extension_state(data_dir).unwrap_or_else(extension_error);
    let runtime_config = embedded_state.runtime().runtime_config();
    let http_status = embedded_http_status(embedded_state.runtime().as_ref());

    format!(
        "embedded runtime ready at {} (node_id={}, {})",
        runtime_config.storage_base_path.display(),
        runtime_config.node_id,
        http_status
    )
}

#[cfg(feature = "embedded")]
#[pg_extern]
pub fn pg_kalam_embedded_sql(sql: &str) -> JsonB {
    let embedded_state = ensure_embedded_extension_state(None).unwrap_or_else(extension_error);
    let payload = crate::EmbeddedSqlService::new(embedded_state)
        .execute_json(sql, current_kalam_user_id().map(UserId::new), session_role())
        .unwrap_or_else(extension_error);

    JsonB(payload)
}

#[cfg(feature = "embedded")]
#[pg_extern]
pub fn pg_kalam_embedded_admin_sql(sql: &str) -> JsonB {
    let embedded_state = ensure_embedded_extension_state(None).unwrap_or_else(extension_error);
    let payload = crate::EmbeddedSqlService::new(embedded_state)
        .execute_json(sql, Some(admin_user_id()), Role::System)
        .unwrap_or_else(extension_error);

    JsonB(payload)
}

#[cfg(feature = "embedded")]
#[pg_extern]
pub fn pg_kalam_embedded_status() -> String {
    match crate::current_embedded_extension_state() {
        Ok(Some(embedded_state)) => {
            let runtime_config = embedded_state.runtime().runtime_config();
            let http_status = embedded_http_status(embedded_state.runtime().as_ref());
            format!(
                "initialized=true path={} node_id={} {}",
                runtime_config.storage_base_path.display(),
                runtime_config.node_id,
                http_status
            )
        }
        Ok(None) => {
            let runtime_config =
                embedded_runtime_config(None).unwrap_or_else(extension_error);
            let http_status = if runtime_config.http.enabled {
                format!(
                    "http=configured host={} port={}",
                    runtime_config.http.host, runtime_config.http.port
                )
            } else {
                "http=disabled".to_string()
            };
            format!(
                "initialized=false path={} node_id={} {}",
                runtime_config.storage_base_path.display(),
                runtime_config.node_id,
                http_status
            )
        }
        Err(err) => extension_error(err),
    }
}

#[cfg(feature = "embedded")]
fn embedded_runtime_config(data_dir: Option<&str>) -> Result<EmbeddedRuntimeConfig, KalamPgError> {
    let mut runtime_config = EmbeddedRuntimeConfig::default();
    runtime_config.storage_base_path = match data_dir.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => PathBuf::from(value),
        None => default_embedded_data_dir()?,
    };
    runtime_config.node_id = "1".to_string();
    runtime_config.http.enabled = env_flag("KALAM_PG_HTTP_ENABLED");
    runtime_config.http.host = env::var("KALAM_PG_HTTP_HOST")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| runtime_config.http.host.clone());
    runtime_config.http.port = env::var("KALAM_PG_HTTP_PORT")
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .unwrap_or(runtime_config.http.port);
    Ok(runtime_config)
}

#[cfg(feature = "embedded")]
fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(feature = "embedded")]
fn embedded_http_status(runtime: &kalam_pg_embedded::EmbeddedKalamRuntime) -> String {
    if let Some(base_url) = runtime.http_base_url() {
        format!("http=ready url={}", base_url)
    } else if runtime.runtime_config().http.enabled {
        format!(
            "http=configured host={} port={}",
            runtime.runtime_config().http.host,
            runtime.runtime_config().http.port
        )
    } else {
        "http=disabled".to_string()
    }
}

#[cfg(feature = "embedded")]
pub(crate) fn ensure_embedded_extension_state(
    data_dir: Option<&str>,
) -> Result<Arc<crate::EmbeddedExtensionState>, KalamPgError> {
    if let Some(embedded_state) = crate::current_embedded_extension_state()? {
        return Ok(embedded_state);
    }

    let runtime_config = embedded_runtime_config(data_dir)?;
    crate::bootstrap_embedded_extension_state(runtime_config)
}

#[cfg(feature = "embedded")]
fn default_embedded_data_dir() -> Result<PathBuf, KalamPgError> {
    let data_dir = unsafe {
        if pgrx::pg_sys::DataDir.is_null() {
            return Err(KalamPgError::Execution(
                "PostgreSQL DataDir is not initialized".to_string(),
            ));
        }

        CStr::from_ptr(pgrx::pg_sys::DataDir)
            .to_str()
            .map_err(|err| KalamPgError::Execution(err.to_string()))?
            .to_string()
    };

    Ok(PathBuf::from(data_dir).join("kalamdb"))
}

#[cfg(feature = "embedded")]
fn admin_user_id() -> UserId {
    UserId::new("postgres_extension")
}

#[cfg(feature = "embedded")]
fn session_role() -> Role {
    if current_kalam_user_id().is_some() {
        Role::User
    } else {
        Role::Anonymous
    }
}

#[cfg(feature = "embedded")]
fn extension_error<T>(err: impl std::fmt::Display) -> T {
    panic!("{}", err);
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    #[cfg(feature = "embedded")]
    use pgrx::Spi;
    #[cfg(feature = "embedded")]
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(feature = "embedded")]
    fn ensure_runtime_and_server() {
        crate::pg_kalam_embedded_start(None);

        Spi::run(
            r#"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_foreign_server
        WHERE srvname = 'kalam_server'
    ) THEN
        CREATE SERVER kalam_server FOREIGN DATA WRAPPER pg_kalam;
    END IF;
END;
$$;
"#,
        )
        .expect("create kalam_server foreign server");
    }

    #[cfg(feature = "embedded")]
    fn cleanup_schema(schema_name: &str, tables: &[&str]) {
        ensure_runtime_and_server();

        for table_name in tables {
            let sql = format!("DROP TABLE IF EXISTS {schema_name}.{table_name}");
            let _ = crate::pg_kalam_embedded_admin_sql(&sql);
        }

        let drop_schema_sql = format!("DROP SCHEMA IF EXISTS \"{schema_name}\" CASCADE");
        let _ = Spi::run(&drop_schema_sql);

        let unregister_sql = format!(
            "DELETE FROM kalam._managed_schemas WHERE schema_name = '{}'",
            schema_name
        );
        let _ = Spi::run(&unregister_sql);
    }

    #[cfg(feature = "embedded")]
    fn run_sql(sql: &str) {
        Spi::run(sql).unwrap_or_else(|err| panic!("SPI failed for `{sql}`: {err}"));
    }

    #[cfg(feature = "embedded")]
    fn query_string(sql: &str) -> String {
        Spi::connect(|client| {
            let table = client.select(sql, Some(1), &[])?;
            for row in table {
                return row.get::<String>(1);
            }
            Ok(None)
        })
        .unwrap_or_else(|err| panic!("SPI select failed for `{sql}`: {err}"))
        .unwrap_or_else(|| panic!("query returned no rows: {sql}"))
    }

    #[cfg(feature = "embedded")]
    fn query_i64(sql: &str) -> i64 {
        Spi::connect(|client| {
            let table = client.select(sql, Some(1), &[])?;
            for row in table {
                return row.get::<i64>(1);
            }
            Ok(None)
        })
        .unwrap_or_else(|err| panic!("SPI select failed for `{sql}`: {err}"))
        .unwrap_or_else(|| panic!("query returned no rows: {sql}"))
    }

    #[cfg(feature = "embedded")]
    fn query_bool(sql: &str) -> bool {
        Spi::connect(|client| {
            let table = client.select(sql, Some(1), &[])?;
            for row in table {
                return row.get::<bool>(1);
            }
            Ok(None)
        })
        .unwrap_or_else(|err| panic!("SPI select failed for `{sql}`: {err}"))
        .unwrap_or_else(|| panic!("query returned no rows: {sql}"))
    }

    #[cfg(feature = "embedded")]
    fn unique_schema_name(prefix: &str) -> String {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        format!("{prefix}_{suffix}")
    }

    #[cfg(feature = "embedded")]
    fn assert_native_create_table_flow() {
        let schema_name = unique_schema_name("kalam_e2e_native_fdw");
        cleanup_schema(&schema_name, &["messages"]);

        run_sql(&format!("SELECT kalam.enable_schema('{schema_name}', 'user')"));
        run_sql("SET kalam.user_id = 'u_native'");
        run_sql(&format!(
            "CREATE TABLE {schema_name}.messages (id TEXT PRIMARY KEY, body TEXT)"
        ));

        assert_eq!(
            query_string(&format!(
                "SELECT c.relkind::text \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = '{schema_name}' AND c.relname = 'messages'"
            )),
            "f"
        );

        run_sql(&format!(
            "INSERT INTO {schema_name}.messages (id, body) VALUES ('m1', 'hello')"
        ));

        assert_eq!(
            query_string(&format!(
                "SELECT body FROM {schema_name}.messages WHERE id = 'm1'"
            )),
            "hello"
        );
        assert_eq!(
            query_string(&format!(
                "SELECT _userid FROM {schema_name}.messages WHERE id = 'm1'"
            )),
            "u_native"
        );
        assert!(query_bool(&format!(
            "SELECT NOT _deleted FROM {schema_name}.messages WHERE id = 'm1'"
        )));
    }

    #[cfg(feature = "embedded")]
    fn assert_alter_update_delete_flow() {
        let schema_name = unique_schema_name("kalam_e2e_alter_fdw");
        cleanup_schema(&schema_name, &["messages"]);

        run_sql(&format!("SELECT kalam.enable_schema('{schema_name}', 'user')"));
        run_sql("SET kalam.user_id = 'u_alter'");
        run_sql(&format!(
            "CREATE TABLE {schema_name}.messages (id TEXT PRIMARY KEY, body TEXT)"
        ));
        run_sql(&format!(
            "INSERT INTO {schema_name}.messages (id, body) VALUES ('m1', 'before')"
        ));
        run_sql(&format!(
            "UPDATE {schema_name}.messages SET body = 'after' WHERE id = 'm1'"
        ));

        assert_eq!(
            query_string(&format!(
                "SELECT body FROM {schema_name}.messages WHERE id = 'm1'"
            )),
            "after"
        );

        run_sql(&format!(
            "DELETE FROM {schema_name}.messages WHERE id = 'm1'"
        ));

        assert_eq!(
            query_i64(&format!("SELECT COUNT(*) FROM {schema_name}.messages")),
            0
        );

        run_sql(&format!(
            "ALTER TABLE {schema_name}.messages ADD COLUMN category TEXT"
        ));
        assert_eq!(
            query_i64(&format!(
                "SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_schema = '{schema_name}' \
                   AND table_name = 'messages' \
                   AND column_name = 'category'"
            )),
            1
        );
    }

    #[cfg(feature = "embedded")]
    fn assert_drop_foreign_table_flow() {
        let schema_name = unique_schema_name("kalam_e2e_drop_fdw");
        cleanup_schema(&schema_name, &["messages"]);

        run_sql(&format!("SELECT kalam.enable_schema('{schema_name}', 'user')"));
        run_sql(&format!(
            "CREATE TABLE {schema_name}.messages (id TEXT PRIMARY KEY, body TEXT)"
        ));

        run_sql(&format!("DROP FOREIGN TABLE {schema_name}.messages"));
        assert!(query_bool(&format!(
            "SELECT to_regclass('{schema_name}.messages') IS NULL"
        )));
    }

    #[cfg(feature = "embedded")]
    fn assert_postgres_join_flow() {
        let schema_name = unique_schema_name("kalam_e2e_join_fdw");
        cleanup_schema(&schema_name, &["messages"]);

        run_sql(&format!("SELECT kalam.enable_schema('{schema_name}', 'user')"));
        run_sql("SET kalam.user_id = 'u_join'");
        run_sql("CREATE TEMP TABLE kalam_e2e_local_profiles (id TEXT PRIMARY KEY, name TEXT NOT NULL)");
        run_sql(
            "INSERT INTO kalam_e2e_local_profiles (id, name) VALUES ('p1', 'Asha'), ('p2', 'Bryn')"
        );

        run_sql(&format!(
            "CREATE TABLE {schema_name}.messages (id TEXT PRIMARY KEY, profile_id TEXT, body TEXT)"
        ));
        run_sql(&format!(
            "INSERT INTO {schema_name}.messages (id, profile_id, body) VALUES \
             ('m1', 'p1', 'hello'), \
             ('m2', 'p2', 'world')"
        ));

        assert_eq!(
            query_string(&format!(
                "SELECT p.name || ':' || m.body \
                 FROM kalam_e2e_local_profiles p \
                 JOIN {schema_name}.messages m ON m.profile_id = p.id \
                 WHERE m.id = 'm1'"
            )),
            "Asha:hello"
        );
    }

    #[cfg(feature = "embedded")]
    fn assert_stream_table_flow() {
        let schema_name = unique_schema_name("kalam_e2e_stream_fdw");
        cleanup_schema(&schema_name, &["events"]);

        run_sql(&format!("SELECT kalam.enable_schema('{schema_name}', 'stream')"));
        run_sql("SET kalam.user_id = 'u_stream'");
        run_sql("SET kalam.table_type = 'stream'");
        run_sql("SET kalam.stream_ttl_seconds = '7200'");
        run_sql(&format!(
            "CREATE TABLE {schema_name}.events (id TEXT PRIMARY KEY, body TEXT)"
        ));

        assert_eq!(
            query_string(&format!(
                "SELECT array_to_string(ft.ftoptions, ',') \
                 FROM pg_foreign_table ft \
                 JOIN pg_class c ON c.oid = ft.ftrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = '{schema_name}' AND c.relname = 'events'"
            )),
            format!("namespace={schema_name},table=events,table_type=stream")
        );

        run_sql(&format!(
            "INSERT INTO {schema_name}.events (id, body) VALUES ('e1', 'stream row')"
        ));

        assert_eq!(
            query_string(&format!(
                "SELECT body FROM {schema_name}.events WHERE id = 'e1'"
            )),
            "stream row"
        );
        assert_eq!(
            query_string(&format!(
                "SELECT _userid FROM {schema_name}.events WHERE id = 'e1'"
            )),
            "u_stream"
        );
    }

    #[cfg(feature = "embedded")]
    #[pg_test]
    fn postgres_extension_end_to_end_flows() {
        assert_native_create_table_flow();
        assert_alter_update_delete_flow();
        assert_drop_foreign_table_flow();
        assert_postgres_join_flow();
        assert_stream_table_flow();
    }
}

