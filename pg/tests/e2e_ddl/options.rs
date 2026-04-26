use std::env;

use tokio_postgres::{Config, NoTls};

use super::common::{
    ensure_schema_exists, pg_kalam_exec, require_ddl_env, unique_name, DdlTestEnv,
};

fn contains_status(text: &str, expected_terms: &[&str]) -> bool {
    let normalized = text.to_ascii_lowercase();
    expected_terms
        .iter()
        .any(|term| normalized.contains(&term.to_ascii_lowercase()))
}

async fn session_row_count_for_backend(env: &DdlTestEnv, backend_pid: i32) -> i64 {
    let sessions = env
        .kalamdb_sql(&format!(
            "SELECT session_id, last_method FROM system.sessions WHERE backend_pid = {backend_pid}"
        ))
        .await;

    sessions["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["row_count"].as_i64())
        .unwrap_or_default()
}

async fn wait_for_backend_session_cleanup(env: &DdlTestEnv, backend_pid: i32, context: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);

    loop {
        let row_count = session_row_count_for_backend(env, backend_pid).await;
        if row_count == 0 {
            return;
        }

        if std::time::Instant::now() >= deadline {
            panic!("backend pid {backend_pid} remained in system.sessions after {context}");
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

struct OwnedPgClient {
    client: tokio_postgres::Client,
    connection_task: Option<tokio::task::JoinHandle<()>>,
}

impl OwnedPgClient {
    async fn connect() -> Self {
        let pg_user = env::var("USER").unwrap_or_else(|_| "postgres".to_string());
        let (client, connection) = Config::new()
            .host("127.0.0.1")
            .port(28816)
            .user(&pg_user)
            .dbname("kalamdb_test")
            .connect(NoTls)
            .await
            .expect("connect to pgrx PostgreSQL");

        let connection_task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("pg connection error: {error}");
            }
        });

        Self {
            client,
            connection_task: Some(connection_task),
        }
    }

    async fn disconnect(mut self) {
        drop(self.client);
        if let Some(connection_task) = self.connection_task.take() {
            let _ = connection_task.await;
        }
    }
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_create_table_using_kalamdb_forwards_shared_options() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = unique_name("shared_opts_ns");
    let table = unique_name("shared_opts_tbl");
    ensure_schema_exists(&pg, &ns).await;

    pg.batch_execute(&format!(
        "CREATE TABLE {ns}.{table} (
            id BIGINT,
            title TEXT
            ) USING kalamdb WITH (
                type = 'shared',
                storage_id = 'local',
                access_level = 'public'
            );"
    ))
    .await
    .expect("create shared Kalam table with forwarded options");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    let metadata = env
        .kalamdb_sql(&format!(
            "SELECT table_type, storage_id, options FROM system.tables WHERE namespace_id = \
             '{ns}' AND table_name = '{table}'"
        ))
        .await;

    let row = metadata["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["rows"].as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .cloned()
        .expect("system.tables metadata row");

    let table_type = row.first().and_then(|value| value.as_str()).unwrap_or("");
    let storage_id = row.get(1).and_then(|value| value.as_str()).unwrap_or("");
    let options = row.get(2).and_then(|value| value.as_str()).unwrap_or("");

    assert!(table_type.eq_ignore_ascii_case("shared"));
    assert_eq!(storage_id, "local");
    assert!(
        options.contains("access_level")
            && options.contains("Public")
            && options.contains("storage_id")
            && options.contains("local"),
        "forwarded options should be persisted in system.tables.options: {options}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_create_table_using_kalamdb_forwards_stream_ttl() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = unique_name("stream_opts_ns");
    let table = unique_name("stream_opts_tbl");
    ensure_schema_exists(&pg, &ns).await;

    pg.batch_execute(&format!(
        "CREATE TABLE {ns}.{table} (
            event_type TEXT,
            payload TEXT
            ) USING kalamdb WITH (
                type = 'stream',
                ttl_seconds = '45'
            );"
    ))
    .await
    .expect("create stream Kalam table with ttl");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    let metadata = env
        .kalamdb_sql(&format!(
            "SELECT table_type, options FROM system.tables WHERE namespace_id = '{ns}' AND \
             table_name = '{table}'"
        ))
        .await;

    let row = metadata["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["rows"].as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .cloned()
        .expect("stream system.tables metadata row");

    let table_type = row.first().and_then(|value| value.as_str()).unwrap_or("");
    let options = row.get(1).and_then(|value| value.as_str()).unwrap_or("");

    assert!(table_type.eq_ignore_ascii_case("stream"));
    assert!(
        options.contains("ttl_seconds") && options.contains("45"),
        "stream TTL should be persisted in system.tables.options: {options}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_drop_multiple_kalam_tables() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = unique_name("drop_many_ns");
    let table_a = unique_name("drop_many_a");
    let table_b = unique_name("drop_many_b");
    ensure_schema_exists(&pg, &ns).await;

    for table in [&table_a, &table_b] {
        pg.batch_execute(&format!(
            "CREATE TABLE {ns}.{table} (
                id TEXT,
                payload TEXT
             ) USING kalamdb WITH (type = 'shared');"
        ))
        .await
        .expect("create table for multi-drop test");
    }

    env.wait_for_kalamdb_table_exists(&ns, &table_a).await;
    env.wait_for_kalamdb_table_exists(&ns, &table_b).await;
    assert!(env.kalamdb_table_exists(&ns, &table_a).await);
    assert!(env.kalamdb_table_exists(&ns, &table_b).await);

    pg.batch_execute(&format!("DROP FOREIGN TABLE {ns}.{table_a}, {ns}.{table_b};"))
        .await
        .expect("drop multiple Kalam tables");
    env.wait_for_kalamdb_table_absent(&ns, &table_a).await;
    env.wait_for_kalamdb_table_absent(&ns, &table_b).await;

    assert!(!env.kalamdb_table_exists(&ns, &table_a).await);
    assert!(!env.kalamdb_table_exists(&ns, &table_b).await);

    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(20000)]
async fn e2e_ddl_kalam_exec_passthrough_statements() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = unique_name("exec_ns");
    let table = unique_name("exec_tbl");

    let create_ns = pg_kalam_exec(&pg, &format!("CREATE NAMESPACE IF NOT EXISTS {ns}")).await;
    assert!(
        contains_status(&create_ns, &["created", "ok", "already exists"]),
        "unexpected CREATE NAMESPACE response: {create_ns}"
    );

    let create_table = pg_kalam_exec(
        &pg,
        &format!(
            "CREATE SHARED TABLE {ns}.{table} (id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(), name \
             TEXT)"
        ),
    )
    .await;
    assert!(
        contains_status(&create_table, &["created", "ok"]),
        "unexpected CREATE TABLE response: {create_table}"
    );

    let insert =
        pg_kalam_exec(&pg, &format!("INSERT INTO {ns}.{table} (name) VALUES ('alice')")).await;
    assert!(
        contains_status(&insert, &["inserted", "ok"]),
        "unexpected INSERT response: {insert}"
    );

    let alter =
        pg_kalam_exec(&pg, &format!("ALTER TABLE {ns}.{table} ADD COLUMN email TEXT")).await;
    assert!(
        contains_status(&alter, &["altered", "ok"]),
        "unexpected ALTER TABLE response: {alter}"
    );

    let select =
        pg_kalam_exec(&pg, &format!("SELECT name FROM {ns}.{table} WHERE name = 'alice'")).await;
    assert!(
        select.contains("alice"),
        "kalam_exec SELECT should return row JSON payloads: {select}"
    );

    let columns = env
        .wait_for_kalamdb_columns(&ns, &table, "kalam_exec alter to include email", |current| {
            current.iter().any(|column| column == "email")
        })
        .await;
    assert!(columns.contains(&"email".to_string()));

    let drop = pg_kalam_exec(&pg, &format!("DROP SHARED TABLE IF EXISTS {ns}.{table}")).await;
    assert!(
        contains_status(&drop, &["dropped", "ok"]),
        "unexpected DROP TABLE response: {drop}"
    );
    env.wait_for_kalamdb_table_absent(&ns, &table).await;
    assert!(!env.kalamdb_table_exists(&ns, &table).await);

    let _ = pg_kalam_exec(&pg, &format!("DROP NAMESPACE IF EXISTS {ns}")).await;
}

#[tokio::test]
#[ntest::timeout(20000)]
async fn e2e_ddl_kalam_exec_json_text_operator() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = unique_name("exec_json_ns");
    let table = unique_name("exec_json_tbl");

    ensure_schema_exists(&pg, &ns).await;
    pg.batch_execute(&format!(
        "CREATE TABLE {ns}.{table} (
            id BIGINT,
            doc JSONB
         ) USING kalamdb WITH (type = 'shared');"
    ))
    .await
    .expect("create Kalam table with json column");

    pg.batch_execute(&format!(
        "INSERT INTO {ns}.{table} (id, doc) VALUES (
            1,
            '{{\"name\":\"alice\",\"profile\":{{\"city\":\"paris\"}}}}'::jsonb
         );"
    ))
    .await
    .expect("insert json row through postgres");

    let result =
        pg_kalam_exec(&pg, &format!("SELECT doc->>'name' AS name FROM {ns}.{table} WHERE id = 1"))
            .await;
    let value: serde_json::Value = serde_json::from_str(&result).expect("parse kalam_exec json");
    let rows = value.as_array().expect("kalam_exec rows array");
    assert_eq!(rows.len(), 1, "unexpected kalam_exec response: {result}");
    assert_eq!(
        rows[0]["name"].as_str(),
        Some("alice"),
        "unexpected kalam_exec response: {result}"
    );

    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(20000)]
async fn e2e_ddl_local_postgres_jsonb_operator_query() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = unique_name("local_json_ns");
    let table = unique_name("local_json_tbl");

    ensure_schema_exists(&pg, &ns).await;
    pg.batch_execute(&format!(
        "CREATE TABLE {ns}.{table} (
            id BIGINT,
            doc JSONB
         ) USING kalamdb WITH (type = 'shared');"
    ))
    .await
    .expect("create Kalam table with local jsonb column");

    pg.batch_execute(&format!(
        "INSERT INTO {ns}.{table} (id, doc) VALUES (
            1,
            '{{\"name\":\"alice\",\"profile\":{{\"city\":\"paris\"}}}}'::jsonb
         );"
    ))
    .await
    .expect("insert json row through postgres");

    let row = pg
        .query_one(
            &format!(
                "SELECT doc->>'name' AS name, doc ? 'profile' AS has_profile FROM {ns}.{table} \
                 WHERE id = 1"
            ),
            &[],
        )
        .await
        .expect("query local Postgres jsonb operators over foreign table");

    let name: String = row.get(0);
    let has_profile: bool = row.get(1);

    assert_eq!(name, "alice");
    assert!(has_profile, "expected profile key to exist");

    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_create_table_using_kalamdb_disconnect_cleans_session_row() {
    let env = require_ddl_env!();
    let pg = OwnedPgClient::connect().await;
    let backend_pid: i32 = pg
        .client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("query backend pid")
        .get(0);

    let ns = unique_name("shared_cleanup_ns");
    let table = unique_name("shared_cleanup_tbl");
    ensure_schema_exists(&pg.client, &ns).await;
    pg.client
        .batch_execute(&format!(
            "CREATE TABLE {ns}.{table} (
                id BIGINT,
                title TEXT
             ) USING kalamdb WITH (type = 'shared');"
        ))
        .await
        .expect("create shared Kalam table");

    pg.disconnect().await;
    wait_for_backend_session_cleanup(
        env,
        backend_pid,
        "disconnect after CREATE TABLE USING kalamdb",
    )
    .await;

    let cleanup = env.pg_connect().await;
    cleanup
        .batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    cleanup
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;"))
        .await
        .ok();
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn e2e_ddl_rejects_unsafe_option_keys() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = unique_name("unsafe_opts_ns");
    let table = unique_name("unsafe_opts_tbl");
    ensure_schema_exists(&pg, &ns).await;

    let error = pg
        .batch_execute(&format!(
            "CREATE TABLE {ns}.{table} (
                id BIGINT,
                title TEXT
                 ) USING kalamdb WITH (
                     type = 'shared',
                     \"9evil\" = 'should-fail'
                 );"
        ))
        .await
        .expect_err("unsafe option keys should be rejected");

    let message = super::common::postgres_error_text(&error);
    assert!(
        message.contains("invalid KalamDB table option")
            || message.contains("unsupported KalamDB option name"),
        "unexpected unsafe option key error: {message}"
    );
    assert!(
        !env.kalamdb_table_exists(&ns, &table).await,
        "unsafe option keys must not create a backing KalamDB table"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_kalam_exec_disconnect_cleans_session_row() {
    let env = require_ddl_env!();
    let pg = OwnedPgClient::connect().await;
    let backend_pid: i32 = pg
        .client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("query backend pid")
        .get(0);

    let result = pg_kalam_exec(&pg.client, "SELECT 1 AS ok").await;
    assert!(result.contains("1"), "unexpected kalam_exec SELECT response: {result}");

    pg.disconnect().await;
    wait_for_backend_session_cleanup(env, backend_pid, "disconnect after kalam_exec").await;
}
