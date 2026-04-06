use super::common::{ensure_schema_exists, pg_kalam_exec, unique_name, DdlTestEnv};

fn contains_status(text: &str, expected_terms: &[&str]) -> bool {
    let normalized = text.to_ascii_lowercase();
    expected_terms
        .iter()
        .any(|term| normalized.contains(&term.to_ascii_lowercase()))
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_create_foreign_table_forwards_shared_options() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("shared_opts_ns");
    let table = unique_name("shared_opts_tbl");
    ensure_schema_exists(&pg, &ns).await;

    pg.batch_execute(&format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id BIGINT,
            title TEXT
         ) SERVER kalam_server
         OPTIONS (
            namespace '{ns}',
            \"table\" '{table}',
            table_type 'shared',
            storage_id 'local',
            access_level 'public'
         );"
    ))
    .await
    .expect("create shared foreign table with forwarded options");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    let metadata = env
        .kalamdb_sql(&format!(
            "SELECT table_type, storage_id, options \
             FROM system.tables \
             WHERE namespace_id = '{ns}' AND table_name = '{table}'"
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
async fn e2e_ddl_create_foreign_table_forwards_stream_ttl() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("stream_opts_ns");
    let table = unique_name("stream_opts_tbl");
    ensure_schema_exists(&pg, &ns).await;

    pg.batch_execute(&format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            event_type TEXT,
            payload TEXT
         ) SERVER kalam_server
         OPTIONS (
            namespace '{ns}',
            \"table\" '{table}',
            table_type 'stream',
            ttl_seconds '45'
         );"
    ))
    .await
    .expect("create stream foreign table with ttl");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    let metadata = env
        .kalamdb_sql(&format!(
            "SELECT table_type, options \
             FROM system.tables \
             WHERE namespace_id = '{ns}' AND table_name = '{table}'"
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
async fn e2e_ddl_drop_multiple_foreign_tables() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("drop_many_ns");
    let table_a = unique_name("drop_many_a");
    let table_b = unique_name("drop_many_b");
    ensure_schema_exists(&pg, &ns).await;

    for table in [&table_a, &table_b] {
        pg.batch_execute(&format!(
            "CREATE FOREIGN TABLE {ns}.{table} (
                id TEXT,
                payload TEXT
             ) SERVER kalam_server
             OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
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
        .expect("drop multiple foreign tables");
    env.wait_for_kalamdb_table_absent(&ns, &table_a).await;
    env.wait_for_kalamdb_table_absent(&ns, &table_b).await;

    assert!(!env.kalamdb_table_exists(&ns, &table_a).await);
    assert!(!env.kalamdb_table_exists(&ns, &table_b).await);

    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(20000)]
async fn e2e_ddl_kalam_exec_passthrough_statements() {
    let env = DdlTestEnv::global().await;
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
            "CREATE SHARED TABLE {ns}.{table} (id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(), name TEXT)"
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
