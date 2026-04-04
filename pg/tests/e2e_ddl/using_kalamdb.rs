use super::common::{unique_name, DdlTestEnv};

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_create_table_using_kalamdb_basic() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("using_ns");
    let table = unique_name("using_tbl");

    pg.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {ns};"))
        .await
        .expect("CREATE SCHEMA");

    pg.batch_execute(&format!(
        "CREATE TABLE {ns}.{table} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created TIMESTAMP DEFAULT NOW()
        ) USING kalamdb WITH (type = 'user');"
    ))
    .await
    .expect("CREATE TABLE USING kalamdb");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    assert!(
        env.kalamdb_table_exists(&ns, &table).await,
        "KalamDB table {ns}.{table} should exist after CREATE TABLE USING kalamdb"
    );

    let pg_rows = pg
        .query(
            "SELECT column_name FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&ns, &table],
        )
        .await
        .expect("query PG columns");
    let pg_cols: Vec<String> = pg_rows.iter().map(|row| row.get(0)).collect();
    assert!(pg_cols.contains(&"id".to_string()), "PG should have id column");
    assert!(pg_cols.contains(&"name".to_string()), "PG should have name column");
    assert!(pg_cols.contains(&"status".to_string()), "PG should have status column");

    let kalam_cols = env.kalamdb_columns(&ns, &table).await;
    assert!(kalam_cols.contains(&"id".to_string()), "KalamDB should have id column");
    assert!(kalam_cols.contains(&"name".to_string()), "KalamDB should have name column");

    let columns = env
        .kalamdb_sql(&format!(
            "SELECT column_name, default_value, primary_key \
             FROM system.columns \
             WHERE namespace_id = '{ns}' AND table_name = '{table}' \
             ORDER BY ordinal"
        ))
        .await;
    let rows = columns["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["rows"].as_array())
        .cloned()
        .unwrap_or_default();

    let id_row = rows.iter().find(|row| {
        row.as_array()
            .and_then(|columns| columns.first())
            .and_then(|value| value.as_str())
            == Some("id")
    });
    assert!(
        id_row.and_then(|row| row.as_array()).map(|columns| {
            let default_val = columns.get(1).and_then(|value| value.as_str()).unwrap_or("");
            let is_pk = columns.get(2).and_then(|value| value.as_bool()).unwrap_or(false);
            default_val.contains("SNOWFLAKE_ID") && is_pk
        }) == Some(true),
        "id column should have SNOWFLAKE_ID default and be PRIMARY KEY in KalamDB: {:?}",
        id_row
    );

    let status_row = rows.iter().find(|row| {
        row.as_array()
            .and_then(|columns| columns.first())
            .and_then(|value| value.as_str())
            == Some("status")
    });
    assert!(
        status_row.and_then(|row| row.as_array()).map(|columns| {
            let default_val = columns.get(1).and_then(|value| value.as_str()).unwrap_or("");
            default_val.contains("active")
        }) == Some(true),
        "status column should have DEFAULT 'active' in KalamDB: {:?}",
        status_row
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_create_table_using_kalamdb_multiple_defaults() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("defs_ns");
    let table = unique_name("defs_tbl");

    pg.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {ns};"))
        .await
        .expect("CREATE SCHEMA");

    pg.batch_execute(&format!(
        "CREATE TABLE {ns}.{table} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            label TEXT DEFAULT 'untitled',
            score INTEGER DEFAULT 0,
            active BOOLEAN DEFAULT true
        ) USING kalamdb WITH (type = 'shared');"
    ))
    .await
    .expect("CREATE TABLE USING kalamdb with multiple defaults");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    assert!(env.kalamdb_table_exists(&ns, &table).await, "KalamDB table should exist");

    let columns = env
        .kalamdb_sql(&format!(
            "SELECT column_name, default_value \
             FROM system.columns \
             WHERE namespace_id = '{ns}' AND table_name = '{table}' \
             ORDER BY ordinal"
        ))
        .await;
    let rows = columns["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["rows"].as_array())
        .cloned()
        .unwrap_or_default();

    let id_default = rows
        .iter()
        .find(|row| {
            row.as_array()
                .and_then(|columns| columns.first())
                .and_then(|value| value.as_str())
                == Some("id")
        })
        .and_then(|row| row.as_array())
        .and_then(|columns| columns.get(1))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    assert!(
        id_default.contains("SNOWFLAKE_ID"),
        "id should have SNOWFLAKE_ID default, got: {id_default}"
    );

    let label_default = rows
        .iter()
        .find(|row| {
            row.as_array()
                .and_then(|columns| columns.first())
                .and_then(|value| value.as_str())
                == Some("label")
        })
        .and_then(|row| row.as_array())
        .and_then(|columns| columns.get(1))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    assert!(
        label_default.contains("untitled"),
        "label should have 'untitled' default, got: {label_default}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_create_table_using_kalamdb_with_options() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("withopts_ns");
    let table = unique_name("withopts_tbl");

    pg.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {ns};"))
        .await
        .expect("CREATE SCHEMA");

    pg.batch_execute(&format!(
        "CREATE TABLE {ns}.{table} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            data TEXT
        ) USING kalamdb WITH (type = 'user');"
    ))
    .await
    .expect("CREATE TABLE USING kalamdb with options");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    let table_info = env
        .kalamdb_sql(&format!(
            "SELECT table_type FROM system.tables \
             WHERE namespace_id = '{ns}' AND table_name = '{table}'"
        ))
        .await;
    let table_type = table_info["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["rows"].as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|columns| columns.first())
        .and_then(|value| value.as_str())
        .unwrap_or("");
    assert!(
        table_type.eq_ignore_ascii_case("user"),
        "Table type should be 'user', got: {table_type}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_ddl_create_table_using_kalamdb_dml_roundtrip() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("dml_ns");
    let table = unique_name("dml_tbl");

    pg.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {ns};"))
        .await
        .expect("CREATE SCHEMA");

    pg.batch_execute(&format!(
        "CREATE TABLE {ns}.{table} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT NOT NULL,
            score INTEGER DEFAULT 0
        ) USING kalamdb WITH (type = 'shared');"
    ))
    .await
    .expect("CREATE TABLE USING kalamdb");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    pg.batch_execute(&format!(
        "INSERT INTO {ns}.{table} (name, score) VALUES ('Alice', 100);
         INSERT INTO {ns}.{table} (name) VALUES ('Bob');"
    ))
    .await
    .expect("INSERT via FDW");

    let rows = pg
        .query(&format!("SELECT name, score FROM {ns}.{table} ORDER BY name"), &[])
        .await
        .expect("SELECT via FDW");
    assert_eq!(rows.len(), 2, "should have 2 rows");
    let alice_score: i32 = rows[0].get(1);
    assert_eq!(alice_score, 100, "Alice should have score 100");
    let bob_score: i32 = rows[1].get(1);
    assert_eq!(bob_score, 0, "Bob should have default score 0");

    let id_rows = pg.query(&format!("SELECT id FROM {ns}.{table}"), &[]).await.expect("SELECT id");
    for row in &id_rows {
        let id: i64 = row.get(0);
        assert!(id > 0, "Auto-generated id should be positive, got: {id}");
    }

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}
