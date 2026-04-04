use super::common::{ensure_schema_exists, unique_name, DdlTestEnv};

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_ddl_create_table_mirrors_columns_identically() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("mirror_cols");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            title TEXT,
            value INTEGER,
            active BOOLEAN
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&sql)
        .await
        .expect("CREATE FOREIGN TABLE for mirror comparison");

    let pg_rows = pg
        .query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&ns, &table],
        )
        .await
        .expect("query postgres mirrored columns");

    let pg_columns: Vec<String> = pg_rows.into_iter().map(|row| row.get(0)).collect();
    let kalam_columns = env
        .wait_for_kalamdb_columns(ns, &table, "mirrored columns to appear", |columns| {
            columns.iter().any(|column| column == "id")
                && columns.iter().any(|column| column == "title")
                && columns.iter().any(|column| column == "value")
                && columns.iter().any(|column| column == "active")
        })
        .await;
    let kalam_user_columns: Vec<String> = kalam_columns
        .iter()
        .filter(|name| {
            name.as_str() != "_userid" && name.as_str() != "_seq" && name.as_str() != "_deleted"
        })
        .cloned()
        .collect();

    assert_eq!(
        pg_columns, kalam_user_columns,
        "PostgreSQL and KalamDB should expose identical mirrored columns"
    );
    assert!(
        kalam_columns.iter().all(|name| {
            pg_columns.contains(name) || name == "_userid" || name == "_seq" || name == "_deleted"
        }),
        "KalamDB should only add known internal columns beyond the PostgreSQL schema: {kalam_columns:?}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_ddl_preserves_primary_key_not_null_and_defaults() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("ddl_constraints");
    let table = unique_name("shared_tbl");
    ensure_schema_exists(&pg, &ns).await;

    pg.batch_execute(&format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id BIGINT NOT NULL,
            title TEXT NOT NULL,
            value INTEGER,
            created TIMESTAMP DEFAULT NOW()
         ) SERVER kalam_server
         OPTIONS (table_type 'shared');"
    ))
    .await
    .expect("create mirrored constrained foreign table");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    let columns = env
        .kalamdb_sql(&format!(
            "SELECT column_name, default_value, nullable, primary_key \
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

    let find_row = |name: &str| {
        rows.iter().find(|row| {
            row.as_array()
                .and_then(|columns| columns.first())
                .and_then(|value| value.as_str())
                == Some(name)
        })
    };

    let id_row = find_row("id");
    let title_row = find_row("title");
    let created_row = find_row("created");

    assert!(
        id_row.and_then(|row| row.as_array()).map(|columns| {
            columns.get(2).and_then(|value| value.as_bool()) == Some(false)
                && columns.get(3).and_then(|value| value.as_bool()) == Some(true)
        }) == Some(true)
            && title_row
                .and_then(|row| row.as_array())
                .map(|columns| columns.get(2).and_then(|value| value.as_bool()) == Some(false))
                == Some(true)
            && created_row
                .and_then(|row| row.as_array())
                .map(|columns| columns.get(1).and_then(|value| value.as_str()) == Some("NOW()"))
                == Some(true),
        "system.columns should expose mirrored defaults and constrained columns: {}",
        serde_json::to_string(&columns).unwrap_or_default()
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_ddl_current_schema_maps_to_namespace_without_namespace_option() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("schema_ns");
    let table = unique_name("schema_mirror");

    pg.batch_execute(&format!(
        "CREATE SCHEMA IF NOT EXISTS {ns};
         SET search_path TO {ns};
         CREATE FOREIGN TABLE {table} (
             id TEXT,
             title TEXT,
             value INTEGER
         ) SERVER kalam_server
         OPTIONS (table_type 'shared');"
    ))
    .await
    .expect("create foreign table using current schema");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    assert!(
        env.kalamdb_table_exists(&ns, &table).await,
        "KalamDB table {ns}.{table} should exist when search_path targets {ns}"
    );

    let pg_rows = pg
        .query(
            "SELECT table_schema
             FROM information_schema.tables
             WHERE table_schema = $1 AND table_name = $2",
            &[&ns, &table],
        )
        .await
        .expect("query postgres schema mapping");
    assert_eq!(pg_rows.len(), 1, "PostgreSQL table should be created in schema {ns}");

    pg.batch_execute(&format!(
        "RESET search_path;
         DROP FOREIGN TABLE IF EXISTS {ns}.{table};
         DROP SCHEMA IF EXISTS {ns} CASCADE;"
    ))
    .await
    .ok();
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_ddl_alter_add_column_preserves_not_null_and_default() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("ddl_addopts");
    let table = unique_name("items");
    ensure_schema_exists(&pg, &ns).await;

    pg.batch_execute(&format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id BIGINT NOT NULL,
            title TEXT NOT NULL
         ) SERVER kalam_server
         OPTIONS (table_type 'shared');"
    ))
    .await
    .expect("create base foreign table");

    pg.batch_execute(&format!(
        "ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';"
    ))
    .await
    .expect("alter add column with default and not null");
    env.wait_for_kalamdb_columns(&ns, &table, "added columns to include status", |columns| {
        columns.iter().any(|column| column == "status")
    })
    .await;

    env.kalamdb_sql(&format!(
        "INSERT INTO {ns}.{table} (id, title) VALUES (SNOWFLAKE_ID(), 'hello add column')"
    ))
    .await;

    let result = env
        .kalamdb_sql(&format!("SELECT status FROM {ns}.{table} WHERE title = 'hello add column'"))
        .await;
    let result_text = serde_json::to_string(&result).unwrap_or_default();
    assert!(
        result_text.contains("pending"),
        "ALTER ADD COLUMN default should be applied on the mirrored KalamDB table: {result_text}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_ddl_alter_column_set_and_drop_not_null() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("ddl_nullable");
    let table = unique_name("items");
    ensure_schema_exists(&pg, &ns).await;

    pg.batch_execute(&format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id BIGINT NOT NULL,
            title TEXT
         ) SERVER kalam_server
         OPTIONS (table_type 'shared');"
    ))
    .await
    .expect("create base table for nullability alter");

    pg.batch_execute(&format!("ALTER FOREIGN TABLE {ns}.{table} ALTER COLUMN title SET NOT NULL;"))
        .await
        .expect("set not null");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    let insert_error = env
        .kalamdb_sql_maybe(&format!(
            "INSERT INTO {ns}.{table} (id, title) VALUES (SNOWFLAKE_ID(), NULL)"
        ))
        .await
        .expect_err("remote insert with NULL title should fail after SET NOT NULL");
    assert!(
        insert_error.contains("null") || insert_error.contains("NOT NULL"),
        "SET NOT NULL should reject NULL inserts remotely: {insert_error}"
    );

    pg.batch_execute(&format!(
        "ALTER FOREIGN TABLE {ns}.{table} ALTER COLUMN title DROP NOT NULL;"
    ))
    .await
    .expect("drop not null");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    env.kalamdb_sql(&format!("INSERT INTO {ns}.{table} (id, title) VALUES (SNOWFLAKE_ID(), NULL)"))
        .await;

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_ddl_alter_column_set_and_drop_default() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("ddl_defaults");
    let table = unique_name("items");
    ensure_schema_exists(&pg, &ns).await;

    pg.batch_execute(&format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id BIGINT NOT NULL,
            status TEXT
         ) SERVER kalam_server
         OPTIONS (table_type 'shared');"
    ))
    .await
    .expect("create base table for default alter");

    pg.batch_execute(&format!(
        "ALTER FOREIGN TABLE {ns}.{table} ALTER COLUMN status SET DEFAULT 'pending';"
    ))
    .await
    .expect("set default");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    env.kalamdb_sql(&format!("INSERT INTO {ns}.{table} (id) VALUES (SNOWFLAKE_ID())"))
        .await;

    let with_default = env
        .kalamdb_sql(&format!("SELECT status FROM {ns}.{table} WHERE status = 'pending'"))
        .await;
    let with_default_text = serde_json::to_string(&with_default).unwrap_or_default();
    assert!(
        with_default_text.contains("pending"),
        "SET DEFAULT should be visible on mirrored remote inserts: {with_default_text}"
    );

    pg.batch_execute(&format!(
        "ALTER FOREIGN TABLE {ns}.{table} ALTER COLUMN status DROP DEFAULT;"
    ))
    .await
    .expect("drop default");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    env.kalamdb_sql(&format!("INSERT INTO {ns}.{table} (id) VALUES (SNOWFLAKE_ID())"))
        .await;

    let without_default = env
        .kalamdb_sql(&format!(
            "SELECT COUNT(*) AS pending_count FROM {ns}.{table} WHERE status = 'pending'"
        ))
        .await;
    let pending_count = without_default["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["rows"].as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|columns| columns.first())
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<u64>().ok());
    assert!(
        pending_count == Some(1),
        "DROP DEFAULT should stop populating the previous default value: {}",
        serde_json::to_string(&without_default).unwrap_or_default()
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}
