use super::common::{ensure_schema_exists, unique_name, DdlTestEnv};

#[tokio::test]
async fn e2e_ddl_create_shared_table() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("shared_tbl");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            title TEXT,
            value INTEGER
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE");
    env.wait_for_kalamdb_table_exists(ns, &table).await;

    assert!(env.kalamdb_table_exists(ns, &table).await, "KalamDB table {ns}.{table} should exist after CREATE FOREIGN TABLE");

    let cols = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] Created {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()), "should have 'id' column");
    assert!(cols.contains(&"title".to_string()), "should have 'title' column");
    assert!(cols.contains(&"value".to_string()), "should have 'value' column");

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_create_user_table() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("user_tbl");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT,
            age INTEGER
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'user');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE (user)");
    env.wait_for_kalamdb_table_exists(ns, &table).await;

    assert!(env.kalamdb_table_exists(ns, &table).await, "KalamDB user table {ns}.{table} should exist");

    let cols = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] Created user table {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()), "should have 'id' column");
    assert!(cols.contains(&"name".to_string()), "should have 'name' column");
    assert!(cols.contains(&"age".to_string()), "should have 'age' column");

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_alter_add_column() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("alter_add");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE");
    let cols_before = env
        .wait_for_kalamdb_columns(ns, &table, "base columns to include name", |columns| {
            columns.iter().any(|column| column == "name")
        })
        .await;
    eprintln!("[DDL] Before ALTER: columns = {cols_before:?}");
    assert!(cols_before.contains(&"name".to_string()));

    let alter_sql = format!("ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN score INTEGER;");
    pg.batch_execute(&alter_sql).await.expect("ALTER ADD COLUMN");
    let cols_after = env
        .wait_for_kalamdb_columns(ns, &table, "added columns to include score", |columns| {
            columns.iter().any(|column| column == "score")
        })
        .await;
    eprintln!("[DDL] After ALTER ADD: columns = {cols_after:?}");
    assert!(
        cols_after.contains(&"score".to_string()),
        "KalamDB should have 'score' column after ALTER ADD COLUMN"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_alter_drop_column() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("alter_drop");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT,
            description TEXT
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE");
    let cols_before = env
        .wait_for_kalamdb_columns(ns, &table, "base columns to include description", |columns| {
            columns.iter().any(|column| column == "description")
        })
        .await;
    eprintln!("[DDL] Before DROP COLUMN: columns = {cols_before:?}");
    assert!(cols_before.contains(&"description".to_string()));

    let alter_sql = format!("ALTER FOREIGN TABLE {ns}.{table} DROP COLUMN description;");
    pg.batch_execute(&alter_sql).await.expect("ALTER DROP COLUMN");
    let cols_after = env
        .wait_for_kalamdb_columns(
            ns,
            &table,
            "dropped columns to exclude description",
            |columns| !columns.iter().any(|column| column == "description"),
        )
        .await;
    eprintln!("[DDL] After DROP COLUMN: columns = {cols_after:?}");
    assert!(
        !cols_after.contains(&"description".to_string()),
        "KalamDB should NOT have 'description' column after ALTER DROP COLUMN"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_drop_table() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("drop_tbl");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            data TEXT
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE");
    env.wait_for_kalamdb_table_exists(ns, &table).await;
    assert!(env.kalamdb_table_exists(ns, &table).await, "table should exist before DROP");

    let drop_sql = format!("DROP FOREIGN TABLE {ns}.{table};");
    pg.batch_execute(&drop_sql).await.expect("DROP FOREIGN TABLE");
    env.wait_for_kalamdb_table_absent(ns, &table).await;

    assert!(
        !env.kalamdb_table_exists(ns, &table).await,
        "KalamDB table {ns}.{table} should NOT exist after DROP FOREIGN TABLE"
    );
}

#[tokio::test]
async fn e2e_ddl_drop_if_exists_no_error() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let result = pg.batch_execute("DROP FOREIGN TABLE IF EXISTS nonexistent_table_xyz;").await;
    assert!(
        result.is_ok(),
        "DROP FOREIGN TABLE IF EXISTS should not error for non-existent table"
    );
}

#[tokio::test]
async fn e2e_ddl_full_lifecycle() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("lifecycle");
    ensure_schema_exists(&pg, ns).await;

    let create_sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&create_sql).await.expect("CREATE");
    env.wait_for_kalamdb_table_exists(ns, &table).await;
    assert!(env.kalamdb_table_exists(ns, &table).await);

    pg.batch_execute(&format!(
        "INSERT INTO {ns}.{table} (id, name) VALUES ('k1', 'Alice'), ('k2', 'Bob');"
    ))
    .await
    .expect("INSERT");

    let rows = pg
        .query(&format!("SELECT id, name FROM {ns}.{table} ORDER BY id"), &[])
        .await
        .expect("SELECT");
    assert_eq!(rows.len(), 2, "should have 2 rows");
    let first_name: &str = rows[0].get(1);
    assert_eq!(first_name, "Alice");

    pg.batch_execute(&format!("ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN email TEXT;"))
        .await
        .expect("ALTER ADD");
    let cols = env
        .wait_for_kalamdb_columns(ns, &table, "added columns to include email", |columns| {
            columns.iter().any(|column| column == "email")
        })
        .await;
    assert!(cols.contains(&"email".to_string()), "should have email column");

    pg.batch_execute(&format!("DROP FOREIGN TABLE {ns}.{table};"))
        .await
        .expect("DROP");
    env.wait_for_kalamdb_table_absent(ns, &table).await;
    assert!(!env.kalamdb_table_exists(ns, &table).await);
}

#[tokio::test]
async fn e2e_ddl_schema_qualified_create() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("schemans");
    let table = unique_name("sqtbl");

    pg.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {ns};"))
        .await
        .expect("CREATE SCHEMA");

    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT,
            age INTEGER
        ) SERVER kalam_server;"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE (schema-qualified)");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    assert!(env.kalamdb_table_exists(&ns, &table).await, "KalamDB table {ns}.{table} should exist after schema-qualified CREATE");

    let cols = env
        .wait_for_kalamdb_columns(&ns, &table, "schema-qualified columns to exist", |columns| {
            columns.iter().any(|column| column == "id")
                && columns.iter().any(|column| column == "name")
                && columns.iter().any(|column| column == "age")
        })
        .await;
    eprintln!("[DDL] Schema-qualified create {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"name".to_string()));
    assert!(cols.contains(&"age".to_string()));

    pg.batch_execute(&format!("ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN email TEXT;"))
        .await
        .expect("ALTER ADD COLUMN (schema-qualified)");
    let cols_after = env
        .wait_for_kalamdb_columns(&ns, &table, "schema-qualified alter to include email", |columns| {
            columns.iter().any(|column| column == "email")
        })
        .await;
    assert!(
        cols_after.contains(&"email".to_string()),
        "should have email column after ALTER"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE {ns}.{table};"))
        .await
        .expect("DROP FOREIGN TABLE (schema-qualified)");
    env.wait_for_kalamdb_table_absent(&ns, &table).await;
    assert!(
        !env.kalamdb_table_exists(&ns, &table).await,
        "table should not exist after DROP"
    );

    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
async fn e2e_ddl_non_kalam_server_ignored() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let result = pg
        .batch_execute(
            "CREATE EXTENSION IF NOT EXISTS file_fdw;
             CREATE SERVER IF NOT EXISTS file_server FOREIGN DATA WRAPPER file_fdw;
             CREATE FOREIGN TABLE IF NOT EXISTS dummy_file_table (line TEXT)
                 SERVER file_server
                 OPTIONS (filename '/dev/null', format 'text');
             DROP FOREIGN TABLE IF EXISTS dummy_file_table;
             DROP SERVER IF EXISTS file_server CASCADE;",
        )
        .await;
    assert!(result.is_ok(), "DDL on non-kalam foreign tables should be silently ignored");
}
