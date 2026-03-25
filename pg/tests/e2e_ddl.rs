// pg/tests/e2e_ddl.rs
//
// End-to-end tests for DDL propagation: CREATE / ALTER / DROP FOREIGN TABLE
// should automatically propagate to KalamDB via the ProcessUtility hook.
//
// Prerequisites:
//   1. KalamDB server running:          cd backend && cargo run
//   2. pgrx PG16 set up:               pg/scripts/pgrx-test-setup.sh
//
// Run:
//   cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e_ddl)'

#![cfg(feature = "e2e")]

mod e2e_ddl_common;

use e2e_ddl_common::DdlTestEnv;

async fn ensure_schema_exists(pg: &tokio_postgres::Client, schema: &str) {
    pg.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
        .await
        .expect("CREATE SCHEMA");
}

// =========================================================================
// Helper: unique table name per test to avoid collisions
// =========================================================================

fn unique_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}_{ts}_{n}")
}

// =========================================================================
// Test 1: CREATE FOREIGN TABLE → table + namespace created in KalamDB
// =========================================================================

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

    // Give the async DDL propagation a moment (hook is synchronous, but just in case)
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify: table should exist in KalamDB
    assert!(
        env.kalamdb_table_exists(ns, &table).await,
        "KalamDB table {ns}.{table} should exist after CREATE FOREIGN TABLE"
    );

    // Verify columns: should have id, title, value (plus system columns)
    let cols = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] Created {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()), "should have 'id' column");
    assert!(cols.contains(&"title".to_string()), "should have 'title' column");
    assert!(cols.contains(&"value".to_string()), "should have 'value' column");

    // Cleanup: drop foreign table (also propagates DROP to KalamDB)
    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

// =========================================================================
// Test 2: CREATE FOREIGN TABLE for USER table type
// =========================================================================

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
            age INTEGER,
            _userid TEXT,
            _seq BIGINT,
            _deleted BOOLEAN
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'user');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE (user)");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert!(
        env.kalamdb_table_exists(ns, &table).await,
        "KalamDB user table {ns}.{table} should exist"
    );

    let cols = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] Created user table {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()), "should have 'id' column");
    assert!(cols.contains(&"name".to_string()), "should have 'name' column");
    assert!(cols.contains(&"age".to_string()), "should have 'age' column");

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

// =========================================================================
// Test 3: ALTER FOREIGN TABLE ADD COLUMN → column added in KalamDB
// =========================================================================

#[tokio::test]
async fn e2e_ddl_alter_add_column() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("alter_add");
    ensure_schema_exists(&pg, ns).await;

    // Create table first
    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify initial columns
    let cols_before = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] Before ALTER: columns = {cols_before:?}");
    assert!(cols_before.contains(&"name".to_string()));

    // ALTER: add a new column
    let alter_sql = format!("ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN score INTEGER;");
    pg.batch_execute(&alter_sql).await.expect("ALTER ADD COLUMN");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify: new column should be visible in KalamDB
    let cols_after = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] After ALTER ADD: columns = {cols_after:?}");
    assert!(
        cols_after.contains(&"score".to_string()),
        "KalamDB should have 'score' column after ALTER ADD COLUMN"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

// =========================================================================
// Test 4: ALTER FOREIGN TABLE DROP COLUMN → column removed in KalamDB
// =========================================================================

#[tokio::test]
async fn e2e_ddl_alter_drop_column() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("alter_drop");
    ensure_schema_exists(&pg, ns).await;

    // Create table with 3 columns
    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT,
            description TEXT
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let cols_before = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] Before DROP COLUMN: columns = {cols_before:?}");
    assert!(cols_before.contains(&"description".to_string()));

    // ALTER: drop the 'description' column
    let alter_sql = format!("ALTER FOREIGN TABLE {ns}.{table} DROP COLUMN description;");
    pg.batch_execute(&alter_sql).await.expect("ALTER DROP COLUMN");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let cols_after = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] After DROP COLUMN: columns = {cols_after:?}");
    assert!(
        !cols_after.contains(&"description".to_string()),
        "KalamDB should NOT have 'description' column after ALTER DROP COLUMN"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

// =========================================================================
// Test 5: DROP FOREIGN TABLE → table dropped in KalamDB
// =========================================================================

#[tokio::test]
async fn e2e_ddl_drop_table() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("drop_tbl");
    ensure_schema_exists(&pg, ns).await;

    // Create
    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            data TEXT
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        env.kalamdb_table_exists(ns, &table).await,
        "table should exist before DROP"
    );

    // DROP
    let drop_sql = format!("DROP FOREIGN TABLE {ns}.{table};");
    pg.batch_execute(&drop_sql).await.expect("DROP FOREIGN TABLE");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify: table should NOT exist in KalamDB
    assert!(
        !env.kalamdb_table_exists(ns, &table).await,
        "KalamDB table {ns}.{table} should NOT exist after DROP FOREIGN TABLE"
    );
}

// =========================================================================
// Test 6: DROP FOREIGN TABLE IF EXISTS (when table doesn't exist locally)
// =========================================================================

#[tokio::test]
async fn e2e_ddl_drop_if_exists_no_error() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    // DROP IF EXISTS on a non-existent table should not error
    let result = pg
        .batch_execute("DROP FOREIGN TABLE IF EXISTS nonexistent_table_xyz;")
        .await;
    assert!(
        result.is_ok(),
        "DROP FOREIGN TABLE IF EXISTS should not error for non-existent table"
    );
}

// =========================================================================
// Test 7: Full lifecycle — CREATE → INSERT → ALTER ADD COLUMN → DROP
// =========================================================================

#[tokio::test]
async fn e2e_ddl_full_lifecycle() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("lifecycle");
    ensure_schema_exists(&pg, ns).await;

    // 1. CREATE
    let create_sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT
        ) SERVER kalam_server
        OPTIONS (namespace '{ns}', \"table\" '{table}', table_type 'shared');"
    );
    pg.batch_execute(&create_sql).await.expect("CREATE");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(env.kalamdb_table_exists(ns, &table).await);

    // 2. INSERT data via the FDW
    pg.batch_execute(&format!(
        "INSERT INTO {ns}.{table} (id, name) VALUES ('k1', 'Alice'), ('k2', 'Bob');"
    ))
    .await
    .expect("INSERT");

    // 3. Verify data is readable
    let rows = pg
        .query(&format!("SELECT id, name FROM {ns}.{table} ORDER BY id"), &[])
        .await
        .expect("SELECT");
    assert_eq!(rows.len(), 2, "should have 2 rows");
    let first_name: &str = rows[0].get(1);
    assert_eq!(first_name, "Alice");

    // 4. ALTER ADD COLUMN
    pg.batch_execute(&format!(
        "ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN email TEXT;"
    ))
    .await
    .expect("ALTER ADD");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let cols = env.kalamdb_columns(ns, &table).await;
    assert!(
        cols.contains(&"email".to_string()),
        "should have email column"
    );

    // 5. DROP
    pg.batch_execute(&format!("DROP FOREIGN TABLE {ns}.{table};"))
        .await
        .expect("DROP");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!env.kalamdb_table_exists(ns, &table).await);
}

// =========================================================================
// Test 8: Schema-qualified CREATE (no OPTIONS) → namespace derived from schema
// =========================================================================

#[tokio::test]
async fn e2e_ddl_schema_qualified_create() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    let ns = unique_name("schemans");
    let table = unique_name("sqtbl");

    // Create PG schema (maps to KalamDB namespace)
    pg.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {ns};"))
        .await
        .expect("CREATE SCHEMA");

    // CREATE FOREIGN TABLE using schema.table syntax (no OPTIONS)
    let sql = format!(
        "CREATE FOREIGN TABLE {ns}.{table} (
            id TEXT,
            name TEXT,
            age INTEGER
        ) SERVER kalam_server;"
    );
    pg.batch_execute(&sql).await.expect("CREATE FOREIGN TABLE (schema-qualified)");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify: table should exist in KalamDB with namespace = schema name
    assert!(
        env.kalamdb_table_exists(&ns, &table).await,
        "KalamDB table {ns}.{table} should exist after schema-qualified CREATE"
    );

    let cols = env.kalamdb_columns(&ns, &table).await;
    eprintln!("[DDL] Schema-qualified create {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"name".to_string()));
    assert!(cols.contains(&"age".to_string()));

    // ALTER: add column using schema-qualified name
    pg.batch_execute(&format!("ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN email TEXT;"))
        .await
        .expect("ALTER ADD COLUMN (schema-qualified)");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let cols_after = env.kalamdb_columns(&ns, &table).await;
    assert!(
        cols_after.contains(&"email".to_string()),
        "should have email column after ALTER"
    );

    // DROP using schema-qualified name
    pg.batch_execute(&format!("DROP FOREIGN TABLE {ns}.{table};"))
        .await
        .expect("DROP FOREIGN TABLE (schema-qualified)");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !env.kalamdb_table_exists(&ns, &table).await,
        "table should not exist after DROP"
    );

    // Cleanup PG schema
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;"))
        .await
        .ok();
}

// =========================================================================
// Test 9: Non-kalam foreign table should NOT be intercepted
// =========================================================================

#[tokio::test]
async fn e2e_ddl_non_kalam_server_ignored() {
    let env = DdlTestEnv::global().await;
    let pg = env.pg_connect().await;

    // Create a dummy FDW + server (not pg_kalam)
    // This should not trigger our DDL hook
    let result = pg
        .batch_execute(
            "CREATE EXTENSION IF NOT EXISTS file_fdw;
             CREATE SERVER IF NOT EXISTS file_server FOREIGN DATA WRAPPER file_fdw;
             CREATE FOREIGN TABLE IF NOT EXISTS dummy_file_table (line TEXT)
                 SERVER file_server
                 OPTIONS (filename '/dev/null', format 'text');
             DROP FOREIGN TABLE IF EXISTS dummy_file_table;
             DROP SERVER IF EXISTS file_server CASCADE;"
        )
        .await;
    // The key assertion: no crash, no DDL propagation attempted
    assert!(
        result.is_ok(),
        "DDL on non-kalam foreign tables should be silently ignored"
    );
}

// =========================================================================
// Test 10: PostgreSQL and KalamDB expose the same mirrored columns
// =========================================================================

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

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
    let kalam_columns = env.kalamdb_columns(ns, &table).await;
    let kalam_user_columns: Vec<String> = kalam_columns
        .iter()
        .filter(|name| name.as_str() != "_userid" && name.as_str() != "_seq" && name.as_str() != "_deleted")
        .cloned()
        .collect();

    assert_eq!(
        pg_columns,
        kalam_user_columns,
        "PostgreSQL and KalamDB should expose identical mirrored columns"
    );
    assert!(
        kalam_columns
            .iter()
            .all(|name| pg_columns.contains(name) || name == "_userid" || name == "_seq" || name == "_deleted"),
        "KalamDB should only add known internal columns beyond the PostgreSQL schema: {kalam_columns:?}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

// =========================================================================
// Test 12: Column constraints/defaults are preserved in mirrored CREATE TABLE
// =========================================================================

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

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
        id_row
            .and_then(|row| row.as_array())
            .map(|columns| {
                columns.get(2).and_then(|value| value.as_bool()) == Some(false)
                    && columns.get(3).and_then(|value| value.as_bool()) == Some(true)
            })
            == Some(true)
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
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;"))
        .await
        .ok();
}

// =========================================================================
// Test 11: Current schema maps directly to the KalamDB namespace
// =========================================================================

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

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

// =========================================================================
// Test 13: ALTER ADD COLUMN preserves NOT NULL and DEFAULT on mirrored schema
// =========================================================================

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    env.kalamdb_sql(&format!(
        "INSERT INTO {ns}.{table} (id, title) VALUES (SNOWFLAKE_ID(), 'hello add column')"
    ))
    .await;

    let result = env
        .kalamdb_sql(&format!(
            "SELECT status FROM {ns}.{table} WHERE title = 'hello add column'"
        ))
        .await;
    let result_text = serde_json::to_string(&result).unwrap_or_default();
    assert!(
        result_text.contains("pending"),
        "ALTER ADD COLUMN default should be applied on the mirrored KalamDB table: {result_text}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;"))
        .await
        .ok();
}

// =========================================================================
// Test 14: ALTER COLUMN SET/DROP NOT NULL mirrors to KalamDB
// =========================================================================

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

    pg.batch_execute(&format!(
        "ALTER FOREIGN TABLE {ns}.{table} ALTER COLUMN title SET NOT NULL;"
    ))
    .await
    .expect("set not null");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    env.kalamdb_sql(&format!(
        "INSERT INTO {ns}.{table} (id, title) VALUES (SNOWFLAKE_ID(), NULL)"
    ))
    .await;

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;"))
        .await
        .ok();
}

// =========================================================================
// Test 15: ALTER COLUMN SET/DROP DEFAULT mirrors to KalamDB
// =========================================================================

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    env.kalamdb_sql(&format!(
        "INSERT INTO {ns}.{table} (id) VALUES (SNOWFLAKE_ID())"
    ))
    .await;

    let with_default = env
        .kalamdb_sql(&format!(
            "SELECT status FROM {ns}.{table} WHERE status = 'pending'"
        ))
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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    env.kalamdb_sql(&format!(
        "INSERT INTO {ns}.{table} (id) VALUES (SNOWFLAKE_ID())"
    ))
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
    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;"))
        .await
        .ok();
}
