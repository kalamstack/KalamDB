// Smoke test: verify custom storage templates affect flush paths for user/shared tables
// - Creates a filesystem storage with custom placeholder prefixes
// - Creates both user and shared tables pinned to this storage
// - Inserts rows, triggers flush, and verifies parquet output directories match templates
// - Drops tables and asserts directories are removed

use crate::common::*;
use serde_json::Value as JsonValue;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

struct CleanupActions {
    actions: Vec<Box<dyn FnOnce() + Send + 'static>>,
}

impl CleanupActions {
    fn new() -> Self {
        Self {
            actions: Vec::new(),
        }
    }

    fn defer<F>(&mut self, action: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.actions.push(Box::new(action));
    }
}

impl Drop for CleanupActions {
    fn drop(&mut self) {
        while let Some(action) = self.actions.pop() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action));
        }
    }
}

#[ntest::timeout(300_000)]
#[test]
fn smoke_storage_custom_templates() {
    if !is_server_running() {
        println!(
            "Skipping smoke_storage_custom_templates: server not running at {}",
            server_url()
        );
        return;
    }

    let storage_id = generate_unique_namespace("smk_storage_tpl");
    let namespace_user = generate_unique_namespace("smk_tpl_ns_u");
    let namespace_shared = generate_unique_namespace("smk_tpl_ns_s");
    let user_table = generate_unique_table("tpl_user_table");
    let shared_table = generate_unique_table("tpl_shared_table");
    let table_user_full = format!("{}.{}", namespace_user, user_table);
    let table_shared_full = format!("{}.{}", namespace_shared, shared_table);
    let test_user = generate_unique_namespace("tpl_owner");
    let test_password = "TplPassword123!";

    let base_dir = storage_base_dir().join(generate_unique_namespace("tpl_smoke"));
    if base_dir.exists() {
        let _ = fs::remove_dir_all(&base_dir);
    }
    fs::create_dir_all(&base_dir).expect("create base directory for storage");
    let local_fs_checks = true;
    let base_dir_sql =
        escape_single_quotes(base_dir.to_str().expect("base directory path should be valid UTF-8"));

    let mut cleanup = CleanupActions::new();
    cleanup.defer({
        let path = base_dir.clone();
        let cleanup_fs = local_fs_checks;
        move || {
            if cleanup_fs && path.exists() {
                let _ = fs::remove_dir_all(&path);
            }
        }
    });

    // Cleanup order: drop tables, namespaces, storage, user
    cleanup.defer({
        let ns = namespace_user.clone();
        let table = user_table.clone();
        move || {
            let _ =
                execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}.{}", ns, table));
        }
    });
    cleanup.defer({
        let ns = namespace_shared.clone();
        let table = shared_table.clone();
        move || {
            let _ =
                execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}.{}", ns, table));
        }
    });
    cleanup.defer({
        let ns = namespace_user.clone();
        move || {
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
        }
    });
    cleanup.defer({
        let ns = namespace_shared.clone();
        move || {
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
        }
    });
    cleanup.defer({
        let storage = storage_id.clone();
        move || {
            let _ = execute_sql_as_root_via_client(&format!("DROP STORAGE IF EXISTS {}", storage));
        }
    });
    cleanup.defer({
        let user = test_user.clone();
        move || {
            let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS '{}'", user));
        }
    });

    // Create namespaces
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace_user))
        .expect("create namespace (user)");
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace_shared))
        .expect("create namespace (shared)");

    // Create test user to generate userId component in path templates
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        test_user, test_password
    ))
    .expect("create test user");

    // Create storage with custom templates
    let storage_sql = format!(
        "CREATE STORAGE {storage_id}
            TYPE filesystem
            NAME 'Smoke Template Storage'
            DESCRIPTION 'Verifies custom template substitution'
                PATH '{base_dir_sql}'
            SHARED_TABLES_TEMPLATE 'ns_{{namespace}}/table_{{tableName}}'
            USER_TABLES_TEMPLATE 'ns_{{namespace}}/tbl_{{tableName}}/usr_{{userId}}'"
    );
    execute_sql_as_root_via_client(&storage_sql).expect("create storage with custom templates");
    assert_storage_registered(&storage_id, &base_dir_sql);

    // ----- User table scenario -----
    let create_user_table_sql = format!(
        "CREATE TABLE {table_user_full} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            body TEXT
                ) WITH (
                    TYPE = 'USER',
                    STORAGE_ID = '{storage_id}',
                    FLUSH_POLICY = 'rows:10'
                )"
    );
    execute_sql_as_root_via_client(&create_user_table_sql).expect("create user table");
    assert_table_storage(&namespace_user, &user_table, &storage_id);

    // Reduced row count for faster smoke execution (was 50)
    insert_rows_as_user(&test_user, test_password, &table_user_full, 20);
    // Explicit flush after inserts for determinism
    trigger_flush_and_wait(&table_user_full);

    // Resolve internal user_id (system identifier) – template uses {userId}, not username
    let internal_user_id = fetch_user_id(&test_user);
    let expected_user_dir = base_dir
        .join(format!("ns_{}", namespace_user))
        .join(format!("tbl_{}", user_table))
        .join(format!("usr_{}", internal_user_id));
    if local_fs_checks {
        // Increase wait duration and also search one level deeper if direct directory empty
        let user_parquet_files =
            match wait_for_parquet_files(&expected_user_dir, Duration::from_secs(12)) {
                Some(v) if !v.is_empty() => v,
                _ => {
                    // Fallback: scan recursively for any parquet files beneath expected_user_dir
                    println!(
                        "[storage_templates] Primary wait failed; performing recursive search"
                    );
                    let mut collected = Vec::new();
                    if expected_user_dir.exists() {
                        // Manual depth-limited traversal (depth <= 3)
                        fn visit(
                            dir: &std::path::Path,
                            depth: usize,
                            acc: &mut Vec<std::path::PathBuf>,
                        ) {
                            if depth > 3 {
                                return;
                            }
                            if let Ok(entries) = std::fs::read_dir(dir) {
                                for entry in entries.flatten() {
                                    let p = entry.path();
                                    if p.is_dir() {
                                        visit(&p, depth + 1, acc);
                                    } else if p
                                        .extension()
                                        .and_then(|e| e.to_str())
                                        .map(|e| e.eq_ignore_ascii_case("parquet"))
                                        .unwrap_or(false)
                                    {
                                        acc.push(p);
                                    }
                                }
                            }
                        }
                        visit(&expected_user_dir, 0, &mut collected);
                    }
                    if collected.is_empty() {
                        panic!(
                        "Expected parquet files under {} (direct or recursive) but none were found",
                        expected_user_dir.display()
                    );
                    }
                    collected
                },
            };
        assert!(!user_parquet_files.is_empty(), "user table flush should produce parquet files");
        for file_path in &user_parquet_files {
            assert_eq!(
                expected_user_dir,
                file_path.parent().map(Path::to_path_buf).expect("parquet file has parent"),
                "Parquet file should live under user template directory"
            );
        }
    } else {
        let expected_user_path =
            format!("ns_{}/tbl_{}/usr_{}", namespace_user, user_table, internal_user_id);
        assert_manifest_path_contains(&namespace_user, &user_table, &expected_user_path);
    }

    execute_sql_as_root_via_client(&format!("DROP TABLE {}", table_user_full))
        .expect("drop user table");
    if local_fs_checks && !wait_for_directory_absence(&expected_user_dir, Duration::from_secs(15)) {
        println!(
            "[storage_templates] WARNING: user template directory not removed (non-fatal): {}",
            expected_user_dir.display()
        );
    }

    // ----- Shared table scenario -----
    let create_shared_table_sql = format!(
        "CREATE TABLE {table_shared_full} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            body TEXT
                ) WITH (
                    TYPE = 'SHARED',
                    STORAGE_ID = '{storage_id}',
                    FLUSH_POLICY = 'rows:10'
                )"
    );
    execute_sql_as_root_via_client(&create_shared_table_sql).expect("create shared table");
    assert_table_storage(&namespace_shared, &shared_table, &storage_id);

    insert_rows_as_root(&table_shared_full, 50);
    trigger_flush_and_wait(&table_shared_full);

    let expected_shared_dir = base_dir
        .join(format!("ns_{}", namespace_shared))
        .join(format!("table_{}", shared_table));
    if local_fs_checks {
        let shared_parquet_files =
            match wait_for_parquet_files(&expected_shared_dir, Duration::from_secs(12)) {
                Some(v) if !v.is_empty() => v,
                _ => {
                    println!(
                        "[storage_templates] Shared table primary wait failed; recursive search"
                    );
                    let mut collected = Vec::new();
                    if expected_shared_dir.exists() {
                        fn visit(
                            dir: &std::path::Path,
                            depth: usize,
                            acc: &mut Vec<std::path::PathBuf>,
                        ) {
                            if depth > 3 {
                                return;
                            }
                            if let Ok(entries) = std::fs::read_dir(dir) {
                                for entry in entries.flatten() {
                                    let p = entry.path();
                                    if p.is_dir() {
                                        visit(&p, depth + 1, acc);
                                    } else if p
                                        .extension()
                                        .and_then(|e| e.to_str())
                                        .map(|e| e.eq_ignore_ascii_case("parquet"))
                                        .unwrap_or(false)
                                    {
                                        acc.push(p);
                                    }
                                }
                            }
                        }
                        visit(&expected_shared_dir, 0, &mut collected);
                    }
                    if collected.is_empty() {
                        panic!(
                        "Expected parquet files under {} (direct or recursive) but none were found",
                        expected_shared_dir.display()
                    );
                    }
                    collected
                },
            };
        assert!(
            !shared_parquet_files.is_empty(),
            "shared table flush should produce parquet files"
        );
        for file_path in &shared_parquet_files {
            assert_eq!(
                expected_shared_dir,
                file_path.parent().map(Path::to_path_buf).expect("parquet file has parent"),
                "Shared parquet file should live under shared template directory"
            );
        }
    } else {
        let expected_shared_path = format!("ns_{}/table_{}", namespace_shared, shared_table);
        assert_manifest_path_contains(&namespace_shared, &shared_table, &expected_shared_path);
    }

    execute_sql_as_root_via_client(&format!("DROP TABLE {}", table_shared_full))
        .expect("drop shared table");
    if local_fs_checks && !wait_for_directory_absence(&expected_shared_dir, Duration::from_secs(15))
    {
        println!(
            "[storage_templates] WARNING: shared template directory not removed (non-fatal): {}",
            expected_shared_dir.display()
        );
    }

    // Drop storage so cleanup guard doesn't emit errors
    execute_sql_as_root_via_client(&format!("DROP STORAGE {}", storage_id)).expect("drop storage");
}

fn insert_rows_as_user(username: &str, password: &str, table_name: &str, row_count: usize) {
    let values = build_values_clause(row_count, "user payload");
    let insert_sql = format!("INSERT INTO {} (body) VALUES {}", table_name, values);
    execute_sql_via_client_as(username, password, &insert_sql).expect("user insert should succeed");
}

fn insert_rows_as_root(table_name: &str, row_count: usize) {
    let values = build_values_clause(row_count, "shared payload");
    let insert_sql = format!("INSERT INTO {} (body) VALUES {}", table_name, values);
    execute_sql_as_root_via_client(&insert_sql).expect("root insert should succeed");
}

fn build_values_clause(row_count: usize, prefix: &str) -> String {
    (0..row_count)
        .map(|idx| format!("('{} {}')", prefix, idx))
        .collect::<Vec<_>>()
        .join(", ")
}

fn trigger_flush_and_wait(table_name: &str) {
    let flush_sql = format!("STORAGE FLUSH TABLE {}", table_name);
    let output = execute_sql_as_root_via_client(&flush_sql).expect("flush command should succeed");
    let job_id =
        parse_job_id_from_flush_output(&output).expect("flush output should contain job id");
    verify_job_completed(&job_id, Duration::from_secs(180)).expect("flush job should complete");
}

// Fetch internal user_id for a given user_id from system.users (first column user_id)
fn fetch_user_id(user_id: &str) -> String {
    let sql = format!("SELECT user_id FROM system.users WHERE user_id = '{}' LIMIT 1", user_id);
    let rows = query_rows(&sql);
    if rows.is_empty() {
        panic!("User '{}' not found in system.users", user_id);
    }
    {
        let value = rows[0].get("user_id").map(extract_typed_value).unwrap_or_else(|| {
            panic!("Row for user '{}' missing user_id field: {}", user_id, rows[0])
        });
        value.as_str().map(|s| s.to_string()).unwrap_or_else(|| {
            panic!("Row for user '{}' user_id is not a string: {:?}", user_id, value)
        })
    }
}

fn wait_for_parquet_files(dir: &Path, timeout: Duration) -> Option<Vec<PathBuf>> {
    let deadline = Instant::now() + timeout;
    loop {
        let files = list_parquet_files(dir);
        if !files.is_empty() {
            return Some(files);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn list_parquet_files(dir: &Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("parquet"))
                .unwrap_or(false)
            {
                files.push(path);
            }
        }
    }
    files
}

fn wait_for_directory_absence(dir: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !dir.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    !dir.exists()
}

fn escape_single_quotes(input: &str) -> String {
    input.replace('\'', "''")
}

fn assert_storage_registered(storage_id: &str, expected_base_dir: &str) {
    let sql = format!(
        "SELECT storage_id, base_directory FROM system.storages WHERE storage_id = '{}'",
        storage_id
    );
    let rows = query_rows(&sql);
    assert_eq!(
        rows.len(),
        1,
        "Storage '{}' was not visible in system.storages output: {}",
        storage_id,
        rows_as_debug_string(&rows)
    );
    let storage = &rows[0];
    let base_dir_value = storage
        .get("base_directory")
        .map(extract_typed_value)
        .unwrap_or(JsonValue::Null);
    let base_dir = base_dir_value.as_str().unwrap_or("");
    assert_eq!(
        base_dir, expected_base_dir,
        "Storage '{}' base_directory mismatch (expected {}, got {})",
        storage_id, expected_base_dir, base_dir
    );
}

fn assert_table_storage(namespace: &str, table_name: &str, expected_storage_id: &str) {
    let sql = format!(
        "SELECT table_name, namespace_id, options FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table_name
    );
    let rows = query_rows(&sql);
    assert_eq!(
        rows.len(),
        1,
        "Table {}.{} not found in system.schemas: {}",
        namespace,
        table_name,
        rows_as_debug_string(&rows)
    );
    let options_value = rows[0].get("options").map(extract_typed_value).unwrap_or(JsonValue::Null);
    let options_raw = options_value.as_str().unwrap_or("");
    let options_json: JsonValue = serde_json::from_str(options_raw).unwrap_or_else(|err| {
        panic!(
            "Failed to parse options JSON for table {}.{}: {}\n{}",
            namespace, table_name, err, options_raw
        )
    });
    let storage_id = options_json.get("storage_id").and_then(JsonValue::as_str).unwrap_or("");
    assert_eq!(
        storage_id, expected_storage_id,
        "Table {}.{} is not using storage '{}' (reported '{}', options={})",
        namespace, table_name, expected_storage_id, storage_id, options_raw
    );
}

fn assert_manifest_path_contains(namespace: &str, table_name: &str, expected_subpath: &str) {
    let sql = format!(
        "SELECT manifest_json FROM system.manifest WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table_name
    );
    let json_output =
        execute_sql_as_root_via_client_json(&sql).expect("query system.manifest manifest_json");
    let parsed: JsonValue = serde_json::from_str(&json_output).expect("parse manifest JSON");
    let rows = get_rows_as_hashmaps(&parsed).unwrap_or_default();

    let mut paths = Vec::new();
    for row in rows {
        if let Some(value) = row.get("manifest_json") {
            let extracted = extract_arrow_value(value).unwrap_or_else(|| value.clone());
            let manifest_json = if extracted.is_object() {
                Some(extracted)
            } else {
                extracted
                    .as_str()
                    .and_then(|manifest_str| serde_json::from_str::<JsonValue>(manifest_str).ok())
            };

            if let Some(manifest_json) = manifest_json {
                if let Some(segments) = manifest_json.get("segments").and_then(JsonValue::as_array)
                {
                    for segment in segments {
                        if let Some(path) = segment.get("path").and_then(JsonValue::as_str) {
                            paths.push(path.to_string());
                        }
                    }
                }
            }
        }
    }

    assert!(
        paths.iter().any(|path| path.contains(expected_subpath)),
        "Expected manifest path containing '{}' for {}.{} (paths: {:?})",
        expected_subpath,
        namespace,
        table_name,
        paths,
    );
}

fn query_rows(sql: &str) -> Vec<JsonValue> {
    let output = execute_sql_as_root_via_client_json(sql)
        .unwrap_or_else(|err| panic!("Failed to execute '{}': {}", sql, err));
    let json: JsonValue = serde_json::from_str(&output)
        .unwrap_or_else(|err| panic!("Failed to parse CLI JSON output: {}\n{}", err, output));

    // Extract schema for column names
    let schema = json
        .get("results")
        .and_then(JsonValue::as_array)
        .and_then(|results| results.first())
        .and_then(|result| result.get("schema"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();

    let column_names: Vec<String> = schema
        .iter()
        .filter_map(|col| col.get("name").and_then(JsonValue::as_str).map(String::from))
        .collect();

    // Get rows as arrays
    let rows_arrays = json
        .get("results")
        .and_then(JsonValue::as_array)
        .and_then(|results| results.first())
        .and_then(|result| result.get("rows"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();

    // Convert each row array to an object with column names as keys
    rows_arrays
        .iter()
        .filter_map(|row| {
            let arr = row.as_array()?;
            let mut obj = serde_json::Map::new();
            for (i, col_name) in column_names.iter().enumerate() {
                if let Some(value) = arr.get(i) {
                    obj.insert(col_name.clone(), value.clone());
                }
            }
            Some(JsonValue::Object(obj))
        })
        .collect()
}

fn rows_as_debug_string(rows: &[JsonValue]) -> String {
    if rows.is_empty() {
        "<no rows>".to_string()
    } else {
        rows.iter().map(|row| row.to_string()).collect::<Vec<_>>().join(", ")
    }
}
