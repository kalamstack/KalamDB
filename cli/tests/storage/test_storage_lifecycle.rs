//! Storage lifecycle integration tests
//!
//! Covers CREATE STORAGE / DROP STORAGE flows to ensure tables block deletion
//! until referencing tables are removed.

use std::{fs, path::PathBuf};

use serde_json::Value as JsonValue;

use crate::common::*;

/// Ensure DROP STORAGE fails while tables still reference the storage
#[test]
fn test_storage_drop_requires_detached_tables() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping storage lifecycle test.");
        return;
    }

    let storage_id = generate_unique_namespace("cli_storage_drop");
    let namespace = generate_unique_namespace("storage_guard_ns");
    let user_table = generate_unique_table("stor_user");
    let shared_table = generate_unique_table("stor_shared");

    let base_dir: PathBuf = storage_base_dir().join(generate_unique_namespace("storage_root"));
    let local_fs_checks = base_dir.parent().map(|p| p.exists()).unwrap_or(false);
    if local_fs_checks {
        if base_dir.exists() {
            let _ = fs::remove_dir_all(&base_dir);
        }
        fs::create_dir_all(&base_dir).expect("create base directory for storage");
    }
    let base_dir_sql = escape_single_quotes(base_dir.to_str().expect("valid storage path"));

    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");

    let create_storage_sql = format!(
        "CREATE STORAGE {storage_id} TYPE filesystem NAME 'CLI Storage Test' PATH '{base_dir}' \
         SHARED_TABLES_TEMPLATE 'ns_{{namespace}}/shared_{{tableName}}' USER_TABLES_TEMPLATE \
         'ns_{{namespace}}/user_{{tableName}}/user_{{userId}}'",
        base_dir = base_dir_sql
    );
    execute_sql_as_root_via_cli(&create_storage_sql).expect("storage creation");

    if local_fs_checks {
        assert!(base_dir.exists(), "filesystem storage should eagerly create its base directory");
    }

    let storage_rows = query_rows(&format!(
        "SELECT storage_id FROM system.storages WHERE storage_id = '{}'",
        storage_id
    ));
    assert_eq!(
        storage_rows.len(),
        1,
        "storage {} should be persisted in system.storages",
        storage_id
    );

    let create_user_table_sql = format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY AUTO_INCREMENT, body TEXT) WITH (TYPE='USER', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:5')",
        namespace, user_table, storage_id
    );
    execute_sql_as_root_via_cli(&create_user_table_sql).expect("user table creation");
    // Insert a row to ensure per-user directory is created (some backends may lazy-create user
    // folder)
    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {}.{} (body) VALUES ('init')",
        namespace, user_table
    ));
    // Flush to force Parquet file creation (directories are created on flush, not on insert)
    let flush_output =
        execute_sql_as_root_via_cli(&format!("STORAGE FLUSH TABLE {}.{}", namespace, user_table))
            .expect("flush user table");
    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        let timeout = if is_cluster_mode() {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_secs(10)
        };
        verify_job_completed(&job_id, timeout).expect("user table flush job should complete");
    } else {
    }

    // For user tables we only require the table directory itself to exist eagerly; the per-user
    // subdirectory may be created lazily on first write depending on backend semantics.
    let user_table_base_path =
        base_dir.join(format!("ns_{}", namespace)).join(format!("user_{}", user_table));
    if local_fs_checks {
        assert!(
            wait_for_path_exists(&user_table_base_path, std::time::Duration::from_secs(5)),
            "user table base path should be created eagerly: {}",
            user_table_base_path.display()
        );
    }

    let create_shared_table_sql = format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY AUTO_INCREMENT, body TEXT) WITH \
         (TYPE='SHARED', STORAGE_ID='{}', FLUSH_POLICY='rows:5')",
        namespace, shared_table, storage_id
    );
    execute_sql_as_root_via_cli(&create_shared_table_sql).expect("shared table creation");
    // Insert a row to ensure shared directory is fully realized
    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {}.{} (body) VALUES ('init_shared')",
        namespace, shared_table
    ));
    // Flush to force Parquet file creation (directories are created on flush, not on insert)
    let flush_output =
        execute_sql_as_root_via_cli(&format!("STORAGE FLUSH TABLE {}.{}", namespace, shared_table))
            .expect("flush shared table");
    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        let timeout = if is_cluster_mode() {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_secs(10)
        };
        verify_job_completed(&job_id, timeout).expect("shared table flush job should complete");
    } else {
    }

    let shared_table_path = base_dir
        .join(format!("ns_{}", namespace))
        .join(format!("shared_{}", shared_table));
    if local_fs_checks {
        assert!(
            wait_for_path_exists(&shared_table_path, std::time::Duration::from_secs(5)),
            "shared table path should be created eagerly: {}",
            shared_table_path.display()
        );
    }

    let drop_err = execute_sql_as_root_via_cli(&format!("DROP STORAGE {}", storage_id));
    assert!(drop_err.is_err(), "drop storage should fail while tables exist");
    let err_msg = drop_err.err().unwrap().to_string();
    assert!(
        err_msg.contains("Cannot drop storage") && err_msg.contains("table(s) still using it"),
        "error message should mention storage still in use: {}",
        err_msg
    );

    execute_sql_as_root_via_cli(&format!("DROP TABLE {}.{}", namespace, user_table))
        .expect("drop user table");
    execute_sql_as_root_via_cli(&format!("DROP TABLE {}.{}", namespace, shared_table))
        .expect("drop shared table");

    execute_sql_as_root_via_cli(&format!("DROP STORAGE {}", storage_id))
        .expect("storage drop should succeed after tables removed");
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

fn query_rows(sql: &str) -> Vec<JsonValue> {
    let output = execute_sql_as_root_via_cli_json(sql)
        .unwrap_or_else(|err| panic!("Failed to execute '{}': {}", sql, err));
    let json: JsonValue = serde_json::from_str(&output)
        .unwrap_or_else(|err| panic!("Failed to parse CLI JSON output: {}\n{}", err, output));
    json.get("results")
        .and_then(JsonValue::as_array)
        .and_then(|results| results.first())
        .and_then(|result| result.get("rows"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default()
}

fn escape_single_quotes(input: &str) -> String {
    input.replace('\'', "''")
}
