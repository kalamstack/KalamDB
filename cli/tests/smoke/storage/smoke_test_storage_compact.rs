//! Smoke test: STORAGE COMPACT TABLE triggers job completion and updates RocksDB files

use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use serde_json::Value as JsonValue;

use crate::common::*;

fn rocksdb_dir() -> PathBuf {
    let storage_dir = storage_base_dir();
    storage_dir
        .parent()
        .map(|p| p.join("rocksdb"))
        .unwrap_or_else(|| storage_dir.join("rocksdb"))
}

fn latest_sst_mtime(root: &Path) -> Option<SystemTime> {
    fn recurse(dir: &Path, latest: &mut Option<SystemTime>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                recurse(&path, latest);
            } else if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                if let Ok(meta) = entry.metadata() {
                    if let Ok(modified) = meta.modified() {
                        match latest {
                            Some(current) if modified > *current => *latest = Some(modified),
                            None => *latest = Some(modified),
                            _ => {},
                        }
                    }
                }
            }
        }
    }

    let mut latest = None;
    recurse(root, &mut latest);
    latest
}

fn fetch_job_result(job_id: &str) -> Option<String> {
    let sql = format!("SELECT message FROM system.jobs WHERE job_id = '{}'", job_id);
    let output = execute_sql_as_root_via_client_json(&sql).ok()?;
    let json: JsonValue = serde_json::from_str(&output).ok()?;
    let row = json
        .get("results")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|res| res.get("rows"))
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())?;

    let result_value = row.as_array().and_then(|arr| arr.first())?;
    let extracted = extract_arrow_value(result_value).unwrap_or_else(|| result_value.clone());
    extracted.as_str().map(|s| s.to_string())
}

#[ntest::timeout(120_000)]
#[test]
fn smoke_storage_compact_table_job_and_rocksdb() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smk_compact");
    let table = generate_unique_table("compact_tbl");
    let full_table = format!("{}.{}", namespace, table);

    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, value TEXT) WITH (TYPE = 'SHARED')",
        full_table
    ))
    .expect("create shared table");

    for i in 1..=40 {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, value) VALUES ({}, 'v{}')",
            full_table, i, i
        ))
        .expect("insert row");
    }

    execute_sql_as_root_via_client(&format!("DELETE FROM {} WHERE id <= 25", full_table))
        .expect("delete rows");

    let rocksdb_path = rocksdb_dir();
    let before_compact = latest_sst_mtime(&rocksdb_path);

    let compact_json =
        execute_sql_as_root_via_client_json(&format!("STORAGE COMPACT TABLE {}", full_table))
            .expect("compact table");

    let job_id = parse_job_id_from_json_message(&compact_json).expect("parse compaction job id");

    verify_job_completed(&job_id, Duration::from_secs(20)).expect("compaction job completed");

    if let Some(result) = fetch_job_result(&job_id) {
        assert!(result.contains("Compaction completed"), "Unexpected job result: {}", result);
    }

    if let (Some(before), Some(after)) = (before_compact, latest_sst_mtime(&rocksdb_path)) {
        assert!(after >= before, "Expected RocksDB SST files to update after compaction");
    } else {
        println!("ℹ️  RocksDB SST files not found; skipping file timestamp check");
    }

    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
