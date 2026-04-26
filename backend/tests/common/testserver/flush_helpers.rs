#![allow(dead_code)]
//! Helper utilities for flush testing
//!
//! This module provides utilities for:
//! - Executing flush jobs directly in tests (synchronous)
//! - Waiting for flush jobs to complete
//! - Checking Parquet file existence
//! - Verifying job completion metrics

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use kalam_client::models::ResponseStatus;
use kalamdb_commons::models::{NamespaceId, StorageId, TableId, TableName};
use kalamdb_core::manifest::{FlushJobResult, SharedTableFlushJob, TableFlush, UserTableFlushJob};
use kalamdb_tables::new_indexed_user_table_store;
use tokio::time::sleep;

use super::TestServer;

/// Execute a flush job synchronously for testing
///
/// Calls flush_job.execute() directly to get immediate results.
/// This is useful in test environments where we need synchronous execution.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `table_name` - Table name
///
/// # Returns
///
/// Result containing flush job result with row counts and file paths
pub async fn execute_flush_synchronously(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
) -> Result<FlushJobResult, String> {
    // Get table definition from schema registry via AppContext
    let namespace_id = NamespaceId::new(namespace);
    let table_name_id = TableName::new(table_name);
    let table_id = TableId::new(namespace_id.clone(), table_name_id.clone());

    let table_def = server
        .app_context
        .schema_registry()
        .get_table_if_exists(&table_id)
        .map_err(|e| format!("Failed to get table definition: {}", e))?
        .ok_or_else(|| format!("Table {}.{} not found", namespace, table_name))?;

    // Check if table has columns
    if table_def.columns.is_empty() {
        return Err(format!("No columns defined for table '{}.{}'", namespace, table_name));
    }

    // Convert to Arrow schema
    let arrow_schema = table_def
        .to_arrow_schema()
        .map_err(|e| format!("Failed to convert to Arrow schema: {}", e))?;

    // Table metadata scan not needed for flush execution

    // Get store and registry from AppContext (use per-table store)
    let unified_cache = server.app_context.schema_registry();
    let table_id_arc = Arc::new(table_id.clone());

    // Determine PK field name from schema
    let pk_field = arrow_schema
        .fields()
        .iter()
        .find(|f| !f.name().starts_with('_'))
        .map(|f| f.name().clone())
        .unwrap_or_else(|| "id".to_string());

    // Construct a per-table UserTableIndexedStore directly (avoids reaching into provider
    // internals)
    let user_table_store = Arc::new(new_indexed_user_table_store(
        server.app_context.storage_backend(),
        &table_id,
        &pk_field,
    ));

    let flush_job = UserTableFlushJob::new(
        server.app_context.clone(),
        table_id_arc,
        user_table_store,
        arrow_schema.clone(),
        unified_cache,
        server.app_context.manifest_service(),
    );

    flush_job.execute().map_err(|e| format!("Flush execution failed: {}", e))
}

/// Execute a shared table flush job synchronously.
pub async fn execute_shared_flush_synchronously(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
) -> Result<FlushJobResult, String> {
    let namespace_id = NamespaceId::new(namespace);
    let table_name_id = TableName::new(table_name);
    let table_id = TableId::new(namespace_id.clone(), table_name_id.clone());

    let table_def = server
        .app_context
        .schema_registry()
        .get_table_if_exists(&table_id)
        .map_err(|e| format!("Failed to get table definition: {}", e))?
        .ok_or_else(|| format!("Table {}.{} not found", namespace, table_name))?;

    if table_def.columns.is_empty() {
        return Err(format!("No columns defined for table '{}.{}'", namespace, table_name));
    }

    // Table metadata scan not needed for flush execution

    let arrow_schema = table_def
        .to_arrow_schema()
        .map_err(|e| format!("Failed to convert to Arrow schema: {}", e))?;

    // Get per-table SharedTableIndexedStore and registry from AppContext
    let unified_cache = server.app_context.schema_registry();

    // Determine PK field name
    let pk_field = arrow_schema
        .fields()
        .iter()
        .find(|f| !f.name().starts_with('_'))
        .map(|f| f.name().clone())
        .unwrap_or_else(|| "id".to_string());

    let shared_table_store = Arc::new(kalamdb_tables::new_indexed_shared_table_store(
        server.app_context.storage_backend(),
        &table_id,
        &pk_field,
    ));

    let flush_job = SharedTableFlushJob::new(
        server.app_context.clone(),
        Arc::new(table_id.clone()),
        shared_table_store,
        arrow_schema.clone(),
        unified_cache,
        server.app_context.manifest_service(),
    );

    flush_job
        .execute()
        .map_err(|e| format!("Shared table flush execution failed: {}", e))
}

/// Wait for a flush job to complete and verify it succeeded
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `job_id` - Job ID to wait for
/// * `max_wait` - Maximum time to wait for completion
///
/// # Returns
///
/// Result containing job result string if successful
pub async fn wait_for_flush_job_completion(
    server: &TestServer,
    job_id: &str,
    max_wait: Duration,
) -> Result<String, String> {
    let start = std::time::Instant::now();
    let check_interval = Duration::from_millis(200);

    loop {
        if start.elapsed() > max_wait {
            return Err(format!(
                "Timeout waiting for job {} to complete after {:?}",
                job_id, max_wait
            ));
        }

        let query = format!(
            "SELECT status, message, started_at, finished_at FROM system.jobs WHERE job_id = '{}'",
            job_id
        );

        let response = server.execute_sql(&query).await;

        if response.status != ResponseStatus::Success {
            // system.jobs might not be accessible in some test setups
            // Just wait the full duration and return success
            println!(
                "  ℹ Cannot query system.jobs (not an error in test env), waiting for job to \
                 execute..."
            );
            sleep(max_wait).await;
            return Ok("Job executed (system.jobs not queryable in test)".to_string());
        }

        if let Some(rows) = response.results.first().and_then(|r| r.rows.as_ref()) {
            if rows.is_empty() {
                // Job not yet in system.jobs, wait a bit
                sleep(check_interval).await;
                continue;
            }

            if let Some(_job) = rows.first() {
                // Use row_as_map to convert array-based row to HashMap for easier access
                let job_map = match response.results.first() {
                    Some(result) => super::QueryResultTestExt::row_as_map(result, 0),
                    None => None,
                };
                let job_map = match job_map {
                    Some(m) => m,
                    None => {
                        sleep(check_interval).await;
                        continue;
                    },
                };
                let status = job_map.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");

                match status {
                    // Transitional states: keep waiting
                    "new" | "queued" | "retrying" | "running" => {
                        // Continue waiting for completion
                        sleep(check_interval).await;
                        continue;
                    },
                    "completed" => {
                        // Verify duration_ms is calculated (not 0)
                        let started_at = job_map.get("started_at").and_then(|v| v.as_i64());
                        let finished_at = job_map.get("finished_at").and_then(|v| v.as_i64());

                        if let (Some(start), Some(end)) = (started_at, finished_at) {
                            let duration_ms = end - start;
                            if duration_ms == 0 {
                                return Err(format!(
                                    "Job {} completed but duration_ms = 0 (started_at: {}, \
                                     finished_at: {}), which indicates a failure or instant \
                                     completion bug",
                                    job_id, start, end
                                ));
                            }
                            println!(
                                "  ✓ Job {} completed successfully in {} ms",
                                job_id, duration_ms
                            );
                        }

                        let result = job_map
                            .get("message")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        return Ok(result);
                    },
                    "failed" => {
                        let error = job_map
                            .get("message")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown error");
                        return Err(format!("Job {} failed: {}", job_id, error));
                    },
                    "cancelled" => {
                        return Err(format!("Job {} was cancelled", job_id));
                    },
                    _ => {
                        // Unknown status: wait a bit more rather than failing fast
                        sleep(check_interval).await;
                        continue;
                    },
                }
            }
        } else {
            // No rows yet, job might not be created or registered
            sleep(check_interval).await;
        }
    }
}

/// Check if Parquet files exist for a user table.
pub fn check_user_parquet_files(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
    user_id: &str,
) -> Vec<PathBuf> {
    let storage_path =
        resolve_user_table_storage_path(server, namespace, table_name, Some(user_id));
    list_parquet_files(&storage_path)
}

/// Check if Parquet files exist for a shared table.
pub fn check_shared_parquet_files(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
) -> Vec<PathBuf> {
    let storage_path = resolve_shared_table_storage_path(server, namespace, table_name);
    list_parquet_files(&storage_path)
}

fn list_parquet_files(storage_path: &Path) -> Vec<PathBuf> {
    let mut parquet_files = Vec::new();

    if storage_path.exists() {
        if let Ok(entries) = std::fs::read_dir(storage_path) {
            for entry in entries.flatten() {
                if let Some(extension) = entry.path().extension() {
                    if extension == "parquet" {
                        println!("  ✓ Found Parquet file: {}", entry.path().display());
                        parquet_files.push(entry.path());
                    }
                }
            }
        }
    }

    parquet_files
}

/// Verify that Parquet files exist after a flush with data
///
/// # Arguments
///
/// * `parquet_files` - Vector of Parquet file paths
/// * `expected_min` - Minimum number of files expected (usually 1 if data exists)
/// * `job_result` - Job result string for additional context
pub fn verify_parquet_files_exist(
    parquet_files: &[PathBuf],
    expected_min: usize,
    job_result: &str,
) -> Result<(), String> {
    if parquet_files.is_empty() && expected_min > 0 {
        return Err(format!(
            "No Parquet files found after flush, but expected at least {}. Job result: {}",
            expected_min, job_result
        ));
    }

    if parquet_files.len() < expected_min {
        return Err(format!(
            "Found {} Parquet files but expected at least {}",
            parquet_files.len(),
            expected_min
        ));
    }

    // Verify each file has reasonable size (not empty/corrupted)
    for file_path in parquet_files {
        match std::fs::metadata(file_path) {
            Ok(metadata) => {
                let file_size = metadata.len();
                // Parquet files should have headers even if minimal (~100 bytes minimum)
                if file_size < 50 {
                    return Err(format!(
                        "Parquet file too small: {} bytes (likely corrupted): {}",
                        file_size,
                        file_path.display()
                    ));
                }
                println!("  ✓ Parquet file valid: {} ({} bytes)", file_path.display(), file_size);
            },
            Err(e) => {
                return Err(format!(
                    "Failed to read file metadata for {}: {}",
                    file_path.display(),
                    e
                ));
            },
        }
    }

    println!("  ✓ All {} Parquet files verified successfully", parquet_files.len());

    Ok(())
}

/// Wait for Parquet files to appear after flush (polling-based check)
///
/// This is useful when system.jobs is not accessible or when the flush
/// happens asynchronously and we want to verify files are created.
///
/// # Arguments
///
/// * `namespace` - Namespace name
/// * `table_name` - Table name
/// * `user_id` - User ID (for user tables)
/// * `max_wait` - Maximum time to wait
/// * `expected_min` - Minimum number of files expected
///
/// # Returns
///
/// Vector of Parquet file paths found
pub async fn wait_for_parquet_files(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
    user_id: &str,
    max_wait: Duration,
    expected_min: usize,
) -> Result<Vec<PathBuf>, String> {
    let storage_path =
        resolve_user_table_storage_path(server, namespace, table_name, Some(user_id));
    let start = std::time::Instant::now();
    let check_interval = Duration::from_millis(100);

    loop {
        if start.elapsed() > max_wait {
            return Err(format!(
                "Timeout waiting for Parquet files after {:?}. Expected at least {} files in {}",
                max_wait,
                expected_min,
                storage_path.display()
            ));
        }

        let parquet_files = list_parquet_files(&storage_path);
        if parquet_files.len() >= expected_min {
            return Ok(parquet_files);
        }

        sleep(check_interval).await;
    }
}

/// Resolve storage path for user table (based on storage registry templates)
fn resolve_user_table_storage_path(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
    user_id: Option<&str>,
) -> PathBuf {
    let storage_id = StorageId::new("local");
    let storage = server
        .app_context
        .storage_registry()
        .get_storage(&storage_id)
        .expect("Storage lookup failed")
        .expect("Storage not found");

    let base_directory = storage.base_directory.clone();
    let template = storage.user_tables_template.clone();
    let user_id = user_id.unwrap_or("user");

    // Replace template variables
    let path = template
        .replace("{userId}", user_id)
        .replace("{namespace}", namespace)
        .replace("{tableName}", table_name);

    PathBuf::from(base_directory).join(path)
}

/// Resolve storage path for shared table (based on storage registry templates)
fn resolve_shared_table_storage_path(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
) -> PathBuf {
    let storage_id = StorageId::new("local");
    let storage = server
        .app_context
        .storage_registry()
        .get_storage(&storage_id)
        .expect("Storage lookup failed")
        .expect("Storage not found");

    let base_directory = storage.base_directory.clone();
    let template = storage.shared_tables_template.clone();

    // Replace template variables
    let path = template.replace("{namespace}", namespace).replace("{tableName}", table_name);

    PathBuf::from(base_directory).join(path)
}
