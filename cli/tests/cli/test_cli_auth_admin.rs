//! CLI integration tests for authentication and admin operations
//!
//! Tests proper authentication flow and admin-level SQL commands using real credentials.
//!
//! # Running Tests
//!
//! ```bash
//! # Start server in one terminal
//! cargo run --release --bin kalamdb-server
//!
//! # Run tests in another terminal
//! cargo test --test test_cli_auth_admin -- --test-threads=1
//! ```
//TODO: Remove this since we have most of the tests covered by the integration tests
#![allow(unused_imports)]
use crate::common::*;
use assert_cmd::Command;
use std::time::Duration;

/// Test that root user can create namespaces
#[tokio::test]
async fn test_root_can_create_namespace() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running at {}. Skipping test.", server_url());
        return;
    }

    // Use unique namespace name to avoid conflicts
    let namespace_name = generate_unique_namespace("test_root_ns");

    // Clean up any existing namespace (just in case)
    let _ = execute_sql_via_http_as_root(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace_name
    ))
    .await;

    // Create namespace as root
    let result =
        match execute_sql_via_http_as_root(&format!("CREATE NAMESPACE {}", namespace_name)).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("execute_sql_as_root failed: {:?}", e);
                panic!("execute_sql_as_root failed: {:?}", e);
            },
        };

    // Should succeed
    if result["status"] != "success" {
        eprintln!("CREATE NAMESPACE failed: {:?}", result);
        panic!("CREATE NAMESPACE failed: {:?}", result);
    }

    // Verify namespace was created
    let select_result = execute_sql_via_http_as_root(&format!(
        "SELECT name FROM system.namespaces WHERE name = '{}'",
        namespace_name
    ))
    .await
    .unwrap();

    if select_result["status"] != "success" {
        eprintln!("SELECT failed: {:?}", select_result);
        panic!("SELECT failed: {:?}", select_result);
    }

    assert_eq!(select_result["status"], "success");
    // Rows are arrays, not objects. Check if any row contains the namespace name.
    // Format: {"rows": [["namespace_name"], ...], "schema": [{"name": "name", "index": 0}, ...]}
    assert!(
        select_result["results"]
            .as_array()
            .and_then(|results| results.first())
            .and_then(|result| result["rows"].as_array())
            .map(|rows| {
                rows.iter().any(|row| {
                    // Row is an array like ["namespace_name"]
                    row.as_array()
                        .and_then(|arr| arr.first())
                        .and_then(|v| v.as_str())
                        .map(|name| name == namespace_name)
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false),
        "Namespace should exist in system.namespaces"
    );

    // Cleanup
    let _ =
        execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace_name)).await;
}

/// Test that root user can create and drop tables
#[tokio::test]
async fn test_root_can_create_drop_tables() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Use unique namespace name to avoid conflicts
    let namespace_name = generate_unique_namespace("test_tables_ns");

    // Ensure namespace exists
    let _ =
        execute_sql_via_http_as_root(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace_name))
            .await;

    // Create table as root
    let result = execute_sql_via_http_as_root(&format!(
        "CREATE TABLE {}.test_table (id INT PRIMARY KEY, name VARCHAR) WITH (TYPE='USER', FLUSH_POLICY='rows:10')",
        namespace_name
    ))
    .await
    .unwrap();

    assert_eq!(
        result["status"], "success",
        "Root user should be able to create tables: {:?}",
        result
    );

    // Drop table
    let result = execute_sql_via_http_as_root(&format!("DROP TABLE {}.test_table", namespace_name))
        .await
        .unwrap();

    assert_eq!(result["status"], "success", "Root user should be able to drop tables");

    // Cleanup
    let _ =
        execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace_name)).await;
}

/// Test CREATE NAMESPACE via CLI with root authentication
#[tokio::test]
async fn test_cli_create_namespace_as_root() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Use unique namespace name to avoid conflicts
    let namespace_name = generate_unique_namespace("cli_test_ns");

    // Clean up any existing namespace
    let drop_result =
        execute_sql_via_http_as_root(&format!("DROP NAMESPACE IF EXISTS {}", namespace_name)).await;
    if let Err(e) = &drop_result {
        eprintln!("DROP NAMESPACE failed: {:?}", e);
    }
    if let Ok(result) = &drop_result {
        if result["status"] != "success" {
            eprintln!("DROP NAMESPACE returned non-success: {:?}", result);
        }
    }

    // Execute CREATE NAMESPACE via CLI with root auth
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace_name))
        .expect("CLI should succeed when creating namespace as root");

    // Verify namespace was created
    let result = execute_sql_via_http_as_root(&format!(
        "SELECT name FROM system.namespaces WHERE name = '{}'",
        namespace_name
    ))
    .await
    .unwrap();

    assert_eq!(result["status"], "success");

    // Cleanup
    let _ =
        execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace_name)).await;
}

/// Test that non-admin users cannot create namespaces
#[tokio::test]
async fn test_regular_user_cannot_create_namespace() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // First, create a regular user as root
    let _ = execute_sql_via_http_as_root("DROP USER IF EXISTS testuser").await;

    let result =
        execute_sql_via_http_as_root("CREATE USER testuser PASSWORD 'testpass' ROLE user").await;

    if result.is_err() || result.as_ref().unwrap()["status"] != "success" {
        eprintln!("⚠️  Failed to create test user, skipping test");
        return;
    }

    // Try to create namespace as regular user
    let result =
        execute_sql_via_http_as("testuser", "testpass", "CREATE NAMESPACE user_test_ns").await;

    // Should fail with authorization error
    if let Ok(response) = result {
        assert!(
            response["status"] == "error"
                && (response["error"].as_str().unwrap_or("").contains("Admin privileges")
                    || response["error"].as_str().unwrap_or("").contains("Unauthorized")),
            "Regular user should not be able to create namespaces: {:?}",
            response
        );
    }

    // Cleanup
    let _ = execute_sql_via_http_as_root("DROP USER testuser").await;
}

/// Test CLI with explicit username/password
#[tokio::test]
async fn test_cli_with_explicit_credentials() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Execute query with explicit admin credentials
    let output = execute_sql_via_cli_as(admin_username(), admin_password(), "SELECT 1 as test")
        .expect("CLI should work with explicit admin credentials");

    assert!(
        output.contains("test"),
        "CLI should work with explicit admin credentials: {}",
        output
    );
}

/// Test admin operations via CLI
#[tokio::test]
async fn test_cli_admin_operations() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Use unique namespace name to avoid conflicts
    let namespace_name = generate_unique_namespace("admin_ops_test");
    // Use nanoseconds + pid for truly unique ID
    let unique_id = format!(
        "{:x}{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
        std::process::id()
    );

    // Clean up with retry to handle propagation delays in cluster
    for _ in 0..3 {
        let _ = execute_sql_via_http_as_root(&format!(
            "DROP NAMESPACE IF EXISTS {} CASCADE",
            namespace_name
        ))
        .await;
    }

    // Execute statements individually to avoid batch execution bug with Raft
    // Step 1: Create namespace
    let _ =
        execute_sql_via_http_as_root(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace_name))
            .await;

    // Step 2: Create table
    let _ = execute_sql_via_http_as_root(&format!(
        "CREATE TABLE IF NOT EXISTS {}.users (id TEXT PRIMARY KEY, name VARCHAR) WITH (TYPE='USER', FLUSH_POLICY='rows:10')",
        namespace_name
    ))
    .await;

    // Step 3: Insert data via CLI to actually test CLI functionality
    let insert_sql = format!(
        "INSERT INTO {}.users (id, name) VALUES ('{}', 'Alice')",
        namespace_name, unique_id
    );
    let stdout = execute_sql_as_root_via_cli(&insert_sql).expect("CLI insert should succeed");

    assert!(
        stdout.contains("1 row") || stdout.contains("Query OK"),
        "INSERT admin command should succeed.\nstdout: {}",
        stdout
    );

    // Step 4: Select to verify
    let select_sql = format!("SELECT * FROM {}.users WHERE id = '{}'", namespace_name, unique_id);
    let stdout = execute_sql_as_root_via_cli(&select_sql).expect("CLI select should succeed");

    assert!(stdout.contains("Alice"), "SELECT should show inserted data. stdout: {}", stdout);

    // Cleanup
    let _ =
        execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace_name)).await;
}

/// Test SHOW NAMESPACES command
#[tokio::test]
async fn test_cli_show_namespaces() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let stdout = execute_sql_as_root_via_cli("SHOW NAMESPACES")
        .expect("SHOW NAMESPACES command should succeed");

    // Should show table format with column headers
    assert!(
        stdout.contains("name") || stdout.contains("namespace"),
        "SHOW NAMESPACES should display namespace names: {}",
        stdout
    );
}

/// Test STORAGE FLUSH TABLE command via CLI
#[tokio::test]
async fn test_cli_flush_table() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Use unique namespace name to avoid conflicts
    let namespace_name = generate_unique_namespace("flush_test_ns");

    // Setup: Create namespace and user table
    let _ = execute_sql_via_http_as_root(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace_name
    ))
    .await;

    let _ = execute_sql_via_http_as_root(&format!("CREATE NAMESPACE {}", namespace_name)).await;

    // Create a USER table with flush policy (SHARED tables cannot be flushed)
    let result = execute_sql_via_http_as_root(&format!(
        "CREATE TABLE {}.metrics (timestamp BIGINT PRIMARY KEY, value DOUBLE) WITH (TYPE='USER', FLUSH_POLICY='rows:5')",
        namespace_name
    ))
    .await
    .unwrap();

    assert_eq!(result["status"], "success", "Should create user table: {:?}", result);

    // Insert some data to trigger potential flush
    for i in 1..=3 {
        let insert_sql = format!(
            "INSERT INTO {}.metrics (timestamp, value) VALUES ({}, {}.5)",
            namespace_name, i, i
        );
        let _ = execute_sql_via_http_as_root(&insert_sql).await;
    }

    // Execute STORAGE FLUSH TABLE via CLI
    let stdout =
        execute_sql_as_root_via_cli(&format!("STORAGE FLUSH TABLE {}.metrics", namespace_name))
            .expect("STORAGE FLUSH TABLE should succeed");

    // Verify the flush command was accepted (should show job info or success message)
    assert!(
        stdout.contains("Flush")
            || stdout.contains("Job")
            || stdout.contains("success")
            || stdout.contains("Query OK"),
        "Output should indicate flush operation: {}",
        stdout
    );

    // Extract job ID from output (format: "Job ID: flush-...")
    let job_id = if let Some(job_id_start) = stdout.find("Job ID: ") {
        let id_start = job_id_start + "Job ID: ".len();
        let id_end = stdout[id_start..].find('\n').unwrap_or(stdout[id_start..].len());
        Some(stdout[id_start..id_start + id_end].trim().to_string())
    } else {
        None
    };

    // If we have a job ID, query for that specific job
    // Note: system.jobs stores namespace/table info inside the JSON `parameters` column.
    let jobs_query = if let Some(ref job_id) = job_id {
        format!(
            "SELECT job_id, job_type, status, parameters, message FROM system.jobs \
             WHERE job_id = '{}' LIMIT 1",
            job_id
        )
    } else {
        // Fallback to querying by type and table name (from `parameters` JSON)
        "SELECT job_id, job_type, status, parameters, message FROM system.jobs \
         WHERE job_type = 'flush' AND parameters LIKE '%\"table_name\":\"metrics\"%' \
         ORDER BY created_at DESC LIMIT 1"
            .to_string()
    };

    // In cluster mode, job creation/visibility can lag due to Raft replication.
    // Poll briefly until the job row becomes visible.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    let (jobs_result, jobs_data) = loop {
        let jobs_result = execute_sql_via_http_as_root(&jobs_query).await.unwrap();

        assert_eq!(
            jobs_result["status"], "success",
            "Should be able to query system.jobs: {:?}",
            jobs_result
        );

        // Handle both "data" array format and "results" format
        let jobs_data = if let Some(data) = jobs_result["data"].as_array() {
            data.clone()
        } else if let Some(results) = jobs_result["results"].as_array() {
            // Results format - extract from first result if available
            if let Some(first_result) = results.first() {
                if let Some(rows) = first_result["rows"].as_array() {
                    rows.clone()
                } else if let Some(data) = first_result["data"].as_array() {
                    data.clone()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        if !jobs_data.is_empty() {
            break (jobs_result, jobs_data);
        }
        if std::time::Instant::now() > deadline {
            break (jobs_result, jobs_data);
        }
    };

    assert!(
        !jobs_data.is_empty(),
        "Should have found the flush job{}: {}",
        if job_id.is_some() {
            " by job ID"
        } else {
            " by table name"
        },
        jobs_result
    );

    // Normalize job row: DataFusion may return row as an array with separate columns metadata.
    println!("DEBUG jobs_result raw: {}", jobs_result); // diagnostics
    println!("DEBUG first job raw row: {}", jobs_data[0]);
    let job = if jobs_data[0].is_array() {
        // Build an object map {column_name: value} using returned schema metadata
        let mut obj = serde_json::Map::new();
        // Schema format: [{"data_type":"Text","index":0,"name":"job_id"}, ...]
        let schema_vec =
            jobs_result["results"][0]["schema"].as_array().cloned().unwrap_or_default();
        let values = jobs_data[0].as_array().unwrap();
        for schema_entry in schema_vec.iter() {
            if let (Some(col_name), Some(idx)) =
                (schema_entry["name"].as_str(), schema_entry["index"].as_u64())
            {
                let val = values.get(idx as usize).cloned().unwrap_or(serde_json::Value::Null);
                obj.insert(col_name.to_string(), val);
            }
        }
        serde_json::Value::Object(obj)
    } else {
        jobs_data[0].clone()
    };

    // If we extracted a job ID, verify it matches
    if let Some(ref expected_job_id) = job_id {
        assert_eq!(
            job["job_id"].as_str().unwrap(),
            expected_job_id,
            "Job ID should match the one returned by FLUSH command"
        );
    }

    assert_eq!(
        job["job_type"].as_str().unwrap().to_lowercase(),
        "flush",
        "Job type should be 'flush' (case-insensitive)"
    );

    let params = job["parameters"].as_str().and_then(|s| {
        if s.is_empty() {
            None
        } else {
            serde_json::from_str::<serde_json::Value>(s).ok()
        }
    });
    let params = params.as_ref().unwrap_or(&serde_json::Value::Null);

    // table_id is serialized as "namespace.table" format
    let table_id = params["table_id"].as_str().unwrap_or("");
    let expected_table_id = format!("{}.metrics", namespace_name);
    assert_eq!(
        table_id, expected_table_id,
        "Job parameters should reference correct table_id (namespace.table format)"
    );

    let final_status = if let Some(expected_job_id) = job_id {
        wait_for_job_finished(&expected_job_id, Duration::from_secs(90))
            .expect("Flush job should reach a terminal state")
    } else {
        job["status"].as_str().unwrap_or("").to_lowercase()
    };

    assert_eq!(
        final_status, "completed",
        "Flush job failed (status: {})",
        final_status
    );

    // If job is completed, result metadata may be empty depending on backend timing.

    // Verify data is still accessible after flush
    let result = execute_sql_via_http_as_root(&format!(
        "SELECT COUNT(*) as count FROM {}.metrics",
        namespace_name
    ))
    .await
    .unwrap();

    assert_eq!(result["status"], "success");

    // Cleanup
    let _ =
        execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace_name)).await;
}

/// Test STORAGE FLUSH ALL command via CLI
#[tokio::test]
async fn test_cli_flush_all_tables() {
    if !is_server_running_with_auth().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Use unique namespace name to avoid conflicts
    let namespace_name = generate_unique_namespace("flush_all_test");

    // Setup: Create namespace with multiple tables
    let _ = execute_sql_via_http_as_root(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace_name
    ))
    .await;

    let _ = execute_sql_via_http_as_root(&format!("CREATE NAMESPACE {}", namespace_name)).await;

    // Create multiple USER tables (SHARED tables cannot be flushed)
    let _ = execute_sql_via_http_as_root(
        &format!("CREATE TABLE {}.table1 (id INT PRIMARY KEY, data VARCHAR) WITH (TYPE='USER', FLUSH_POLICY='rows:10')", namespace_name),
    )
    .await;
    let _ = execute_sql_via_http_as_root(
        &format!("CREATE TABLE {}.table2 (id INT PRIMARY KEY, value DOUBLE) WITH (TYPE='USER', FLUSH_POLICY='rows:10')", namespace_name),
    )
    .await;

    // Insert some data
    let _ = execute_sql_via_http_as_root(&format!(
        "INSERT INTO {}.table1 (id, data) VALUES (1, 'test')",
        namespace_name
    ))
    .await;
    let _ = execute_sql_via_http_as_root(&format!(
        "INSERT INTO {}.table2 (id, value) VALUES (1, 42.0)",
        namespace_name
    ))
    .await;

    // Execute STORAGE FLUSH ALL via CLI
    let stdout = execute_sql_as_root_via_cli(&format!("STORAGE FLUSH ALL IN {}", namespace_name))
        .expect("STORAGE FLUSH ALL should succeed");

    // Verify the command was accepted
    assert!(
        stdout.contains("Flush")
            || stdout.contains("Job")
            || stdout.contains("success")
            || stdout.contains("Query OK"),
        "Output should indicate flush operation: {}",
        stdout
    );

    // Extract all job IDs from output
    // Format: "Job ID: flush-..." (single) or "Job ID: [flush-..., flush-...]" (multiple)
    let job_ids: Vec<String> = if let Some(pos) = stdout.find("Job ID:") {
        let rest = &stdout[pos + 7..].trim();
        if rest.starts_with('[') {
            // Multiple job IDs (STORAGE FLUSH ALL)
            let end = rest.find(']').unwrap_or(rest.len());
            let ids_str = &rest[1..end];
            ids_str
                .split(',')
                .map(|id| id.trim().to_string())
                .filter(|id| !id.is_empty())
                .collect()
        } else {
            // Single job ID
            vec![rest.split_whitespace().next().unwrap_or("").to_string()]
        }
    } else {
        vec![]
    };

    println!("Extracted job IDs: {:?}", job_ids);

    // Wait for jobs to complete

    // If we have job IDs, query for those specific jobs
    let jobs_query = if !job_ids.is_empty() {
        let job_id_list =
            job_ids.iter().map(|id| format!("'{}'", id)).collect::<Vec<_>>().join(", ");
        format!(
            "SELECT job_id, job_type, status, parameters, message FROM system.jobs \
             WHERE job_id IN ({}) \
             ORDER BY created_at DESC",
            job_id_list
        )
    } else {
        // Fallback to querying by namespace
        format!(
            "SELECT job_id, job_type, status, parameters, message FROM system.jobs \
         WHERE job_type = 'flush' AND parameters LIKE '%\"namespace_id\":\"{}\"%' \
         ORDER BY created_at DESC",
            namespace_name
        )
    };

    let jobs_result = execute_sql_via_http_as_root(&jobs_query).await.unwrap();

    assert_eq!(
        jobs_result["status"], "success",
        "Should be able to query system.jobs: {:?}",
        jobs_result
    );

    // Handle both "data" array format and "results" format
    let jobs_data = if let Some(data) = jobs_result["data"].as_array() {
        data
    } else if let Some(results) = jobs_result["results"].as_array() {
        // Results format - extract from first result if available
        if let Some(first_result) = results.first() {
            if let Some(rows) = first_result["rows"].as_array() {
                rows
            } else if let Some(data) = first_result["data"].as_array() {
                data
            } else {
                &vec![]
            }
        } else {
            &vec![]
        }
    } else {
        &vec![]
    };

    // Normalize rows to objects if DataFusion returns arrays.
    let schema_vec = jobs_result["results"][0]["schema"].as_array().cloned().unwrap_or_default();
    let jobs_data: Vec<serde_json::Value> = jobs_data
        .iter()
        .map(|row| {
            if row.is_array() {
                let mut obj = serde_json::Map::new();
                let values = row.as_array().unwrap();
                for schema_entry in schema_vec.iter() {
                    if let (Some(col_name), Some(idx)) =
                        (schema_entry["name"].as_str(), schema_entry["index"].as_u64())
                    {
                        let val =
                            values.get(idx as usize).cloned().unwrap_or(serde_json::Value::Null);
                        obj.insert(col_name.to_string(), val);
                    }
                }
                serde_json::Value::Object(obj)
            } else {
                row.clone()
            }
        })
        .collect();

    // Note: May have 0 jobs if tables were empty and nothing to flush
    if jobs_data.is_empty() {
        if !job_ids.is_empty() {
            panic!("Expected to find jobs with IDs {:?}, but found none", job_ids);
        }
        eprintln!(
            "Warning: No flush jobs found. Tables may have been empty or jobs not created yet."
        );
    }

    // If we extracted job IDs, verify we found all of them
    if !job_ids.is_empty() && !jobs_data.is_empty() {
        let found_job_ids: Vec<&str> =
            jobs_data.iter().filter_map(|job| job["job_id"].as_str()).collect();

        for expected_job_id in &job_ids {
            assert!(
                found_job_ids.contains(&expected_job_id.as_str()),
                "Should have found job ID {} in system.jobs. Found: {:?}",
                expected_job_id,
                found_job_ids
            );
        }
    }

    // If we have jobs, verify table names (from `parameters` JSON)
    if !jobs_data.is_empty() {
        // Verify both tables have flush jobs
        let table_names: Vec<String> = jobs_data
            .iter()
            .filter_map(|job| {
                job["parameters"].as_str().and_then(|s| {
                    serde_json::from_str::<serde_json::Value>(s)
                        .ok()
                        .and_then(|p| p["table_name"].as_str().map(|t| t.to_string()))
                })
            })
            .collect();

        assert!(
            table_names.iter().any(|t| t == "table1") || table_names.iter().any(|t| t == "table2"),
            "Should have flush jobs for table1 and/or table2, got: {:?}",
            table_names
        );
    }

    // Verify at least one job completed successfully (if any jobs exist)
    if !jobs_data.is_empty() {
        let completed_jobs: Vec<_> = jobs_data
            .iter()
            .filter(|job| job["status"].as_str().unwrap_or("") == "completed")
            .collect();

        if !completed_jobs.is_empty() {
            let job = completed_jobs[0];
            let result_str = job["result"].as_str().unwrap_or("");
            assert!(
                !result_str.is_empty()
                    || result_str.contains("rows")
                    || result_str.contains("Flushed"),
                "Completed job should have result information: {}",
                result_str
            );
        }
    }

    // Cleanup
    let _ =
        execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace_name)).await;
}
