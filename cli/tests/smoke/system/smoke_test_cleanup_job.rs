use crate::common::*;
use std::time::Duration;

fn wait_for_table_absent(
    namespace: &str,
    table: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_millis(500);

    loop {
        let output = execute_sql_as_root_via_client_json(&format!(
            "SELECT table_name FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
            namespace, table
        ))?;
        let json: serde_json::Value = serde_json::from_str(&output)?;
        let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
        if rows.is_empty() {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timed out waiting for table {}.{} to disappear",
                namespace, table
            )
            .into());
        }

        std::thread::sleep(poll_interval);
    }
}

fn parse_cleanup_job_id(output: &str) -> Result<String, Box<dyn std::error::Error>> {
    let marker = "Cleanup job: ";
    if let Some(idx) = output.find(marker) {
        let after = &output[idx + marker.len()..];
        let job_id = after
            .split_whitespace()
            .next()
            .ok_or("Missing cleanup job id after marker")?
            .trim_end_matches(|c: char| c == '.' || c == ',' || c == ';')
            .to_string();
        if job_id.is_empty() {
            return Err("Cleanup job id was empty".into());
        }
        return Ok(job_id);
    }

    Err(format!("Failed to parse cleanup job id from output: {}", output).into())
}

#[test]
fn smoke_cleanup_job_completes() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("cleanup_job_ns");
    let table = generate_unique_table("cleanup_job_table");
    let full_table = format!("{}.{}", namespace, table);

    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace");

    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, value TEXT) WITH (TYPE = 'SHARED')",
        full_table
    ))
    .expect("create table");

    execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (id, value) VALUES (1, 'cleanup')",
        full_table
    ))
    .expect("insert row");

    match execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table)) {
        Ok(drop_output) => {
            if let Ok(job_id) = parse_cleanup_job_id(&drop_output) {
                let status = wait_for_job_finished(&job_id, Duration::from_secs(60))
                    .expect("wait for cleanup job to finish");
                assert_eq!(status, "completed", "cleanup job did not complete");
            } else {
                wait_for_table_absent(&namespace, &table, Duration::from_secs(60))
                    .expect("table should disappear after cleanup fallback");
            }
        },
        Err(error) => {
            wait_for_table_absent(&namespace, &table, Duration::from_secs(60)).unwrap_or_else(
                |_| panic!("drop table: {:?}", error),
            );
        },
    }

    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
