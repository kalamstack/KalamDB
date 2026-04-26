use std::{collections::HashMap, thread, time::Duration};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde_json::Value;

use crate::common::*;

fn extract_first_row_from_cli_json(output: &str) -> Value {
    let json: Value = parse_json_from_cli_output(output)
        .unwrap_or_else(|e| panic!("Failed to parse CLI JSON output: {e}. Raw: {output}"));

    let result = json
        .get("results")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .unwrap_or_else(|| panic!("No results found in CLI JSON output. Raw: {output}"));

    // Get the schema to map column names to indices
    let schema = result
        .get("schema")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("No schema found in CLI JSON output. Raw: {output}"));

    let row = result
        .get("rows")
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .unwrap_or_else(|| panic!("No rows found in CLI JSON output. Raw: {output}"));

    // Build a map from column name to value
    let mut row_map: HashMap<String, Value> = HashMap::new();
    for (i, col) in schema.iter().enumerate() {
        if let Some(name) = col.get("name").and_then(|n| n.as_str()) {
            if let Some(value) = row.get(i) {
                row_map.insert(name.to_string(), value.clone());
            }
        }
    }

    Value::Object(row_map.into_iter().collect())
}

fn normalize_value(value: &Value) -> Value {
    extract_arrow_value(value).unwrap_or_else(|| extract_typed_value(value))
}

fn value_as_i64(value: &Value, column: &str, raw_output: &str) -> i64 {
    match value {
        Value::Number(n) => n.as_i64().unwrap_or_else(|| {
            panic!("Column '{column}' expected i64, got {value}. Raw: {raw_output}")
        }),
        Value::String(s) => s.parse::<i64>().unwrap_or_else(|_| {
            panic!("Column '{column}' expected numeric string, got '{s}'. Raw: {raw_output}")
        }),
        _ => panic!("Column '{column}' expected number, got {value}. Raw: {raw_output}"),
    }
}

fn value_as_f64(value: &Value, column: &str, raw_output: &str) -> f64 {
    match value {
        Value::Number(n) => n.as_f64().unwrap_or_else(|| {
            panic!("Column '{column}' expected f64, got {value}. Raw: {raw_output}")
        }),
        Value::String(s) => s.parse::<f64>().unwrap_or_else(|_| {
            panic!("Column '{column}' expected numeric string, got '{s}'. Raw: {raw_output}")
        }),
        _ => panic!("Column '{column}' expected number, got {value}. Raw: {raw_output}"),
    }
}

fn assert_decimal_column_eq(row: &Value, column: &str, expected: f64, raw_output: &str) {
    let value = row
        .get(column)
        .unwrap_or_else(|| panic!("Missing column '{column}' in row: {row}. Raw: {raw_output}"));

    // Extract from Arrow JSON format first if needed
    let value = extract_arrow_value(value).unwrap_or_else(|| value.clone());

    let actual = match &value {
        Value::Object(obj) if obj.contains_key("value") && obj.contains_key("scale") => {
            // Arrow Decimal128 format: {"precision": 10, "scale": 2, "value": 20075}
            let unscaled = obj.get("value").and_then(|v| v.as_i64()).unwrap_or_else(|| {
                panic!("Decimal128 'value' field should be i64. Got: {value}. Raw: {raw_output}")
            }) as i128;
            let scale = obj.get("scale").and_then(|v| v.as_u64()).unwrap_or_else(|| {
                panic!("Decimal128 'scale' field should be u64. Got: {value}. Raw: {raw_output}")
            }) as u32;

            let denom = 10_f64.powi(scale as i32);
            (unscaled as f64) / denom
        },
        Value::Number(n) => n.as_f64().unwrap_or_else(|| {
            panic!("Decimal column '{column}' was non-f64 number: {value}. Raw: {raw_output}")
        }),
        Value::String(s) => {
            if let Ok(parsed) = s.parse::<f64>() {
                parsed
            } else if let Some(rest) = s.strip_prefix("Some(") {
                // Old server JSON format for DECIMAL: "Some(<unscaled>),<precision>,<scale>"
                // Example: "Some(20075),10,2" => 200.75
                let (unscaled_part, rest) = rest.split_once(')').unwrap_or_else(|| {
                    panic!("Unexpected DECIMAL string format '{s}'. Raw: {raw_output}")
                });
                let unscaled: i128 = unscaled_part.parse().unwrap_or_else(|e| {
                    panic!("Invalid unscaled DECIMAL in '{s}': {e}. Raw: {raw_output}")
                });

                let rest = rest.strip_prefix(',').unwrap_or(rest);
                let mut parts = rest.split(',');
                let _precision = parts.next();
                let scale: u32 = parts.next().unwrap_or("0").parse().unwrap_or_else(|e| {
                    panic!("Invalid DECIMAL scale in '{s}': {e}. Raw: {raw_output}")
                });

                let denom = 10_f64.powi(scale as i32);
                (unscaled as f64) / denom
            } else {
                panic!("Decimal column '{column}' was non-numeric string '{s}'. Raw: {raw_output}")
            }
        },
        _ => {
            panic!("Decimal column '{column}' had unexpected JSON type: {value}. Raw: {raw_output}")
        },
    };

    assert!(
        (actual - expected).abs() < 1e-9,
        "Decimal mismatch for '{column}': expected {expected}, got {actual}. Raw: {raw_output}"
    );
}

/// Test updating all data types in a USER table
#[test]
fn test_update_all_types_user_table() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("all_types_user");
    let namespace = generate_unique_namespace("test_update_types");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Setup namespace
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));

    // Create table with all supported types
    // Note: Skipping EMBEDDING and BYTES for simplicity in SQL literals for now,
    // but covering all scalar types.
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id VARCHAR PRIMARY KEY,
            col_bool BOOLEAN,
            col_int INT,
            col_bigint BIGINT,
            col_double DOUBLE,
            col_float FLOAT,
            col_text TEXT,
            col_timestamp TIMESTAMP,
            col_date DATE,
            col_datetime DATETIME,
            col_time TIME,
            col_json JSON,
            col_uuid UUID,
            col_decimal DECIMAL(10, 2),
            col_smallint SMALLINT
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&create_sql).unwrap();
    assert!(
        output.contains("created")
            || output.contains("Success")
            || output.contains("Query OK")
            || output.contains("Raft consensus"),
        "Table creation failed: {}",
        output
    );

    // Insert initial data
    let insert_sql = format!(
        r#"INSERT INTO {} (
            id, col_bool, col_int, col_bigint, col_double, col_float, col_text, 
            col_timestamp, col_date, col_datetime, col_time, col_json,
            col_decimal, col_smallint
        ) VALUES (
            'row1', true, 123, 1234567890, 123.45, 12.34, 'initial text',
            '2023-01-01 10:00:00', '2023-01-01', '2023-01-01 10:00:00', '10:00:00',
            '{{"key": "initial"}}', 100.50, 100
        )"#,
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&insert_sql).unwrap();
    assert!(
        output.contains("1 row(s) affected")
            || output.contains("1 rows affected")
            || output.contains("Inserted 1 row(s)")
            || output.contains("Success"),
        "Insert failed: {}",
        output
    );

    // Verify initial data
    let query_sql = format!("SELECT * FROM {} WHERE id = 'row1'", full_table_name);
    let output = wait_for_query_contains_with(
        &query_sql,
        "initial text",
        Duration::from_secs(5),
        execute_sql_as_root_via_cli_json,
    )
    .unwrap();
    assert!(output.contains("initial text"), "Initial data not found: {}", output);
    assert!(output.contains("123"), "Initial int not found");
    let row = extract_first_row_from_cli_json(&output);

    let col_bool = normalize_value(row.get("col_bool").expect("Missing col_bool"));
    assert_eq!(col_bool.as_bool(), Some(true));

    let col_int = normalize_value(row.get("col_int").expect("Missing col_int"));
    assert_eq!(value_as_i64(&col_int, "col_int", &output), 123);

    let col_bigint = normalize_value(row.get("col_bigint").expect("Missing col_bigint"));
    assert_eq!(value_as_i64(&col_bigint, "col_bigint", &output), 1234567890);

    let col_double = normalize_value(row.get("col_double").expect("Missing col_double"));
    let double_val = value_as_f64(&col_double, "col_double", &output);
    assert!((double_val - 123.45).abs() < 1e-9, "col_double mismatch: {double_val}");

    let col_float = normalize_value(row.get("col_float").expect("Missing col_float"));
    let float_val = value_as_f64(&col_float, "col_float", &output);
    assert!((float_val - 12.34).abs() < 1e-4, "col_float mismatch: {float_val}");

    let col_text = normalize_value(row.get("col_text").expect("Missing col_text"));
    assert_eq!(col_text.as_str(), Some("initial text"));

    let ts_expected = NaiveDateTime::parse_from_str("2023-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")
        .expect("Invalid timestamp literal")
        .and_utc()
        .timestamp_micros();
    let col_timestamp = normalize_value(row.get("col_timestamp").expect("Missing col_timestamp"));
    assert_eq!(value_as_i64(&col_timestamp, "col_timestamp", &output), ts_expected);

    let date_expected = NaiveDate::parse_from_str("2023-01-01", "%Y-%m-%d")
        .expect("Invalid date literal")
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days();
    let col_date = normalize_value(row.get("col_date").expect("Missing col_date"));
    assert_eq!(value_as_i64(&col_date, "col_date", &output), date_expected);

    let dt_expected = NaiveDateTime::parse_from_str("2023-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")
        .expect("Invalid datetime literal")
        .and_utc()
        .timestamp_micros();
    let col_datetime = normalize_value(row.get("col_datetime").expect("Missing col_datetime"));
    assert_eq!(value_as_i64(&col_datetime, "col_datetime", &output), dt_expected);

    let time_expected =
        NaiveTime::parse_from_str("10:00:00", "%H:%M:%S").expect("Invalid time literal");
    let time_micros = (time_expected.num_seconds_from_midnight() as i64) * 1_000_000
        + (time_expected.nanosecond() as i64 / 1_000);
    let col_time = normalize_value(row.get("col_time").expect("Missing col_time"));
    assert_eq!(value_as_i64(&col_time, "col_time", &output), time_micros);

    let col_json = normalize_value(row.get("col_json").expect("Missing col_json"));
    match col_json {
        Value::String(s) => {
            let parsed: Value = serde_json::from_str(&s)
                .unwrap_or_else(|_| panic!("Invalid JSON in col_json: {s}"));
            assert_eq!(parsed["key"], "initial");
        },
        Value::Object(obj) => assert_eq!(obj.get("key"), Some(&Value::String("initial".into()))),
        other => panic!("Unexpected JSON type for col_json: {other}"),
    }

    let col_smallint = normalize_value(row.get("col_smallint").expect("Missing col_smallint"));
    assert_eq!(value_as_i64(&col_smallint, "col_smallint", &output), 100);

    assert_decimal_column_eq(&row, "col_decimal", 100.50, &output);

    // --- NEW SCENARIO: Flush initial data to cold storage before update ---
    println!("Flushing initial data to cold storage...");
    let flush_sql = format!("STORAGE FLUSH TABLE {}", full_table_name);
    let flush_output = execute_sql_as_root_via_cli(&flush_sql).unwrap();
    println!("[DEBUG] Flush output: {}", flush_output);

    // Wait for flush to complete - MUST succeed, otherwise data is not visible
    let job_id = match parse_job_id_from_flush_output(&flush_output) {
        Ok(id) => {
            println!("[DEBUG] Parsed initial flush job ID: {}", id);
            id
        },
        Err(e) => {
            panic!(
                "[ERROR] Failed to parse job ID from flush output: {} | Output: {}",
                e, flush_output
            );
        },
    };

    println!("[DEBUG] Waiting for initial flush job {}...", job_id);
    match verify_job_completed(&job_id, Duration::from_secs(20)) {
        Ok(()) => println!("[DEBUG] Initial flush job {} completed successfully", job_id),
        Err(e) => {
            // Query job status for debugging
            if let Ok(status_out) = execute_sql_as_root_via_cli_json(&format!(
                "SELECT job_id, status, message FROM system.jobs WHERE job_id = '{}'",
                job_id
            )) {
                eprintln!("[DEBUG] Job status query result: {}", status_out);
            }
            panic!("[ERROR] Initial flush job {} failed or timed out: {}", job_id, e);
        },
    }

    // Verify initial data is still readable after flush
    let output = wait_for_query_contains_with(
        &query_sql,
        "initial text",
        Duration::from_secs(5),
        execute_sql_as_root_via_cli_json,
    )
    .unwrap();
    assert!(
        output.contains("initial text"),
        "Initial data not found after flush: {}",
        output
    );
    assert!(output.contains("123"), "Initial int not found after flush");
    // ---------------------------------------------------------------------

    // Update all columns
    let update_sql = format!(
        r#"UPDATE {} SET 
            col_bool = false,
            col_int = 456,
            col_bigint = 9876543210,
            col_double = 987.65,
            col_float = 56.78,
            col_text = 'updated text',
            col_timestamp = '2023-12-31 23:59:59',
            col_date = '2023-12-31',
            col_datetime = '2023-12-31 23:59:59',
            col_time = '23:59:59',
            col_json = '{{"key": "updated"}}',
            col_decimal = 200.75,
            col_smallint = 200
        WHERE id = 'row1'"#,
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&update_sql).unwrap();
    assert!(
        output.contains("1 row(s) affected")
            || output.contains("1 rows affected")
            || output.contains("Updated 1 row(s)")
            || output.contains("Success"),
        "Update failed: {}",
        output
    );

    // Verify updated data (before flush)
    let output = execute_sql_as_root_via_cli_json(&query_sql).unwrap();
    assert!(output.contains("updated text"), "Updated text not found: {}", output);
    assert!(output.contains("456"), "Updated int not found");
    let row = extract_first_row_from_cli_json(&output);
    assert_decimal_column_eq(&row, "col_decimal", 200.75, &output);
    // Note: JSON formatting might vary (whitespace), so we might need loose check or just check
    // presence of "updated"
    assert!(output.contains("updated"), "Updated JSON content not found");

    // Flush table
    let flush_sql = format!("STORAGE FLUSH TABLE {}", full_table_name);
    let output = execute_sql_as_root_via_cli(&flush_sql).unwrap();

    // Wait for flush to complete
    if let Ok(job_id) = parse_job_id_from_flush_output(&output) {
        println!("Waiting for flush job {}...", job_id);
        if let Err(e) = verify_job_completed(&job_id, Duration::from_secs(10)) {
            eprintln!("Flush job failed or timed out: {}", e);
            // Continue anyway to see if data is readable
        }
    } else {
        // If we can't parse job ID, just wait a bit
        thread::sleep(Duration::from_millis(20));
    }

    // Verify updated data (after flush)
    let output = wait_for_query_contains_with(
        &query_sql,
        "updated text",
        Duration::from_secs(5),
        execute_sql_as_root_via_cli_json,
    )
    .unwrap();
    assert!(
        output.contains("updated text"),
        "Updated text not found after flush: {}",
        output
    );
    assert!(output.contains("456"), "Updated int not found after flush");
    let row = extract_first_row_from_cli_json(&output);
    assert_decimal_column_eq(&row, "col_decimal", 200.75, &output);

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test updating all data types in a SHARED table
#[test]
fn test_update_all_types_shared_table() {
    if cfg!(windows) {
        eprintln!(
            "⚠️  Skipping on Windows due to intermittent access violations in shared table tests."
        );
        return;
    }
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("all_types_shared");
    let namespace = generate_unique_namespace("test_update_types");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Setup namespace
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));

    // Create table with all supported types
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id VARCHAR PRIMARY KEY,
            col_bool BOOLEAN,
            col_int INT,
            col_bigint BIGINT,
            col_double DOUBLE,
            col_float FLOAT,
            col_text TEXT,
            col_timestamp TIMESTAMP,
            col_date DATE,
            col_datetime DATETIME,
            col_time TIME,
            col_json JSON,
            col_uuid UUID,
            col_decimal DECIMAL(10, 2),
            col_smallint SMALLINT
        ) WITH (TYPE='SHARED', FLUSH_POLICY='rows:10')"#,
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&create_sql).unwrap();
    assert!(
        output.contains("created")
            || output.contains("Success")
            || output.contains("Query OK")
            || output.contains("Raft consensus"),
        "Table creation failed: {}",
        output
    );

    // Insert initial data
    let insert_sql = format!(
        r#"INSERT INTO {} (
            id, col_bool, col_int, col_bigint, col_double, col_float, col_text, 
            col_timestamp, col_date, col_datetime, col_time, col_json,
            col_decimal, col_smallint
        ) VALUES (
            'row1', true, 123, 1234567890, 123.45, 12.34, 'initial text',
            '2023-01-01 10:00:00', '2023-01-01', '2023-01-01 10:00:00', '10:00:00',
            '{{"key": "initial"}}', 100.50, 100
        )"#,
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&insert_sql).unwrap();
    assert!(
        output.contains("1 row(s) affected")
            || output.contains("1 rows affected")
            || output.contains("Inserted 1 row(s)")
            || output.contains("Success"),
        "Insert failed: {}",
        output
    );

    // Verify initial data
    let query_sql = format!("SELECT * FROM {} WHERE id = 'row1'", full_table_name);
    let output = wait_for_query_contains_with(
        &query_sql,
        "initial text",
        Duration::from_secs(5),
        execute_sql_as_root_via_cli_json,
    )
    .unwrap();
    assert!(output.contains("initial text"), "Initial data not found: {}", output);
    assert!(output.contains("123"), "Initial int not found");

    // --- NEW SCENARIO: Flush initial data to cold storage before update ---
    println!("Flushing initial data to cold storage...");
    let flush_sql = format!("STORAGE FLUSH TABLE {}", full_table_name);
    let output = execute_sql_as_root_via_cli(&flush_sql).unwrap();

    // Wait for flush to complete
    if let Ok(job_id) = parse_job_id_from_flush_output(&output) {
        println!("Waiting for initial flush job {}...", job_id);
        if let Err(e) = verify_job_completed(&job_id, Duration::from_secs(10)) {
            eprintln!("Initial flush job failed or timed out: {}", e);
        }
    } else {
        thread::sleep(Duration::from_millis(20));
    }

    // Verify initial data is still readable after flush
    let output = execute_sql_as_root_via_cli_json(&query_sql).unwrap();
    assert!(
        output.contains("initial text"),
        "Initial data not found after flush: {}",
        output
    );
    assert!(output.contains("123"), "Initial int not found after flush");
    // ---------------------------------------------------------------------

    // Update all columns
    let update_sql = format!(
        r#"UPDATE {} SET 
            col_bool = false,
            col_int = 456,
            col_bigint = 9876543210,
            col_double = 987.65,
            col_float = 56.78,
            col_text = 'updated text',
            col_timestamp = '2023-12-31 23:59:59',
            col_date = '2023-12-31',
            col_datetime = '2023-12-31 23:59:59',
            col_time = '23:59:59',
            col_json = '{{"key": "updated"}}',
            col_decimal = 200.75,
            col_smallint = 200
        WHERE id = 'row1'"#,
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&update_sql).unwrap();
    assert!(
        output.contains("1 row(s) affected")
            || output.contains("1 rows affected")
            || output.contains("Updated 1 row(s)")
            || output.contains("Success"),
        "Update failed: {}",
        output
    );

    // Verify updated data (before flush)
    let output = execute_sql_as_root_via_cli_json(&query_sql).unwrap();
    assert!(output.contains("updated text"), "Updated text not found: {}", output);
    assert!(output.contains("456"), "Updated int not found");
    let row = extract_first_row_from_cli_json(&output);
    assert_decimal_column_eq(&row, "col_decimal", 200.75, &output);
    assert!(output.contains("updated"), "Updated JSON content not found");

    // Flush table
    let flush_sql = format!("STORAGE FLUSH TABLE {}", full_table_name);
    let output = execute_sql_as_root_via_cli(&flush_sql).unwrap();

    // Wait for flush to complete
    if let Ok(job_id) = parse_job_id_from_flush_output(&output) {
        println!("Waiting for flush job {}...", job_id);
        if let Err(e) = verify_job_completed(&job_id, Duration::from_secs(10)) {
            eprintln!("Flush job failed or timed out: {}", e);
        }
    } else {
        thread::sleep(Duration::from_millis(20));
    }

    // Verify updated data (after flush)
    let output = execute_sql_as_root_via_cli_json(&query_sql).unwrap();
    assert!(
        output.contains("updated text"),
        "Updated text not found after flush: {}",
        output
    );
    assert!(output.contains("456"), "Updated int not found after flush");
    let row = extract_first_row_from_cli_json(&output);
    assert_decimal_column_eq(&row, "col_decimal", 200.75, &output);

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
