//! KalamDataType preservation smoke test
//!
//! Verifies that all KalamDataTypes are correctly preserved through
//! the CREATE TABLE -> SELECT * pipeline and returned correctly in
//! the JSON schema response.
//!
//! This test ensures types like FILE, JSON, UUID, DECIMAL, EMBEDDING, etc.
//! are not incorrectly mapped to TEXT or other default types.
//!
//! Run with: cargo test --test smoke smoke_test_datatype_preservation

use crate::common::{
    force_auto_test_server_url_async, generate_unique_namespace, get_access_token_for_url,
    test_context,
};
use reqwest::Client;
use serde_json::Value;

/// Test that all KalamDataTypes are preserved correctly in query results
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_all_kalam_datatypes_are_preserved() {
    let ctx = test_context();
    let client = Client::new();
    let base_url = force_auto_test_server_url_async().await;
    let ns = generate_unique_namespace("dtypes");
    let table = "all_types";
    let token = get_access_token_for_url(&base_url, &ctx.username, &ctx.password)
        .await
        .expect("Failed to get access token");

    // 1. Create namespace
    let create_ns_sql = format!("CREATE NAMESPACE {}", ns);
    let result = execute_sql(&client, &base_url, &token, &create_ns_sql).await;
    assert!(result.is_ok(), "Failed to create namespace: {:?}", result);

    // 2. Create table with ALL KalamDataTypes
    // Order: BOOLEAN, INT, BIGINT, DOUBLE, FLOAT, TEXT, TIMESTAMP, DATE, DATETIME,
    //        TIME, JSON, BYTES, EMBEDDING, UUID, DECIMAL, SMALLINT, FILE
    let create_sql = format!(
        r#"CREATE TABLE {ns}.{table} (
            id BIGINT PRIMARY KEY,
            col_boolean BOOLEAN,
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
            col_bytes BYTES,
            col_embedding EMBEDDING(384),
            col_uuid UUID,
            col_decimal DECIMAL(18, 4),
            col_smallint SMALLINT,
            col_file FILE
        ) WITH (TYPE = 'USER')"#,
        ns = ns,
        table = table
    );

    let result = execute_sql(&client, &base_url, &token, &create_sql).await;
    assert!(result.is_ok(), "CREATE TABLE with all datatypes failed: {:?}", result);

    // 3. Query the table to get schema
    let query_sql = format!("SELECT * FROM {}.{}", ns, table);
    let result = execute_sql(&client, &base_url, &token, &query_sql).await.expect("Query failed");

    // 4. Extract schema from result
    let schema = result["results"][0]["schema"].as_array().expect("No schema in query result");

    // 5. Build a map of column name -> data_type
    let mut type_map = std::collections::HashMap::new();
    for col in schema {
        let name = col["name"].as_str().expect("column name should be string");
        let data_type_val = &col["data_type"];

        let data_type_str = match data_type_val {
            Value::String(s) => s.clone(),
            Value::Object(obj) => {
                // Handle complex types like {"Embedding": 384} or {"Decimal": {"precision": 10, "scale": 2}}
                if let Some((key, _)) = obj.iter().next() {
                    key.clone()
                } else {
                    format!("{:?}", obj)
                }
            },
            other => format!("{:?}", other),
        };
        type_map.insert(name.to_string(), data_type_str);
    }

    println!("Schema types found: {:?}", type_map);

    // 6. Define expected types for each column
    let expected_types = [
        ("id", "BigInt"),
        ("col_boolean", "Boolean"),
        ("col_int", "Int"),
        ("col_bigint", "BigInt"),
        ("col_double", "Double"),
        ("col_float", "Float"),
        ("col_text", "Text"),
        ("col_timestamp", "Timestamp"),
        ("col_date", "Date"),
        ("col_datetime", "DateTime"),
        ("col_time", "Time"),
        ("col_json", "Json"),
        ("col_bytes", "Bytes"),
        ("col_embedding", "Embedding"), // May include dimension in format
        ("col_uuid", "Uuid"),
        ("col_decimal", "Decimal"), // May include precision/scale
        ("col_smallint", "SmallInt"),
        ("col_file", "File"),
        // System columns
        ("_seq", "BigInt"),
        ("_deleted", "Boolean"),
    ];

    // 7. Verify each type
    let mut failures = Vec::new();
    for (col_name, expected_type) in expected_types {
        if let Some(actual_type) = type_map.get(col_name) {
            // For types like Embedding(384) or Decimal(18,4), check if it starts with expected
            let matches = actual_type == expected_type || actual_type.starts_with(expected_type);

            if !matches {
                failures.push(format!(
                    "Column '{}': expected type starting with '{}', got '{}'",
                    col_name, expected_type, actual_type
                ));
            } else {
                println!("✅ Column '{}': {} (expected: {})", col_name, actual_type, expected_type);
            }
        } else {
            failures.push(format!("Column '{}' not found in schema", col_name));
        }
    }

    // 8. Report all failures at once
    if !failures.is_empty() {
        panic!("❌ Datatype preservation test failed:\n{}", failures.join("\n"));
    }

    println!("🎉 All KalamDataTypes preserved correctly!");

    // 9. Cleanup
    let drop_sql = format!("DROP TABLE {}.{}", ns, table);
    let _ = execute_sql(&client, &base_url, &token, &drop_sql).await;
    let drop_ns_sql = format!("DROP NAMESPACE {}", ns);
    let _ = execute_sql(&client, &base_url, &token, &drop_ns_sql).await;
}

/// Test that system.schemas also shows correct data types in the columns JSON
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_system_tables_shows_correct_datatypes() {
    let ctx = test_context();
    let client = Client::new();
    let base_url = force_auto_test_server_url_async().await;
    let ns = generate_unique_namespace("systypes");
    let table = "type_check";
    let token = get_access_token_for_url(&base_url, &ctx.username, &ctx.password)
        .await
        .expect("Failed to get access token");

    // 1. Create namespace
    let create_ns_sql = format!("CREATE NAMESPACE {}", ns);
    let _ = execute_sql(&client, &base_url, &token, &create_ns_sql).await;

    // 2. Create table with specific types we want to verify
    let create_sql = format!(
        r#"CREATE TABLE {}.{} (
            id BIGINT PRIMARY KEY,
            data JSON,
            attachment FILE,
            vector EMBEDDING(768)
        ) WITH (TYPE = 'USER')"#,
        ns, table
    );
    let _ = execute_sql(&client, &base_url, &token, &create_sql).await;

    // 3. Query system.schemas to get the columns JSON
    let query_sql = format!(
        "SELECT columns FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
        ns, table
    );
    let result = execute_sql(&client, &base_url, &token, &query_sql)
        .await
        .expect("Query system.schemas failed");

    let rows = result["results"][0]["rows"]
        .as_array()
        .expect("No rows in system.schemas result");

    assert!(!rows.is_empty(), "Table not found in system.schemas");

    // 4. Parse the columns JSON
    let columns_json_str = rows[0][0].as_str().expect("columns should be a string");
    let columns: Vec<Value> =
        serde_json::from_str(columns_json_str).expect("Failed to parse columns JSON");

    // 5. Build map of column name -> data_type
    let mut type_map = std::collections::HashMap::new();
    for col in &columns {
        let name = col["column_name"].as_str().expect("column_name should be string");
        let data_type_val = &col["data_type"];

        // Handle both String (simple types) and Object (complex types)
        let data_type_str = match data_type_val {
            Value::String(s) => s.clone(),
            Value::Object(obj) => {
                // For complex types like {"Embedding": 768}, we just want the key "Embedding"
                // or serialize it to debug string if key extraction fails
                if let Some((key, _)) = obj.iter().next() {
                    key.clone()
                } else {
                    format!("{:?}", obj)
                }
            },
            other => format!("{:?}", other),
        };

        type_map.insert(name.to_string(), data_type_str);
    }

    println!("system.schemas column types: {:?}", type_map);

    // 6. Verify specific types
    assert_eq!(
        type_map.get("data"),
        Some(&"Json".to_string()),
        "Expected 'Json' for data column in system.schemas, got {:?}",
        type_map.get("data")
    );

    assert_eq!(
        type_map.get("attachment"),
        Some(&"File".to_string()),
        "Expected 'File' for attachment column in system.schemas, got {:?}",
        type_map.get("attachment")
    );

    // Embedding should be stored as "Embedding" with the dimension
    let vector_type = type_map.get("vector").expect("vector column missing");
    assert!(
        vector_type.starts_with("Embedding"),
        "Expected 'Embedding(...)' for vector column, got: {}",
        vector_type
    );

    println!("✅ system.schemas shows correct data types!");

    // 7. Cleanup
    let drop_sql = format!("DROP TABLE {}.{}", ns, table);
    let _ = execute_sql(&client, &base_url, &token, &drop_sql).await;
    let drop_ns_sql = format!("DROP NAMESPACE {}", ns);
    let _ = execute_sql(&client, &base_url, &token, &drop_ns_sql).await;
}

// ============================================================================
// Helper functions
// ============================================================================

async fn execute_sql(
    client: &Client,
    base_url: &str,
    token: &str,
    sql: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client
        .post(format!("{}/v1/api/sql", base_url))
        .bearer_auth(token)
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await?;

    let body = response.text().await?;
    let parsed: Value = serde_json::from_str(&body)?;
    Ok(parsed)
}
