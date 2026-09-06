use std::time::Duration;

use crate::common::{
    assert_flush_storage_files_exist, execute_sql_as_root_via_cli,
    execute_sql_as_root_via_client_json, extract_typed_value, get_rows_as_hashmaps,
    is_cluster_mode, parse_job_id_from_flush_output, verify_job_completed,
};

pub(super) fn embedding_literal(dimension: usize, active_index: usize) -> String {
    assert!(dimension > 0, "embedding dimension must be positive");
    assert!(active_index < dimension, "embedding index must be in range");

    let mut values = Vec::with_capacity(dimension);
    for index in 0..dimension {
        if index == active_index {
            values.push("1.0");
        } else {
            values.push("0.0");
        }
    }

    format!("[{}]", values.join(","))
}

pub(super) fn vector_query_ids(full_table: &str, query_vector: &str) -> Vec<i64> {
    let sql = format!(
        "SELECT id FROM {} ORDER BY COSINE_DISTANCE(embedding, '{}') LIMIT 2",
        full_table, query_vector
    );
    let output = execute_sql_as_root_via_client_json(&sql).expect("vector similarity query");
    let json: serde_json::Value = serde_json::from_str(&output).expect("vector query json");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();

    rows.into_iter()
        .filter_map(|row| {
            row.get("id").and_then(|value| {
                let extracted = extract_typed_value(value);
                extracted
                    .as_i64()
                    .or_else(|| extracted.as_str().and_then(|text| text.parse::<i64>().ok()))
            })
        })
        .collect()
}

pub(super) fn flush_user_table_and_wait(
    namespace: &str,
    table_name: &str,
    full_table: &str,
    context: &str,
) {
    let flush_output = execute_sql_as_root_via_cli(&format!("STORAGE FLUSH TABLE {}", full_table))
        .expect("storage flush table");

    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        let timeout = if is_cluster_mode() {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(10)
        };
        verify_job_completed(&job_id, timeout).expect("flush job should complete");
    }

    assert_flush_storage_files_exist(namespace, table_name, true, context);
}
