use crate::common::*;

#[ntest::timeout(180000)]
#[test]
fn smoke_test_stream_explain_planning() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_stream_explain_planning: server not running at {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_stream_explain");
    let stream_table = generate_unique_table("events");
    let full_stream_table = format!("{}.{}", namespace, stream_table);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (event_id TEXT PRIMARY KEY, payload TEXT) WITH (TYPE = 'STREAM', TTL_SECONDS = 60)",
        full_stream_table
    ))
    .expect("CREATE STREAM TABLE should succeed");

    let output = execute_sql_as_root_via_client(&format!(
        "EXPLAIN SELECT * FROM {} WHERE event_id = 'evt-1'",
        full_stream_table
    ))
    .expect("EXPLAIN stream query should succeed");

    assert!(
        output.contains("logical_plan | TableScan: "),
        "stream explain should include a table scan, got: {}",
        output
    );
    assert!(
        output.contains("physical_plan | CooperativeExec"),
        "stream explain should use CooperativeExec, got: {}",
        output
    );
    assert!(
        output.contains("DeferredBatchExec: source=stream_table_scan"),
        "stream explain should use the deferred stream scan source, got: {}",
        output
    );
    assert!(
        !output.contains("MemoryExec"),
        "stream explain should not use MemoryExec, got: {}",
        output
    );
    assert!(
        !output.contains("MemTable"),
        "stream explain should not expose MemTable wrappers, got: {}",
        output
    );
    assert!(
        !output.contains("DataSourceExec"),
        "stream explain should not fall back to DataSourceExec, got: {}",
        output
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}