use crate::common::*;

#[ntest::timeout(180000)]
#[test]
fn smoke_test_provider_exec_models() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_provider_exec_models: server not running at {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_exec_models");
    let shared_table = generate_unique_table("items");
    let stream_table = generate_unique_table("events");
    let full_shared_table = format!("{}.{}", namespace, shared_table);
    let full_stream_table = format!("{}.{}", namespace, stream_table);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'PUBLIC')",
        full_shared_table
    ))
    .expect("CREATE SHARED TABLE should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (event_id TEXT PRIMARY KEY, payload TEXT) WITH (TYPE = 'STREAM', TTL_SECONDS = 60)",
        full_stream_table
    ))
    .expect("CREATE STREAM TABLE should succeed");

    let explain_queries = vec![
        (
            "system provider",
            "EXPLAIN SELECT * FROM system.users".to_string(),
            vec![
                "logical_plan | TableScan: system.users",
                "physical_plan | CooperativeExec",
                "DeferredBatchExec: source=indexed_system_scan",
            ],
        ),
        (
            "view provider",
            "EXPLAIN SELECT * FROM system.datatypes".to_string(),
            vec![
                "logical_plan | TableScan: system.datatypes",
                "physical_plan | CooperativeExec",
                "DeferredBatchExec: source=view_scan",
            ],
        ),
        (
            "shared provider",
            format!("EXPLAIN SELECT * FROM {}", full_shared_table),
            vec![
                "logical_plan | TableScan: ",
                "physical_plan | CooperativeExec",
                "DeferredBatchExec: source=shared_table_scan",
            ],
        ),
        (
            "stream provider",
            format!("EXPLAIN SELECT * FROM {}", full_stream_table),
            vec![
                "logical_plan | TableScan: ",
                "physical_plan | CooperativeExec",
                "DeferredBatchExec: source=stream_table_scan",
            ],
        ),
    ];

    for (label, sql, expected_fragments) in explain_queries {
        let output = execute_sql_as_root_via_client(&sql)
            .unwrap_or_else(|error| panic!("{} explain failed for '{}': {}", label, sql, error));
        for expected_fragment in expected_fragments {
            assert!(
                output.contains(expected_fragment),
                "{} explain should contain '{}', got: {}",
                label,
                expected_fragment,
                output
            );
        }
    }

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}