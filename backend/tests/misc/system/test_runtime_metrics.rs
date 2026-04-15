use super::test_support::TestServer;
use kalam_client::models::ResponseStatus;
use serial_test::serial;

#[tokio::test]
#[ntest::timeout(10000)] // local runs are fast, but shared-server startup can spike under CI load
#[serial(memory_metrics)]
async fn test_system_stats_expose_memory_breakdown_and_allocator_metrics() {
    let server = TestServer::new_shared().await;

    let response = server
        .execute_sql(
            "SELECT metric_name, metric_value FROM system.stats \
             WHERE metric_name IN (\
                'memory_usage_mb',\
                'memory_usage_source',\
                'memory_rss_mb',\
                'memory_virtual_mb',\
                'memory_rss_gap_mb',\
                'allocator_name',\
                'mimalloc_process_commit_bytes',\
                'mimalloc_reserved_current_bytes',\
                'mimalloc_malloc_requested_current_bytes'\
             ) ORDER BY metric_name",
        )
        .await;

    assert_eq!(response.status, ResponseStatus::Success, "{:?}", response.error);

    let rows = response.rows_as_maps();
    let metric_names: std::collections::HashSet<String> = rows
        .iter()
        .filter_map(|row| row.get("metric_name").and_then(|value| value.as_str()))
        .map(ToOwned::to_owned)
        .collect();

    for required in [
        "memory_usage_mb",
        "memory_usage_source",
        "memory_rss_mb",
        "memory_virtual_mb",
        "memory_rss_gap_mb",
        "allocator_name",
        "mimalloc_process_commit_bytes",
        "mimalloc_reserved_current_bytes",
        "mimalloc_malloc_requested_current_bytes",
    ] {
        assert!(metric_names.contains(required), "missing metric: {required}");
    }

    #[cfg(target_os = "macos")]
    {
        let source = rows
            .iter()
            .find_map(|row| {
                let name = row.get("metric_name")?.as_str()?;
                if name == "memory_usage_source" {
                    row.get("metric_value")?.as_str()
                } else {
                    None
                }
            })
            .expect("memory_usage_source row");
        assert_eq!(source, "physical_footprint");
    }
}
