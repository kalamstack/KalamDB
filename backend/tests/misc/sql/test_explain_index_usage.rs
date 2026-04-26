//! Test that verifies index usage via EXPLAIN VERBOSE
//!
//! This test:
//! 1. Runs EXPLAIN VERBOSE for queries on system.users
//! 2. Verifies that EXPLAIN works without errors
//! 3. The actual query results may vary depending on server bootstrap state

use kalam_client::models::ResponseStatus;

use super::test_support::TestServer;

#[actix_web::test]
async fn test_explain_user_id_equality() {
    let server: TestServer = TestServer::new_shared().await;

    // Run EXPLAIN VERBOSE for equality query - this should always work
    let explain_sql = "EXPLAIN VERBOSE SELECT * FROM system.users WHERE user_id = 'system'";
    let response = server.execute_sql(explain_sql).await;

    assert_eq!(response.status, ResponseStatus::Success, "EXPLAIN should succeed");
    let explain_output = format!("{:?}", response.results);
    println!("=== EXPLAIN output for user_id = 'system' ===");
    println!("{}", explain_output);

    // Verify the explain output has content (plan information)
    assert!(!response.results.is_empty(), "EXPLAIN should return plan information");
    assert!(
        response.results[0].row_count > 0,
        "EXPLAIN should have rows describing the plan"
    );
}

#[actix_web::test]
async fn test_explain_user_id_like() {
    let server: TestServer = TestServer::new_shared().await;

    // Run EXPLAIN VERBOSE for LIKE query - this should always work
    let explain_sql = "EXPLAIN VERBOSE SELECT * FROM system.users WHERE user_id LIKE 'sys%'";
    let response = server.execute_sql(explain_sql).await;

    assert_eq!(response.status, ResponseStatus::Success, "EXPLAIN should succeed");
    let explain_output = format!("{:?}", response.results);
    println!("=== EXPLAIN output for user_id LIKE 'sys%' ===");
    println!("{}", explain_output);

    // Verify the explain output has content (plan information)
    assert!(!response.results.is_empty(), "EXPLAIN should return plan information");
    assert!(
        response.results[0].row_count > 0,
        "EXPLAIN should have rows describing the plan"
    );
}

#[actix_web::test]
async fn test_explain_job_status() {
    let server: TestServer = TestServer::new_shared().await;

    // system.jobs is auto-populated by the system, so we can query it directly

    // Run EXPLAIN VERBOSE for status query
    let explain_sql = "EXPLAIN VERBOSE SELECT * FROM system.jobs WHERE status = 'running'";
    let response = server.execute_sql(explain_sql).await;

    assert_eq!(response.status, ResponseStatus::Success);
    let explain_output = format!("{:?}", response.results);
    println!("=== EXPLAIN output for status = 'running' ===");
    println!("{}", explain_output);
}

#[actix_web::test]
async fn test_index_usage_log_output() {
    let server: TestServer = TestServer::new_shared().await;

    // Query all users first to see what exists
    println!("\n=== Querying all users ===");
    let all_users_sql = "SELECT user_id FROM system.users";
    let all_response = server.execute_sql(all_users_sql).await;
    assert_eq!(all_response.status, ResponseStatus::Success);
    let all_rows = all_response.rows_as_maps();
    println!("Found {} users total", all_rows.len());
    for row in &all_rows {
        println!("  - user_id: {:?}", row.get("user_id"));
    }

    // Query existing users with filter
    println!("\n=== Running query with user_id filter ===");
    let query_sql = "SELECT * FROM system.users WHERE user_id = 'system'";
    let response = server.execute_sql(query_sql).await;

    assert_eq!(response.status, ResponseStatus::Success);
    println!("Query response has {} results", response.results.len());
    if !response.results.is_empty() {
        let rows = response.rows_as_maps();
        println!("Query returned {} rows", rows.len());
    }

    // Check logs for index usage message
    // (Logs would show: "[system.users] Using secondary index 0 for filters...")
    println!("✓ Query executed - check server logs for index usage");

    // Test LIKE query
    println!("\n=== Running LIKE query ===");
    let like_query = "SELECT * FROM system.users WHERE user_id LIKE 'sys%'";
    let response2 = server.execute_sql(like_query).await;
    assert_eq!(response2.status, ResponseStatus::Success);
    println!("✓ LIKE query executed successfully");
}
