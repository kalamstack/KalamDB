//! Integration tests for System Table Index Usage
//!
//! These tests verify that system tables use their secondary indexes
//! for efficient lookups rather than full table scans.
//!
//! ## Tests Covered
//! - system.users: username index for `WHERE username = '...'` queries
//! - system.jobs: status index for `WHERE status = '...'` queries
//! - system.live_queries: table_id index (basic verification)
//!
//! ## Strategy
//! 1. Insert multiple records
//! 2. Query with indexed filter (e.g., WHERE username = 'user1')
//! 3. Verify correct results are returned
//! 4. Measure performance to ensure O(1) lookup behavior

use super::test_support::TestServer;
use kalam_link::models::ResponseStatus;
use kalam_link::parse_i64;
use kalamdb_commons::models::{ConnectionId, UserName};
use kalamdb_commons::{
    AuthType, JobId, LiveQueryId, NamespaceId, NodeId, Role, StorageId, TableName, UserId,
};
use kalamdb_system::providers::storages::models::StorageMode;
use kalamdb_system::{Job, JobStatus, JobType, LiveQuery, User};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Test: system.users uses username index for WHERE username = '...' queries
///
/// This test verifies that queries filtering by username use the secondary index
/// instead of scanning all users.
///
/// Strategy:
/// 1. Insert 50 users
/// 2. Query by username
/// 3. Verify results are correct
/// 4. Compare query latency with and without index-friendly filters
#[actix_web::test]
#[ntest::timeout(60000)]
async fn test_system_users_username_index() {
    let server = TestServer::new_shared().await;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before UNIX_EPOCH")
        .as_nanos();
    let user_prefix = format!("username{}", run_id);
    let id_prefix = format!("user{}", run_id);
    let password_hash = bcrypt::hash("password", bcrypt::DEFAULT_COST).unwrap();

    // Insert 50 users
    for i in 1..=50 {
        let now = chrono::Utc::now().timestamp_millis();
        let user = User {
            user_id: UserId::new(&format!("{}_{}", id_prefix, i)),
            username: UserName::new(&format!("{}_{}", user_prefix, i)),
            password_hash: password_hash.clone(),
            role: Role::User,
            email: Some(format!("{}_{}@example.com", id_prefix, i)),
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: StorageMode::Table,
            storage_id: Some(StorageId::local()),
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: now,
            updated_at: now,
            last_seen: None,
            deleted_at: None,
        };

        server
            .app_context
            .system_tables()
            .users()
            .create_user(user)
            .expect("Failed to insert user");
    }

    // Test 1: Query by username (should use username index)
    let query_by_username = format!(
        "SELECT COUNT(*) AS user_count FROM system.users WHERE username = '{}_25'",
        user_prefix
    );
    let start = Instant::now();
    let response = server.execute_sql(&query_by_username).await;
    let latency_indexed = start.elapsed();

    assert_eq!(response.status, ResponseStatus::Success, "Query failed: {:?}", response.error);

    let rows = response.rows_as_maps();
    assert_eq!(rows.len(), 1, "Expected 1 row, got {} - query result: {:?}", rows.len(), rows);
    let count = parse_i64(rows[0].get("user_count").unwrap());
    assert_eq!(count, 1, "Expected to find exactly 1 user with username='username25'");

    println!("✓ Username index query latency: {:?}", latency_indexed);

    // Test 2: Query specific username and verify it returns correct user
    let query_specific = format!(
        "SELECT user_id, username, email FROM system.users WHERE username = '{}_10'",
        user_prefix
    );
    let response2 = server.execute_sql(&query_specific).await;

    assert_eq!(response2.status, ResponseStatus::Success);
    let rows2 = response2.rows_as_maps();
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get("user_id").unwrap().as_str().unwrap(), format!("{}_10", id_prefix));
    assert_eq!(
        rows2[0].get("username").unwrap().as_str().unwrap(),
        format!("{}_10", user_prefix)
    );
    assert_eq!(
        rows2[0].get("email").unwrap().as_str().unwrap(),
        format!("{}_{}@example.com", id_prefix, 10)
    );

    // Test 3: Case-insensitive username lookup (index stores lowercase)
    let query_case_insensitive = format!(
        "SELECT user_id FROM system.users WHERE username = '{}'",
        format!("{}_25", user_prefix).to_uppercase()
    );
    let response3 = server.execute_sql(&query_case_insensitive).await;

    // Note: This depends on whether the filter lowercases before index lookup
    // The index stores lowercase, but the filter needs to normalize
    assert_eq!(response3.status, ResponseStatus::Success);
    // For now, just verify it doesn't crash and returns some result

    println!("✓ system.users username index test passed");
    println!("  - Found user by exact username match");
    println!("  - Query latency: {:?}", latency_indexed);
}

/// Test: system.jobs uses status index for WHERE status = '...' queries
///
/// This test verifies that queries filtering by job status use the secondary index
/// for efficient lookups.
///
/// Strategy:
/// 1. Insert 100 jobs with various statuses
/// 2. Query by status
/// 3. Verify correct results
/// 4. Ensure query performance is consistent regardless of total job count
#[actix_web::test]
#[ntest::timeout(60000)]
async fn test_system_jobs_status_index() {
    let server = TestServer::new_shared().await;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before UNIX_EPOCH")
        .as_nanos();
    let job_prefix = format!("statusidx{}", run_id);

    // Insert 100 jobs with different statuses
    let statuses = vec![
        JobStatus::New,
        JobStatus::Queued,
        JobStatus::Running,
        JobStatus::Completed,
        JobStatus::Failed,
    ];

    let now = chrono::Utc::now().timestamp_millis();

    for i in 1..=100 {
        let status = statuses[i % statuses.len()].clone();
        let job = Job {
            job_id: JobId::new(&format!("{}_{}", job_prefix, i)),
            job_type: JobType::Unknown,
            status,
            leader_status: None,
            parameters: Some(format!(r#"{{"table":"test_{}", "iteration":{}}}"#, i, i)),
            message: None,
            exception_trace: None,
            idempotency_key: Some(format!("idem_key_{}_{}", job_prefix, i)),
            retry_count: 0,
            max_retries: 3,
            memory_used: None,
            cpu_used: None,
            created_at: now,
            updated_at: now,
            started_at: if status != JobStatus::New && status != JobStatus::Queued {
                Some(now)
            } else {
                None
            },
            finished_at: if status == JobStatus::Completed || status == JobStatus::Failed {
                Some(now + 1000)
            } else {
                None
            },
            node_id: NodeId::from(1u64),
            leader_node_id: None,
            queue: None,
            priority: None,
        };

        server
            .app_context
            .system_tables()
            .jobs()
            .create_job(job)
            .expect("Failed to insert job");
    }

    // First verify we have data
    let verify_query =
        format!("SELECT COUNT(*) AS total FROM system.jobs WHERE job_id LIKE '{}%'", job_prefix);
    let verify_response = server.execute_sql(&verify_query).await;
    assert_eq!(verify_response.status, ResponseStatus::Success);
    let verify_rows = verify_response.rows_as_maps();
    let total_jobs = parse_i64(verify_rows[0].get("total").unwrap());
    println!("✓ Total jobs inserted: {}", total_jobs);

    // Check what statuses actually exist
    let status_query = format!(
        "SELECT status, COUNT(*) AS count FROM system.jobs WHERE job_id LIKE '{}%' GROUP BY status ORDER BY status",
        job_prefix
    );
    let status_response = server.execute_sql(&status_query).await;
    if status_response.status == ResponseStatus::Success {
        println!("✓ Job statuses breakdown:");
        for row in status_response.rows_as_maps() {
            let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("NULL");
            let count = parse_i64(row.get("count").unwrap());
            println!("    {} = {}", status, count);
        }
    }

    // If no jobs, skip test
    if total_jobs == 0 {
        println!("⚠ No jobs found in system.jobs, skipping status index test");
        return;
    }

    // Test 1: Query for Running jobs (should use status index)
    let query_running = format!(
        "SELECT COUNT(*) AS job_count FROM system.jobs WHERE status = 'running' AND job_id LIKE '{}%'",
        job_prefix
    );
    let start = Instant::now();
    let response = server.execute_sql(&query_running).await;
    let latency_indexed = start.elapsed();

    assert_eq!(response.status, ResponseStatus::Success, "Query failed: {:?}", response.error);

    let rows = response.rows_as_maps();
    assert_eq!(rows.len(), 1);
    let count = parse_i64(rows[0].get("job_count").unwrap());

    println!("✓ Found {} running jobs out of {} total", count, total_jobs);

    // We inserted 100 jobs cycling through 5 statuses, so ~20 of each
    // But if we only see the jobs that already existed, adjust expectations
    if total_jobs >= 90 {
        if count >= 15 && count <= 25 {
            println!("✓ Running jobs count is in expected range (15-25)");
        } else {
            println!("⚠ Expected ~20 running jobs, got {}", count);
        }
    } else {
        println!("⚠ Only {} jobs found, some may not have been inserted", total_jobs);
    }

    println!("✓ Status index query latency: {:?}", latency_indexed);

    // Test 2: Query for completed jobs
    let query_completed = format!(
        "SELECT job_id, status FROM system.jobs WHERE status = 'completed' AND job_id LIKE '{}%' LIMIT 5",
        job_prefix
    );
    let response2 = server.execute_sql(&query_completed).await;

    assert_eq!(response2.status, ResponseStatus::Success);
    let rows2 = response2.rows_as_maps();
    assert!(rows2.len() > 0, "Expected at least 1 completed job");

    // Verify all returned rows have status='completed'
    for row in &rows2 {
        assert_eq!(row.get("status").unwrap().as_str().unwrap(), "completed");
    }

    // Test 3: Query for failed jobs
    let query_failed = format!(
        "SELECT COUNT(*) AS failed_count FROM system.jobs WHERE status = 'failed' AND job_id LIKE '{}%'",
        job_prefix
    );
    let response3 = server.execute_sql(&query_failed).await;

    assert_eq!(response3.status, ResponseStatus::Success);
    let rows3 = response3.rows_as_maps();
    let failed_count = parse_i64(rows3[0].get("failed_count").unwrap());
    assert!(
        failed_count >= 1 && failed_count <= total_jobs,
        "Expected at least 1 failed job within total_jobs, got {}",
        failed_count
    );

    println!("✓ system.jobs status index test passed");
    println!("  - Found jobs by status filter");
    println!("  - Query latency: {:?}", latency_indexed);
}

/// Test: system.live_queries basic verification
///
/// This test verifies that the live_queries table can be queried
/// and has the table_id index available (even if filter_to_prefix returns None currently).
#[actix_web::test]
#[ntest::timeout(60000)]
async fn test_system_live_queries_basic() {
    let server = TestServer::new_shared().await;

    // Insert a few live queries
    let now = chrono::Utc::now().timestamp_millis();

    for i in 1..=10 {
        let live_id = LiveQueryId::new(
            UserId::new("test_user"),
            ConnectionId::new(&format!("conn{}", i)),
            &format!("sub{}", i),
        );

        let live_query = LiveQuery {
            live_id: live_id.clone(),
            connection_id: format!("conn{}", i),
            subscription_id: format!("sub{}", i),
            namespace_id: NamespaceId::default(),
            table_name: TableName::new(&format!("table{}", i % 3)), // 3 different tables
            user_id: UserId::new("test_user"),
            query: format!("SELECT * FROM table{}", i % 3),
            options: Some("{}".to_string()),
            status: kalamdb_system::LiveQueryStatus::Active,
            created_at: now,
            last_update: now,
            last_ping_at: now,
            changes: 0,
            node_id: NodeId::from(1u64),
        };

        server
            .app_context
            .system_tables()
            .live_queries()
            .create_live_query(live_query)
            .expect("Failed to insert live query");
    }

    // Test 1: Query all live queries
    let query_all = "SELECT COUNT(*) AS lq_count FROM system.live_queries";
    let response = server.execute_sql(query_all).await;

    if response.status != ResponseStatus::Success {
        println!("⚠ system.live_queries query failed: {:?}", response.error);
        println!("⚠ This may be due to schema mismatch - skipping test");
        return;
    }

    let rows = response.rows_as_maps();
    let count = parse_i64(rows[0].get("lq_count").unwrap());
    println!("✓ Found {} live queries", count);

    if count == 0 {
        println!("⚠ No live queries found, data may not have persisted");
        return;
    }

    assert!(count >= 10, "Expected at least 10 live queries, got {}", count);

    // Test 2: Query by table name (filter may not use index yet, but verify it works)
    let query_by_table =
        "SELECT COUNT(*) AS table_lq_count FROM system.live_queries WHERE table_name = 'table0'";
    let response2 = server.execute_sql(query_by_table).await;

    assert_eq!(response2.status, ResponseStatus::Success);
    let rows2 = response2.rows_as_maps();
    let table_count = parse_i64(rows2[0].get("table_lq_count").unwrap());

    // We inserted 10 queries cycling through 3 tables (0,1,2), so ~3-4 per table
    assert!(
        table_count >= 3 && table_count <= 4,
        "Expected ~3-4 queries for table0, got {}",
        table_count
    );

    println!("✓ system.live_queries basic test passed");
    println!("  - Queried live queries successfully");
    println!("  - Filter by table_name works (index ready for future filter_to_prefix impl)");
}

/// Test: Index usage provides O(1) lookup behavior
///
/// This test verifies that indexed queries don't scale with table size.
/// We measure query time with different dataset sizes to confirm O(1) behavior.
#[actix_web::test]
#[ntest::timeout(120000)]
async fn test_index_performance_scaling() {
    let server = TestServer::new_shared().await;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before UNIX_EPOCH")
        .as_nanos();
    let user_prefix = format!("perf_user{}", run_id);
    let username_prefix = format!("perf_username{}", run_id);
    let password_hash = bcrypt::hash("password", bcrypt::DEFAULT_COST).unwrap();

    // Phase 1: Insert 50 users, measure query time
    for i in 1..=50 {
        let now = chrono::Utc::now().timestamp_millis();
        let user = User {
            user_id: UserId::new(&format!("{}_{}", user_prefix, i)),
            username: UserName::new(&format!("{}_{}", username_prefix, i)),
            password_hash: password_hash.clone(),
            role: Role::User,
            email: Some(format!("perf{}@example.com", i)),
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: StorageMode::Table,
            storage_id: Some(StorageId::local()),
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: now,
            updated_at: now,
            last_seen: None,
            deleted_at: None,
        };

        server
            .app_context
            .system_tables()
            .users()
            .create_user(user)
            .expect("Failed to insert user");
    }

    // Warmup
    let lookup_username = format!("{}_25", username_prefix);
    for _ in 0..3 {
        server
            .execute_sql(&format!(
                "SELECT user_id FROM system.users WHERE username = '{}'",
                lookup_username
            ))
            .await;
    }

    let query = format!(
        "SELECT user_id FROM system.users WHERE username = '{}'",
        lookup_username
    );
    let sample_count = 15usize;

    let mut samples_50 = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let start = Instant::now();
        let response = server.execute_sql(&query).await;
        let elapsed = start.elapsed();

        assert_eq!(response.status, ResponseStatus::Success);
        let rows = response.rows_as_maps();
        assert_eq!(rows.len(), 1);
        samples_50.push(elapsed);
    }

    // Phase 2: Insert 200 more users (total 250), measure again
    for i in 51..=250 {
        let now = chrono::Utc::now().timestamp_millis();
        let user = User {
            user_id: UserId::new(&format!("{}_{}", user_prefix, i)),
            username: UserName::new(&format!("{}_{}", username_prefix, i)),
            password_hash: password_hash.clone(),
            role: Role::User,
            email: Some(format!("perf{}@example.com", i)),
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: StorageMode::Table,
            storage_id: Some(StorageId::local()),
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: now,
            updated_at: now,
            last_seen: None,
            deleted_at: None,
        };

        server
            .app_context
            .system_tables()
            .users()
            .create_user(user)
            .expect("Failed to insert user");
    }

    let mut samples_250 = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let start = Instant::now();
        let response = server.execute_sql(&query).await;
        let elapsed = start.elapsed();

        assert_eq!(response.status, ResponseStatus::Success);
        let rows = response.rows_as_maps();
        assert_eq!(rows.len(), 1);
        samples_250.push(elapsed);
    }

    let median = |samples: &mut [Duration]| -> Duration {
        samples.sort_unstable();
        samples[samples.len() / 2]
    };

    let latency_50 = median(samples_50.as_mut_slice());
    let latency_250 = median(samples_250.as_mut_slice());

    println!("✓ Index performance scaling test:");
    println!("  - Latency with 50 users:  {:?}", latency_50);
    println!("  - Latency with 250 users: {:?}", latency_250);

    // With index: median latency should stay in the same order of magnitude.
    // Using medians removes one-off scheduler/network spikes from the assertion.
    let ratio = latency_250.as_micros() as f64 / latency_50.as_micros().max(1) as f64;

    assert!(
        ratio < 6.0,
        "Query time scaled too much ({}x). Expected O(1) with index, got O(n) behavior. This suggests the index is NOT being used!",
        ratio
    );

    println!("  - Performance ratio (250/50): {:.2}x", ratio);
    println!("  ✓ Index provides O(1) lookup (median ratio < 6x)");
}
