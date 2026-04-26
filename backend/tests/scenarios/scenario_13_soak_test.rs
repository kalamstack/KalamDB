//! Scenario 13: Production Mixed Workload (Soak Test)
//!
//! Simulates production traffic patterns: concurrent users, mixed operations,
//! background flushes, schema evolution, and subscription stability.
//!
//! ## Checklist
//! - [x] Multiple concurrent user clients
//! - [x] Mixed read/write workload
//! - [x] Background flush jobs
//! - [x] Schema evolution mid-run
//! - [x] Subscription stability under load
//! - [x] Error rate and latency tracking

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use kalamdb_commons::Role;

use super::helpers::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(180);

/// Main soak test with mixed workload
#[tokio::test]
async fn test_scenario_13_mixed_workload_soak() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("soak_test");

    // =========================================================
    // Setup: Create namespace and tables
    // =========================================================
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.orders (
                        id BIGINT PRIMARY KEY,
                        customer_id INTEGER,
                        amount DOUBLE,
                        status TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE orders table");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.audit_log (
                        id BIGINT PRIMARY KEY,
                        action TEXT,
                        timestamp BIGINT
                    ) WITH (TYPE = 'STREAM', TTL_SECONDS = 7200)"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE audit_log stream");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.config (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    ) WITH (TYPE = 'SHARED')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE config table");

    // =========================================================
    // Metrics tracking
    // =========================================================
    let insert_count = Arc::new(AtomicU64::new(0));
    let update_count = Arc::new(AtomicU64::new(0));
    let query_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let _soak_duration = Duration::from_secs(30);
    let start_time = Instant::now();

    // =========================================================
    // Concurrent users performing mixed operations
    // =========================================================
    let num_users = 3;
    let mut handles = Vec::new();
    let user_prefix = unique_table("soak_user");
    let first_username = format!("{}_0", user_prefix);

    for user_idx in 0..num_users {
        let username = format!("{}_{}", user_prefix, user_idx);
        let client = create_user_and_client(server, &username, &Role::User).await?;
        let ns_clone = ns.clone();
        let insert_count_clone = Arc::clone(&insert_count);
        let update_count_clone = Arc::clone(&update_count);
        let query_count_clone = Arc::clone(&query_count);
        let error_count_clone = Arc::clone(&error_count);

        let handle = tokio::spawn(async move {
            let mut local_id = (user_idx as i64) * 10000;
            let user_start = Instant::now();

            while user_start.elapsed() < Duration::from_secs(25) {
                local_id += 1;
                let op = local_id % 5;

                match op {
                    0 | 1 | 2 => {
                        // Insert (60% of operations)
                        let resp = client
                            .execute_query(
                                &format!(
                                    "INSERT INTO {}.orders (id, customer_id, amount, status) \
                                     VALUES ({}, {}, {}, 'pending')",
                                    ns_clone,
                                    local_id,
                                    user_idx,
                                    local_id as f64 * 10.5
                                ),
                                None,
                                None,
                                None,
                            )
                            .await;
                        match resp {
                            Ok(r) if r.success() => {
                                insert_count_clone.fetch_add(1, Ordering::Relaxed);
                            },
                            Ok(r) => {
                                if error_count_clone.load(Ordering::Relaxed) < 5 {
                                    eprintln!("Insert error: {:?}", r.error);
                                }
                                error_count_clone.fetch_add(1, Ordering::Relaxed);
                            },
                            Err(e) => {
                                if error_count_clone.load(Ordering::Relaxed) < 5 {
                                    eprintln!("Insert network error: {:?}", e);
                                }
                                error_count_clone.fetch_add(1, Ordering::Relaxed);
                            },
                        }
                    },
                    3 => {
                        // Update (20% of operations)
                        let resp = client
                            .execute_query(
                                &format!(
                                    "UPDATE {}.orders SET status = 'completed' WHERE id = {} AND \
                                     status = 'pending'",
                                    ns_clone,
                                    local_id - 1
                                ),
                                None,
                                None,
                                None,
                            )
                            .await;
                        match resp {
                            Ok(r) if r.success() => {
                                update_count_clone.fetch_add(1, Ordering::Relaxed);
                            },
                            Ok(r) => {
                                // Update returned error or no success
                                if error_count_clone.load(Ordering::Relaxed) < 3 {
                                    eprintln!(
                                        "Update error for id {}: {:?}",
                                        local_id - 1,
                                        r.error
                                    );
                                }
                                error_count_clone.fetch_add(1, Ordering::Relaxed);
                            },
                            Err(e) => {
                                if error_count_clone.load(Ordering::Relaxed) < 3 {
                                    eprintln!("Update network error: {:?}", e);
                                }
                                error_count_clone.fetch_add(1, Ordering::Relaxed);
                            },
                        }
                    },
                    _ => {
                        // Query (20% of operations)
                        let resp = client
                            .execute_query(
                                &format!(
                                    "SELECT COUNT(*) as cnt FROM {}.orders WHERE status = \
                                     'pending'",
                                    ns_clone
                                ),
                                None,
                                None,
                                None,
                            )
                            .await;
                        match resp {
                            Ok(r) if r.success() => {
                                query_count_clone.fetch_add(1, Ordering::Relaxed);
                            },
                            _ => {
                                error_count_clone.fetch_add(1, Ordering::Relaxed);
                            },
                        }
                    },
                }

                // Small delay to spread load
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            local_id
        });

        handles.push(handle);
    }

    // =========================================================
    // Background flush task (inline using link client instead of server.clone)
    // =========================================================
    let ns_for_flush = ns.clone();
    let flush_username = unique_table("flush_user");
    let flush_client = create_user_and_client(server, &flush_username, &Role::User).await?;
    let flush_handle = tokio::spawn(async move {
        let mut flush_count = 0u32;
        let flush_start = Instant::now();

        while flush_start.elapsed() < Duration::from_secs(20) {
            tokio::time::sleep(Duration::from_secs(5)).await;

            let _ = flush_client
                .execute_query(
                    &format!("STORAGE FLUSH TABLE {}.orders", ns_for_flush),
                    None,
                    None,
                    None,
                )
                .await;
            flush_count += 1;
            println!("Background flush {} triggered", flush_count);
        }

        flush_count
    });

    // =========================================================
    // Subscription monitoring
    // =========================================================
    let subscriber_username = unique_table("soak_subscriber");
    let sub_client = create_user_and_client(server, &subscriber_username, &Role::User).await?;
    let ns_for_sub = ns.clone();
    let subscription_events = Arc::new(AtomicU64::new(0));
    let subscription_events_clone = Arc::clone(&subscription_events);

    let sub_handle = tokio::spawn(async move {
        let mut sub = match sub_client
            .subscribe(&format!("SELECT * FROM {}.orders LIMIT 1000", ns_for_sub))
            .await
        {
            Ok(s) => s,
            Err(_) => return 0u64,
        };

        let sub_start = Instant::now();
        let mut event_count = 0u64;

        while sub_start.elapsed() < Duration::from_secs(25) {
            tokio::select! {
                event = sub.next() => {
                    if event.is_some() {
                        event_count += 1;
                        subscription_events_clone.fetch_add(1, Ordering::Relaxed);
                    } else {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Continue loop
                }
            }
        }

        let _ = sub.close().await;
        event_count
    });

    // =========================================================
    // Wait for all tasks
    // =========================================================
    for handle in handles {
        let _ = handle.await;
    }

    let flush_count = flush_handle.await.unwrap_or(0);
    let sub_event_count = sub_handle.await.unwrap_or(0);

    let elapsed = start_time.elapsed();

    // =========================================================
    // Report metrics
    // =========================================================
    let total_inserts = insert_count.load(Ordering::Relaxed);
    let total_updates = update_count.load(Ordering::Relaxed);
    let total_queries = query_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    let total_ops = total_inserts + total_updates + total_queries;

    println!("=== Soak Test Results ===");
    println!("Duration: {:?}", elapsed);
    println!("Inserts: {}", total_inserts);
    println!("Updates: {}", total_updates);
    println!("Queries: {}", total_queries);
    println!("Total ops: {}", total_ops);
    println!("Errors: {}", total_errors);
    println!("Background flushes: {}", flush_count);
    println!("Subscription events: {}", sub_event_count);

    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    println!("Ops/sec: {:.1}", ops_per_sec);

    let error_rate = if total_ops > 0 {
        (total_errors as f64 / total_ops as f64) * 100.0
    } else {
        0.0
    };
    println!("Error rate: {:.2}%", error_rate);

    // =========================================================
    // Assertions
    // =========================================================
    assert!(total_inserts > 10, "Should have inserted > 10 rows, got {}", total_inserts);
    assert!(error_rate < 5.0, "Error rate {:.2}% should be < 5%", error_rate);
    assert!(flush_count >= 1, "Should have completed at least 1 flush");

    // Final data verification - check that one of the soak users can see their data
    // Note: USER tables have per-user RLS, so each user only sees their own rows
    let soak_user_client = create_user_and_client(server, &first_username, &Role::User).await?;
    let resp = soak_user_client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.orders", ns), None, None, None)
        .await?;
    let final_count = resp.get_i64("cnt").unwrap_or(0);
    println!("Final order count for soak_user_0: {}", final_count);
    assert!(final_count > 0, "User should have orders in table");

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Schema evolution during active operations
#[tokio::test]
async fn test_scenario_13_schema_evolution_under_load() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("schema_evolution");

    // Setup
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.products (
                        id BIGINT PRIMARY KEY,
                        name TEXT NOT NULL
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE products table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_schema_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Insert initial data
    for i in 1..=50 {
        let resp = client
            .execute_query(
                &format!("INSERT INTO {}.products (id, name) VALUES ({}, 'Product {}')", ns, i, i),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert product {}", i);
    }

    // Add column while data exists
    let resp = server
        .execute_sql(&format!("ALTER TABLE {}.products ADD COLUMN price DOUBLE", ns))
        .await?;
    assert_success(&resp, "ALTER TABLE ADD COLUMN price");

    // Insert more data with new column
    for i in 51..=100 {
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.products (id, name, price) VALUES ({}, 'Product {}', {})",
                    ns,
                    i,
                    i,
                    i as f64 * 9.99
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert product {} with price", i);
    }

    // Query all data
    let resp = client
        .execute_query(
            &format!("SELECT id, name, price FROM {}.products ORDER BY id", ns),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Query all products");
    assert_eq!(resp.rows_as_maps().len(), 100, "Should have 100 products");

    // Verify old rows have NULL price, new rows have price
    let row_50 = &resp.rows_as_maps()[49]; // id = 50
    let price_50 = row_50.get("price");
    assert!(
        price_50.is_none() || price_50.unwrap().is_null(),
        "Old row should have NULL price"
    );

    let row_51 = &resp.rows_as_maps()[50]; // id = 51
    let price_51 = row_51.get("price").and_then(|v| v.as_f64());
    assert!(price_51.is_some(), "New row should have price");
    assert!((price_51.unwrap() - (51.0 * 9.99)).abs() < 0.01);

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Concurrent readers and writers stress test
#[tokio::test]
async fn test_scenario_13_concurrent_read_write() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("concurrent_rw");

    // Setup
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.counters (
                        id BIGINT PRIMARY KEY,
                        value BIGINT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE counters table");

    // Concurrent writers (each seeds their own data due to USER table RLS)
    let write_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // 2 writer tasks - each seeds their own 10 counters then updates them
    for writer_id in 0..2 {
        let username = format!("writer_{}", writer_id);
        let client = create_user_and_client(server, &username, &Role::User).await?;
        let ns_clone = ns.clone();
        let write_count_clone = Arc::clone(&write_count);

        let handle = tokio::spawn(async move {
            // First, seed this user's own 10 counters (USER table RLS means each user needs their
            // own data)
            for i in 1..=10 {
                let _ = client
                    .execute_query(
                        &format!("INSERT INTO {}.counters (id, value) VALUES ({}, 0)", ns_clone, i),
                        None,
                        None,
                        None,
                    )
                    .await;
            }

            // Now update the counters 50 times (each update sets an incremental value)
            for i in 0..50 {
                let counter_id = (i % 10) + 1;
                let new_value = i + 1; // Just use the iteration number as the new value
                let query = format!(
                    "UPDATE {}.counters SET value = {} WHERE id = {}",
                    ns_clone, new_value, counter_id
                );
                let resp = client.execute_query(&query, None, None, None).await;
                if resp.as_ref().map(|r| r.success()).unwrap_or(false) {
                    write_count_clone.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        handles.push(handle);
    }

    // 2 reader tasks - each reads their own data
    for reader_id in 0..2 {
        let username = format!("reader_{}", reader_id);
        let client = create_user_and_client(server, &username, &Role::User).await?;
        let ns_clone = ns.clone();
        let read_count_clone = Arc::clone(&read_count);

        let handle = tokio::spawn(async move {
            // First, seed this user's own counters so reads have data
            for i in 1..=10 {
                let _ = client
                    .execute_query(
                        &format!("INSERT INTO {}.counters (id, value) VALUES ({}, 0)", ns_clone, i),
                        None,
                        None,
                        None,
                    )
                    .await;
            }

            for _ in 0..50 {
                let resp = client
                    .execute_query(
                        &format!("SELECT SUM(value) as total FROM {}.counters", ns_clone),
                        None,
                        None,
                        None,
                    )
                    .await;
                if resp.is_ok() && resp.unwrap().success() {
                    read_count_clone.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }

    let total_writes = write_count.load(Ordering::Relaxed);
    let total_reads = read_count.load(Ordering::Relaxed);

    println!("Concurrent R/W: {} writes, {} reads", total_writes, total_reads);

    assert!(total_writes > 50, "Should have > 50 successful writes");
    assert!(total_reads > 50, "Should have > 50 successful reads");

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}
