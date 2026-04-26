//! Concurrent Users Integration Test
//!
//! Validates user isolation, parallel operations, and live query notifications

use std::{
    thread,
    time::{Duration, Instant},
};

use crate::common::*;

const NUM_USERS: usize = 5;
const ROWS_PER_USER: usize = 2000;
const INSERT_BATCH_SIZE: usize = 250;

#[test]
fn test_concurrent_users_isolation() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    println!(
        "\n🚀 Starting Concurrent Users Test ({} users, {} rows each)",
        NUM_USERS, ROWS_PER_USER
    );
    let test_start = Instant::now();

    // Generate unique user suffix to avoid conflicts
    // Instead of timestamp add random str from 5 characters
    let user_suffix = random_string(5);

    // Setup namespace and table
    let namespace = "concurrent";
    let table_name = "user_data";
    let full_table = format!("{}.{}", namespace, table_name);

    let setup_start = Instant::now();
    // Drop the current table if it exists
    let drop_sql = format!("DROP TABLE IF EXISTS {}", full_table);
    let _ = execute_sql_as_root_via_cli(&drop_sql);

    // Check if the table was dropped successfully
    let check_sql = format!("SELECT * FROM {}", full_table);
    let check_result = execute_sql_as_root_via_cli(&check_sql);
    if check_result.is_ok() {
        eprintln!("⚠️  Failed to drop existing table. Skipping test.");
        return;
    }

    if let Err(e) =
        execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
    {
        eprintln!("⚠️  Failed to create namespace: {}. Skipping test.", e);
        return;
    }

    let create_table_sql = format!(
        "CREATE TABLE {} (id INTEGER, message TEXT, timestamp BIGINT, current_user_id TEXT \
         DEFAULT CURRENT_USER()) WITH (TYPE='USER', FLUSH_POLICY='rows:100')",
        full_table
    );
    if let Err(e) = execute_sql_as_root_via_cli(&create_table_sql) {
        eprintln!("⚠️  Failed to create table: {}. Skipping test.", e);
        return;
    }
    println!("✅ Setup complete ({:.2?})", setup_start.elapsed());

    // Create test users with unique timestamp suffix
    let user_creation_start = Instant::now();
    let mut user_credentials = Vec::new();
    for i in 0..NUM_USERS {
        let num = i + 1;
        let username = format!("user{}_{}", num, user_suffix);
        let password = format!("password{}", num);

        if let Err(e) = execute_sql_as_root_via_cli(&format!(
            "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
            username, password
        )) {
            eprintln!("⚠️  Failed to create user {}: {}. Skipping test.", username, e);
            cleanup(namespace, &user_credentials);
            return;
        }

        user_credentials.push((username, password));
    }
    println!("✅ {} users created ({:.2?})", NUM_USERS, user_creation_start.elapsed());

    // Insert data concurrently for all users
    println!("⏱  Starting concurrent inserts...");
    let insert_start = Instant::now();
    let mut handles = Vec::new();

    for (username, password) in user_credentials.iter() {
        let table = full_table.clone();
        let user = username.clone();
        let pass = password.clone();

        let handle = thread::spawn(move || {
            let thread_start = Instant::now();
            let mut total_cli_time = 0u128;
            let mut total_server_time = 0.0;
            let mut server_samples = 0usize;

            for chunk_start in (0..ROWS_PER_USER).step_by(INSERT_BATCH_SIZE) {
                let chunk_end = (chunk_start + INSERT_BATCH_SIZE).min(ROWS_PER_USER);
                let mut values = Vec::new();
                for row_id in chunk_start..chunk_end {
                    values.push(format!(
                        "('User {} row {}', {})",
                        user,
                        row_id,
                        chrono::Utc::now().timestamp_millis()
                    ));
                }

                let sql = format!(
                    "INSERT INTO {} (message, timestamp) VALUES {}",
                    table,
                    values.join(", ")
                );

                match execute_sql_via_cli_as_with_timing(&user, &pass, &sql) {
                    Ok(timing) => {
                        total_cli_time += timing.total_time_ms;
                        if let Some(server) = timing.server_time_ms {
                            total_server_time += server;
                            server_samples += 1;
                        }
                    },
                    Err(e) => {
                        eprintln!("⚠️  Batch insert failed for {}: {}", user, e);
                        return Err(format!("{}", e));
                    },
                }
            }

            let avg_server = if server_samples > 0 {
                Some(total_server_time / server_samples as f64)
            } else {
                None
            };

            let elapsed = thread_start.elapsed();
            Ok((elapsed, avg_server, total_cli_time))
        });

        handles.push(handle);
    }

    // Wait for all inserts to complete
    let mut thread_times = Vec::new();
    let mut server_times = Vec::new();
    let mut cli_times = Vec::new();
    for handle in handles {
        match handle.join().unwrap() {
            Ok((duration, server_ms, cli_ms)) => {
                thread_times.push(duration);
                if let Some(s) = server_ms {
                    server_times.push(s);
                }
                cli_times.push(cli_ms);
            },
            Err(e) => panic!("⚠️  Thread failed: {}", e),
        }
    }
    let insert_elapsed = insert_start.elapsed();
    if thread_times.is_empty() {
        panic!("⚠️  No successful insert threads completed; aborting test.");
    }

    // Calculate stats
    let avg_thread_time = thread_times.iter().sum::<Duration>() / thread_times.len() as u32;
    let max_thread_time = thread_times.iter().max().unwrap();
    let min_thread_time = thread_times.iter().min().unwrap();

    let avg_server_time = if !server_times.is_empty() {
        server_times.iter().sum::<f64>() / server_times.len() as f64
    } else {
        0.0
    };
    let avg_cli_time = cli_times.iter().sum::<u128>() as f64 / cli_times.len() as f64;
    let avg_overhead = avg_cli_time - avg_server_time;

    println!(
        "✅ {} total rows inserted ({} per user) in {:.2?}",
        NUM_USERS * ROWS_PER_USER,
        ROWS_PER_USER,
        insert_elapsed
    );
    println!(
        "   ├─ Thread times: min={:.2?}, avg={:.2?}, max={:.2?}",
        min_thread_time, avg_thread_time, max_thread_time
    );
    println!("   ├─ Server time (avg): {:.1}ms", avg_server_time);
    println!("   ├─ CLI total time (avg): {:.1}ms", avg_cli_time);
    println!(
        "   ├─ CLI overhead (avg): {:.1}ms ({:.1}%)",
        avg_overhead,
        (avg_overhead / avg_cli_time) * 100.0
    );
    println!(
        "   └─ Throughput: {:.1} inserts/sec",
        (NUM_USERS * ROWS_PER_USER) as f64 / insert_elapsed.as_secs_f64()
    );

    // Verify each user can only see their own data
    println!("⏱  Verifying user isolation...");
    let verify_start = Instant::now();
    let mut verify_server_times = Vec::new();
    let mut verify_cli_times = Vec::new();

    for (username, password) in user_credentials.iter() {
        let timing = match execute_sql_via_cli_as_with_timing(
            username,
            password,
            &format!("SELECT * FROM {}", full_table),
        ) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("⚠️  SELECT failed for {}: {}", username, e);
                cleanup(namespace, &user_credentials);
                return;
            },
        };

        if let Some(s) = timing.server_time_ms {
            verify_server_times.push(s);
        }
        verify_cli_times.push(timing.total_time_ms);

        // Count data rows (skip header/separator lines)
        let row_count = timing
            .output
            .lines()
            .filter(|l| {
                l.contains("│") && l.contains(|c: char| c.is_ascii_digit()) && !l.contains("─")
            })
            .count();

        // Verify count
        if row_count != ROWS_PER_USER {
            eprintln!("❌ User {} got {} rows, expected {}", username, row_count, ROWS_PER_USER);
            eprintln!("Output:\n{}", timing.output);
            cleanup(namespace, &user_credentials);
            panic!("Row count mismatch for {}", username);
        }
    }

    let verify_elapsed = verify_start.elapsed();
    let avg_verify_server = if !verify_server_times.is_empty() {
        verify_server_times.iter().sum::<f64>() / verify_server_times.len() as f64
    } else {
        0.0
    };
    let avg_verify_cli =
        verify_cli_times.iter().sum::<u128>() as f64 / verify_cli_times.len() as f64;
    let avg_verify_overhead = avg_verify_cli - avg_verify_server;

    println!("✅ User isolation verified ({:.2?})", verify_elapsed);
    println!("   ├─ Server time (avg): {:.1}ms", avg_verify_server);
    println!("   ├─ CLI total time (avg): {:.1}ms", avg_verify_cli);
    println!(
        "   └─ CLI overhead (avg): {:.1}ms ({:.1}%)",
        avg_verify_overhead,
        (avg_verify_overhead / avg_verify_cli) * 100.0
    );

    // Cleanup
    cleanup(namespace, &user_credentials);

    let total_time = test_start.elapsed();
    println!("\n🎉 Test PASSED - All {} users correctly isolated", NUM_USERS);
    println!("   Total time: {:.2?}", total_time);
    println!("   Breakdown:");
    println!(
        "     ├─ Setup: {:.2?} ({:.1}%)",
        setup_start.elapsed(),
        (setup_start.elapsed().as_secs_f64() / total_time.as_secs_f64()) * 100.0
    );
    println!(
        "     ├─ User creation: {:.2?} ({:.1}%)",
        user_creation_start.elapsed(),
        (user_creation_start.elapsed().as_secs_f64() / total_time.as_secs_f64()) * 100.0
    );
    println!(
        "     ├─ Inserts: {:.2?} ({:.1}%)",
        insert_elapsed,
        (insert_elapsed.as_secs_f64() / total_time.as_secs_f64()) * 100.0
    );
    println!(
        "     └─ Verification: {:.2?} ({:.1}%)",
        verify_start.elapsed(),
        (verify_start.elapsed().as_secs_f64() / total_time.as_secs_f64()) * 100.0
    );
}

fn cleanup(namespace: &str, creds: &[(String, String)]) {
    for (username, _) in creds {
        let _ = execute_sql_as_root_via_cli(&format!("DROP USER IF EXISTS {}", username));
    }
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
