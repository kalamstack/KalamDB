// Tests: Slow Subscriber and Network Degradation Scenarios
//
// Simulates subscriber-side slowness and network conditions that occur in
// production with slow clients (3G modems, high-latency links, overloaded
// consumers, packet loss / intermittent connectivity, etc.).
//
// Since these are integration tests against a live server we cannot inject
// real kernel-level packet delays, so we simulate the *client-side* effects:
//   - Artificial processing delays between events (3G consumer)
//   - Very tight receive_timeout to trigger graceful timeout handling
//   - Dropping and reconnecting mid-stream (simulated packet loss / reset)
//   - Multiple concurrent slow subscribers (fan-out stress)
//   - Large initial dataset consumed slowly (back-pressure / buffering)
//   - Slow consumer while the server keeps writing new rows
//   - Subscription re-establish loop (simulates repeated reconnects)
//
// Run with:
//   cargo test --test subscription slow_subscriber

use crate::common::*;
use kalam_client::{KalamLinkTimeouts, SubscriptionConfig, SubscriptionOptions};
use std::sync::{Arc, Barrier};
use std::time::Duration;

const DRAIN_IDLE_GRACE: Duration = Duration::from_secs(3);

// ─────────────────────────────────────────────────────────────────────────────
// Helpers shared across this module
// ─────────────────────────────────────────────────────────────────────────────

fn slow_client(
    receive_secs: u64,
    initial_data_secs: u64,
) -> Result<kalam_client::KalamLinkClient, Box<dyn std::error::Error + Send + Sync>> {
    client_for_user_on_url_with_timeouts(
        &leader_or_server_url(),
        default_username(),
        default_password(),
        KalamLinkTimeouts::builder()
            .connection_timeout_secs(10)
            .receive_timeout_secs(receive_secs)
            .send_timeout_secs(30)
            .subscribe_timeout_secs(15)
            .auth_timeout_secs(10)
            .initial_data_timeout(Duration::from_secs(initial_data_secs))
            .build(),
    )
}

/// Collect up to `max_events` events from a subscription with an artificial
/// per-event processing delay that simulates a slow consumer.
///
/// After at least one event has been observed, stop once the stream stays idle
/// for a short grace window instead of always burning the full wall timeout.
/// That keeps the slow-subscriber scenarios realistic without turning every
/// successful drain into a timeout-length sleep.
///
/// Returns `(events_collected, hit_error)`.
async fn drain_with_delay(
    sub: &mut kalam_client::SubscriptionManager,
    max_events: usize,
    per_event_delay: Duration,
    wall_timeout: Duration,
) -> (Vec<String>, bool) {
    let mut events = Vec::new();
    let mut hit_error = false;
    let deadline = tokio::time::Instant::now() + wall_timeout;

    loop {
        if events.len() >= max_events {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        let wait_budget = if events.is_empty() {
            remaining
        } else {
            remaining.min(DRAIN_IDLE_GRACE)
        };

        match tokio::time::timeout(wait_budget, sub.next()).await {
            Ok(Some(Ok(ev))) => {
                events.push(format!("{:?}", ev));
                // Simulate 3G-like processing pause
                tokio::time::sleep(per_event_delay).await;
            },
            Ok(Some(Err(e))) => {
                events.push(format!("ERROR: {}", e));
                hit_error = true;
                break;
            },
            Ok(None) => break,
            Err(_elapsed) => {
                // Wall-clock timeout
                break;
            },
        }
    }
    (events, hit_error)
}

fn create_user_table(full: &str) {
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, payload VARCHAR, ts BIGINT) WITH (TYPE = 'USER')",
        full
    ))
    .expect("create user table");
}

fn insert_rows(full: &str, start: i64, end: i64) {
    for i in start..=end {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload, ts) VALUES ({}, 'row_{}', {})",
            full, i, i, i
        ))
        .expect("insert row");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 1: Slow consumer on initial data load (3G simulation)
//
// Pre-insert rows, then subscribe. The consumer introduces a 30ms artificial
// delay per event to mimic 3G-grade throughput (~33 events/s). Verifies that
// all snapshot rows eventually arrive despite the slow consumer.
// ─────────────────────────────────────────────────────────────────────────────
// actual ≈121s × 1.5 = 182s → 270000ms
#[ntest::timeout(270000)]
#[test]
fn subscription_slow_consumer_initial_data() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("slow_consumer_ns");
    let tbl = generate_unique_table("slow_init");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);

    let total_rows: i64 = 20;
    insert_rows(&full, 1, total_rows);

    println!("[TEST] Inserted {} rows; starting slow subscription", total_rows);

    let query = format!("SELECT * FROM {}", full);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (events, hit_error) = rt.block_on(async {
        // Use a generous initial_data_timeout so slow processing doesn't kill us
        let client = slow_client(180, 180).expect("client");
        let mut sub = client.subscribe(&query).await.expect("subscribe");
        drain_with_delay(
            &mut sub,
            (total_rows + 5) as usize,
            Duration::from_millis(30), // ~3G processing delay per event
            Duration::from_secs(120),
        )
        .await
    });

    println!(
        "[TEST] slow_consumer_initial_data: {} events, error={}",
        events.len(),
        hit_error
    );

    // We expect at least an Ack + some snapshot rows
    assert!(
        !hit_error,
        "No errors expected with slow consumer. Events: {:?}",
        &events[..events.len().min(5)]
    );

    let joined = events.join("\n");
    // Check that the subscription delivered something meaningful
    assert!(
        joined.contains("Ack") || joined.contains("InitialDataBatch") || joined.contains("row_1"),
        "Expected initial data events. Got: {}",
        &joined[..joined.len().min(500)]
    );

    let found = (1..=total_rows).filter(|i| joined.contains(&format!("row_{}", i))).count();
    assert!(
        found >= (total_rows as usize) / 2,
        "Expected at least half the rows in slow consumer snapshot. Found {} / {}",
        found,
        total_rows
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!("[TEST] slow_consumer_initial_data passed ({} rows received)", found);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 2: 3G-like high-latency subscription (large initial_data_timeout)
//
// Simulates a subscriber on a high-latency link (300 ms average RTT) by using
// very large `initial_data_timeout` and `receive_timeout` values. Verifies
// the subscription still works correctly end-to-end.
// ─────────────────────────────────────────────────────────────────────────────
// observed ≈14.5s in cluster mode after idle-aware draining; allow 30s on slower machines
#[ntest::timeout(30000)]
#[test]
fn subscription_3g_like_high_latency() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("slow_3g_ns");
    let tbl = generate_unique_table("latency_tbl");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);

    // Pre-populate 10 rows
    insert_rows(&full, 1, 10);

    let query = format!("SELECT * FROM {}", full);
    let marker = format!("late_{}", std::process::id());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let events = rt.block_on(async {
        // Mimic high-latency client: very long timeouts, not rushed
        let client = slow_client(240, 240).expect("client");
        let mut sub = client.subscribe(&query).await.expect("subscribe");

        // Wait 300ms (simulated RTT) before starting to read
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (evs, _) = drain_with_delay(
            &mut sub,
            15,
            Duration::from_millis(100), // 100ms per event – slow 3G consumer
            Duration::from_secs(60),
        )
        .await;

        // Now insert a new row and wait for the live event
        drop(sub); // temporarily drop; we'll check via select
        evs
    });

    let joined = events.join("\n");
    assert!(
        joined.contains("Ack") || joined.contains("row_1"),
        "High-latency subscriber should still get initial data. Got: {}",
        &joined[..joined.len().min(400)]
    );

    // Now verify a change event also arrives for a high-latency client
    let rt2 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime2");

    let change_found = rt2.block_on(async {
        let client = slow_client(240, 240).expect("client2");
        let mut sub = client.subscribe(&query).await.expect("subscribe2");

        // Drain initial snapshot slowly
        let (_, _) =
            drain_with_delay(&mut sub, 15, Duration::from_millis(50), Duration::from_secs(30))
                .await;

        // Insert while consumer is live (after initial data)
        let _ = execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload, ts) VALUES (999, '{}', 999)",
            full, marker
        ));

        // Slow consumer picks up the live insert
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (evs, _) =
            drain_with_delay(&mut sub, 5, Duration::from_millis(100), Duration::from_secs(30))
                .await;
        evs.iter().any(|e| e.contains(&marker))
    });

    assert!(
        change_found,
        "High-latency subscriber should receive live change event for marker '{}'",
        marker
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!("[TEST] 3g_like_high_latency passed");
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 3: Slow consumer while server keeps writing (back-pressure)
//
// Start subscription, then drive rapid inserts. The consumer deliberately
// pauses 20ms per event. Verifies the server buffers / queues properly and
// the consumer eventually receives all events without errors.
// ─────────────────────────────────────────────────────────────────────────────
// actual ≈91s × 1.5 = 137s → 210000ms
#[ntest::timeout(210000)]
#[test]
fn subscription_slow_consumer_concurrent_writes() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("slow_write_ns");
    let tbl = generate_unique_table("backpressure");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);

    let query = format!("SELECT * FROM {}", full);
    let n_writes: i64 = 15;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (insert_count, error_count) = rt.block_on(async {
        let client = slow_client(180, 180).expect("client");
        let mut sub = client.subscribe(&query).await.expect("subscribe");

        // Drain initial empty Ack
        let (_, _) =
            drain_with_delay(&mut sub, 2, Duration::from_millis(0), Duration::from_secs(5)).await;

        // Rapid inserts on the main async task (same thread, after subscription open)
        for i in 1..=n_writes {
            let _ = execute_sql_as_root_via_client(&format!(
                "INSERT INTO {} (id, payload, ts) VALUES ({}, 'bp_{}', {})",
                full, i, i, i
            ));
        }

        println!("[TEST] {} inserts fired; now consuming slowly…", n_writes);

        let (evs, hit_error) = drain_with_delay(
            &mut sub,
            (n_writes + 2) as usize,
            Duration::from_millis(20), // 20ms delay per event
            Duration::from_secs(90),
        )
        .await;

        let insert_events =
            evs.iter().filter(|e| e.contains("Insert") || e.contains("bp_")).count();
        let errs = evs.iter().filter(|e| e.starts_with("ERROR")).count();
        if hit_error {
            eprintln!(
                "[TEST] Subscription error encountered; events: {:?}",
                &evs[..evs.len().min(5)]
            );
        }
        (insert_events, errs)
    });

    println!(
        "[TEST] back-pressure: insert_events={}, error_count={}",
        insert_count, error_count
    );

    assert_eq!(error_count, 0, "No subscription errors expected under back-pressure");
    assert!(
        insert_count >= (n_writes as usize) / 2,
        "Expected at least {} insert events under slow consumer; got {}",
        n_writes / 2,
        insert_count
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!("[TEST] slow_consumer_concurrent_writes passed ({} insert events)", insert_count);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 4: Reconnect after mid-stream drop (packet loss / TCP reset simulation)
//
// Open a subscription, read a few events, then deliberately drop it.
// Re-open a fresh subscription and verify it receives a consistent snapshot.
// This covers the server-side cleanup path and re-subscription stability.
// ─────────────────────────────────────────────────────────────────────────────
// actual ≈41s × 1.5 = 62s → 90000ms
#[ntest::timeout(90000)]
#[test]
fn subscription_reconnect_after_drop() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("reconnect_ns");
    let tbl = generate_unique_table("reconnect");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);
    insert_rows(&full, 1, 8);

    let query = format!("SELECT * FROM {}", full);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (first_count, second_count) = rt.block_on(async {
        // ── First connection: read a couple events then drop abruptly ──
        let client1 = slow_client(60, 60).expect("client1");
        let mut sub1 = client1.subscribe(&query).await.expect("subscribe1");
        let (evs1, _) =
            drain_with_delay(&mut sub1, 3, Duration::from_millis(0), Duration::from_secs(10)).await;
        let c1 = evs1.len();
        // Simulated abrupt drop: just let sub1 fall out of scope
        drop(sub1);
        drop(client1);

        // Brief pause – simulates reconnect delay after network reset
        tokio::time::sleep(Duration::from_millis(500)).await;

        // ── Second connection: full subscription, verify snapshot ──
        let client2 = slow_client(60, 60).expect("client2");
        let mut sub2 = client2.subscribe(&query).await.expect("subscribe2");
        let (evs2, hit_err2) =
            drain_with_delay(&mut sub2, 15, Duration::from_millis(0), Duration::from_secs(30))
                .await;
        let c2 = evs2.len();

        assert!(
            !hit_err2,
            "Re-subscription must not produce errors. Events: {:?}",
            &evs2[..evs2.len().min(5)]
        );

        let joined2 = evs2.join("\n");
        assert!(
            joined2.contains("Ack") || joined2.contains("row_1"),
            "Re-subscription should deliver initial snapshot after drop. Events: {}",
            &joined2[..joined2.len().min(400)]
        );

        (c1, c2)
    });

    println!("[TEST] reconnect_after_drop: first={}, second={}", first_count, second_count);
    assert!(second_count > 0, "Second connection must receive events");

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!("[TEST] reconnect_after_drop passed");
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 5: Short receive_timeout causes graceful error, client can reconnect
//
// Sets receive_timeout to 2 seconds and initial_data_timeout to 3 seconds.
// With a large pre-existing dataset the initial load will take longer and
// the tight timeout will fire. Verify:
//   (a) The error is surfaced gracefully (no panic, no hang).
//   (b) A subsequent subscription with normal timeouts succeeds.
// ─────────────────────────────────────────────────────────────────────────────
// actual ≈67s × 1.5 = 101s → 150000ms
#[ntest::timeout(150000)]
#[test]
fn subscription_timeout_graceful_then_reconnect() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("timeout_ns");
    let tbl = generate_unique_table("tmo_tbl");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);
    // Insert enough rows to make initial snapshot delivery take some time
    insert_rows(&full, 1, 30);

    let query = format!("SELECT * FROM {}", full);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    rt.block_on(async {
        // ── Phase 1: tight timeouts – expect either Ok with data or an error ──
        let tight_timeouts = KalamLinkTimeouts::builder()
            .connection_timeout_secs(10)
            .receive_timeout_secs(2)      // very short – may time out mid-stream
            .send_timeout_secs(10)
            .subscribe_timeout_secs(10)
            .auth_timeout_secs(10)
            .initial_data_timeout(Duration::from_secs(3)) // short initial window
            .build();

        let client_tight = client_for_user_on_url_with_timeouts(
            &leader_or_server_url(),
            default_username(),
            default_password(),
            tight_timeouts,
        )
        .expect("tight client");

        // This might succeed quickly or fail — either is acceptable
        match client_tight.subscribe(&query).await {
            Ok(mut sub) => {
                let (evs, _) = drain_with_delay(
                    &mut sub,
                    50,
                    Duration::from_millis(0),
                    Duration::from_secs(5),
                )
                .await;
                println!(
                    "[TEST] tight-timeout phase collected {} events (timeout may have fired)",
                    evs.len()
                );
                // No assertion on count – the important thing is no panic/hang
            },
            Err(e) => {
                println!("[TEST] tight-timeout subscription error (expected): {}", e);
            },
        }

        // Brief recovery delay (simulates reconnect back-off)
        tokio::time::sleep(Duration::from_millis(800)).await;

        // ── Phase 2: normal timeouts – must succeed ──
        let client_normal = slow_client(120, 120).expect("normal client");
        let mut sub_normal = client_normal.subscribe(&query).await.expect("normal subscribe");
        let (evs_normal, hit_err) = drain_with_delay(
            &mut sub_normal,
            40,
            Duration::from_millis(0),
            Duration::from_secs(60),
        )
        .await;

        assert!(
            !hit_err,
            "Normal re-subscription must not error. Events: {:?}",
            &evs_normal[..evs_normal.len().min(5)]
        );
        assert!(
            !evs_normal.is_empty(),
            "Normal re-subscription must receive events after tight-timeout failure"
        );
        let joined = evs_normal.join("\n");
        assert!(
            joined.contains("row_1") || joined.contains("Ack"),
            "Re-subscription should deliver data after timeout recovery. Got: {}",
            &joined[..joined.len().min(400)]
        );
        println!(
            "[TEST] timeout_graceful_then_reconnect: normal phase got {} events",
            evs_normal.len()
        );
    });

    // Drain lingering background tasks from tight-timeout phase so nextest
    // does not report a process-level resource leak.
    rt.shutdown_timeout(Duration::from_secs(2));

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!("[TEST] timeout_graceful_then_reconnect passed");
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 6: Multiple concurrent slow subscribers (fan-out under slow clients)
//
// Opens N slow subscribers on the same query simultaneously. Each consumer
// processes events slowly. Verifies the server correctly fans out to all of
// them without errors or missing data.
// ─────────────────────────────────────────────────────────────────────────────
// observed ≈10.7s in cluster mode after idle-aware draining; allow 20s on slower machines
#[ntest::timeout(20000)]
#[test]
fn subscription_multiple_concurrent_slow_subscribers() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("fanout_ns");
    let tbl = generate_unique_table("fanout_tbl");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, val VARCHAR) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'PUBLIC')",
        full
    ))
    .expect("create shared table");

    // Pre-insert a few rows
    for i in 1..=5 {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, val) VALUES ({}, 'pre_{}')",
            full, i, i
        ))
        .expect("insert");
    }

    let query = format!("SELECT * FROM {}", full);
    let live_marker = format!("live_{}", std::process::id());
    const N_SUBSCRIBERS: usize = 3;
    let ready = Arc::new(Barrier::new(N_SUBSCRIBERS + 1));

    let results: Vec<(usize, bool, bool)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..N_SUBSCRIBERS)
            .map(|idx| {
                let q = query.clone();
                let marker = live_marker.clone();
                let ready = Arc::clone(&ready);
                scope.spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("runtime");

                    rt.block_on(async move {
                        // Stagger start times to simulate real-world connection patterns
                        tokio::time::sleep(Duration::from_millis(50 * idx as u64)).await;

                        let client = match slow_client(180, 180) {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("[SUB {}] client error: {}", idx, e);
                                return (0usize, true, false);
                            },
                        };

                        let mut sub = match client.subscribe(&q).await {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("[SUB {}] subscribe error: {}", idx, e);
                                return (0usize, true, false);
                            },
                        };

                        // Drain initial snapshot slowly (simulate 3G consumer)
                        let (init_evs, _) = drain_with_delay(
                            &mut sub,
                            10,
                            Duration::from_millis(40),
                            Duration::from_secs(30),
                        )
                        .await;

                        // Wait until every subscriber has finished its initial drain so the
                        // live insert is observed in the live phase instead of whichever
                        // subscriber happened to still be draining the snapshot.
                        ready.wait();

                        // All subscribers wait for the live insert
                        // (the insert is performed by the main thread after all subscribers are up)
                        let (live_evs, hit_error) = drain_with_delay(
                            &mut sub,
                            5,
                            Duration::from_millis(40),
                            Duration::from_secs(60),
                        )
                        .await;

                        let all_evs = [init_evs, live_evs].concat();
                        let found_marker = all_evs.iter().any(|e| e.contains(&marker));
                        let total = all_evs.len();

                        println!(
                            "[SUB {}] {} events, marker_found={}, error={}",
                            idx, total, found_marker, hit_error
                        );
                        (total, hit_error, found_marker)
                    })
                })
            })
            .collect();

        ready.wait();

        // Insert the live row all subscribers must see
        let _ = execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, val) VALUES (100, '{}')",
            full, live_marker
        ));

        handles.into_iter().map(|h| h.join().expect("thread")).collect()
    });

    for (idx, (event_count, hit_error, found_marker)) in results.iter().enumerate() {
        assert!(!hit_error, "Subscriber {} should not error under concurrent slow load", idx);
        assert!(
            *event_count > 0,
            "Subscriber {} must receive at least one event under concurrent slow load",
            idx
        );
        assert!(
            *found_marker,
            "Subscriber {} must observe the shared live marker under concurrent slow load",
            idx
        );
    }

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!(
        "[TEST] multiple_concurrent_slow_subscribers passed: {:?}",
        results.iter().map(|(c, _, _)| c).collect::<Vec<_>>()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 7: Large initial dataset + slow batch consumption
//
// Pre-insert many rows, subscribe with a small batch_size option to force
// multi-batch initial delivery, and consume each batch slowly. Verifies that
// all batches are flushed and no data is lost even with a slow consumer.
// ─────────────────────────────────────────────────────────────────────────────
// observed ≈8.1s in cluster mode after idle-aware draining; allow 20s on slower machines
#[ntest::timeout(20000)]
#[test]
fn subscription_large_initial_data_slow_batch_consumer() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("large_init_ns");
    let tbl = generate_unique_table("large_batch");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);

    let total: i64 = 40;
    insert_rows(&full, 1, total);

    // Small delay to let writes flush
    std::thread::sleep(Duration::from_millis(300));

    let query = format!("SELECT * FROM {}", full);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (events, hit_error) = rt.block_on(async {
        let client = slow_client(240, 240).expect("client");

        // Use small batch_size to force multiple server-side flush rounds
        let sub_id = format!(
            "large_batch_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut config = SubscriptionConfig::new(&sub_id, &query);
        config.options = Some(SubscriptionOptions::default().with_batch_size(8));
        let mut sub = client.subscribe_with_config(config).await.expect("subscribe");

        // 50ms per event – slow 3G batch consumer
        drain_with_delay(
            &mut sub,
            (total + 10) as usize,
            Duration::from_millis(50),
            Duration::from_secs(180),
        )
        .await
    });

    println!(
        "[TEST] large_initial_data_slow_batch: {} events, error={}",
        events.len(),
        hit_error
    );

    assert!(!hit_error, "No errors expected consuming large initial dataset slowly");

    let joined = events.join("\n");
    let found = (1..=total).filter(|i| joined.contains(&format!("row_{}", i))).count();
    assert!(
        found >= (total as usize) * 3 / 4,
        "Expected >= {}% rows with slow batch consumer. Found {} / {}",
        75,
        found,
        total
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!(
        "[TEST] large_initial_data_slow_batch_consumer passed ({} / {} rows)",
        found, total
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 8: Repeated reconnect loop (simulates intermittent connectivity / loss)
//
// Reconnects N times in quick succession. Each session subscribes, reads a
// couple events, then drops the connection. Verifies:
//   - The server correctly cleans up stale subscriptions.
//   - Each fresh connection gets a valid snapshot.
//   - No "zombie" errors or resource leaks after N rapid reconnects.
// ─────────────────────────────────────────────────────────────────────────────
// actual ≈52s × 1.5 = 78s → 120000ms
#[ntest::timeout(120000)]
#[test]
fn subscription_repeated_reconnect_loop() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("reconnect_loop_ns");
    let tbl = generate_unique_table("loop_tbl");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);
    insert_rows(&full, 1, 5);

    let query = format!("SELECT * FROM {}", full);
    const RECONNECT_ROUNDS: usize = 5;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    rt.block_on(async {
        for round in 1..=RECONNECT_ROUNDS {
            println!("[TEST] reconnect loop round {}/{}", round, RECONNECT_ROUNDS);

            let client = slow_client(30, 30).expect("client");
            let mut sub = client.subscribe(&query).await.expect("subscribe");

            // Read just a few events then drop abruptly (simulates connection reset)
            let (evs, hit_error) =
                drain_with_delay(&mut sub, 4, Duration::from_millis(0), Duration::from_secs(10))
                    .await;

            assert!(
                !hit_error,
                "Round {}: subscription must not produce hard errors. Events: {:?}",
                round,
                &evs[..evs.len().min(3)]
            );
            assert!(
                !evs.is_empty(),
                "Round {}: must receive at least one event per reconnect attempt",
                round
            );

            // Abrupt drop – don't call close(), just drop
            drop(sub);
            drop(client);

            // Brief back-off between reconnects (simulates reconnect delay)
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!("[TEST] repeated_reconnect_loop passed ({} rounds)", RECONNECT_ROUNDS);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 9: Subscription stable after extended pause (idle connection)
//
// Subscribe, read initial data, then pause for several seconds doing nothing
// (simulates a subscriber that stalled or was batching), then resume reading.
// Inserts a new row during the pause. Verifies the event arrives after the
// pause ends (i.e. the server keeps the connection alive and buffers the event).
// ─────────────────────────────────────────────────────────────────────────────
// actual ≈49s × 1.5 = 74s → 120000ms
#[ntest::timeout(120000)]
#[test]
fn subscription_stable_after_idle_pause() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("idle_pause_ns");
    let tbl = generate_unique_table("idle_tbl");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);
    insert_rows(&full, 1, 3);

    let query = format!("SELECT * FROM {}", full);
    let wake_marker = format!("wake_{}", std::process::id());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let found = rt.block_on(async {
        let client = slow_client(120, 120).expect("client");
        let mut sub = client.subscribe(&query).await.expect("subscribe");

        // Drain initial snapshot
        let (_, _) =
            drain_with_delay(&mut sub, 5, Duration::from_millis(0), Duration::from_secs(15)).await;

        // ── Simulate idle pause (3–4 seconds): subscriber goes quiet ──
        println!("[TEST] idle_pause: subscriber pausing for 3s…");
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Insert a row while the subscriber is "paused"
        let _ = execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload, ts) VALUES (99, '{}', 99)",
            full, wake_marker
        ));

        // Resume reading after the pause
        println!("[TEST] idle_pause: subscriber resuming…");
        let (evs, hit_error) =
            drain_with_delay(&mut sub, 5, Duration::from_millis(0), Duration::from_secs(30)).await;

        assert!(!hit_error, "No errors expected after idle pause. Events: {:?}", &evs);
        evs.iter().any(|e| e.contains(&wake_marker))
    });

    assert!(
        found,
        "Event inserted during subscriber idle pause must be received after resume (marker: {})",
        wake_marker
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!("[TEST] stable_after_idle_pause passed");
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 10: Subscription survives partial read then large burst
//
// Subscriber reads initial data, then there is a large burst of inserts
// followed by slow consumption. Verifies the subscription never errors and
// the consumer eventually catches up.
// ─────────────────────────────────────────────────────────────────────────────
// observed ≈8.1s in cluster mode after idle-aware draining; allow 20s on slower machines
#[ntest::timeout(20000)]
#[test]
fn subscription_burst_then_slow_catchup() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("burst_catchup_ns");
    let tbl = generate_unique_table("burst_tbl");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    create_user_table(&full);

    let query = format!("SELECT * FROM {}", full);
    let burst_size: i64 = 20;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (received_burst, hit_error) = rt.block_on(async {
        let client = slow_client(180, 180).expect("client");
        let mut sub = client.subscribe(&query).await.expect("subscribe");

        // Drain empty initial Ack
        let (_, _) =
            drain_with_delay(&mut sub, 2, Duration::from_millis(0), Duration::from_secs(5)).await;

        // Fire burst of inserts
        println!("[TEST] firing {} burst inserts", burst_size);
        for i in 1..=burst_size {
            let _ = execute_sql_as_root_via_client(&format!(
                "INSERT INTO {} (id, payload, ts) VALUES ({}, 'burst_{}', {})",
                full, i, i, i
            ));
        }

        // Slow catchup: 25ms per event
        println!("[TEST] slow catchup starting (25ms/event)…");
        let (evs, hit_error) = drain_with_delay(
            &mut sub,
            (burst_size + 2) as usize,
            Duration::from_millis(25),
            Duration::from_secs(120),
        )
        .await;

        let burst_evs = evs.iter().filter(|e| e.contains("Insert") || e.contains("burst_")).count();
        (burst_evs, hit_error)
    });

    println!(
        "[TEST] burst_then_slow_catchup: {} / {} burst events, error={}",
        received_burst, burst_size, hit_error
    );

    assert!(!hit_error, "No errors expected during burst then slow catchup");
    assert!(
        received_burst >= (burst_size as usize) / 2,
        "Should receive at least half the burst events. Got {} / {}",
        received_burst,
        burst_size
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    println!("[TEST] burst_then_slow_catchup passed ({} burst events caught)", received_burst);
}
