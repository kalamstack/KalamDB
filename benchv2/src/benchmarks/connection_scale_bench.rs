use std::future::Future;
use std::io::{self, IsTerminal, Write};
use std::pin::Pin;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kalam_link::{ChangeEvent, SubscriptionConfig};
use sysinfo::{MemoryRefreshKind, Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use tokio::sync::{watch, Semaphore};

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;

pub struct ConnectionScaleBench;

const FIRST_CONNECTION_CHECKPOINT: u32 = 1_000;
const CONNECTION_CHECKPOINT_STEP: u32 = 10_000;
const DEFAULT_CONNECT_BATCH: usize = 1_000;
const DEFAULT_CONNECT_WAVE_SIZE: usize = 500;
const DEFAULT_CONNECT_WAVE_PAUSE_MS: u64 = 0;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_DELIVERY_WINDOW_SECS: u64 = 3;
const DEFAULT_DELIVERY_TOLERANCE: f64 = 0.99;
const DELIVERY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const LIVE_PROGRESS_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_MEMORY_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(45);
const TARGET_VALIDATE_TIMEOUT: Duration = Duration::from_secs(5);
const TABLE_INFO_WIDTH: usize = 120;

impl Benchmark for ConnectionScaleBench {
    fn name(&self) -> &str {
        "connection_scale"
    }

    fn category(&self) -> &str {
        "Scale"
    }

    fn description(&self) -> &str {
        "One WebSocket connection per subscription tier test (up to --max-subscribers, default 100K)"
    }

    fn single_run(&self) -> bool {
        true
    }

    fn setup<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            client
                .sql_ok(&format!("CREATE NAMESPACE IF NOT EXISTS {}", config.namespace))
                .await?;
            let _ = client
                .sql(&format!("DROP SHARED TABLE IF EXISTS {}.conn_scale", config.namespace))
                .await;
            client
                .sql_ok(&format!(
                    "CREATE SHARED TABLE {}.conn_scale (id INT PRIMARY KEY, payload TEXT)",
                    config.namespace
                ))
                .await?;
            client
                .sql_ok(&format!(
                    "INSERT INTO {}.conn_scale (id, payload) VALUES (1, 'seed')",
                    config.namespace
                ))
                .await?;
            Ok(())
        })
    }

    fn run<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
        iteration: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let _ = client
                .sql(&format!("DELETE FROM {}.conn_scale WHERE id >= 1000000", config.namespace))
                .await;

            let max_connections = config.max_subscribers;
            let connect_batch = connect_batch_limit();
            let connect_wave_size = connect_wave_size_limit();
            let connect_wave_pause = connect_wave_pause_duration();
            let connect_timeout = connect_timeout_duration();
            let delivery_tolerance = delivery_tolerance_ratio();
            let ws_targets = validate_ws_targets(
                client,
                &config.namespace,
                resolve_ws_targets(config),
            )
            .await?;
            let ws_targets = Arc::new(ws_targets);
            let mut managed_server_memory = ManagedServerMemoryTracker::from_env();
            let live_row_enabled = io::stdout().is_terminal();
            let single_target_ws_limit = detected_single_target_ws_limit().unwrap_or(32_000);
            let single_target_ws_limit_label = format_num(single_target_ws_limit as u32);

            if ws_targets.len() == 1
                && max_connections > single_target_ws_limit as u32
                && std::env::var("KALAMDB_ALLOW_SINGLE_WS_TARGET").ok().as_deref() != Some("1")
            {
                return Err(format!(
                    "Single WS target ({}) would require about {} WebSocket connections, which is likely capped by local ephemeral ports on this host near {}. Use --urls with multiple endpoints or set KALAMDB_ALLOW_SINGLE_WS_TARGET=1 to force this run.",
                    ws_targets[0],
                    max_connections,
                    single_target_ws_limit_label,
                ));
            }

            let checkpoints = build_connection_checkpoints(max_connections);

            let semaphore = Arc::new(Semaphore::new(connect_batch));
            let connected = Arc::new(AtomicU32::new(0));
            let failed = Arc::new(AtomicU32::new(0));
            let delivered = Arc::new(AtomicU32::new(0));
            let connect_failures = Arc::new(AtomicU32::new(0));
            let delivery_failures = Arc::new(AtomicU32::new(0));
            let probe_epoch = Arc::new(AtomicU32::new(0));
            let (stop_tx, _stop_rx) = watch::channel(false);
            let mut handles = Vec::with_capacity(max_connections as usize);
            let mut max_achieved = 0;
            let mut launched_connections = 0u32;
            let benchmark_start = Instant::now();
            let benchmark_start_memory = managed_server_memory
                .as_mut()
                .and_then(ManagedServerMemoryTracker::sample_rss_bytes);
            let mut last_live_render = Instant::now();
            let mut last_memory_refresh = Instant::now();
            let benchmark_start_rss_display = format_memory(benchmark_start_memory);
            let mut live_rss_display = benchmark_start_rss_display.clone();

            println!();
            println!(
                "  ┌─────────────┬───────────┬──────────┬──────────────┬───────────────┬──────────────┬──────────────┬──────────────┬──────────────┐"
            );
            println!(
                "  │ Target Conn │ Connected │  Failed  │ Connect Time │ Chg Received  │ Deliver Time │  Loop Time   │  Start RSS   │   End RSS    │"
            );
            println!(
                "  ├─────────────┼───────────┼──────────┼──────────────┼───────────────┼──────────────┼──────────────┼──────────────┼──────────────┤"
            );
            println!(
                "  Settings: connect_batch={}, wave_size={}, wave_pause={}ms, connect_timeout={}, ws_targets={}, delivery_tolerance={:.1}%",
                connect_batch,
                connect_wave_size,
                connect_wave_pause.as_millis(),
                format_duration(connect_timeout),
                ws_targets.len(),
                delivery_tolerance * 100.0,
            );
            println!("  WebSocket target validation: {} endpoint(s) reachable", ws_targets.len());
            if !client.ws_local_bind_addresses().is_empty() {
                println!(
                    "  WS local bind pool: {} address(es) [{}] | effective single-target ceiling ~{}",
                    client.ws_local_bind_addresses().len(),
                    summarize_bind_pool(client.ws_local_bind_addresses()),
                    single_target_ws_limit_label,
                );
            }

            if live_row_enabled {
                render_live_tier_row(
                    &render_connection_scale_row(
                        checkpoints[0],
                        0,
                        0,
                        format_duration(Duration::ZERO),
                        format!("{}/{}", format_num(0), format_num(checkpoints[0])),
                        "...".to_string(),
                        format_duration(Duration::ZERO),
                        benchmark_start_rss_display.as_str(),
                        live_rss_display.as_str(),
                    ),
                    false,
                );
            }

            for (checkpoint_index, &checkpoint_target) in checkpoints.iter().enumerate() {
                let delivery_window = delivery_window_for_tier(checkpoint_target);

                while launched_connections < checkpoint_target {
                    let wave_remaining = (checkpoint_target - launched_connections)
                        .min(connect_wave_size as u32);
                    for _ in 0..wave_remaining {
                        let connection_id = launched_connections;
                        let bench_client = client.clone();
                        let namespace = config.namespace.clone();
                        let ws_target =
                            ws_targets[(connection_id as usize) % ws_targets.len()].clone();
                        let bind_address = bench_client
                            .ws_local_bind_address_for_index(connection_id as usize)
                            .map(str::to_string);
                        let semaphore = semaphore.clone();
                        let connected = connected.clone();
                        let failed = failed.clone();
                        let delivered = delivered.clone();
                        let connect_failures = connect_failures.clone();
                        let delivery_failures = delivery_failures.clone();
                        let probe_epoch = probe_epoch.clone();
                        let mut stop_rx = stop_tx.subscribe();

                        let handle = tokio::spawn(async move {
                            let _permit = semaphore.acquire().await.unwrap();

                            if *stop_rx.borrow() {
                                return;
                            }

                            let link = match bench_client
                                .new_isolated_link_for_ws_url_with_bind_address(
                                    Some(&ws_target),
                                    bind_address.as_deref(),
                                ) {
                                Ok(link) => link,
                                Err(_) => {
                                    failed.fetch_add(1, Ordering::Relaxed);
                                    connect_failures.fetch_add(1, Ordering::Relaxed);
                                    return;
                                },
                            };

                            let setup_result = tokio::select! {
                                _ = stop_rx.changed() => Err("cancelled".to_string()),
                                result = tokio::time::timeout(connect_timeout, async {
                                    let mut sub_config = SubscriptionConfig::without_initial_data(
                                        format!("conn_scale_{}_{}", iteration, connection_id),
                                        format!("SELECT * FROM {}.conn_scale", namespace),
                                    );
                                    sub_config.ws_url = Some(ws_target.clone());
                                    link.subscribe_with_config(sub_config).await
                                }) => {
                                    match result {
                                        Ok(Ok(sub)) => Ok(sub),
                                        Ok(Err(err)) => Err(err.to_string()),
                                        Err(_) => Err("timeout".to_string()),
                                    }
                                }
                            };

                            drop(_permit);

                            let mut subscription = match setup_result {
                                Ok(subscription) => {
                                    connected.fetch_add(1, Ordering::Relaxed);
                                    subscription
                                },
                                Err(err) if err == "cancelled" => {
                                    let _ = link.disconnect().await;
                                    return;
                                },
                                Err(_) => {
                                    failed.fetch_add(1, Ordering::Relaxed);
                                    connect_failures.fetch_add(1, Ordering::Relaxed);
                                    let _ = link.disconnect().await;
                                    return;
                                },
                            };

                            let mut seen_probe_epoch = 0u32;
                            loop {
                                tokio::select! {
                                    biased;
                                    _ = stop_rx.changed() => {
                                        let _ = subscription.close().await;
                                        let _ = link.disconnect().await;
                                        return;
                                    }
                                    msg = subscription.next() => {
                                        match msg {
                                            Some(Ok(ChangeEvent::Insert { .. })) => {
                                                let active_probe_epoch = probe_epoch.load(Ordering::Relaxed);
                                                if active_probe_epoch > 0 && active_probe_epoch > seen_probe_epoch {
                                                    seen_probe_epoch = active_probe_epoch;
                                                    delivered.fetch_add(1, Ordering::Relaxed);
                                                }
                                            }
                                            Some(Ok(_)) => {}
                                            Some(Err(_)) | None => {
                                                failed.fetch_add(1, Ordering::Relaxed);
                                                delivery_failures.fetch_add(1, Ordering::Relaxed);
                                                let _ = link.disconnect().await;
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        });

                        handles.push(handle);
                        launched_connections += 1;
                    }

                    if connect_wave_pause > Duration::ZERO && launched_connections < checkpoint_target {
                        tokio::time::sleep(connect_wave_pause).await;
                    }

                    if live_row_enabled && last_live_render.elapsed() >= LIVE_PROGRESS_REFRESH_INTERVAL {
                        refresh_live_rss(
                            &mut managed_server_memory,
                            &mut last_memory_refresh,
                            &mut live_rss_display,
                        );
                        render_live_tier_row(
                            &render_connection_scale_row(
                                checkpoint_target,
                                connected.load(Ordering::Relaxed),
                                failed.load(Ordering::Relaxed),
                                format_duration(benchmark_start.elapsed()),
                                format!(
                                    "{}/{}",
                                    format_num(delivered.load(Ordering::Relaxed)),
                                    format_num(checkpoint_target),
                                ),
                                "...".to_string(),
                                format_duration(benchmark_start.elapsed()),
                                benchmark_start_rss_display.as_str(),
                                live_rss_display.as_str(),
                            ),
                            false,
                        );
                        last_live_render = Instant::now();
                    }
                }

                let mut last_done = connected.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
                let mut stall_deadline =
                    tokio::time::Instant::now() + connect_timeout + Duration::from_secs(5);
                loop {
                    let done = connected.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
                    if done >= checkpoint_target {
                        break;
                    }

                    if done > last_done {
                        last_done = done;
                        stall_deadline =
                            tokio::time::Instant::now() + connect_timeout + Duration::from_secs(5);
                    } else if tokio::time::Instant::now() >= stall_deadline {
                        break;
                    }

                    if live_row_enabled && last_live_render.elapsed() >= LIVE_PROGRESS_REFRESH_INTERVAL {
                        refresh_live_rss(
                            &mut managed_server_memory,
                            &mut last_memory_refresh,
                            &mut live_rss_display,
                        );
                        render_live_tier_row(
                            &render_connection_scale_row(
                                checkpoint_target,
                                connected.load(Ordering::Relaxed),
                                failed.load(Ordering::Relaxed),
                                format_duration(benchmark_start.elapsed()),
                                format!(
                                    "{}/{}",
                                    format_num(delivered.load(Ordering::Relaxed)),
                                    format_num(checkpoint_target),
                                ),
                                "...".to_string(),
                                format_duration(benchmark_start.elapsed()),
                                benchmark_start_rss_display.as_str(),
                                live_rss_display.as_str(),
                            ),
                            false,
                        );
                        last_live_render = Instant::now();
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                let connected_count = connected.load(Ordering::Relaxed);
                let failed_count = failed.load(Ordering::Relaxed);
                let connect_fail_count = connect_failures.load(Ordering::Relaxed);
                let delivery_fail_count = delivery_failures.load(Ordering::Relaxed);
                let connect_time = benchmark_start.elapsed();

                let mut delivery_time = Duration::ZERO;
                let mut checkpoint_error = None;
                if connected_count < checkpoint_target {
                    checkpoint_error = Some(format!(
                        "checkpoint {} reached only {}/{} connections (connect_failures={}, delivery_failures={})",
                        format_num(checkpoint_target),
                        format_num(connected_count),
                        format_num(checkpoint_target),
                        format_num(connect_fail_count),
                        format_num(delivery_fail_count),
                    ));
                } else {
                    let probe_settle = probe_settle_duration_for_tier(checkpoint_target);
                    if probe_settle > Duration::ZERO {
                        tokio::time::sleep(probe_settle).await;
                    }

                    delivered.store(0, Ordering::Relaxed);
                    probe_epoch.store((checkpoint_index + 1) as u32, Ordering::Relaxed);

                    let write_id = 1_000_000 + checkpoint_target;
                    let write_result = client
                        .sql_ok(&format!(
                            "INSERT INTO {}.conn_scale (id, payload) VALUES ({}, 'checkpoint_{}')",
                            config.namespace, write_id, checkpoint_target
                        ))
                        .await;

                    if let Err(err) = write_result {
                        checkpoint_error = Some(format!(
                            "checkpoint {} failed to insert probe row: {}",
                            format_num(checkpoint_target),
                            err,
                        ));
                    } else {
                        let delivery_threshold = ((connected_count as f64) * delivery_tolerance).floor() as u32;
                        let wait_start = Instant::now();
                        let mut last_progress_count = 0u32;
                        let mut last_progress_time = Instant::now();
                        let stall_cutoff = Duration::from_secs(5);
                        loop {
                            let delivered_count = delivered.load(Ordering::Relaxed);
                            if delivered_count >= connected_count {
                                delivery_time = wait_start.elapsed();
                                break;
                            }

                            // Track delivery progress for stall detection
                            if delivered_count > last_progress_count {
                                last_progress_count = delivered_count;
                                last_progress_time = Instant::now();
                            }

                            // If delivery stalled and we're above tolerance, stop waiting
                            if delivered_count >= delivery_threshold
                                && last_progress_time.elapsed() >= stall_cutoff
                            {
                                delivery_time = wait_start.elapsed();
                                println!(
                                    "  │ {:^width$} │",
                                    format!(
                                        "⚠ {}/{} delivered ({:.2}% >= {:.1}% tolerance) – {} stragglers (stalled {})",
                                        format_num(delivered_count),
                                        format_num(connected_count),
                                        (delivered_count as f64 / connected_count as f64) * 100.0,
                                        delivery_tolerance * 100.0,
                                        format_num(connected_count - delivered_count),
                                        format_duration(stall_cutoff),
                                    ),
                                    width = TABLE_INFO_WIDTH,
                                );
                                break;
                            }

                            let elapsed = wait_start.elapsed();
                            if elapsed >= delivery_window {
                                let final_delivered_count = delivered.load(Ordering::Relaxed);
                                if final_delivered_count >= connected_count {
                                    delivery_time = elapsed;
                                    break;
                                }
                                delivery_time = elapsed;
                                if final_delivered_count >= delivery_threshold {
                                    // Within tolerance – pass with a note
                                    println!(
                                        "  │ {:^width$} │",
                                        format!(
                                            "⚠ {}/{} delivered ({:.2}% >= {:.1}% tolerance) – {} stragglers within {}",
                                            format_num(final_delivered_count),
                                            format_num(connected_count),
                                            (final_delivered_count as f64 / connected_count as f64) * 100.0,
                                            delivery_tolerance * 100.0,
                                            format_num(connected_count - final_delivered_count),
                                            format_duration(delivery_window),
                                        ),
                                        width = TABLE_INFO_WIDTH,
                                    );
                                } else {
                                    checkpoint_error = Some(format!(
                                        "checkpoint {} delivered {}/{} insert events within {} (below {:.1}% tolerance)",
                                        format_num(checkpoint_target),
                                        format_num(final_delivered_count),
                                        format_num(connected_count),
                                        format_duration(delivery_window),
                                        delivery_tolerance * 100.0,
                                    ));
                                }
                                break;
                            }

                            if live_row_enabled && last_live_render.elapsed() >= LIVE_PROGRESS_REFRESH_INTERVAL {
                                refresh_live_rss(
                                    &mut managed_server_memory,
                                    &mut last_memory_refresh,
                                    &mut live_rss_display,
                                );
                                render_live_tier_row(
                                    &render_connection_scale_row(
                                        checkpoint_target,
                                        connected_count,
                                        failed_count,
                                        format_duration(connect_time),
                                        format!(
                                            "{}/{}",
                                            format_num(delivered_count),
                                            format_num(connected_count),
                                        ),
                                        format_duration(wait_start.elapsed()),
                                        format_duration(benchmark_start.elapsed()),
                                        benchmark_start_rss_display.as_str(),
                                        live_rss_display.as_str(),
                                    ),
                                    false,
                                );
                                last_live_render = Instant::now();
                            }

                            tokio::time::sleep(
                                DELIVERY_POLL_INTERVAL.min(delivery_window.saturating_sub(elapsed)),
                            )
                            .await;
                        }
                    }
                }

                let memory_after = managed_server_memory
                    .as_mut()
                    .and_then(ManagedServerMemoryTracker::sample_rss_bytes);

                let final_row = render_connection_scale_row(
                    checkpoint_target,
                    connected_count,
                    failed_count,
                    format_duration(connect_time),
                    format!(
                        "{}/{}",
                        format_num(delivered.load(Ordering::Relaxed)),
                        format_num(connected_count.max(checkpoint_target)),
                    ),
                    format_duration(delivery_time),
                    format_duration(benchmark_start.elapsed()),
                    benchmark_start_rss_display.as_str(),
                    format_memory(memory_after).as_str(),
                );
                if live_row_enabled {
                    render_live_tier_row(&final_row, true);
                } else {
                    println!("{}", final_row);
                }
                if let Some(memory_delta_info) = render_memory_delta_info(
                    benchmark_start_memory,
                    memory_after,
                    connected_count,
                ) {
                    println!("  │ {:<width$} │", memory_delta_info, width = TABLE_INFO_WIDTH);
                }

                if let Some(err) = checkpoint_error {
                    let _ = stop_tx.send(true);
                    wait_for_task_shutdown(&mut handles).await;
                    println!(
                        "  │ {:^width$} │",
                        format!(
                            "⚠ Failure breakdown: connect_failures={}, delivery_failures={}, window={}",
                            format_num(connect_fail_count),
                            format_num(delivery_fail_count),
                            format_duration(delivery_window),
                        ),
                        width = TABLE_INFO_WIDTH,
                    );
                    println!(
                        "  │ {:^width$} │",
                        err,
                        width = TABLE_INFO_WIDTH,
                    );
                    println!(
                        "  └─────────────┴───────────┴──────────┴──────────────┴───────────────┴──────────────┴──────────────┴──────────────┴──────────────┘"
                    );
                    println!("  Max sustained concurrent connections: {}", format_num(max_achieved));
                    println!();
                    return Err(format!(
                        "connection_scale stopped at checkpoint {} after last successful checkpoint {}",
                        format_num(checkpoint_target),
                        format_num(max_achieved),
                    ));
                }

                max_achieved = connected_count;

                if live_row_enabled && checkpoint_index + 1 < checkpoints.len() {
                    delivered.store(0, Ordering::Relaxed);
                    render_live_tier_row(
                        &render_connection_scale_row(
                            checkpoints[checkpoint_index + 1],
                            connected_count,
                            failed_count,
                            format_duration(benchmark_start.elapsed()),
                            format!(
                                "{}/{}",
                                format_num(0),
                                format_num(checkpoints[checkpoint_index + 1]),
                            ),
                            "...".to_string(),
                            format_duration(benchmark_start.elapsed()),
                            benchmark_start_rss_display.as_str(),
                            live_rss_display.as_str(),
                        ),
                        false,
                    );
                    last_live_render = Instant::now();
                }
            }

            let _ = stop_tx.send(true);
            wait_for_task_shutdown(&mut handles).await;

            println!(
                "  └─────────────┴───────────┴──────────┴──────────────┴───────────────┴──────────────┴──────────────┴──────────────┴──────────────┘"
            );
            println!("  Max sustained concurrent connections: {}", format_num(max_achieved));
            println!();

            Ok(())
        })
    }

    fn teardown<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(45);
            loop {
                let count_sql = format!(
                    "SELECT COUNT(*) AS c FROM system.live WHERE namespace_id = '{}' AND table_name = 'conn_scale'",
                    config.namespace
                );

                let active = match client.sql(&count_sql).await {
                    Ok(resp) => extract_first_count(&resp).unwrap_or(0),
                    Err(_) => break,
                };

                if active == 0 {
                    break;
                }

                if tokio::time::Instant::now() >= drain_deadline {
                    println!(
                        "  ⚠ teardown: {} live queries still active; skipping table drop to avoid late subscribe race",
                        active
                    );
                    return Ok(());
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            let _ = client
                .sql(&format!("DROP SHARED TABLE IF EXISTS {}.conn_scale", config.namespace))
                .await;
            Ok(())
        })
    }
}

fn extract_first_count(resp: &crate::client::SqlResponse) -> Option<u64> {
    let result = resp.results.first()?;
    let rows = result.rows.as_ref()?;
    let first_row = rows.first()?;
    let first_cell = first_row.first()?;

    match first_cell {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn format_num(n: u32) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

fn format_duration(d: Duration) -> String {
    let ms = d.as_millis();
    if ms < 1_000 {
        format!("{}ms", ms)
    } else if ms < 60_000 {
        format!("{:.1}s", ms as f64 / 1_000.0)
    } else {
        format!("{:.1}m", ms as f64 / 60_000.0)
    }
}

fn format_memory(memory_bytes: Option<u64>) -> String {
    let Some(bytes) = memory_bytes else {
        return "n/a".to_string();
    };

    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / 1024.0 / 1024.0)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn render_memory_delta_info(
    start_memory: Option<u64>,
    end_memory: Option<u64>,
    connected_count: u32,
) -> Option<String> {
    let (Some(start), Some(end)) = (start_memory, end_memory) else {
        return None;
    };

    let delta = end.saturating_sub(start);
    if connected_count == 0 {
        return Some(format!("ΔRSS={} | approx/conn=n/a", format_memory(Some(delta))));
    }

    let per_connection = delta / connected_count as u64;
    Some(format!(
        "ΔRSS={} | approx/conn={}",
        format_memory(Some(delta)),
        format_memory(Some(per_connection))
    ))
}

fn summarize_bind_pool(bind_addresses: &[String]) -> String {
    if bind_addresses.len() <= 6 {
        return bind_addresses.join(", ");
    }

    format!(
        "{}, {}, {} ... {}, {}, {}",
        bind_addresses[0],
        bind_addresses[1],
        bind_addresses[2],
        bind_addresses[bind_addresses.len() - 3],
        bind_addresses[bind_addresses.len() - 2],
        bind_addresses[bind_addresses.len() - 1],
    )
}

fn render_connection_scale_row(
    tier_target: u32,
    connected_count: u32,
    failed_count: u32,
    connect_time: String,
    delivered: String,
    delivery_time: String,
    loop_time: String,
    start_rss: &str,
    end_rss: &str,
) -> String {
    format!(
        "  │ {:>11} │ {:>9} │ {:>8} │ {:>12} │ {:>13} │ {:>12} │ {:>12} │ {:>12} │ {:>12} │",
        format_num(tier_target),
        format_num(connected_count),
        format_num(failed_count),
        connect_time,
        delivered,
        delivery_time,
        loop_time,
        start_rss,
        end_rss,
    )
}

fn render_live_tier_row(row: &str, finalize: bool) {
    print!("\r\x1b[2K{}", row);
    if finalize {
        println!();
    }
    let _ = io::stdout().flush();
}

async fn wait_for_task_shutdown(handles: &mut Vec<tokio::task::JoinHandle<()>>) {
    let shutdown_deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
    loop {
        if handles.iter().all(tokio::task::JoinHandle::is_finished) {
            break;
        }
        if tokio::time::Instant::now() >= shutdown_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for handle in handles.iter() {
        if !handle.is_finished() {
            handle.abort();
        }
    }

    for handle in handles.drain(..) {
        let _ = handle.await;
    }
}

fn build_connection_checkpoints(max_connections: u32) -> Vec<u32> {
    if max_connections == 0 {
        return Vec::new();
    }

    let mut checkpoints = Vec::new();
    if max_connections <= FIRST_CONNECTION_CHECKPOINT {
        checkpoints.push(max_connections);
        return checkpoints;
    }

    checkpoints.push(FIRST_CONNECTION_CHECKPOINT);

    let mut checkpoint = CONNECTION_CHECKPOINT_STEP;
    while checkpoint < max_connections {
        checkpoints.push(checkpoint);
        checkpoint += CONNECTION_CHECKPOINT_STEP;
    }

    if checkpoints.last().copied() != Some(max_connections) {
        checkpoints.push(max_connections);
    }

    checkpoints
}

fn refresh_live_rss(
    managed_server_memory: &mut Option<ManagedServerMemoryTracker>,
    last_memory_refresh: &mut Instant,
    live_rss_display: &mut String,
) {
    if last_memory_refresh.elapsed() < LIVE_MEMORY_REFRESH_INTERVAL {
        return;
    }

    if let Some(current_rss) = managed_server_memory
        .as_mut()
        .and_then(ManagedServerMemoryTracker::sample_rss_bytes)
    {
        *live_rss_display = format_memory(Some(current_rss));
    }
    *last_memory_refresh = Instant::now();
}

fn delivery_window_for_tier(tier_target: u32) -> Duration {
    match tier_target {
        0..=1_000 => Duration::from_secs(DEFAULT_DELIVERY_WINDOW_SECS),
        1_001..=10_000 => Duration::from_secs(5),
        10_001..=50_000 => Duration::from_secs(8),
        50_001..=100_000 => Duration::from_secs(20),
        100_001..=150_000 => Duration::from_secs(30),
        150_001..=200_000 => Duration::from_secs(45),
        200_001..=250_000 => Duration::from_secs(60),
        _ => Duration::from_secs(75),
    }
}

fn probe_settle_duration_for_tier(tier_target: u32) -> Duration {
    match tier_target {
        0..=10_000 => Duration::from_millis(100),
        10_001..=50_000 => Duration::from_millis(500),
        50_001..=100_000 => Duration::from_secs(5),
        100_001..=200_000 => Duration::from_secs(8),
        _ => Duration::from_secs(10),
    }
}

fn delivery_tolerance_ratio() -> f64 {
    std::env::var("KALAMDB_BENCH_CONNECTION_SCALE_DELIVERY_TOLERANCE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| (0.0..=1.0).contains(value))
        .unwrap_or(DEFAULT_DELIVERY_TOLERANCE)
}

fn connect_batch_limit() -> usize {
    std::env::var("KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONNECT_BATCH)
}

fn connect_wave_size_limit() -> usize {
    std::env::var("KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_WAVE_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONNECT_WAVE_SIZE)
}

fn connect_wave_pause_duration() -> Duration {
    Duration::from_millis(
        std::env::var("KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_WAVE_PAUSE_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CONNECT_WAVE_PAUSE_MS),
    )
}

fn connect_timeout_duration() -> Duration {
    Duration::from_secs(
        std::env::var("KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS),
    )
}

fn resolve_ws_targets(config: &Config) -> Vec<String> {
    let mut targets = Vec::new();

    for raw in &config.urls {
        if let Some(target) = normalize_ws_endpoint(raw) {
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
    }

    targets
}

fn normalize_ws_endpoint(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let normalized = trimmed.trim_end_matches('/');
    let mut url = if normalized.starts_with("ws://") || normalized.starts_with("wss://") {
        normalized.to_string()
    } else if normalized.starts_with("http://") || normalized.starts_with("https://") {
        normalized.replace("http://", "ws://").replace("https://", "wss://")
    } else {
        format!("ws://{}", normalized)
    };

    let authority_start = url.find("://").map(|idx| idx + 3).unwrap_or(0);
    let has_path = url[authority_start..].contains('/');
    if !has_path {
        url.push_str("/v1/ws");
    }

    Some(url)
}

fn detected_single_target_ws_limit() -> Option<usize> {
    let bind_count = configured_ws_local_bind_address_count().max(1);

    #[cfg(target_os = "macos")]
    {
        macos_high_ephemeral_port_capacity().map(|limit| limit.saturating_mul(bind_count))
    }

    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
fn macos_high_ephemeral_port_capacity() -> Option<usize> {
    let first = read_sysctl_usize("net.inet.ip.portrange.hifirst")?;
    let last = read_sysctl_usize("net.inet.ip.portrange.hilast")?;
    (last >= first).then_some(last - first + 1)
}

#[cfg(target_os = "macos")]
fn read_sysctl_usize(name: &str) -> Option<usize> {
    let output = Command::new("sysctl").args(["-n", name]).output().ok()?;
    if !output.status.success() {
        return None;
    }

    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<usize>()
        .ok()
}

fn configured_ws_local_bind_address_count() -> usize {
    std::env::var("KALAMDB_BENCH_WS_LOCAL_BIND_ADDRESSES")
        .ok()
        .map(|raw| raw.split(',').filter(|entry| !entry.trim().is_empty()).count())
        .unwrap_or(0)
}

async fn validate_ws_targets(
    client: &KalamClient,
    namespace: &str,
    targets: Vec<String>,
) -> Result<Vec<String>, String> {
    if targets.is_empty() {
        return Err("No valid WebSocket targets were resolved.".to_string());
    }

    let mut failures = Vec::new();
    let probe_sql = format!("SELECT * FROM {}.conn_scale", namespace);
    let probe_seed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();

    for (idx, target) in targets.iter().enumerate() {
        let mut cfg = SubscriptionConfig::without_initial_data(
            format!("conn_scale_probe_{}_{}", probe_seed, idx),
            probe_sql.clone(),
        );
        cfg.ws_url = Some(target.clone());

        let probe = client.new_isolated_link_for_ws_url(Some(target))?;
        let result = tokio::time::timeout(TARGET_VALIDATE_TIMEOUT, probe.subscribe_with_config(cfg)).await;
        match result {
            Ok(Ok(mut subscription)) => {
                let _ = subscription.close().await;
                probe.disconnect().await;
            }
            Ok(Err(err)) => {
                probe.disconnect().await;
                failures.push(format!("{} -> {}", target, err));
            }
            Err(_) => {
                probe.disconnect().await;
                failures.push(format!("{} -> timeout after {}s", target, TARGET_VALIDATE_TIMEOUT.as_secs()));
            }
        }
    }

    if failures.is_empty() {
        Ok(targets)
    } else {
        let mut message = format!(
            "Failed WebSocket validation for {}/{} configured target(s).",
            failures.len(),
            failures.len() + targets.len() - failures.len(),
        );
        for failure in failures {
            message.push_str("\n  - ");
            message.push_str(&failure);
        }
        Err(message)
    }
}

struct ManagedServerMemoryTracker {
    pid: Pid,
    system: System,
}

impl ManagedServerMemoryTracker {
    fn from_env() -> Option<Self> {
        if std::env::var("KALAMDB_BENCH_MANAGED_SERVER").ok().as_deref() != Some("1") {
            return None;
        }

        let pid = std::env::var("KALAMDB_BENCH_SERVER_PID")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())?;

        Some(Self {
            pid: Pid::from_u32(pid),
            system: System::new_with_specifics(
                RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
            ),
        })
    }

    fn sample_rss_bytes(&mut self) -> Option<u64> {
        let process_refresh = ProcessRefreshKind::nothing().with_memory();
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            false,
            process_refresh,
        );
        self.system.process(self.pid).map(|process| process.memory())
    }
}