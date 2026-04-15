//! High-volume endurance test for a chat-style workload.
//!
//! The default profile runs for one hour with 100 concurrent users.
//! Override with env vars for shorter verification runs:
//! - `KALAMDB_ENDURANCE_DURATION_SECS`
//! - `KALAMDB_ENDURANCE_USER_COUNT`
//! - `KALAMDB_ENDURANCE_SUBSCRIBER_COUNT`
//! - `KALAMDB_ENDURANCE_ROOM_COUNT`

#[path = "common/testserver/mod.rs"]
mod test_support;

use anyhow::{Context, Result};
use kalam_client::models::ChangeEvent;
use kalam_client::{KalamLinkClient, SubscriptionManager};
use kalamdb_commons::Role;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::VecDeque;
use std::env;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use test_support::consolidated_helpers::{create_user_and_client, unique_namespace, unique_table};
use test_support::http_server::get_global_server;

const DEFAULT_DURATION_SECS: u64 = 60 * 60;
const DEFAULT_USER_COUNT: usize = 100;
const DEFAULT_SUBSCRIBER_COUNT: usize = 24;
const DEFAULT_ROOM_COUNT: usize = 12;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn sql_quote(value: &str) -> String {
    value.replace('\'', "''")
}

async fn execute_write(client: &KalamLinkClient, sql: &str, errors: &AtomicU64) -> Result<bool> {
    let response = client.execute_query(sql, None, None, None).await?;
    if response.success() {
        return Ok(true);
    }

    errors.fetch_add(1, Ordering::Relaxed);
    Ok(false)
}

async fn wait_for_ack(subscription: &mut SubscriptionManager, deadline: Instant) -> Result<()> {
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Ack { .. }))) => return Ok(()),
            Ok(Some(Ok(ChangeEvent::InitialDataBatch { .. }))) => continue,
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(error))) => anyhow::bail!("subscription failed before ack: {error:?}"),
            Ok(None) => anyhow::bail!("subscription stream ended before ack"),
            Err(_) => continue,
        }
    }

    anyhow::bail!("timed out waiting for subscription ack")
}

async fn run_subscriber(
    client: KalamLinkClient,
    namespace: String,
    conversation_id: i64,
    stop_at: Instant,
    received_events: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) -> Result<()> {
    let mut subscription = client
        .subscribe(&format!(
            "SELECT id, conversation_id, role_id, content FROM {}.messages WHERE conversation_id = {}",
            namespace, conversation_id
        ))
        .await
        .with_context(|| format!("failed to subscribe to conversation {}", conversation_id))?;

    wait_for_ack(&mut subscription, Instant::now() + Duration::from_secs(20)).await?;

    while Instant::now() < stop_at {
        match tokio::time::timeout(Duration::from_secs(2), subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Insert { rows, .. }))) => {
                received_events.fetch_add(rows.len() as u64, Ordering::Relaxed);
            },
            Ok(Some(Ok(ChangeEvent::Update { rows, .. }))) => {
                received_events.fetch_add(rows.len() as u64, Ordering::Relaxed);
            },
            Ok(Some(Ok(ChangeEvent::Delete { old_rows, .. }))) => {
                received_events.fetch_add(old_rows.len() as u64, Ordering::Relaxed);
            },
            Ok(Some(Ok(ChangeEvent::InitialDataBatch { rows, .. }))) => {
                received_events.fetch_add(rows.len() as u64, Ordering::Relaxed);
            },
            Ok(Some(Ok(ChangeEvent::Ack { .. }))) => {},
            Ok(Some(Ok(_))) => {},
            Ok(Some(Err(error))) => {
                errors.fetch_add(1, Ordering::Relaxed);
                anyhow::bail!("subscription stream error: {error:?}");
            },
            Ok(None) => break,
            Err(_) => {},
        }
    }

    subscription.close().await?;
    Ok(())
}

async fn run_chat_user(
    client: KalamLinkClient,
    namespace: String,
    username: String,
    user_index: usize,
    conversation_count: usize,
    stop_at: Instant,
    next_message_id: Arc<AtomicI64>,
    inserts: Arc<AtomicU64>,
    updates: Arc<AtomicU64>,
    deletes: Arc<AtomicU64>,
    queries: Arc<AtomicU64>,
    typing_events: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) -> Result<()> {
    let mut rng = StdRng::seed_from_u64(0xC0FFEE_u64 + user_index as u64);
    let mut own_messages = VecDeque::new();
    let mut op_counter = 0u64;

    while Instant::now() < stop_at {
        op_counter += 1;
        let conversation_id = rng.random_range(1..=(conversation_count as i64));
        let operation = rng.random_range(0..100);

        if operation < 55 {
            let message_id = next_message_id.fetch_add(1, Ordering::Relaxed);
            let body = format!(
                "user={} conversation={} seq={} op={}",
                username, conversation_id, message_id, op_counter
            );
            let sql = format!(
                "INSERT INTO {}.messages (id, conversation_id, role_id, content, created_at_ms, edited_at_ms) VALUES ({}, {}, 'user', '{}', {}, 0)",
                namespace,
                message_id,
                conversation_id,
                sql_quote(&body),
                chrono::Utc::now().timestamp_millis()
            );

            if execute_write(&client, &sql, &errors).await? {
                inserts.fetch_add(1, Ordering::Relaxed);
                own_messages.push_back(message_id);
                if own_messages.len() > 32 {
                    own_messages.pop_front();
                }
            }
        } else if operation < 72 {
            if let Some(message_id) = own_messages.back().copied() {
                let sql = format!(
                    "UPDATE {}.messages SET content = 'edited:{}:{}', edited_at_ms = {} WHERE id = {}",
                    namespace,
                    sql_quote(&username),
                    op_counter,
                    chrono::Utc::now().timestamp_millis(),
                    message_id
                );

                if execute_write(&client, &sql, &errors).await? {
                    updates.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else if operation < 82 {
            if let Some(message_id) = own_messages.pop_front() {
                let sql = format!("DELETE FROM {}.messages WHERE id = {}", namespace, message_id);
                if execute_write(&client, &sql, &errors).await? {
                    deletes.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else if operation < 92 {
            let event_id = next_message_id.fetch_add(1, Ordering::Relaxed);
            let sql = format!(
                "INSERT INTO {}.typing_events (id, conversation_id, event_type, created_at_ms) VALUES ({}, {}, 'typing', {})",
                namespace,
                event_id,
                conversation_id,
                chrono::Utc::now().timestamp_millis()
            );

            if execute_write(&client, &sql, &errors).await? {
                typing_events.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM {}.messages WHERE conversation_id = {}",
                namespace, conversation_id
            );
            let response = client.execute_query(&sql, None, None, None).await?;
            if response.success() {
                queries.fetch_add(1, Ordering::Relaxed);
            } else {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        if op_counter % 20 == 0 {
            sleep(Duration::from_millis(rng.random_range(5..25))).await;
        }
    }

    Ok(())
}

#[tokio::test]
#[ignore = "Long-running endurance scenario; run explicitly with cargo nextest --run-ignored"]
#[ntest::timeout(5400000)]
async fn test_chat_app_endurance_with_100_parallel_users() -> Result<()> {
    let server = get_global_server().await;
    let namespace = unique_namespace("endurance_chat");

    let duration =
        Duration::from_secs(env_u64("KALAMDB_ENDURANCE_DURATION_SECS", DEFAULT_DURATION_SECS));
    let user_count = env_usize("KALAMDB_ENDURANCE_USER_COUNT", DEFAULT_USER_COUNT);
    let subscriber_count =
        env_usize("KALAMDB_ENDURANCE_SUBSCRIBER_COUNT", DEFAULT_SUBSCRIBER_COUNT.min(user_count))
            .min(user_count);
    let conversation_count = env_usize("KALAMDB_ENDURANCE_ROOM_COUNT", DEFAULT_ROOM_COUNT);

    let create_namespace = server.execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await?;
    assert!(
        create_namespace.success(),
        "CREATE NAMESPACE failed: {:?}",
        create_namespace.error
    );

    let create_conversations = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.conversations (
                id BIGINT PRIMARY KEY,
                title TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL
            ) WITH (TYPE = 'USER', STORAGE_ID = 'local')"#,
            namespace
        ))
        .await?;
    assert!(
        create_conversations.success(),
        "CREATE conversations failed: {:?}",
        create_conversations.error
    );

    let create_messages = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.messages (
                id BIGINT PRIMARY KEY,
                conversation_id BIGINT NOT NULL,
                role_id TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                edited_at_ms BIGINT NOT NULL
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local',
                FLUSH_POLICY = 'rows:500'
            )"#,
            namespace
        ))
        .await?;
    assert!(create_messages.success(), "CREATE messages failed: {:?}", create_messages.error);

    let create_typing_events = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.typing_events (
                id BIGINT PRIMARY KEY,
                conversation_id BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL
            ) WITH (TYPE = 'STREAM', TTL_SECONDS = 3600)"#,
            namespace
        ))
        .await?;
    assert!(
        create_typing_events.success(),
        "CREATE typing_events failed: {:?}",
        create_typing_events.error
    );

    let inserts = Arc::new(AtomicU64::new(0));
    let updates = Arc::new(AtomicU64::new(0));
    let deletes = Arc::new(AtomicU64::new(0));
    let queries = Arc::new(AtomicU64::new(0));
    let typing_events = Arc::new(AtomicU64::new(0));
    let subscriber_events = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let health_checks = Arc::new(AtomicU64::new(0));
    let next_message_id = Arc::new(AtomicI64::new(10_000));

    let mut user_clients = Vec::with_capacity(user_count);
    let user_prefix = unique_table("endurance_user");

    for user_index in 0..user_count {
        let username = format!("{}_{}", user_prefix, user_index);
        let client = create_user_and_client(server, &username, &Role::User)
            .await
            .with_context(|| format!("failed to create user {}", username))?;
        user_clients.push((username, client));
    }

    for (_, client) in &user_clients {
        for conversation_id in 1..=conversation_count {
            let sql = format!(
                "INSERT INTO {}.conversations (id, title, created_at_ms) VALUES ({}, 'Conversation {}', {})",
                namespace,
                conversation_id,
                conversation_id,
                chrono::Utc::now().timestamp_millis()
            );
            let response = client.execute_query(&sql, None, None, None).await?;
            assert!(
                response.success(),
                "seed conversation {} failed: {:?}",
                conversation_id,
                response.error
            );
        }
    }

    let stop_at = Instant::now() + duration;
    let health_user = user_clients
        .first()
        .map(|(username, _)| username.clone())
        .context("expected at least one endurance user")?;

    let assistant_namespace = namespace.clone();
    let assistant_users =
        user_clients.iter().map(|(username, _)| username.clone()).collect::<Vec<_>>();
    let assistant_errors = Arc::clone(&errors);
    let assistant_message_ids = Arc::clone(&next_message_id);
    let assistant_task = tokio::spawn(async move {
        let mut rng = StdRng::seed_from_u64(0xA55157_u64);
        while Instant::now() < stop_at {
            let user_slot = rng.random_range(0..assistant_users.len());
            let target_user = &assistant_users[user_slot];
            let conversation_id = rng.random_range(1..=(conversation_count as i64));
            let message_id = assistant_message_ids.fetch_add(1, Ordering::Relaxed);
            let sql = format!(
                "EXECUTE AS USER '{}' (INSERT INTO {}.messages (id, conversation_id, role_id, content, created_at_ms, edited_at_ms) VALUES ({}, {}, 'assistant', 'assistant-reply:{}:{}', {}, 0))",
                sql_quote(target_user),
                assistant_namespace,
                message_id,
                conversation_id,
                sql_quote(target_user),
                message_id,
                chrono::Utc::now().timestamp_millis()
            );
            match server.execute_sql(&sql).await {
                Ok(response) if response.success() => {},
                Ok(_) | Err(_) => {
                    assistant_errors.fetch_add(1, Ordering::Relaxed);
                },
            }
            sleep(Duration::from_millis(30)).await;
        }
        Result::<()>::Ok(())
    });

    let health_namespace = namespace.clone();
    let health_checks_clone = Arc::clone(&health_checks);
    let health_errors = Arc::clone(&errors);
    let health_task = tokio::spawn(async move {
        while Instant::now() < stop_at {
            let response = server
                .execute_sql(&format!(
                    "EXECUTE AS USER '{}' (SELECT COUNT(*) AS cnt FROM {}.messages)",
                    sql_quote(&health_user),
                    health_namespace
                ))
                .await;
            match response {
                Ok(query_response) if query_response.success() => {
                    health_checks_clone.fetch_add(1, Ordering::Relaxed);
                },
                Ok(_) | Err(_) => {
                    health_errors.fetch_add(1, Ordering::Relaxed);
                },
            }
            sleep(Duration::from_secs(5)).await;
        }
        Result::<()>::Ok(())
    });

    let flush_namespace = namespace.clone();
    let flush_errors = Arc::clone(&errors);
    let flush_task = tokio::spawn(async move {
        while Instant::now() < stop_at {
            sleep(Duration::from_secs(30)).await;
            if Instant::now() >= stop_at {
                break;
            }

            let response = server
                .execute_sql(&format!("STORAGE FLUSH TABLE {}.messages", flush_namespace))
                .await;
            match response {
                Ok(query_response) if query_response.success() => {},
                Ok(query_response) => {
                    let transient_conflict = query_response
                        .error
                        .as_ref()
                        .map(|error| {
                            error.message.contains("conflict")
                                || error.message.contains("Idempotent")
                        })
                        .unwrap_or(false);
                    if !transient_conflict {
                        flush_errors.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(_) => {
                    flush_errors.fetch_add(1, Ordering::Relaxed);
                },
            }
        }
        Result::<()>::Ok(())
    });

    let mut tasks = Vec::with_capacity(user_count + subscriber_count);

    for (user_index, (username, client)) in user_clients.iter().cloned().enumerate() {
        tasks.push(tokio::spawn(run_chat_user(
            client,
            namespace.clone(),
            username,
            user_index,
            conversation_count,
            stop_at,
            Arc::clone(&next_message_id),
            Arc::clone(&inserts),
            Arc::clone(&updates),
            Arc::clone(&deletes),
            Arc::clone(&queries),
            Arc::clone(&typing_events),
            Arc::clone(&errors),
        )));
    }

    for subscriber_index in 0..subscriber_count {
        let (_, client) = user_clients[subscriber_index].clone();
        let conversation_id = (subscriber_index % conversation_count) as i64 + 1;
        tasks.push(tokio::spawn(run_subscriber(
            client,
            namespace.clone(),
            conversation_id,
            stop_at,
            Arc::clone(&subscriber_events),
            Arc::clone(&errors),
        )));
    }

    for task in tasks {
        task.await.context("user or subscriber task panicked")??;
    }

    health_task.await.context("health task panicked")??;
    flush_task.await.context("flush task panicked")??;
    assistant_task.await.context("assistant task panicked")??;

    let final_health = server
        .execute_sql(&format!(
            "EXECUTE AS USER '{}' (SELECT COUNT(*) AS cnt FROM {}.messages)",
            sql_quote(&user_clients[0].0),
            namespace
        ))
        .await?;
    assert!(final_health.success(), "final health query failed: {:?}", final_health.error);

    let duplicate_check = server
        .execute_sql(&format!(
            "EXECUTE AS USER '{}' (SELECT COUNT(*) AS total_rows, COUNT(DISTINCT id) AS distinct_rows FROM {}.messages)",
            sql_quote(&user_clients[0].0),
            namespace
        ))
        .await?;
    assert!(duplicate_check.success(), "duplicate check failed: {:?}", duplicate_check.error);

    let totals = duplicate_check.rows_as_maps();
    let total_rows = totals
        .first()
        .and_then(|row| row.get("total_rows"))
        .and_then(|value| value.as_i64())
        .unwrap_or_default();
    let distinct_rows = totals
        .first()
        .and_then(|row| row.get("distinct_rows"))
        .and_then(|value| value.as_i64())
        .unwrap_or_default();

    let insert_count = inserts.load(Ordering::Relaxed);
    let update_count = updates.load(Ordering::Relaxed);
    let delete_count = deletes.load(Ordering::Relaxed);
    let typing_event_count = typing_events.load(Ordering::Relaxed);
    let query_count = queries.load(Ordering::Relaxed);
    let subscriber_event_count = subscriber_events.load(Ordering::Relaxed);
    let health_check_count = health_checks.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);

    eprintln!(
        "Endurance workload complete: duration_secs={} users={} subscribers={} conversations={} inserts={} updates={} deletes={} typing_events={} queries={} subscriber_events={} health_checks={} final_rows={}",
        duration.as_secs(),
        user_count,
        subscriber_count,
        conversation_count,
        insert_count,
        update_count,
        delete_count,
        typing_event_count,
        query_count,
        subscriber_event_count,
        health_check_count,
        total_rows,
    );

    assert_eq!(total_rows, distinct_rows, "message primary keys must stay unique");
    assert!(
        insert_count >= user_count as u64,
        "expected at least one successful insert per user, got inserts={} users={} errors={}",
        insert_count,
        user_count,
        error_count
    );
    assert!(
        query_count > 0,
        "expected successful read traffic during endurance run, got queries={} errors={}",
        query_count,
        error_count
    );
    assert!(
        health_check_count > 0,
        "server health checks never succeeded, errors={}",
        error_count
    );
    assert!(
        subscriber_event_count > 0,
        "live subscriptions never observed any events, inserts={} updates={} deletes={}",
        insert_count,
        update_count,
        delete_count
    );
    assert_eq!(error_count, 0, "endurance workload recorded backend errors");

    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", namespace)).await;
    Ok(())
}
