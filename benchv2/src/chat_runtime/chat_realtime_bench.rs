use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
};

use tokio::{sync::Barrier, time::sleep};

use crate::{
    benchmarks::Benchmark,
    chat_runtime::{
        ai::{ai_worker_start_delay, run_ai_worker, AiInboxAgent},
        common::{
            ai_inbox_topic_name, build_chat_users, chat_stability_error, format_chat_errors,
            minimum_active_chat_user_count, prewarm_user_clients, print_chat_summary,
            run_sql_with_retry, sql_literal, target_active_chat_user_count, wait_for_topic_ready,
            ChatManagedServerMemoryProbe, ChatWorkloadSettings, ChatWorkloadStats, UserClientPool,
            CHAT_USER_PASSWORD,
        },
        shared::{run_shared_worker, shared_worker_start_delay},
    },
    client::KalamClient,
    config::Config,
    metrics::BenchmarkDetail,
};

pub struct ChatRealtimeBench;

impl Benchmark for ChatRealtimeBench {
    fn name(&self) -> &str {
        "chat_realtime"
    }

    fn category(&self) -> &str {
        "Load"
    }

    fn description(&self) -> &str {
        "Timed Masky-style chat mix: USER-table AI conversations plus RLS-protected shared rooms"
    }

    fn report_description(&self, _config: &Config) -> String {
        let settings = ChatWorkloadSettings::for_report();
        format!(
            "Realtime chat scenario for {}m with {} regular users, {} conversations ({}), and {}",
            settings.minutes,
            settings.user_count,
            settings.realtime_conversations,
            settings.mix_label(),
            settings.message_rate_label()
        )
    }

    fn report_full_description(&self, _config: &Config) -> String {
        let settings = ChatWorkloadSettings::for_report();
        format!(
            "Creates {} regular KalamDB users, then runs {} concurrent conversations for {} \
             minute(s): {} personal AI chats on USER tables (conversations_ai/messages_ai) with a \
             topic-consuming agent, plus {} 2-user and {} 3-user rooms on SHARED \
             conversations/messages with CREATE POLICY membership RLS. Each conversation paces at \
             {}. Workers {}, load historic messages before a cutoff, disconnect and reconnect \
             with snapshot replay, and reopen a conversation with SELECT.",
            settings.user_count,
            settings.realtime_conversations,
            settings.minutes,
            settings.ai_conversations,
            settings.shared_pair_conversations,
            settings.shared_triple_conversations,
            settings.message_rate_label(),
            settings.mutation_label(),
        )
    }

    fn report_details(&self, _config: &Config) -> Vec<BenchmarkDetail> {
        let settings = ChatWorkloadSettings::for_report();
        vec![
            BenchmarkDetail::new("Runtime", format!("{} minute(s)", settings.minutes)),
            BenchmarkDetail::new("Regular Users", settings.user_count.to_string()),
            BenchmarkDetail::new(
                "Active Conversations",
                settings.realtime_conversations.to_string(),
            ),
            BenchmarkDetail::new("AI Conversations", settings.ai_conversations.to_string()),
            BenchmarkDetail::new(
                "Shared Pair Conversations",
                settings.shared_pair_conversations.to_string(),
            ),
            BenchmarkDetail::new(
                "Shared Triple Conversations",
                settings.shared_triple_conversations.to_string(),
            ),
            BenchmarkDetail::new("Conversation Message Rate", settings.message_rate_label()),
            BenchmarkDetail::new("Message Mutations", settings.mutation_label()),
            BenchmarkDetail::new(
                "Tables",
                "USER conversations_ai/messages_ai, SHARED \
                 conversations/conversation_members/messages with RLS, STREAM typing_events, \
                 TOPIC AI inbox",
            ),
        ]
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
            let settings = ChatWorkloadSettings::from_env()?;
            let usernames = build_chat_users(&config.namespace, settings.user_count);

            client
                .sql_ok(&format!("CREATE NAMESPACE IF NOT EXISTS {}", config.namespace))
                .await?;

            drop_chat_schema(client, &config.namespace).await;

            for username in &usernames {
                let _ = client.sql(&format!("DROP USER IF EXISTS {}", sql_literal(username))).await;
            }

            create_chat_schema(client, &config.namespace).await?;

            for username in &usernames {
                run_sql_with_retry(
                    client,
                    &format!(
                        "CREATE USER {} WITH PASSWORD {} ROLE user",
                        sql_literal(username),
                        sql_literal(CHAT_USER_PASSWORD),
                    ),
                )
                .await?;
            }

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
            let settings = ChatWorkloadSettings::from_env()?;
            let users = Arc::new(build_chat_users(&config.namespace, settings.user_count));
            let target_active_user_count = target_active_chat_user_count(&settings);
            let minimum_active_user_count = minimum_active_chat_user_count(&settings);
            let stats = Arc::new(ChatWorkloadStats::default());
            let user_pool = Arc::new(UserClientPool::new(
                config.urls.clone(),
                CHAT_USER_PASSWORD,
                stats.clone(),
            ));
            let global_stop = Arc::new(AtomicBool::new(false));
            let memory_probe = ChatManagedServerMemoryProbe::start(stats.clone());
            let conversation_ids =
                Arc::new(AtomicU64::new(40_000_000_000 + u64::from(iteration) * 1_000_000));
            let message_ids =
                Arc::new(AtomicU64::new(50_000_000_000 + u64::from(iteration) * 10_000_000));
            let typing_ids =
                Arc::new(AtomicU64::new(60_000_000_000 + u64::from(iteration) * 10_000_000));
            let ai_agent = AiInboxAgent::start(
                client.clone(),
                config.namespace.clone(),
                stats.clone(),
                global_stop.clone(),
                message_ids.clone(),
                typing_ids,
                iteration,
            );

            let prewarmed_active_users = prewarm_user_clients(
                user_pool.clone(),
                users.clone(),
                target_active_user_count,
                minimum_active_user_count,
            )
            .await?;
            let active_users = Arc::new(prewarmed_active_users.usernames);

            if prewarmed_active_users.failed_attempts > 0 {
                println!(
                    "  Active user prewarm: warmed={} target={} failed_login_attempts={}",
                    active_users.len(),
                    target_active_user_count,
                    prewarmed_active_users.failed_attempts,
                );
            }

            let scenario_started = tokio::time::Instant::now();
            let run_deadline = scenario_started + settings.duration();
            let mut handles = Vec::with_capacity(settings.realtime_conversations as usize);
            let shared_worker_count = settings
                .shared_pair_conversations
                .saturating_add(settings.shared_triple_conversations);
            let shared_setup_barrier = (shared_worker_count > 0)
                .then(|| Arc::new(Barrier::new(shared_worker_count as usize)));

            println!(
                "  Chat workload settings: duration={}m, regular_users={}, \
                 target_active_chat_users={}, mix={}, message_rate={}",
                settings.minutes,
                settings.user_count,
                target_active_user_count,
                settings.mix_label(),
                settings.message_rate_label(),
            );

            let mut next_worker_id = 0_u32;
            for _ in 0..settings.ai_conversations {
                let worker_id = next_worker_id;
                next_worker_id += 1;
                let namespace = config.namespace.clone();
                let worker_stats = stats.clone();
                let worker_pool = user_pool.clone();
                let worker_users = active_users.clone();
                let worker_stop = global_stop.clone();
                let worker_conversations = conversation_ids.clone();
                let worker_messages = message_ids.clone();
                let worker_settings = settings;
                let worker_start_delay = ai_worker_start_delay(worker_id);
                let worker_deadline = run_deadline + worker_start_delay;

                handles.push(tokio::spawn(async move {
                    if !worker_start_delay.is_zero() {
                        sleep(worker_start_delay).await;
                    }

                    run_ai_worker(
                        worker_id,
                        namespace,
                        worker_deadline,
                        worker_stats,
                        worker_pool,
                        worker_users,
                        worker_conversations,
                        worker_messages,
                        worker_settings,
                        worker_stop,
                    )
                    .await
                }));
            }

            for member_count in [2_usize, 3_usize] {
                let worker_count = if member_count == 2 {
                    settings.shared_pair_conversations
                } else {
                    settings.shared_triple_conversations
                };

                for _ in 0..worker_count {
                    let worker_id = next_worker_id;
                    next_worker_id += 1;
                    let namespace = config.namespace.clone();
                    let worker_stats = stats.clone();
                    let worker_pool = user_pool.clone();
                    let worker_users = active_users.clone();
                    let worker_stop = global_stop.clone();
                    let worker_conversations = conversation_ids.clone();
                    let worker_messages = message_ids.clone();
                    let worker_settings = settings;
                    let worker_start_delay = shared_worker_start_delay(worker_id);
                    let worker_deadline = run_deadline + worker_start_delay;
                    let worker_setup_barrier = shared_setup_barrier.clone();

                    handles.push(tokio::spawn(async move {
                        if !worker_start_delay.is_zero() {
                            sleep(worker_start_delay).await;
                        }

                        run_shared_worker(
                            worker_id,
                            namespace,
                            member_count,
                            worker_deadline,
                            worker_stats,
                            worker_pool,
                            worker_users,
                            worker_conversations,
                            worker_messages,
                            worker_settings,
                            worker_stop,
                            worker_setup_barrier,
                        )
                        .await
                    }));
                }
            }

            let mut errors = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok(Ok(())) => {},
                    Ok(Err(error)) => errors.push(error),
                    Err(error) => errors.push(format!("worker join error: {}", error)),
                }
            }

            global_stop.store(true, std::sync::atomic::Ordering::Relaxed);

            if let Err(error) = ai_agent.shutdown().await {
                errors.push(error);
            }

            let memory_summary = memory_probe.finish().await;
            print_chat_summary(&stats, settings, scenario_started.elapsed(), &memory_summary);
            if let Some(error) = chat_stability_error(&memory_summary) {
                errors.push(error);
            }

            if errors.is_empty() {
                Ok(())
            } else {
                Err(format_chat_errors(&errors))
            }
        })
    }

    fn teardown<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let settings = ChatWorkloadSettings::from_env()?;
            let users = build_chat_users(&config.namespace, settings.user_count);
            drop_chat_schema(client, &config.namespace).await;
            for username in users {
                let _ =
                    client.sql(&format!("DROP USER IF EXISTS {}", sql_literal(&username))).await;
            }
            Ok(())
        })
    }
}

async fn drop_chat_schema(client: &KalamClient, namespace: &str) {
    let ai_inbox = ai_inbox_topic_name(namespace);
    let _ = client.sql(&format!("DROP TOPIC IF EXISTS {}", ai_inbox)).await;
    let _ = client
        .sql(&format!("DROP TOPIC IF EXISTS {}.chat_conversation_events", namespace))
        .await;
    let _ = client
        .sql(&format!("DROP TOPIC IF EXISTS {}.chat_message_events", namespace))
        .await;
    let _ = client
        .sql(&format!("DROP TOPIC IF EXISTS {}.chat_typing_events", namespace))
        .await;
    let _ = client
        .sql(&format!("DROP STREAM TABLE IF EXISTS {}.typing_events", namespace))
        .await;
    let _ = client.sql(&format!("DROP SHARED TABLE IF EXISTS {}.messages", namespace)).await;
    let _ = client
        .sql(&format!("DROP SHARED TABLE IF EXISTS {}.conversation_members", namespace))
        .await;
    let _ = client
        .sql(&format!("DROP SHARED TABLE IF EXISTS {}.conversations", namespace))
        .await;
    let _ = client
        .sql(&format!("DROP USER TABLE IF EXISTS {}.messages_ai", namespace))
        .await;
    let _ = client
        .sql(&format!("DROP USER TABLE IF EXISTS {}.conversations_ai", namespace))
        .await;
    let _ = client.sql(&format!("DROP USER TABLE IF EXISTS {}.messages", namespace)).await;
    let _ = client
        .sql(&format!("DROP USER TABLE IF EXISTS {}.conversations", namespace))
        .await;
}

async fn create_chat_schema(client: &KalamClient, namespace: &str) -> Result<(), String> {
    run_sql_with_retry(
        client,
        &format!(
            "CREATE USER TABLE IF NOT EXISTS {}.conversations_ai (id BIGINT PRIMARY KEY, title \
             TEXT NOT NULL, state TEXT NOT NULL, created_at_ms BIGINT NOT NULL) WITH \
             (FLUSH_POLICY = 'rows:10000')",
            namespace
        ),
    )
    .await?;

    run_sql_with_retry(
        client,
        &format!(
            "CREATE USER TABLE IF NOT EXISTS {}.messages_ai (id BIGINT PRIMARY KEY, \
             conversation_id BIGINT NOT NULL, role TEXT NOT NULL, sender_user TEXT NOT NULL, body \
             TEXT NOT NULL, created_at_ms BIGINT NOT NULL) WITH (FLUSH_POLICY = 'rows:10000')",
            namespace
        ),
    )
    .await?;

    run_sql_with_retry(
        client,
        &format!(
            "CREATE SHARED TABLE IF NOT EXISTS {}.conversations (id BIGINT PRIMARY KEY, title \
             TEXT NOT NULL, created_by TEXT NOT NULL, created_at_ms BIGINT NOT NULL) WITH \
             (FLUSH_POLICY = 'rows:10000')",
            namespace
        ),
    )
    .await?;

    run_sql_with_retry(
        client,
        &format!(
            "CREATE SHARED TABLE IF NOT EXISTS {}.conversation_members (id TEXT PRIMARY KEY, \
             user_id TEXT NOT NULL, conversation_id BIGINT NOT NULL) WITH (FLUSH_POLICY = \
             'rows:10000')",
            namespace
        ),
    )
    .await?;

    run_sql_with_retry(
        client,
        &format!(
            "CREATE SHARED TABLE IF NOT EXISTS {}.messages (id BIGINT PRIMARY KEY, \
             conversation_id BIGINT NOT NULL, sender_user TEXT NOT NULL, body TEXT NOT NULL, \
             created_at_ms BIGINT NOT NULL) WITH (FLUSH_POLICY = 'rows:10000')",
            namespace
        ),
    )
    .await?;

    run_sql_with_retry(
        client,
        &format!(
            "CREATE STREAM TABLE IF NOT EXISTS {}.typing_events (id BIGINT PRIMARY KEY, \
             conversation_id BIGINT NOT NULL, sender_user TEXT NOT NULL, recipient_user TEXT NOT \
             NULL, phase TEXT NOT NULL, created_at_ms BIGINT NOT NULL) WITH (TTL_SECONDS = 30)",
            namespace
        ),
    )
    .await?;

    run_sql_with_retry(
        client,
        &format!(
            "CREATE INDEX IF NOT EXISTS idx_messages_ai_conversation ON {}.messages_ai \
             (conversation_id)",
            namespace
        ),
    )
    .await?;
    run_sql_with_retry(
        client,
        &format!(
            "CREATE INDEX IF NOT EXISTS idx_messages_conversation ON {}.messages (conversation_id)",
            namespace
        ),
    )
    .await?;
    run_sql_with_retry(
        client,
        &format!(
            "CREATE INDEX IF NOT EXISTS idx_conversation_members_user ON {}.conversation_members \
             (user_id)",
            namespace
        ),
    )
    .await?;

    run_sql_with_retry(
        client,
        &format!(
            "CREATE POLICY conversations_member_select ON {}.conversations FOR SELECT TO user \
             USING (id IN (SELECT conversation_id FROM {}.conversation_members WHERE user_id = \
             CURRENT_USER))",
            namespace, namespace
        ),
    )
    .await?;
    run_sql_with_retry(
        client,
        &format!(
            "CREATE POLICY conversations_create ON {}.conversations FOR INSERT TO user WITH CHECK \
             (true)",
            namespace
        ),
    )
    .await?;
    run_sql_with_retry(
        client,
        &format!(
            "CREATE POLICY conversation_members_self ON {}.conversation_members FOR ALL TO user \
             USING (user_id = CURRENT_USER) WITH CHECK (user_id = CURRENT_USER)",
            namespace
        ),
    )
    .await?;
    run_sql_with_retry(
        client,
        &format!(
            "CREATE POLICY messages_member_select ON {}.messages FOR SELECT TO user USING \
             (conversation_id IN (SELECT conversation_id FROM {}.conversation_members WHERE \
             user_id = CURRENT_USER))",
            namespace, namespace
        ),
    )
    .await?;
    run_sql_with_retry(
        client,
        &format!(
            "CREATE POLICY messages_member_insert ON {}.messages FOR INSERT TO user WITH CHECK \
             (conversation_id IN (SELECT conversation_id FROM {}.conversation_members WHERE \
             user_id = CURRENT_USER))",
            namespace, namespace
        ),
    )
    .await?;
    run_sql_with_retry(
        client,
        &format!(
            "CREATE POLICY messages_member_update ON {}.messages FOR UPDATE TO user USING \
             (conversation_id IN (SELECT conversation_id FROM {}.conversation_members WHERE \
             user_id = CURRENT_USER)) WITH CHECK (conversation_id IN (SELECT conversation_id FROM \
             {}.conversation_members WHERE user_id = CURRENT_USER))",
            namespace, namespace, namespace
        ),
    )
    .await?;
    run_sql_with_retry(
        client,
        &format!(
            "CREATE POLICY messages_member_delete ON {}.messages FOR DELETE TO user USING \
             (conversation_id IN (SELECT conversation_id FROM {}.conversation_members WHERE \
             user_id = CURRENT_USER))",
            namespace, namespace
        ),
    )
    .await?;

    let ai_inbox = ai_inbox_topic_name(namespace);
    run_sql_with_retry(client, &format!("CREATE TOPIC {}", ai_inbox)).await?;
    run_sql_with_retry(
        client,
        &format!("ALTER TOPIC {} ADD SOURCE {}.messages_ai ON INSERT", ai_inbox, namespace),
    )
    .await?;
    wait_for_topic_ready(client, &ai_inbox).await?;
    Ok(())
}
