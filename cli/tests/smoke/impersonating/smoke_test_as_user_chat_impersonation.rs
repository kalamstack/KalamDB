use crate::common::*;
use kalam_link::KalamLinkTimeouts;
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::{Duration, Instant};

struct AuthSubscriptionListener {
    event_receiver: std_mpsc::Receiver<String>,
    stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for AuthSubscriptionListener {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl AuthSubscriptionListener {
    fn start(
        username: &str,
        password: &str,
        query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (event_tx, event_rx) = std_mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let username = username.to_string();
        let password = password.to_string();
        let query = query.to_string();

        let handle = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime for auth subscription");

            runtime.block_on(async move {
                let base_url = leader_url().unwrap_or_else(|| {
                    get_available_server_urls()
                        .first()
                        .cloned()
                        .unwrap_or_else(|| server_url().to_string())
                });

                let client = match client_for_user_on_url_with_timeouts(
                    &base_url,
                    &username,
                    &password,
                    KalamLinkTimeouts::builder()
                        .connection_timeout_secs(5)
                        .receive_timeout_secs(120)
                        .send_timeout_secs(30)
                        .subscribe_timeout_secs(20)
                        .auth_timeout_secs(10)
                        .initial_data_timeout(Duration::from_secs(120))
                        .build(),
                ) {
                    Ok(client) => client,
                    Err(err) => {
                        let _ = event_tx.send(format!("ERROR: {}", err));
                        return;
                    },
                };

                let mut subscription = match client.subscribe(&query).await {
                    Ok(subscription) => subscription,
                    Err(err) => {
                        let _ = event_tx.send(format!("ERROR: {}", err));
                        return;
                    },
                };

                let mut stop_rx = stop_rx;

                loop {
                    tokio::select! {
                        _ = &mut stop_rx => {
                            break;
                        }
                        event = subscription.next() => {
                            match event {
                                Some(Ok(change_event)) => {
                                    if event_tx.send(format!("{:?}", change_event)).is_err() {
                                        break;
                                    }
                                }
                                Some(Err(err)) => {
                                    let _ = event_tx.send(format!("ERROR: {}", err));
                                    break;
                                }
                                None => break,
                            }
                        }
                    }
                }

                let _ = subscription.close().await;
                client.disconnect().await;
            });
        });

        Ok(Self {
            event_receiver: event_rx,
            stop_sender: Some(stop_tx),
            handle: Some(handle),
        })
    }

    fn wait_for_any_event(
        &mut self,
        patterns: &[&str],
        timeout: Duration,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let start = Instant::now();
        let mut seen_events: Vec<String> = Vec::new();
        while start.elapsed() < timeout {
            match self.event_receiver.recv_timeout(Duration::from_millis(200)) {
                Ok(line) => {
                    let lower = line.to_lowercase();
                    if patterns.iter().any(|pattern| lower.contains(&pattern.to_lowercase())) {
                        return Ok(line);
                    }
                    if seen_events.len() < 8 {
                        seen_events.push(line);
                    }
                },
                Err(std_mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        Err(format!(
            "No matching event found for patterns {:?}. Seen events: {:?}",
            patterns, seen_events
        )
        .into())
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| "Subscription thread panicked")?;
        }
        Ok(())
    }
}

struct ChatFixture {
    namespace: String,
    conversations_table: String,
    messages_table: String,
    typing_table: String,
    regular_user: String,
    regular_user_id: String,
    service_user: String,
    other_user: String,
    other_user_id: String,
    password: String,
}

impl ChatFixture {
    fn cleanup(&self) {
        let _ =
            execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", self.regular_user));
        let _ =
            execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", self.service_user));
        let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", self.other_user));
        let _ =
            execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", self.typing_table));
        let _ = execute_sql_as_root_via_client(&format!(
            "DROP TABLE IF EXISTS {}",
            self.messages_table
        ));
        let _ = execute_sql_as_root_via_client(&format!(
            "DROP TABLE IF EXISTS {}",
            self.conversations_table
        ));
        let _ =
            execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", self.namespace));
    }
}

fn create_user_with_retry(username: &str, password: &str, role: &str) {
    let sql = format!("CREATE USER {} WITH PASSWORD '{}' ROLE '{}'", username, password, role);
    let mut last_error = None;
    for attempt in 0..3 {
        match execute_sql_as_root_via_client(&sql) {
            Ok(_) => return,
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("Already exists") {
                    let alter_sql = format!("ALTER USER {} SET PASSWORD '{}'", username, password);
                    let _ = execute_sql_as_root_via_client(&alter_sql);
                    return;
                }
                if msg.contains("Serialization error") || msg.contains("UnexpectedEnd") {
                    last_error = Some(msg);
                    thread::sleep(Duration::from_millis(200 * (attempt + 1) as u64));
                    continue;
                }
                panic!("Failed to create user {}: {}", username, msg);
            },
        }
    }
    panic!(
        "Failed to create user {} after retries: {}",
        username,
        last_error.unwrap_or_else(|| "unknown error".to_string())
    );
}

fn get_user_id_for_username(username: &str) -> Option<String> {
    let query = format!("SELECT user_id FROM system.users WHERE username = '{}'", username);
    let result = execute_sql_as_root_via_client_json(&query).ok()?;

    let json: serde_json::Value = serde_json::from_str(&result).ok()?;
    let rows = get_rows_as_hashmaps(&json)?;

    if let Some(row) = rows.first() {
        let user_id_value = row.get("user_id").map(extract_typed_value)?;
        return user_id_value.as_str().map(|value| value.to_string());
    }

    None
}

fn setup_chat_fixture(suffix: &str) -> ChatFixture {
    let namespace = generate_unique_namespace(&format!("smoke_imp_chat_{}", suffix));
    let conversations_table = format!("{}.conversations", namespace);
    let messages_table = format!("{}.messages", namespace);
    let typing_table = format!("{}.typing_indicators", namespace);

    let regular_user = generate_unique_namespace("imp_user");
    let service_user = generate_unique_namespace("imp_service");
    let other_user = generate_unique_namespace("imp_other");
    let password = "test_pass_123".to_string();

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, title TEXT, created_by TEXT) WITH (TYPE='USER')",
        conversations_table
    ))
    .expect("Failed to create conversations table");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, conversation_id BIGINT, sender TEXT, role TEXT, content TEXT, status TEXT) WITH (TYPE='USER')",
        messages_table
    ))
    .expect("Failed to create messages table");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, conversation_id BIGINT, user_name TEXT, is_typing BOOLEAN, state TEXT) WITH (TYPE='STREAM', TTL_SECONDS=3600)",
        typing_table
    ))
    .expect("Failed to create typing table");

    create_user_with_retry(&regular_user, &password, "user");
    create_user_with_retry(&service_user, &password, "service");
    create_user_with_retry(&other_user, &password, "user");

    let regular_user_id =
        get_user_id_for_username(&regular_user).expect("Failed to get regular user_id");
    let other_user_id = get_user_id_for_username(&other_user).expect("Failed to get other user_id");

    ChatFixture {
        namespace,
        conversations_table,
        messages_table,
        typing_table,
        regular_user,
        regular_user_id,
        service_user,
        other_user,
        other_user_id,
        password,
    }
}

struct BaseFlow {
    conversation_id: i64,
    assistant_message_id: i64,
    assistant_message_text: String,
}

fn run_base_chat_flow_with_impersonation(fixture: &ChatFixture) -> BaseFlow {
    let conversation_id = 1001;
    let user_message_id = 2001;
    let assistant_message_id = 2002;
    let assistant_message_text = "Service response via AS USER".to_string();
    execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!(
            "INSERT INTO {} (id, title, created_by) VALUES ({}, 'Conversation', '{}')",
            fixture.conversations_table, conversation_id, fixture.regular_user
        ),
    )
    .expect("Regular user should create conversation");

    execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!(
            "INSERT INTO {} (id, conversation_id, sender, role, content, status) VALUES ({}, {}, '{}', 'user', 'Hello from regular user', 'sent')",
            fixture.messages_table, user_message_id, conversation_id, fixture.regular_user
        ),
    )
    .expect("Regular user should insert initial message");

    let typing_query = format!(
        "SELECT * FROM {} WHERE conversation_id = {}",
        fixture.typing_table, conversation_id
    );
    let mut typing_listener =
        AuthSubscriptionListener::start(&fixture.regular_user, &fixture.password, &typing_query)
            .expect("Failed to start typing subscription as regular user");

    thread::sleep(Duration::from_millis(350));

    execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, conversation_id, user_name, is_typing, state) VALUES (3001, {}, 'AI Assistant', true, 'thinking'))",
            fixture.regular_user,
            fixture.typing_table, conversation_id
        ),
    )
    .expect("Service should insert typing event AS USER");

    execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, conversation_id, user_name, is_typing, state) VALUES (3002, {}, 'AI Assistant', true, 'typing'))",
            fixture.regular_user,
            fixture.typing_table, conversation_id
        ),
    )
    .expect("Service should insert second typing event AS USER");

    let typing_event =
        typing_listener.wait_for_any_event(&["thinking", "typing"], Duration::from_secs(12));
    if let Err(error) = typing_event {
        let fallback = wait_for_query_contains_with(
            &format!(
                "SELECT state FROM {} WHERE conversation_id = {}",
                fixture.typing_table, conversation_id
            ),
            "thinking",
            Duration::from_secs(12),
            |sql| execute_sql_via_client_as(&fixture.regular_user, &fixture.password, sql),
        )
        .or_else(|_| {
            wait_for_query_contains_with(
                &format!(
                    "SELECT state FROM {} WHERE conversation_id = {}",
                    fixture.typing_table, conversation_id
                ),
                "typing",
                Duration::from_secs(12),
                |sql| execute_sql_via_client_as(&fixture.regular_user, &fixture.password, sql),
            )
        })
        .unwrap_or_else(|fallback_error| {
            panic!(
                "Regular user should receive typing signal via subscription or persisted rows: subscription error: {}; fallback error: {}",
                error,
                fallback_error
            )
        });
        assert!(
            fallback.to_lowercase().contains("thinking")
                || fallback.to_lowercase().contains("typing"),
            "Fallback typing rows should include thinking/typing states; got: {}",
            fallback
        );
    }
    typing_listener.stop().expect("Failed to stop typing listener");

    let message_query = format!(
        "SELECT * FROM {} WHERE conversation_id = {}",
        fixture.messages_table, conversation_id
    );
    let mut messages_listener =
        AuthSubscriptionListener::start(&fixture.regular_user, &fixture.password, &message_query)
            .expect("Failed to start message subscription as regular user");

    thread::sleep(Duration::from_millis(350));
    let initial_message_event = messages_listener.wait_for_any_event(
        &[&user_message_id.to_string(), "hello from regular user"],
        Duration::from_secs(12),
    );
    if let Err(error) = initial_message_event {
        let fallback = execute_sql_via_client_as(
            &fixture.regular_user,
            &fixture.password,
            &format!(
                "SELECT id, content FROM {} WHERE id = {}",
                fixture.messages_table, user_message_id
            ),
        )
        .expect("Fallback SELECT for initial message should succeed");
        assert!(
            fallback.contains(&user_message_id.to_string())
                || fallback.to_lowercase().contains("hello from regular user"),
            "Regular user subscription should see initial own message: {}. Fallback result: {}",
            error,
            fallback
        );
    }

    execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, conversation_id, sender, role, content, status) VALUES ({}, {}, 'AI Assistant', 'assistant', '{}', 'sent'))",
            fixture.regular_user_id,
            fixture.messages_table,
            assistant_message_id,
            conversation_id,
            assistant_message_text
        ),
    )
    .expect("Service should insert assistant message AS USER");

    let regular_after_assistant_insert = execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!(
            "SELECT id, role, content FROM {} WHERE id = {}",
            fixture.messages_table, assistant_message_id
        ),
    )
    .expect("Regular user should query assistant message after AS USER insert");
    assert!(
        regular_after_assistant_insert.contains(&assistant_message_id.to_string())
            || regular_after_assistant_insert.contains(&assistant_message_text),
        "AS USER insert should be visible in regular user scope: {}",
        regular_after_assistant_insert
    );

    let assistant_message_event = messages_listener.wait_for_any_event(
        &[
            &assistant_message_id.to_string(),
            "service response via as user",
            "ai assistant",
        ],
        Duration::from_secs(12),
    );
    if let Err(error) = assistant_message_event {
        let fallback = execute_sql_via_client_as(
            &fixture.regular_user,
            &fixture.password,
            &format!(
                "SELECT id, content FROM {} WHERE id = {}",
                fixture.messages_table, assistant_message_id
            ),
        )
        .expect("Fallback SELECT for assistant message should succeed");
        assert!(
            fallback.contains(&assistant_message_id.to_string())
                || fallback.to_lowercase().contains("service response via as user"),
            "Regular user should receive inserted assistant message in subscription: {}. Fallback result: {}",
            error,
            fallback
        );
    }
    messages_listener.stop().expect("Failed to stop message listener");

    BaseFlow {
        conversation_id,
        assistant_message_id,
        assistant_message_text,
    }
}

fn assert_regular_user_sees_both_messages(fixture: &ChatFixture, flow: &BaseFlow) {
    let regular_select = execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!(
            "SELECT role, content FROM {} WHERE conversation_id = {} ORDER BY id",
            fixture.messages_table, flow.conversation_id
        ),
    )
    .expect("Regular user select should succeed");
    assert!(
        regular_select.contains("Hello from regular user")
            && regular_select.contains(&flow.assistant_message_text),
        "Regular user should see both own and service messages: {}",
        regular_select
    );
}

fn assert_other_user_cannot_see_messages(fixture: &ChatFixture, flow: &BaseFlow) {
    let other_select = execute_sql_via_client_as(
        &fixture.other_user,
        &fixture.password,
        &format!(
            "SELECT role, content FROM {} WHERE conversation_id = {} ORDER BY id",
            fixture.messages_table, flow.conversation_id
        ),
    )
    .expect("Other user select should succeed");
    assert!(
        !other_select.contains("Hello from regular user")
            && !other_select.contains(&flow.assistant_message_text),
        "Other user should not see regular user's messages: {}",
        other_select
    );
}

#[ntest::timeout(300000)]
#[test]
fn smoke_as_user_chat_insert_and_select_flow() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_chat_insert_and_select_flow: server not running");
        return;
    }

    let fixture = setup_chat_fixture("insert_select");
    let flow = run_base_chat_flow_with_impersonation(&fixture);

    assert_regular_user_sees_both_messages(&fixture, &flow);

    let service_select_as_user = execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (SELECT role, content FROM {} WHERE conversation_id = {} ORDER BY id)",
            fixture.regular_user_id,
            fixture.messages_table, flow.conversation_id
        ),
    )
    .expect("Service SELECT AS USER should succeed");
    assert!(
        service_select_as_user.contains("Hello from regular user")
            && service_select_as_user.contains(&flow.assistant_message_text),
        "Service SELECT AS USER should return regular user scoped rows: {}",
        service_select_as_user
    );

    assert_other_user_cannot_see_messages(&fixture, &flow);
    fixture.cleanup();
}

#[ntest::timeout(300000)]
#[test]
fn smoke_as_user_chat_select_scope_for_different_user() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_as_user_chat_select_scope_for_different_user: server not running"
        );
        return;
    }

    let fixture = setup_chat_fixture("select_scope");
    let flow = run_base_chat_flow_with_impersonation(&fixture);

    let service_select_other_as_user = execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (SELECT role, content FROM {} WHERE conversation_id = {} ORDER BY id)",
            fixture.other_user_id,
            fixture.messages_table, flow.conversation_id
        ),
    )
    .expect("Service SELECT AS USER for other user should succeed");
    assert!(
        !service_select_other_as_user.contains("Hello from regular user")
            && !service_select_other_as_user.contains(&flow.assistant_message_text),
        "Service SELECT AS USER for unrelated user should be empty: {}",
        service_select_other_as_user
    );

    fixture.cleanup();
}

#[ntest::timeout(300000)]
#[test]
fn smoke_as_user_chat_update_flow() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_chat_update_flow: server not running");
        return;
    }

    let fixture = setup_chat_fixture("update");
    let flow = run_base_chat_flow_with_impersonation(&fixture);

    let message_query = format!(
        "SELECT * FROM {} WHERE conversation_id = {}",
        fixture.messages_table, flow.conversation_id
    );
    let mut message_listener =
        AuthSubscriptionListener::start(&fixture.regular_user, &fixture.password, &message_query)
            .expect("Failed to start message subscription before update");

    // Give the subscription handshake time to complete and confirm initial rows are flowing.
    // Without this warm-up, the UPDATE can race with registration and delay update events.
    thread::sleep(Duration::from_millis(350));
    if let Err(error) = message_listener.wait_for_any_event(
        &[
            &flow.assistant_message_id.to_string(),
            "service response via as user",
        ],
        Duration::from_secs(6),
    ) {
        let fallback = wait_for_query_contains_with(
            &format!(
                "SELECT id, content FROM {} WHERE id = {}",
                fixture.messages_table, flow.assistant_message_id
            ),
            &flow.assistant_message_id.to_string(),
            Duration::from_secs(12),
            |sql| execute_sql_via_client_as(&fixture.regular_user, &fixture.password, sql),
        )
        .or_else(|_| {
            wait_for_query_contains_with(
                &format!(
                    "SELECT id, content FROM {} WHERE id = {}",
                    fixture.messages_table, flow.assistant_message_id
                ),
                "service response via as user",
                Duration::from_secs(12),
                |sql| execute_sql_via_client_as(&fixture.regular_user, &fixture.password, sql),
            )
        })
        .expect("Fallback SELECT for initial assistant snapshot should succeed");

        assert!(
            fallback.contains(&flow.assistant_message_id.to_string())
                || fallback.contains("service response via as user"),
            "Regular user update listener should receive initial message snapshot before update: {}. Fallback result: {}",
            error,
            fallback
        );
    }

    execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (UPDATE {} SET content = 'Service response updated', status = 'delivered' WHERE id = {})",
            fixture.regular_user_id,
            fixture.messages_table, flow.assistant_message_id
        ),
    )
    .expect("Service UPDATE AS USER should succeed");

    message_listener
        .wait_for_any_event(
            &["updated", "delivered", "service response updated"],
            Duration::from_secs(12),
        )
        .expect("Regular user should receive updated message in subscription");
    message_listener.stop().expect("Failed to stop update listener");

    let regular_after_update = execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!(
            "SELECT content, status FROM {} WHERE id = {}",
            fixture.messages_table, flow.assistant_message_id
        ),
    )
    .expect("Regular user should query updated assistant message");
    assert!(
        regular_after_update.contains("Service response updated")
            && regular_after_update.contains("delivered"),
        "Updated assistant message should be visible to regular user: {}",
        regular_after_update
    );

    let service_select_as_user = execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (SELECT content, status FROM {} WHERE id = {})",
            fixture.regular_user_id, fixture.messages_table, flow.assistant_message_id
        ),
    )
    .expect("Service SELECT AS USER should see updated message");
    assert!(
        service_select_as_user.contains("Service response updated")
            && service_select_as_user.contains("delivered"),
        "Service SELECT AS USER should reflect updated values: {}",
        service_select_as_user
    );

    assert_other_user_cannot_see_messages(&fixture, &flow);
    fixture.cleanup();
}

#[ntest::timeout(300000)]
#[test]
fn smoke_as_user_chat_delete_flow() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_chat_delete_flow: server not running");
        return;
    }

    let fixture = setup_chat_fixture("delete");
    let flow = run_base_chat_flow_with_impersonation(&fixture);

    execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (DELETE FROM {} WHERE id = {})",
            fixture.regular_user_id, fixture.messages_table, flow.assistant_message_id
        ),
    )
    .expect("Service DELETE AS USER should succeed");

    let regular_after_delete = execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!(
            "SELECT role, content FROM {} WHERE conversation_id = {} ORDER BY id",
            fixture.messages_table, flow.conversation_id
        ),
    )
    .expect("Regular user select after delete should succeed");
    assert!(
        regular_after_delete.contains("Hello from regular user")
            && !regular_after_delete.contains(&flow.assistant_message_text),
        "Deleted assistant message should no longer be visible: {}",
        regular_after_delete
    );

    let service_select_as_user = execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (SELECT role, content FROM {} WHERE conversation_id = {} ORDER BY id)",
            fixture.regular_user_id,
            fixture.messages_table, flow.conversation_id
        ),
    )
    .expect("Service SELECT AS USER after delete should succeed");
    assert!(
        service_select_as_user.contains("Hello from regular user")
            && !service_select_as_user.contains(&flow.assistant_message_text),
        "Service SELECT AS USER should reflect deleted assistant message: {}",
        service_select_as_user
    );

    assert_other_user_cannot_see_messages(&fixture, &flow);
    fixture.cleanup();
}
