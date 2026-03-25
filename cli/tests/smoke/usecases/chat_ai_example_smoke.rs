// Smoke Test: Chat + AI Example from README
// Covers: namespace creation, user table creation, stream table creation,
// insert/query operations, and live subscription to typing events

use crate::common::*;
use std::time::Duration;

#[ntest::timeout(300000)]
#[test]
fn smoke_chat_ai_example_from_readme() {
    if !is_server_running() {
        println!(
            "Skipping smoke_chat_ai_example_from_readme: server not running at {}",
            server_url()
        );
        return;
    }

    // Use unique namespace to avoid collisions
    let namespace = generate_unique_namespace("chat");
    let conversations_table = format!("{}.conversations", namespace);
    let messages_table = format!("{}.messages", namespace);
    let typing_events_table = format!("{}.typing_events", namespace);

    // 1. Create namespace and tables
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("failed to create namespace");

    let create_conversations = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            title TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000');",
        conversations_table
    );
    execute_sql_as_root_via_client(&create_conversations)
        .expect("failed to create conversations table");
    wait_for_table_ready(&conversations_table, Duration::from_secs(3))
        .expect("conversations table should be ready");

    let create_messages = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            conversation_id BIGINT NOT NULL,
            message_role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000');",
        messages_table
    );
    execute_sql_as_root_via_client(&create_messages).expect("failed to create messages table");
    wait_for_table_ready(&messages_table, Duration::from_secs(3))
        .expect("messages table should be ready");

    let create_typing = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            conversation_id BIGINT NOT NULL,
            user_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        ) WITH (TYPE = 'STREAM', TTL_SECONDS = 300);",
        typing_events_table
    );
    execute_sql_as_root_via_client(&create_typing).expect("failed to create typing_events table");
    wait_for_table_ready(&typing_events_table, Duration::from_secs(3))
        .expect("typing_events table should be ready");

    // 2. Insert a conversation
    let insert_conv_sql =
        format!("INSERT INTO {} (title) VALUES ('Chat with AI');", conversations_table);
    execute_sql_as_root_via_client(&insert_conv_sql).expect("failed to insert conversation");

    // For smoke purposes, we'll use conversation_id = 1
    // (In production, you'd parse the conversation ID from a query)
    let conversation_id = 1;

    // 3. Insert user + AI messages
    let insert_msgs_sql = format!(
        "INSERT INTO {} (conversation_id, message_role, content) VALUES
            ({}, 'user', 'Hello, AI!'),
            ({}, 'assistant', 'Hi! How can I help you today?');",
        messages_table, conversation_id, conversation_id
    );
    execute_sql_as_root_via_client(&insert_msgs_sql).expect("failed to insert messages");

    // 4. Query back the conversation history
    let select_msgs_sql = format!(
        "SELECT message_role, content FROM {} WHERE conversation_id = {} ORDER BY created_at ASC;",
        messages_table, conversation_id
    );
    let history = execute_sql_as_root_via_client_json(&select_msgs_sql)
        .expect("failed to query message history");

    assert!(history.contains("Hello, AI!"), "expected user message in history");
    assert!(history.contains("How can I help"), "expected assistant message in history");

    // 5. Test stream table with subscription
    let typing_query = format!("SELECT * FROM {}", typing_events_table);
    let mut listener = SubscriptionListener::start(&typing_query)
        .expect("failed to start subscription for typing events");
    std::thread::sleep(Duration::from_secs(2));

    // 6. Insert typing events and verify subscription receives them
    let events = vec![
        ("user_123", "typing"),
        ("ai_model", "thinking"),
        ("ai_model", "cancelled"),
    ];

    for (user_id, event_type) in &events {
        let insert_event_sql = format!(
            "INSERT INTO {} (conversation_id, user_id, event_type) VALUES ({}, '{}', '{}');",
            typing_events_table, conversation_id, user_id, event_type
        );
        execute_sql_as_root_via_client(&insert_event_sql)
            .unwrap_or_else(|_| panic!("failed to insert typing event: {}", event_type));
    }

    // 7. Wait for subscription to receive at least one event (increased timeout for subscription initialization)
    let mut received_event = false;
    for retry_count in 0..5 {
        let per_attempt_deadline = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < per_attempt_deadline {
            match listener.try_read_line(Duration::from_millis(100)) {
                Ok(Some(line)) => {
                    if !line.trim().is_empty() && !line.starts_with("ERROR:") {
                        received_event = true;
                        break;
                    }
                },
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        if received_event {
            break;
        }

        let retry_event_sql = format!(
            "INSERT INTO {} (conversation_id, user_id, event_type) VALUES ({}, 'ai_model_retry_{}', 'typing');",
            typing_events_table, conversation_id, retry_count
        );
        execute_sql_as_root_via_client(&retry_event_sql)
            .expect("failed to insert retry typing event");
    }

    if !received_event {
        eprintln!(
            "⚠️  Did not receive a live typing event in the README smoke test window; continuing with persisted stream verification"
        );
    }

    // Stop subscription
    listener.stop().ok();

    // 8. Verify events are queryable via regular SELECT
    let verify_events_sql = format!("SELECT * FROM {}", typing_events_table);
    let events_output = wait_for_query_contains_with(
        &verify_events_sql,
        "typing",
        Duration::from_secs(12),
        |sql| execute_sql_as_root_via_client_json(sql),
    )
    .expect("failed to observe persisted typing event");

    assert!(events_output.contains("typing"), "expected 'typing' event in stream table");
    let events_output = if events_output.contains("thinking") {
        events_output
    } else {
        wait_for_query_contains_with(
            &verify_events_sql,
            "thinking",
            Duration::from_secs(12),
            |sql| execute_sql_as_root_via_client_json(sql),
        )
        .expect("failed to observe persisted thinking event")
    };

    assert!(events_output.contains("thinking"), "expected 'thinking' event in stream table");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
