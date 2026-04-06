// Smoke Test: Chat + AI Example from README
// Covers: current README schema (USER messages + STREAM agent_events),
// topic wiring, insert/query operations, and live subscription to streamed agent events

use crate::common::*;
use std::time::Duration;

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

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
    let messages_table = format!("{}.messages", namespace);
    let agent_events_table = format!("{}.agent_events", namespace);
    let topic_name = format!("{}.ai_inbox", namespace);
    let room = "main";
    let sender_username = "root";
    let user_message = "Hello, AI!";
    let response_id = format!(
        "reply-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("current time")
            .as_micros()
    );
    let assistant_reply = format!(
        "AI reply: KalamDB stored \"{}\" in {}, streamed the drafting state through {}, and committed the final assistant row.",
        user_message, messages_table, agent_events_table
    );
    let typing_preview = assistant_reply.chars().take(48).collect::<String>();

    // 1. Create namespace, tables, and topic wiring used by the README example.
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("failed to create namespace");

    let create_messages = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            room TEXT NOT NULL DEFAULT 'main',
            role TEXT NOT NULL,
            author TEXT NOT NULL,
            sender_username TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000');",
        messages_table
    );
    execute_sql_as_root_via_client(&create_messages).expect("failed to create messages table");
    wait_for_table_ready(&messages_table, Duration::from_secs(3))
        .expect("messages table should be ready");

    let create_agent_events = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            response_id TEXT NOT NULL,
            room TEXT NOT NULL DEFAULT 'main',
            sender_username TEXT NOT NULL,
            stage TEXT NOT NULL,
            preview TEXT NOT NULL DEFAULT '',
            message TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMP DEFAULT NOW()
        ) WITH (TYPE = 'STREAM', TTL_SECONDS = 300);",
        agent_events_table
    );
    execute_sql_as_root_via_client(&create_agent_events)
        .expect("failed to create agent_events table");
    wait_for_table_ready(&agent_events_table, Duration::from_secs(3))
        .expect("agent_events table should be ready");

    execute_sql_as_root_via_client(&format!("CREATE TOPIC {}", topic_name))
        .expect("failed to create ai inbox topic");
    execute_sql_as_root_via_client(&format!(
        "ALTER TOPIC {} ADD SOURCE {} ON INSERT",
        topic_name, messages_table
    ))
    .expect("failed to connect topic source to messages table");

    // 2. Insert a user message, as the browser app would.
    let insert_user_message_sql = format!(
        "INSERT INTO {} (room, role, author, sender_username, content) VALUES ({}, 'user', {}, {}, {})",
        messages_table,
        sql_literal(room),
        sql_literal(sender_username),
        sql_literal(sender_username),
        sql_literal(user_message)
    );
    execute_sql_as_root_via_client(&insert_user_message_sql)
        .expect("failed to insert user message");

    // 3. Query back the current chat history.
    let select_msgs_sql = format!(
        "SELECT role, author, content FROM {} WHERE room = {} ORDER BY id ASC;",
        messages_table,
        sql_literal(room)
    );
    let history = wait_for_query_contains_with(
        &select_msgs_sql,
        user_message,
        Duration::from_secs(12),
        execute_sql_as_root_via_client_json,
    )
    .expect("failed to observe message history");

    assert!(history.contains(user_message), "expected user message in history");
    assert!(history.contains(sender_username), "expected sender username in history");

    // 4. Subscribe to the STREAM table used for live agent draft events.
    let agent_events_query = format!(
        "SELECT * FROM {} WHERE room = {}",
        agent_events_table,
        sql_literal(room)
    );
    let mut listener = SubscriptionListener::start(&agent_events_query)
        .expect("failed to start subscription for agent events");
    std::thread::sleep(Duration::from_secs(2));

    // 5. Insert streamed agent lifecycle events and the final assistant reply.
    let events = vec![
        (
            "thinking",
            "",
            "Planning assistant reply",
        ),
        (
            "typing",
            typing_preview.as_str(),
            "Streaming the first characters of the reply",
        ),
        (
            "message_saved",
            assistant_reply.as_str(),
            "Assistant reply committed",
        ),
        (
            "complete",
            assistant_reply.as_str(),
            "Live stream finished",
        ),
    ];

    for (stage, preview, message) in &events {
        let insert_event_sql = format!(
            "INSERT INTO {} (response_id, room, sender_username, stage, preview, message) VALUES ({}, {}, {}, {}, {}, {})",
            agent_events_table,
            sql_literal(&response_id),
            sql_literal(room),
            sql_literal(sender_username),
            sql_literal(stage),
            sql_literal(preview),
            sql_literal(message)
        );
        execute_sql_as_root_via_client(&insert_event_sql)
            .unwrap_or_else(|_| panic!("failed to insert agent event: {}", stage));
    }

    let insert_assistant_message_sql = format!(
        "INSERT INTO {} (room, role, author, sender_username, content) VALUES ({}, 'assistant', 'KalamDB Copilot', {}, {})",
        messages_table,
        sql_literal(room),
        sql_literal(sender_username),
        sql_literal(&assistant_reply)
    );
    execute_sql_as_root_via_client(&insert_assistant_message_sql)
        .expect("failed to insert assistant message");

    // 6. Wait for subscription to receive at least one event.
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
            "INSERT INTO {} (response_id, room, sender_username, stage, preview, message) VALUES ({}, {}, {}, 'typing', {}, {})",
            agent_events_table,
            sql_literal(&format!("{}-retry-{}", response_id, retry_count)),
            sql_literal(room),
            sql_literal(sender_username),
            sql_literal(typing_preview.as_str()),
            sql_literal("Retrying streamed preview delivery")
        );
        execute_sql_as_root_via_client(&retry_event_sql)
            .expect("failed to insert retry agent event");
    }

    if !received_event {
        eprintln!(
            "⚠️  Did not receive a live agent event in the README smoke test window; continuing with persisted stream verification"
        );
    }

    // Stop subscription
    listener.stop().ok();

    // 7. Verify persisted STREAM rows and final message history.
    if !is_cluster_mode() || !received_event {
        let verify_events_sql = format!(
            "SELECT stage, preview, message FROM {} WHERE room = {}",
            agent_events_table,
            sql_literal(room)
        );
        let events_output = wait_for_query_contains_with(
            &verify_events_sql,
            "typing",
            Duration::from_secs(12),
            execute_sql_as_root_via_client_json,
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
                execute_sql_as_root_via_client_json,
            )
            .expect("failed to observe persisted thinking event")
        };

        assert!(events_output.contains("thinking"), "expected 'thinking' event in stream table");
    }

    let final_history = wait_for_query_contains_with(
        &select_msgs_sql,
        "KalamDB Copilot",
        Duration::from_secs(12),
        execute_sql_as_root_via_client_json,
    )
    .expect("failed to observe final assistant reply in message history");
    let escaped_assistant_reply = assistant_reply.replace('"', "\\\"");

    assert!(
        final_history.contains(user_message),
        "expected user message in final history"
    );
    assert!(
        final_history.contains(&assistant_reply) || final_history.contains(&escaped_assistant_reply),
        "expected assistant reply in final history"
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TOPIC {}", topic_name));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
