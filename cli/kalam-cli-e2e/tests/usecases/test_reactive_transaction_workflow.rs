//! Reactive application workflow e2e test.
//!
//! This scenario stitches together the pieces real apps use at the same time:
//! committed transaction batches on USER + SHARED tables, ephemeral STREAM writes,
//! live subscribers, rollback visibility, and schema evolution while clients are
//! already subscribed.

use std::time::{Duration, Instant};

use serde_json::Value;

use crate::common::*;

fn execute_http_as_root(sql: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(execute_sql_via_http_as_root(sql))
}

fn assert_success(response: &Value, context: &str) {
    let status = response.get("status").and_then(|value| value.as_str()).unwrap_or("error");
    assert!(status.eq_ignore_ascii_case("success"), "{context} failed: {response}");
}

fn assert_error_contains(response: &Value, expected: &str, context: &str) {
    let status = response.get("status").and_then(|value| value.as_str()).unwrap_or("success");
    let message = response
        .get("error")
        .and_then(|error| error.get("message").or(Some(error)))
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| response.to_string());

    assert!(
        status.eq_ignore_ascii_case("error"),
        "{context} should fail, got success response: {response}"
    );
    assert!(
        message.to_lowercase().contains(&expected.to_lowercase()),
        "{context} should mention '{expected}', got: {message}"
    );
}

fn assert_error_contains_any(response: &Value, expected: &[&str], context: &str) {
    let status = response.get("status").and_then(|value| value.as_str()).unwrap_or("success");
    let message = response
        .get("error")
        .and_then(|error| error.get("message").or(Some(error)))
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| response.to_string());

    assert!(
        status.eq_ignore_ascii_case("error"),
        "{context} should fail, got success response: {response}"
    );
    assert!(
        expected
            .iter()
            .any(|expected| message.to_lowercase().contains(&expected.to_lowercase())),
        "{context} should mention one of {:?}, got: {message}",
        expected
    );
}

fn wait_for_subscription_value(
    listener: &mut SubscriptionListener,
    event_kind: &str,
    token: &str,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut seen = Vec::new();

    while start.elapsed() < timeout {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                if line.contains(event_kind) && line.contains(token) {
                    return Ok(line);
                }
                seen.push(line);
            },
            Ok(None) => break,
            Err(_) => {},
        }
    }

    Err(format!(
        "did not receive {event_kind} containing '{token}' within {timeout:?}; seen: {}",
        seen.join("\n")
    )
    .into())
}

fn json_count_is_zero(output: &str) -> bool {
    let Ok(parsed) = parse_json_from_cli_output(output) else {
        return false;
    };
    let Some(rows) = get_rows_as_hashmaps(&parsed) else {
        return false;
    };

    rows.first()
        .and_then(|row| row.get("cnt"))
        .map(extract_typed_value)
        .and_then(|value| match value {
            Value::Number(number) => number.as_i64(),
            Value::String(value) => value.parse::<i64>().ok(),
            _ => None,
        })
        == Some(0)
}

#[ntest::timeout(180000)]
#[test]
fn test_reactive_transactions_schema_and_stream_workflow() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("reactive_workflow");
    let conversations = format!("{}.conversations", namespace);
    let messages = format!("{}.messages", namespace);
    let typing_events = format!("{}.typing_events", namespace);
    let conversation_id = 7001_i64;
    let event_timeout = if is_cluster_mode() {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(15)
    };

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            title TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at_ms BIGINT NOT NULL
        ) WITH (TYPE='SHARED')"#,
        conversations
    ))
    .expect("create shared conversations table");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT NOT NULL,
            actor TEXT NOT NULL,
            content TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:25')"#,
        messages
    ))
    .expect("create user messages table");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            conversation_id BIGINT NOT NULL,
            actor TEXT NOT NULL,
            event_type TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        ) WITH (TYPE='STREAM', TTL_SECONDS=120)"#,
        typing_events
    ))
    .expect("create stream typing table");

    let message_query = format!(
        "SELECT id, content, status FROM {} WHERE conversation_id = {}",
        messages, conversation_id
    );
    let conversation_query =
        format!("SELECT id, title, state FROM {} WHERE id = {}", conversations, conversation_id);
    let typing_query = format!(
        "SELECT id, actor, event_type FROM {} WHERE conversation_id = {}",
        typing_events, conversation_id
    );

    let mut message_listener =
        SubscriptionListener::start(&message_query).expect("message subscription should start");
    let mut conversation_listener = SubscriptionListener::start(&conversation_query)
        .expect("conversation subscription should start");
    let mut typing_listener =
        SubscriptionListener::start(&typing_query).expect("typing subscription should start");

    message_listener
        .wait_for_event("Ack", event_timeout)
        .expect("message subscription should be acknowledged before writes");
    conversation_listener
        .wait_for_event("Ack", event_timeout)
        .expect("conversation subscription should be acknowledged before writes");
    typing_listener
        .wait_for_event("Ack", event_timeout)
        .expect("typing subscription should be acknowledged before writes");

    let cross_group_tx = execute_http_as_root(&format!(
        "BEGIN; INSERT INTO {} (id, title, state, updated_at_ms) VALUES ({}, 'Launch plan', \
         'open', 1); INSERT INTO {} (id, conversation_id, actor, content, status, created_at_ms) \
         VALUES (1, {}, 'user', 'Draft the launch checklist', 'draft', 1); COMMIT;",
        conversations, conversation_id, messages, conversation_id
    ))
    .expect("cross-group transaction request should return");
    assert_error_contains_any(
        &cross_group_tx,
        &["cannot access table", "data raft group"],
        "cross USER+SHARED explicit transaction",
    );

    let shared_insert_tx = execute_http_as_root(&format!(
        "BEGIN; INSERT INTO {} (id, title, state, updated_at_ms) VALUES ({}, 'Launch plan', \
         'open', 1); COMMIT;",
        conversations, conversation_id
    ))
    .expect("shared insert transaction request should return");
    assert_success(&shared_insert_tx, "shared insert transaction");

    let user_insert_tx = execute_http_as_root(&format!(
        "BEGIN; INSERT INTO {} (id, conversation_id, actor, content, status, created_at_ms) \
         VALUES (1, {}, 'user', 'Draft the launch checklist', 'draft', 1); COMMIT;",
        messages, conversation_id
    ))
    .expect("user insert transaction request should return");
    assert_success(&user_insert_tx, "user insert transaction");

    wait_for_subscription_value(
        &mut message_listener,
        "Insert",
        "Draft the launch checklist",
        event_timeout,
    )
    .expect("message subscriber should receive committed row");
    wait_for_subscription_value(&mut conversation_listener, "Insert", "Launch plan", event_timeout)
        .expect("shared subscriber should receive committed row");

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, conversation_id, actor, event_type, created_at_ms) VALUES (10, {}, \
         'assistant', 'typing', 2)",
        typing_events, conversation_id
    ))
    .expect("stream write outside explicit transaction should succeed");
    wait_for_subscription_value(&mut typing_listener, "Insert", "typing", event_timeout)
        .expect("stream subscriber should receive ephemeral event");

    let message_update_tx = execute_http_as_root(&format!(
        "BEGIN; UPDATE {} SET status = 'sent', content = 'Launch checklist committed' WHERE id = \
         1; COMMIT;",
        messages
    ))
    .expect("message update transaction request should return");
    assert_success(&message_update_tx, "message update transaction");

    let conversation_update_tx = execute_http_as_root(&format!(
        "BEGIN; UPDATE {} SET state = 'active', updated_at_ms = 3 WHERE id = {}; COMMIT;",
        conversations, conversation_id
    ))
    .expect("conversation update transaction request should return");
    assert_success(&conversation_update_tx, "conversation update transaction");

    wait_for_subscription_value(
        &mut message_listener,
        "Update",
        "Launch checklist committed",
        event_timeout,
    )
    .expect("message subscriber should receive committed update");
    wait_for_subscription_value(&mut conversation_listener, "Update", "active", event_timeout)
        .expect("shared subscriber should receive committed update");

    execute_sql_as_root_via_client(&format!("ALTER TABLE {} ADD COLUMN sentiment TEXT", messages))
        .expect("alter active message table should succeed");
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, conversation_id, actor, content, status, sentiment, created_at_ms) \
         VALUES (2, {}, 'assistant', 'Checklist is ready', 'sent', 'positive', 4)",
        messages, conversation_id
    ))
    .expect("insert using evolved schema should succeed");
    wait_for_subscription_value(
        &mut message_listener,
        "Insert",
        "Checklist is ready",
        event_timeout,
    )
    .expect("message subscription should survive schema evolution");

    let message_rollback_tx = execute_http_as_root(&format!(
        "BEGIN; INSERT INTO {} (id, conversation_id, actor, content, status, sentiment, \
         created_at_ms) VALUES (3, {}, 'assistant', 'This row rolls back', 'draft', 'negative', \
         5); ROLLBACK;",
        messages, conversation_id
    ))
    .expect("message rollback transaction request should return");
    assert_success(&message_rollback_tx, "message rollback transaction block");

    let shared_rollback_tx = execute_http_as_root(&format!(
        "BEGIN; UPDATE {} SET state = 'rolled_back', updated_at_ms = 5 WHERE id = {}; ROLLBACK;",
        conversations, conversation_id
    ))
    .expect("shared rollback transaction request should return");
    assert_success(&shared_rollback_tx, "shared rollback transaction block");

    let final_messages = execute_sql_as_root_via_client_json(&format!(
        "SELECT id, content, status, sentiment FROM {} WHERE conversation_id = {} ORDER BY id",
        messages, conversation_id
    ))
    .expect("final messages select should succeed");
    assert!(
        final_messages.contains("Launch checklist committed")
            && final_messages.contains("Checklist is ready")
            && final_messages.contains("positive"),
        "committed and evolved rows should be visible: {}",
        final_messages
    );
    assert!(
        !final_messages.contains("This row rolls back"),
        "rolled back message should not be visible: {}",
        final_messages
    );

    let final_conversation = execute_sql_as_root_via_client_json(&format!(
        "SELECT state FROM {} WHERE id = {}",
        conversations, conversation_id
    ))
    .expect("final conversation select should succeed");
    assert!(
        final_conversation.contains("active") && !final_conversation.contains("rolled_back"),
        "rolled back shared update should not be visible: {}",
        final_conversation
    );

    let stream_tx = execute_http_as_root(&format!(
        "BEGIN; INSERT INTO {} (id, conversation_id, actor, event_type, created_at_ms) VALUES \
         (11, {}, 'assistant', 'committed_in_tx', 6); COMMIT;",
        typing_events, conversation_id
    ))
    .expect("stream transaction request should return");
    assert_error_contains(
        &stream_tx,
        "stream tables are not supported inside explicit transactions",
        "stream write inside explicit transaction",
    );

    let stream_rows = execute_sql_as_root_via_client_json(&format!(
        "SELECT event_type FROM {} WHERE conversation_id = {}",
        typing_events, conversation_id
    ))
    .expect("stream select should succeed");
    assert!(
        stream_rows.contains("typing"),
        "committed stream event missing: {}",
        stream_rows
    );
    assert!(
        !stream_rows.contains("committed_in_tx"),
        "failed stream transaction should not leave a visible event: {}",
        stream_rows
    );

    let active_transactions = execute_sql_as_root_via_client_json(
        "SELECT COUNT(*) AS cnt FROM system.transactions WHERE origin = 'SqlBatch'",
    )
    .expect("system.transactions should be readable by root");
    assert!(
        json_count_is_zero(&active_transactions),
        "explicit transaction cleanup should leave no active SqlBatch transaction: {}",
        active_transactions
    );

    message_listener.stop().ok();
    conversation_listener.stop().ok();
    typing_listener.stop().ok();
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
