//! Chat-shaped smoke tests for EXECUTE AS USER subject isolation and service delegation.

use std::time::Duration;

use crate::common::*;

struct ChatFixture {
    namespace:      String,
    messages_table: String,
    service_user:   String,
    regular_user:   String,
    other_user:     String,
    password:       String,
}

impl ChatFixture {
    fn cleanup(&self) {
        let _ =
            execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", self.service_user));
        let _ =
            execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", self.regular_user));
        let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", self.other_user));
        let _ = execute_sql_as_root_via_client(&format!(
            "DROP TABLE IF EXISTS {}",
            self.messages_table
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
                    std::thread::sleep(Duration::from_millis(200 * (attempt + 1) as u64));
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

fn setup_chat_fixture(suffix: &str) -> ChatFixture {
    let namespace = generate_unique_namespace(&format!("smoke_chat_as_user_{}", suffix));
    let messages_table = format!("{}.messages", namespace);
    let service_user = generate_unique_namespace("chat_service");
    let regular_user = generate_unique_namespace("chat_regular");
    let other_user = generate_unique_namespace("chat_other");
    let password = "test_pass_123".to_string();

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, conversation_id BIGINT, sender TEXT, role TEXT, \
         content TEXT, status TEXT) WITH (TYPE='USER')",
        messages_table
    ))
    .expect("Failed to create messages table");

    create_user_with_retry(&service_user, &password, "service");
    create_user_with_retry(&regular_user, &password, "user");
    create_user_with_retry(&other_user, &password, "user");

    ChatFixture {
        namespace,
        messages_table,
        service_user,
        regular_user,
        other_user,
        password,
    }
}

fn seed_regular_message(fixture: &ChatFixture) {
    execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!(
            "INSERT INTO {} (id, conversation_id, sender, role, content, status) VALUES (2001, \
             1001, '{}', 'user', 'Hello from regular user', 'sent')",
            fixture.messages_table, fixture.regular_user
        ),
    )
    .expect("Regular user should insert own chat message");
}

#[ntest::timeout(180000)]
#[test]
fn smoke_as_user_chat_service_can_insert_for_regular_user() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_as_user_chat_service_can_insert_for_regular_user: server not running"
        );
        return;
    }

    let fixture = setup_chat_fixture("insert_allowed");
    execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, conversation_id, sender, role, content, \
             status) VALUES (2002, 1001, 'AI Assistant', 'assistant', 'allowed', 'sent'))",
            fixture.regular_user, fixture.messages_table
        ),
    )
    .expect("Service should insert for regular user through EXECUTE AS USER");

    let regular_view = execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!("SELECT content FROM {}", fixture.messages_table),
    )
    .expect("Regular user select should succeed");
    assert!(
        regular_view.contains("allowed"),
        "Assistant row was not inserted for regular user: {}",
        regular_view
    );

    let service_direct = execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!("SELECT content FROM {}", fixture.messages_table),
    )
    .expect("Service direct select should succeed");
    assert!(
        !service_direct.contains("allowed"),
        "Service direct select leaked delegated row: {}",
        service_direct
    );
    fixture.cleanup();
}

#[ntest::timeout(180000)]
#[test]
fn smoke_as_user_chat_service_can_select_update_and_delete_regular_rows() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_as_user_chat_service_can_select_update_and_delete_regular_rows: \
             server not running"
        );
        return;
    }

    let fixture = setup_chat_fixture("dml_allowed");
    seed_regular_message(&fixture);

    let selected = execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (SELECT content FROM {} WHERE conversation_id = 1001)",
            fixture.regular_user, fixture.messages_table
        ),
    )
    .expect("Service should select regular user rows through EXECUTE AS USER");
    assert!(selected.contains("Hello from regular user"));

    execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (UPDATE {} SET content = 'changed' WHERE id = 2001)",
            fixture.regular_user, fixture.messages_table
        ),
    )
    .expect("Service should update regular user rows through EXECUTE AS USER");

    let regular_after_update = execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!("SELECT content FROM {} WHERE id = 2001", fixture.messages_table),
    )
    .expect("Regular user select should succeed");
    assert!(regular_after_update.contains("changed"));

    let service_direct = execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!("SELECT content FROM {}", fixture.messages_table),
    )
    .expect("Service direct select should succeed");
    assert!(!service_direct.contains("changed"));

    execute_sql_via_client_as(
        &fixture.service_user,
        &fixture.password,
        &format!(
            "EXECUTE AS USER '{}' (DELETE FROM {} WHERE id = 2001)",
            fixture.regular_user, fixture.messages_table
        ),
    )
    .expect("Service should delete regular user rows through EXECUTE AS USER");

    let regular_view = execute_sql_via_client_as(
        &fixture.regular_user,
        &fixture.password,
        &format!("SELECT content FROM {} WHERE id = 2001", fixture.messages_table),
    )
    .expect("Regular user select should succeed");
    assert!(!regular_view.contains("changed"));
    assert!(!regular_view.contains("Hello from regular user"));

    let other_view = execute_sql_via_client_as(
        &fixture.other_user,
        &fixture.password,
        &format!("SELECT content FROM {}", fixture.messages_table),
    )
    .expect("Other user select should succeed");
    assert!(!other_view.contains("Hello from regular user"));
    fixture.cleanup();
}
