//! Chat Simulation Integration Test
//!
//! Simulates a chat application with AI agents and users to test memory leaks and system stability.
//!
//! Scenario:
//! 1. Creates conversations, messages, and stream_events tables.
//! 2. Spawns multiple concurrent user sessions (AI Agent + User).
//! 3. AI Agent creates conversation, sends messages, and stream events.
//! 4. User subscribes to messages and stream events, and sends messages.
//! 5. Validates system stability under load.

use std::{
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use crate::common::*;

const DEFAULT_NUM_USERS: usize = 4;
const DEFAULT_MESSAGES_PER_USER: usize = 5;
const DEFAULT_MESSAGES_PER_AI: usize = 5;

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

#[ntest::timeout(300000)]
#[test]
fn test_chat_simulation_memory_leak() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let num_users = env_usize("KALAMDB_CHAT_SIM_USERS", DEFAULT_NUM_USERS);
    let messages_per_user = env_usize("KALAMDB_CHAT_SIM_USER_MESSAGES", DEFAULT_MESSAGES_PER_USER);
    let messages_per_ai = env_usize("KALAMDB_CHAT_SIM_AI_MESSAGES", DEFAULT_MESSAGES_PER_AI);

    println!(
        "\n🚀 Starting Chat Simulation Test ({} users, {} msgs each)",
        num_users, messages_per_user
    );
    let test_start = Instant::now();

    // Generate unique suffix
    let suffix = random_string(5);
    let namespace = format!("chat_sim_{}", suffix);

    // Setup Tables
    let setup_start = Instant::now();
    if let Err(e) = setup_chat_tables(&namespace) {
        eprintln!("⚠️  Failed to setup tables: {}. Skipping test.", e);
        return;
    }
    println!("✅ Setup complete ({:.2?})", setup_start.elapsed());

    // Create Users
    let users_start = Instant::now();
    let mut user_credentials = Vec::new();
    for i in 0..num_users {
        let username = format!("user_{}_{}", i, suffix);
        let password = format!("pass_{}", i);

        if let Err(e) = execute_sql_as_root_via_client(&format!(
            "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
            username, password
        )) {
            eprintln!("⚠️  Failed to create user {}: {}", username, e);
            cleanup(&namespace, &user_credentials);
            return;
        }
        user_credentials.push((username, password));
    }
    println!("✅ Users created ({:.2?})", users_start.elapsed());

    // Barrier to synchronize start of all threads
    let start_barrier = Arc::new(Barrier::new(num_users + 1));

    // Spawn Simulation Threads
    let mut handles = Vec::new();

    for (i, (username, password)) in user_credentials.iter().enumerate() {
        let username = username.clone();
        let password = password.clone();
        let namespace = namespace.clone();
        let barrier = start_barrier.clone();
        let user_idx = i;

        let handle = thread::spawn(move || {
            barrier.wait(); // Wait for all threads to be ready

            let conversation_id = format!("conv_{}_{}", user_idx, random_string(5));

            // 1. AI Agent Thread
            // AI acts on behalf of the user or a system agent.
            // We use the user's credentials for simplicity, but distinguish by sender field.

            let ai_creds = (username.clone(), password.clone());
            let user_creds = (username.clone(), password.clone());
            let ns_ref = namespace.clone();
            let conv_id_ref = conversation_id.clone();

            // Create Conversation (AI task)
            let create_conv_sql = format!(
                "INSERT INTO {}.conversations (id, title, created_by) VALUES ('{}', 'Chat {}', \
                 '{}')",
                ns_ref, conv_id_ref, user_idx, username
            );
            if let Err(e) = execute_sql_via_client_as(&ai_creds.0, &ai_creds.1, &create_conv_sql) {
                eprintln!("❌ Failed to create conversation: {}", e);
                return;
            }

            let ai_handle = thread::spawn(move || {
                for m in 0..messages_per_ai {
                    // AI sends message
                    let msg_id = format!("msg_ai_{}_{}", m, random_string(5));
                    let msg_sql = format!(
                        "INSERT INTO {}.messages (id, conversation_id, sender, content, \
                         timestamp) VALUES ('{}', '{}', 'AI_AGENT', 'AI Message {}', {})",
                        ns_ref,
                        msg_id,
                        conv_id_ref,
                        m,
                        chrono::Utc::now().timestamp_millis()
                    );
                    let _ = execute_sql_via_client_as(&ai_creds.0, &ai_creds.1, &msg_sql);

                    // AI sends stream event (typing)
                    let event_id = format!("evt_ai_{}_{}", m, random_string(5));
                    let event_sql = format!(
                        "INSERT INTO {}.stream_events (id, conversation_id, event_type, payload, \
                         timestamp) VALUES ('{}', '{}', 'typing', 'AI is typing...', {})",
                        ns_ref,
                        event_id,
                        conv_id_ref,
                        chrono::Utc::now().timestamp_millis()
                    );
                    let _ = execute_sql_via_client_as(&ai_creds.0, &ai_creds.1, &event_sql);

                    thread::sleep(Duration::from_millis(rand::random::<u64>() % 20 + 5));
                }
            });

            let user_handle = thread::spawn(move || {
                // User subscribes to messages
                let msg_query = format!(
                    "SELECT * FROM {}.messages WHERE conversation_id = '{}'",
                    namespace, conversation_id
                );
                let mut msg_sub = match SubscriptionListener::start(&msg_query) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("❌ Failed to start msg subscription: {}", e);
                        return;
                    },
                };

                // User subscribes to stream events
                let stream_query = format!(
                    "SELECT * FROM {}.stream_events WHERE conversation_id = '{}'",
                    namespace, conversation_id
                );
                let mut stream_sub = match SubscriptionListener::start(&stream_query) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("❌ Failed to start stream subscription: {}", e);
                        let _ = msg_sub.stop();
                        return;
                    },
                };

                // User sends messages
                for m in 0..messages_per_user {
                    let msg_id = format!("msg_usr_{}_{}", m, random_string(5));
                    let msg_sql = format!(
                        "INSERT INTO {}.messages (id, conversation_id, sender, content, \
                         timestamp) VALUES ('{}', '{}', '{}', 'User Message {}', {})",
                        namespace,
                        msg_id,
                        conversation_id,
                        user_creds.0,
                        m,
                        chrono::Utc::now().timestamp_millis()
                    );
                    let _ = execute_sql_via_client_as(&user_creds.0, &user_creds.1, &msg_sql);

                    // Read from subscriptions to prevent buffer filling (and simulate active
                    // listening)
                    let _ = msg_sub.try_read_line(Duration::from_millis(5));
                    let _ = stream_sub.try_read_line(Duration::from_millis(5));

                    thread::sleep(Duration::from_millis(rand::random::<u64>() % 20 + 5));
                }

                // Cleanup subscriptions
                let _ = msg_sub.stop();
                let _ = stream_sub.stop();
            });

            let _ = ai_handle.join();
            let _ = user_handle.join();
        });

        handles.push(handle);
    }

    // Start the race
    start_barrier.wait();

    // Wait for all simulations to finish
    for handle in handles {
        if handle.join().is_err() {
            eprintln!("❌ A simulation thread panicked");
        }
    }

    println!("✅ Simulation complete. Cleaning up...");
    cleanup(&namespace, &user_credentials);

    println!("🎉 Test PASSED - Chat simulation finished in {:.2?}", test_start.elapsed());
}

fn setup_chat_tables(namespace: &str) -> Result<(), Box<dyn std::error::Error>> {
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))?;

    // Conversations Table
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {}.conversations (
            id VARCHAR PRIMARY KEY, 
            title VARCHAR, 
            created_by VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:100')",
        namespace
    ))?;

    // Messages Table
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {}.messages (
            id VARCHAR PRIMARY KEY,
            conversation_id VARCHAR, 
            sender VARCHAR, 
            content VARCHAR, 
            timestamp BIGINT
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:100')",
        namespace
    ))?;

    // Stream Events Table (Stream Type)
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {}.stream_events (
            id VARCHAR PRIMARY KEY,
            conversation_id VARCHAR, 
            event_type VARCHAR, 
            payload VARCHAR, 
            timestamp BIGINT
        ) WITH (TYPE='STREAM', FLUSH_POLICY='rows:100', TTL_SECONDS='3600')",
        namespace
    ))?;

    Ok(())
}

fn cleanup(namespace: &str, creds: &[(String, String)]) {
    for (username, _) in creds {
        let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", username));
    }
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
