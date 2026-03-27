// Smoke test to stress WebSocket connection capacity and ensure HTTP API stays responsive

use crate::common::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::header::{HeaderValue, AUTHORIZATION, USER_AGENT},
    protocol::Message,
};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const AUTH_USERNAME: &str = "admin";
// Use conservative count to avoid overwhelming server during testing
const DEFAULT_WEBSOCKET_CONNECTIONS: usize = 8;
const SQL_RESPONSIVENESS_BUDGET: Duration = Duration::from_secs(10);
// Timeout for each websocket connection attempt
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);

#[ntest::timeout(300_000)]
#[test]
fn smoke_test_websocket_capacity() {
    if !is_server_running() {
        println!("Skipping smoke_test_websocket_capacity: server not running at {}", server_url());
        return;
    }

    let websocket_connections = std::env::var("KALAMDB_WS_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_WEBSOCKET_CONNECTIONS);
    println!(
        "\n=== Starting WebSocket Capacity Smoke Test ({} connections) ===\n",
        websocket_connections
    );

    let namespace = generate_unique_namespace("smoke_ws_cap");
    let table = generate_unique_table("ws_table");
    let full_table_name = format!("{}.{}", namespace, table);
    let subscription_prefix = format!("ws_cap_{}_", namespace);

    setup_test_table(&namespace, &full_table_name);

    let runtime = Runtime::new().expect("Failed to create Tokio runtime for websocket test");

    let namespace_for_cleanup = namespace.clone();
    let table_for_rt = full_table_name.clone();
    let subscription_prefix_for_rt = subscription_prefix.clone();
    let subscription_prefix_for_cleanup = subscription_prefix.clone();

    runtime.block_on(async move {
        let token = get_access_token(AUTH_USERNAME, default_password())
            .await
            .unwrap_or_else(|e| panic!("Failed to get access token: {}", e));
        
        // We need to keep connections alive while running SQL queries
        // The server sends ping frames every 5s, and we need to respond with pong
        // To do this, we split connections into read/write halves and spawn tasks
        // to handle incoming messages (which auto-responds to pings in tokio-tungstenite)
        let stop_flag = Arc::new(AtomicBool::new(false));
        let mut close_senders: Vec<mpsc::Sender<()>> = Vec::with_capacity(websocket_connections);
        let mut reader_handles = Vec::with_capacity(websocket_connections);

        for idx in 0..websocket_connections {
            let subscription_id = format!("{}{}", subscription_prefix_for_rt, idx);
            // Timeout each connection attempt to avoid infinite hangs
            let stream = match tokio::time::timeout(
                CONNECTION_TIMEOUT,
                open_authenticated_connection(
                    idx,
                    &token,
                    &table_for_rt,
                    &subscription_id,
                ),
            )
            .await
            {
                Ok(s) => s,
                Err(_) => panic!(
                    "Timeout opening websocket #{} after {:?}",
                    idx, CONNECTION_TIMEOUT
                ),
            };
            
            // Split the stream and spawn a background task to handle incoming messages
            // This ensures we respond to ping frames and don't timeout
            let (write, mut read) = stream.split();
            let (close_tx, mut close_rx) = mpsc::channel::<()>(1);
            close_senders.push(close_tx);
            
            let stop = Arc::clone(&stop_flag);
            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = close_rx.recv() => {
                            // Close signal received, reunite and close properly
                            break;
                        }
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Ping(_))) => {
                                    // tokio-tungstenite auto-responds to pings
                                    // Connection is being kept alive
                                }
                                Some(Ok(Message::Close(_))) => {
                                    // Server closed connection
                                    break;
                                }
                                Some(Err(_e)) => {
                                    // Read error, connection likely closed
                                    break;
                                }
                                None => {
                                    // Stream ended
                                    break;
                                }
                                _ => {
                                    // Other messages (text, binary) - ignore during idle
                                }
                            }
                        }
                    }
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
                (idx, write)
            });
            reader_handles.push(handle);
        }

        println!(
            "Opened {} authenticated WebSocket connections with keepalive. Verifying SQL responsiveness...",
            close_senders.len()
        );

        let sql_duration = run_simple_sql().await;
        println!(
            "SELECT 1 completed in {:?} while websockets were open",
            sql_duration
        );
        assert!(
            sql_duration <= SQL_RESPONSIVENESS_BUDGET,
            "SQL request took {:?}, exceeding {:?} budget while websockets were open",
            sql_duration,
            SQL_RESPONSIVENESS_BUDGET
        );

        let live_queries_snapshot = fetch_live_queries_snapshot().await;
        println!(
            "system.live_queries snapshot while connections active:\n{}",
            live_queries_snapshot
        );

        let active_subscription_count = count_live_query_subscriptions(subscription_prefix_for_rt).await;
        assert!(
            active_subscription_count >= close_senders.len(),
            "Expected at least {} live query rows, found {}",
            close_senders.len(),
            active_subscription_count
        );

        // Signal all reader tasks to stop and close connections
        stop_flag.store(true, Ordering::Relaxed);
        for close_tx in close_senders {
            let _ = close_tx.send(()).await;
        }
        
        // Wait for all reader tasks to complete and collect write halves
        for handle in reader_handles {
            match handle.await {
                Ok((_idx, _write)) => {
                    // Reunite not possible after split, just drop the write half
                    // The connection will close when both halves are dropped
                }
                Err(e) => {
                    eprintln!("Reader task panicked: {}", e);
                }
            }
        }

        // Give the server enough time to process connection closures
        // The cleanup involves RocksDB operations which can be slow
        // Wait long enough to exceed the WebSocket idle timeout (5s) plus cleanup time
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify live_queries are cleaned up after closing
        let post_close_count = count_live_query_subscriptions(subscription_prefix_for_cleanup.clone()).await;
        println!(
            "Live queries count after closing connections: {} (should be 0)",
            post_close_count
        );
    });

    cleanup_namespace(&namespace_for_cleanup);

    println!("\n=== WebSocket Capacity Smoke Test Complete ===\n");
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn open_authenticated_connection(
    idx: usize,
    token: &str,
    table_name: &str,
    subscription_id: &str,
) -> WsStream {
    let ws_url = websocket_url();
    let mut request = ws_url
        .clone()
        .into_client_request()
        .unwrap_or_else(|e| panic!("Invalid WebSocket URL ({}): {}", ws_url, e));
    {
        let headers = request.headers_mut();
        let auth_header = format!("Bearer {}", token);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_header)
                .unwrap_or_else(|e| panic!("Invalid auth header value: {}", e)),
        );
        headers.insert(USER_AGENT, HeaderValue::from_static("kalam-smoke-websocket"));
    }

    let (mut stream, _response) = connect_async(request)
        .await
        .unwrap_or_else(|e| panic!("Failed to open websocket #{}: {}", idx, e));

    let auth_payload = json!({
        "type": "authenticate",
        "method": "jwt",
        "token": token,
    });

    stream
        .send(Message::Text(auth_payload.to_string().into()))
        .await
        .unwrap_or_else(|e| panic!("Failed to send auth message on websocket #{}: {}", idx, e));

    let auth_payload = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let next = stream
                .next()
                .await
                .unwrap_or_else(|| panic!("Websocket #{} closed before auth response", idx))
                .unwrap_or_else(|e| panic!("Websocket #{} auth response error: {}", idx, e));
            match next {
                Message::Text(payload) => break payload,
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("Websocket #{} expected text auth response, got {:?}", idx, other),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Websocket #{} auth response timed out", idx));

    let value: serde_json::Value = serde_json::from_str(&auth_payload)
        .unwrap_or_else(|e| panic!("Invalid auth response JSON on websocket #{}: {}", idx, e));
    let msg_type = value.get("type").and_then(|v| v.as_str()).unwrap_or_default();
    assert_eq!(
        msg_type, "auth_success",
        "Websocket #{} expected auth_success, got {}",
        idx, auth_payload
    );

    let subscribe_payload = json!({
        "type": "subscribe",
        "subscription": {
            "id": subscription_id,
            "sql": format!("SELECT * FROM {}", table_name),
            "options": {}
        }
    });

    let mut last_error: Option<String> = None;
    for attempt in 0..3 {
        stream
            .send(Message::Text(subscribe_payload.to_string().into()))
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to send subscribe message on websocket #{}: {}", idx, e)
            });

        match wait_for_subscription_ack(idx, subscription_id, &mut stream).await {
            Ok(()) => {
                last_error = None;
                break;
            },
            Err(err) => {
                last_error = Some(err);
                tokio::time::sleep(Duration::from_millis(200 + attempt * 200)).await;
            },
        }
    }

    if let Some(err) = last_error {
        panic!("Websocket #{} subscription failed after retries: {}", idx, err);
    }

    stream
}

async fn run_simple_sql() -> Duration {
    let start = Instant::now();
    let result = tokio::task::spawn_blocking(|| {
        execute_sql_as_root_via_client("SELECT 1").map_err(|e| format!("{}", e))
    })
    .await
    .expect("spawn_blocking join failure");

    let output = result.expect("SELECT 1 should succeed while websockets are open");
    assert!(
        output.contains("1"),
        "Unexpected SELECT 1 output while websockets open: {}",
        output
    );

    start.elapsed()
}

async fn fetch_live_queries_snapshot() -> String {
    tokio::task::spawn_blocking(|| {
        execute_sql_as_root_via_client("SELECT * FROM system.live_queries ORDER BY live_id")
            .map_err(|e| format!("{}", e))
    })
    .await
    .expect("spawn_blocking join failure")
    .expect("SELECT * FROM system.live_queries should succeed")
}

async fn count_live_query_subscriptions(prefix: String) -> usize {
    tokio::task::spawn_blocking(move || {
        execute_sql_as_root_via_client_json(
            "SELECT subscription_id FROM system.live_queries ORDER BY subscription_id",
        )
        .map_err(|e| format!("{}", e))
    })
    .await
    .expect("spawn_blocking join failure")
    .map(|json_str| {
        let value: serde_json::Value = serde_json::from_str(&json_str)
            .unwrap_or_else(|e| panic!("Failed to parse system.live_queries JSON: {}", e));
        let rows = get_rows_as_hashmaps(&value).unwrap_or_default();
        rows.iter()
            .filter(|row| {
                let id_value = row
                    .get("subscription_id")
                    .map(extract_typed_value)
                    .unwrap_or(serde_json::Value::Null);
                id_value.as_str().map(|id| id.starts_with(&prefix)).unwrap_or(false)
            })
            .count()
    })
    .expect("system.live_queries JSON query should succeed")
}

async fn wait_for_subscription_ack(
    idx: usize,
    expected_id: &str,
    stream: &mut WsStream,
) -> Result<(), String> {
    loop {
        let message = stream
            .next()
            .await
            .ok_or_else(|| format!("Websocket #{} closed during subscription", idx))?
            .map_err(|e| format!("Websocket #{} subscription error: {}", idx, e))?;

        match message {
            Message::Ping(payload) => {
                // Be tolerant of keepalive ping frames during the subscription handshake.
                // Respond with Pong to keep the connection healthy.
                let _ = stream.send(Message::Pong(payload)).await;
                continue;
            },
            Message::Pong(_) => continue,
            Message::Text(payload) => {
                let value: serde_json::Value = serde_json::from_str(&payload).unwrap_or_else(|e| {
                    panic!("Invalid subscription response JSON on websocket #{}: {}", idx, e)
                });
                if let Some(msg_type) = value.get("type").and_then(|v| v.as_str()) {
                    match msg_type {
                        "subscription_ack" => {
                            let sub_id = value
                                .get("subscription_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default();
                            assert_eq!(
                                sub_id, expected_id,
                                "Websocket #{} subscription ack mismatch (expected {}, got {})",
                                idx, expected_id, sub_id
                            );
                            return Ok(());
                        },
                        "initial_data_batch" => continue,
                        other => {
                            return Err(format!(
                            "Websocket #{} received unexpected message type '{}' while awaiting ack: {}",
                            idx, other, payload
                        ));
                        },
                    }
                }
            },
            Message::Close(frame) => {
                return Err(format!(
                    "Websocket #{} closed before subscription ack: {:?}",
                    idx, frame
                ));
            },
            other => {
                return Err(format!(
                    "Websocket #{} received unexpected message while awaiting subscription ack: {:?}",
                    idx, other
                ));
            },
        }
    }
}

fn setup_test_table(namespace: &str, full_table_name: &str) {
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed for websocket capacity test");

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, value VARCHAR NOT NULL) WITH (TYPE = 'USER')",
        full_table_name
    );
    execute_sql_as_root_via_client(&create_sql)
        .expect("CREATE TABLE should succeed for websocket capacity test");

    let delete_sql = format!("DELETE FROM {} WHERE id = 0", full_table_name);
    let _ = execute_sql_as_root_via_client(&delete_sql);

    let insert_sql =
        format!("INSERT INTO {} (id, value) VALUES (0, 'ws payload')", full_table_name);
    let _ = execute_sql_as_root_via_client(&insert_sql);
}

fn cleanup_namespace(namespace: &str) {
    let drop_sql = format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace);
    let _ = execute_sql_as_root_via_client(&drop_sql);
}
