#![allow(dead_code)]

use crate::common;
use kalam_link::auth::AuthProvider;
use kalam_link::seq_tracking::{extract_max_seq, row_seq};
use kalam_link::{
    ChangeEvent, ConnectionOptions, EventHandlers, KalamCellValue, KalamLinkClient,
    KalamLinkTimeouts, SeqId,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn reconnect_test_timeouts() -> KalamLinkTimeouts {
    KalamLinkTimeouts {
        connection_timeout: Duration::from_secs(2),
        receive_timeout: Duration::from_secs(5),
        send_timeout: Duration::from_secs(2),
        subscribe_timeout: Duration::from_secs(2),
        auth_timeout: Duration::from_secs(2),
        initial_data_timeout: Duration::from_secs(20),
        idle_timeout: Duration::ZERO,
        keepalive_interval: Duration::from_secs(1),
        pong_timeout: Duration::from_secs(2),
    }
}

fn reconnect_test_connection_options() -> ConnectionOptions {
    ConnectionOptions::new()
        .with_auto_reconnect(true)
        .with_reconnect_delay_ms(150)
        .with_max_reconnect_delay_ms(1_500)
        .with_ping_interval_ms(1_000)
}

/// Create a test client authenticated with root credentials.
pub fn create_test_client() -> Result<KalamLinkClient, kalam_link::KalamLinkError> {
    create_test_client_for_base_url(common::isolated_server_url())
}

pub fn upstream_server_url() -> &'static str {
    common::isolated_server_url()
}

pub fn create_test_client_for_base_url(
    base_url: &str,
) -> Result<KalamLinkClient, kalam_link::KalamLinkError> {
    let token = common::isolated_root_access_token_blocking()
        .map_err(|e| kalam_link::KalamLinkError::ConfigurationError(e.to_string()))?;
    KalamLinkClient::builder()
        .base_url(base_url)
        .timeout(Duration::from_secs(10))
        .auth(AuthProvider::jwt_token(token))
        .timeouts(reconnect_test_timeouts())
        .connection_options(reconnect_test_connection_options())
        .build()
}

pub fn create_test_client_with_events_for_base_url(
    base_url: &str,
) -> Result<(KalamLinkClient, Arc<AtomicU32>, Arc<AtomicU32>), kalam_link::KalamLinkError> {
    let token = common::isolated_root_access_token_blocking()
        .map_err(|e| kalam_link::KalamLinkError::ConfigurationError(e.to_string()))?;

    let connect_count = Arc::new(AtomicU32::new(0));
    let disconnect_count = Arc::new(AtomicU32::new(0));
    let cc = connect_count.clone();
    let dc = disconnect_count.clone();

    let client = KalamLinkClient::builder()
        .base_url(base_url)
        .timeout(Duration::from_secs(10))
        .auth(AuthProvider::jwt_token(token))
        .event_handlers(
            EventHandlers::new()
                .on_connect(move || {
                    cc.fetch_add(1, Ordering::SeqCst);
                })
                .on_disconnect(move |_reason| {
                    dc.fetch_add(1, Ordering::SeqCst);
                }),
        )
        .timeouts(reconnect_test_timeouts())
        .connection_options(reconnect_test_connection_options())
        .build()?;

    Ok((client, connect_count, disconnect_count))
}

/// Ensure a test table exists with a simple schema.
pub async fn ensure_table(client: &KalamLinkClient, table: &str) {
    let _ = client
        .execute_query(
            &format!("CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, value TEXT)", table),
            None,
            None,
            None,
        )
        .await;
}

pub async fn query_max_seq(client: &KalamLinkClient, table: &str) -> SeqId {
    let result = client
        .execute_query(&format!("SELECT MAX(_seq) AS max_seq FROM {}", table), None, None, None)
        .await
        .expect("max seq query should succeed");

    let max_seq = result
        .get_i64("max_seq")
        .unwrap_or_else(|| panic!("max seq query should return a value for {}", table));

    SeqId::from_i64(max_seq)
}

pub fn change_event_rows(event: &ChangeEvent) -> Option<&[HashMap<String, KalamCellValue>]> {
    match event {
        ChangeEvent::Insert { rows, .. }
        | ChangeEvent::Update { rows, .. }
        | ChangeEvent::InitialDataBatch { rows, .. } => Some(rows.as_slice()),
        ChangeEvent::Delete { old_rows, .. } => Some(old_rows.as_slice()),
        _ => None,
    }
}

pub fn row_id(row: &HashMap<String, KalamCellValue>) -> Option<&str> {
    row.get("id").and_then(|value| value.as_str())
}

pub fn event_last_seq(event: &ChangeEvent) -> Option<SeqId> {
    match event {
        ChangeEvent::Ack { batch_control, .. }
        | ChangeEvent::InitialDataBatch { batch_control, .. } => batch_control.last_seq_id,
        ChangeEvent::Insert { rows, .. } | ChangeEvent::Update { rows, .. } => {
            extract_max_seq(rows)
        },
        ChangeEvent::Delete { old_rows, .. } => extract_max_seq(old_rows),
        _ => None,
    }
}

pub fn assert_event_rows_strictly_after(event: &ChangeEvent, from: SeqId, context: &str) {
    let Some(rows) = change_event_rows(event) else {
        return;
    };

    for row in rows {
        if let Some(seq) = row_seq(row) {
            assert!(
                seq > from,
                "{}: received stale row with _seq={} at/before from={}; id={:?}; row={:?}",
                context,
                seq,
                from,
                row_id(row),
                row
            );
        }
    }
}

pub fn collect_ids_and_track_seq(
    event: &ChangeEvent,
    ids: &mut Vec<String>,
    max_seq: &mut Option<SeqId>,
    strict_from: Option<SeqId>,
    context: &str,
) {
    if let Some(from) = strict_from {
        assert_event_rows_strictly_after(event, from, context);
    }

    if let Some(seq) = event_last_seq(event) {
        *max_seq = Some(max_seq.map_or(seq, |prev| prev.max(seq)));
    }

    let Some(rows) = change_event_rows(event) else {
        return;
    };

    for row in rows {
        if let Some(id) = row_id(row) {
            ids.push(id.to_string());
        }
    }
}

pub fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
