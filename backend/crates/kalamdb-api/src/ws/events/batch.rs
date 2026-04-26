//! WebSocket batch handler
//!
//! Handles the NextBatch message for paginated initial data fetching.

use std::sync::Arc;

use actix_ws::Session;
use kalamdb_commons::{ids::SeqId, websocket::BatchControl, WebSocketMessage};
use kalamdb_core::providers::arrow_json_conversion::row_into_json_map;
use kalamdb_live::{LiveQueryManager, SharedConnectionState};
use log::error;
use tracing::debug;

use super::{send_error, send_message};
use crate::ws::models::WsErrorCode;

/// Handle next batch request
///
/// Uses subscription metadata from ConnectionState for batch fetching.
/// Tracks batch_num in subscription state and increments it after each batch.
pub async fn handle_next_batch(
    connection_state: &SharedConnectionState,
    subscription_id: &str,
    last_seq_id: Option<SeqId>,
    session: &mut Session,
    live_query_manager: &Arc<LiveQueryManager>,
    compression_enabled: bool,
) -> Result<(), String> {
    // Increment batch number and get the new value
    // This is done BEFORE fetching to get the correct batch_num for this request
    let batch_num = connection_state.increment_batch_num(subscription_id).unwrap_or(0);

    debug!(
        "Processing NextBatch request: subscription_id={}, batch_num={}, last_seq_id={:?}",
        subscription_id, batch_num, last_seq_id
    );

    match live_query_manager
        .fetch_initial_data_batch(connection_state, subscription_id, last_seq_id)
        .await
    {
        Ok(result) => {
            // Use BatchControl::new() which handles status based on batch_num and has_more
            let batch_control = BatchControl::new(batch_num, result.has_more, result.last_seq);

            debug!(
                "Sending batch {}: {} rows, has_more={}",
                batch_num,
                result.rows.len(),
                result.has_more
            );

            // Convert Row objects to HashMap
            let mut rows_json = Vec::with_capacity(result.rows.len());
            for row in result.rows {
                match row_into_json_map(row) {
                    Ok(json) => rows_json.push(json),
                    Err(e) => {
                        error!("Failed to convert row to JSON: {}", e);
                        return send_error(
                            session,
                            subscription_id,
                            WsErrorCode::ConversionError,
                            &format!("Failed to convert row data: {}", e),
                            compression_enabled,
                        )
                        .await
                        .map_err(|_| "Failed to send error message".to_string());
                    },
                }
            }

            let msg = WebSocketMessage::initial_data_batch(
                subscription_id.to_string(),
                rows_json,
                batch_control,
            );
            let ser = connection_state.serialization_type();
            let _ = send_message(session, &msg, ser, compression_enabled).await;

            if !result.has_more {
                let flushed = connection_state.complete_initial_load(subscription_id);
                if flushed > 0 {
                    debug!(
                        "Flushed {} buffered notifications after initial load for {}",
                        flushed, subscription_id
                    );
                }
            }
            Ok(())
        },
        Err(e) => {
            let _ = send_error(
                session,
                subscription_id,
                WsErrorCode::BatchFetchFailed,
                &e.to_string(),
                compression_enabled,
            )
            .await;
            Ok(())
        },
    }
}
