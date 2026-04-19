//! WebSocket subscription handler
//!
//! Handles the Subscribe message for live query subscriptions.

use actix_ws::Session;
use kalamdb_commons::websocket::{BatchControl, SubscriptionRequest, MAX_ROWS_PER_BATCH};
use kalamdb_commons::WebSocketMessage;
use kalamdb_core::providers::arrow_json_conversion::row_into_json_map;
use kalamdb_live::{InitialDataOptions, LiveQueryManager, SharedConnectionState};
use log::{debug, error, warn};
use std::sync::Arc;

use crate::limiter::RateLimiter;
use crate::ws::models::WsErrorCode;

use super::{send_error, send_message};

/// Handle subscription request
///
/// Validates subscription ID and rate limits, then delegates to LiveQueryManager
/// which handles all SQL parsing, permission checks, and registration.
///
/// Uses connection_id from SharedConnectionState, no separate parameter needed.
pub async fn handle_subscribe(
    connection_state: &SharedConnectionState,
    subscription: SubscriptionRequest,
    session: &mut Session,
    rate_limiter: &Arc<RateLimiter>,
    live_query_manager: &Arc<LiveQueryManager>,
    compression_enabled: bool,
) -> Result<(), String> {
    let user_id = connection_state.user_id().cloned().ok_or("Not authenticated")?;

    // Validate subscription ID
    if subscription.id.trim().is_empty() {
        let _ = send_error(
            session,
            "invalid_subscription",
            WsErrorCode::InvalidSubscriptionId,
            "Subscription ID cannot be empty",
            compression_enabled,
        )
        .await;
        return Ok(());
    }

    // Rate limit check
    if !rate_limiter.check_subscription_limit(&user_id) {
        let _ = send_error(
            session,
            &subscription.id,
            WsErrorCode::SubscriptionLimitExceeded,
            "Maximum subscriptions reached",
            compression_enabled,
        )
        .await;
        return Ok(());
    }

    let subscription_id = subscription.id.clone();
    let subscription_options = subscription.options.clone();

    // Determine batch size for initial data options
    let batch_size = subscription_options
        .as_ref()
        .and_then(|options| options.batch_size)
        .unwrap_or(MAX_ROWS_PER_BATCH);

    // Create initial data options respecting all three options:
    // - from: Resume from a specific sequence ID
    // - last_rows: Fetch the last N rows
    // - batch_size: Hint for server-side batch sizing
    let initial_opts = subscription_options.map(|options| {
        if let Some(from_seq) = options.from {
            InitialDataOptions::batch(Some(from_seq), options.snapshot_end_seq, batch_size)
        } else if let Some(n) = options.last_rows {
            InitialDataOptions::last(n as usize)
        } else {
            InitialDataOptions::batch(None, None, batch_size)
        }
    });

    // Register subscription with initial data fetch
    // LiveQueryManager handles all SQL parsing, permission checks, and registration internally
    match live_query_manager
        .register_subscription_with_initial_data(connection_state, &subscription, initial_opts)
        .await
    {
        Ok(result) => {
            if let Some(ref initial) = result.initial_data {
                debug!("Initial data: {} rows, has_more={}", initial.rows.len(), initial.has_more);
            }

            // Update rate limiter
            rate_limiter.increment_subscription(&user_id);

            // Send response
            // Use BatchControl::new() which handles status based on batch_num and has_more
            let batch_control = if let Some(ref initial) = result.initial_data {
                BatchControl::new(
                    0, // batch_num
                    initial.has_more,
                    initial.last_seq,
                    initial.snapshot_end_seq,
                )
            } else {
                // No initial data - empty result, ready immediately
                BatchControl::new(0, false, None, None)
            };

            let ack = WebSocketMessage::subscription_ack(
                subscription_id.clone(),
                0,
                batch_control.clone(),
                result.schema.clone(),
            );
            let ser = connection_state.serialization_type();
            let _ = send_message(session, &ack, ser, compression_enabled).await;

            if let Some(initial) = result.initial_data {
                // Convert Row objects to HashMap (always using simple JSON format)
                let mut rows_json = Vec::with_capacity(initial.rows.len());
                for row in initial.rows {
                    match row_into_json_map(row) {
                        Ok(json) => rows_json.push(json),
                        Err(e) => {
                            error!("Failed to convert row to JSON: {}", e);
                            // Cleanup: unregister the subscription since we can't
                            // deliver initial data and the client will get an error
                            if let Err(cleanup_err) = live_query_manager
                                .unregister_subscription(
                                    connection_state,
                                    &subscription_id,
                                    &result.live_id,
                                )
                                .await
                            {
                                error!(
                                    "Failed to cleanup subscription {} after conversion error: {}",
                                    subscription_id, cleanup_err
                                );
                            }
                            rate_limiter.decrement_subscription(&user_id);
                            return send_error(
                                session,
                                &subscription_id,
                                WsErrorCode::ConversionError,
                                &format!("Failed to convert row data: {}", e),
                                compression_enabled,
                            )
                            .await
                            .map_err(|_| "Failed to send error message".to_string());
                        },
                    }
                }

                let batch_msg = WebSocketMessage::initial_data_batch(
                    subscription_id.clone(),
                    rows_json,
                    batch_control,
                );
                let _ = send_message(session, &batch_msg, ser, compression_enabled).await;

                if !initial.has_more {
                    let flushed = connection_state.complete_initial_load(&subscription_id);
                    if flushed > 0 {
                        debug!(
                            "Flushed {} buffered notifications after initial load for {}",
                            flushed, subscription_id
                        );
                    }
                }
            } else {
                let flushed = connection_state.complete_initial_load(&subscription_id.clone());
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
            // Map error types to appropriate WebSocket error codes
            let code = match &e {
                kalamdb_live::error::LiveError::PermissionDenied(_) => WsErrorCode::Unauthorized,
                kalamdb_live::error::LiveError::NotFound(_) => WsErrorCode::NotFound,
                kalamdb_live::error::LiveError::InvalidSql(_) => WsErrorCode::InvalidSql,
                kalamdb_live::error::LiveError::InvalidOperation(_) => WsErrorCode::Unsupported,
                _ => WsErrorCode::SubscriptionFailed,
            };
            let message = e.to_string();
            // Use warn for "expected" client errors (table gone, bad SQL) to avoid
            // flooding logs during benchmark teardown; keep error for server-side issues.
            match &code {
                WsErrorCode::NotFound | WsErrorCode::InvalidSql | WsErrorCode::Unauthorized => {
                    warn!(
                        "Failed to register subscription {}: {} (sql: '{}')",
                        subscription_id, e, subscription.sql
                    );
                },
                _ => {
                    error!(
                        "Failed to register subscription {}: {} (sql: '{}')",
                        subscription_id, e, subscription.sql
                    );
                },
            }
            let _ =
                send_error(session, &subscription_id, code, &message, compression_enabled).await;
            Ok(())
        },
    }
}
