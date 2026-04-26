use std::collections::HashMap;

use super::registry::{
    cache_entry_seq, clear_startup_deadline, effective_entry_seq, now_ms, refresh_startup_deadline,
    resolve_subscription_key, SubEntry,
};
use crate::{
    connection::{send_client_message, send_next_batch_request_with_format, WebSocketStream},
    error::{KalamLinkError, Result},
    models::{
        ChangeEvent, ClientMessage, SerializationType, SubscriptionOptions, SubscriptionRequest,
    },
    seq_id::SeqId,
    subscription::{batch_envelope, filter_replayed_event, subscription_start_ready},
    timeouts::KalamLinkTimeouts,
};

pub(super) async fn send_subscribe(
    ws: &mut WebSocketStream,
    id: &str,
    sql: &str,
    options: Option<SubscriptionOptions>,
    serialization: SerializationType,
) -> Result<()> {
    let message = ClientMessage::Subscribe {
        subscription: SubscriptionRequest {
            id: id.to_string(),
            sql: sql.to_string(),
            options,
        },
    };
    send_client_message(ws, &message, serialization).await
}

pub(super) async fn send_unsubscribe(
    ws: &mut WebSocketStream,
    id: &str,
    serialization: SerializationType,
) -> Result<()> {
    let message = ClientMessage::Unsubscribe {
        subscription_id: id.to_string(),
    };
    send_client_message(ws, &message, serialization).await
}

pub(super) async fn route_event(
    event: ChangeEvent,
    ws: &mut WebSocketStream,
    subs: &mut HashMap<String, SubEntry>,
    seq_id_cache: &mut HashMap<String, SeqId>,
    timeouts: &KalamLinkTimeouts,
    serialization: SerializationType,
) {
    let incoming_sub_id = match event.subscription_id() {
        Some(id) => id.to_string(),
        None => return,
    };
    let matched_key = resolve_subscription_key(&incoming_sub_id, subs);
    let resume_from = matched_key
        .as_ref()
        .and_then(|key| subs.get(key.as_str(&incoming_sub_id)))
        .and_then(effective_entry_seq);
    let Some(event) = filter_replayed_event(event, resume_from) else {
        return;
    };
    let event_time_ms = now_ms();

    let auto_request_next_batch = matches!(event, ChangeEvent::InitialDataBatch { .. });

    if let Some(batch) = batch_envelope(&event) {
        if let Some(key) = matched_key.as_ref() {
            if let Some(entry) = subs.get_mut(key.as_str(&incoming_sub_id)) {
                if let Some(seq_id) = batch.last_seq_id {
                    entry.batch_seq_id = Some(seq_id);
                }
                entry.is_loading = batch.status != crate::models::BatchStatus::Ready;
                entry.last_event_time_ms = Some(event_time_ms);
                if entry.is_loading {
                    refresh_startup_deadline(entry, timeouts);
                }
            }
        }
        if auto_request_next_batch && batch.has_more {
            let last_seq = matched_key
                .as_ref()
                .and_then(|key| subs.get(key.as_str(&incoming_sub_id)))
                .and_then(|entry| entry.batch_seq_id.or(entry.last_seq_id));
            if let Err(error) =
                send_next_batch_request_with_format(ws, &incoming_sub_id, last_seq, serialization)
                    .await
            {
                log::warn!("Failed to send NextBatch for {}: {}", incoming_sub_id, error);
            }
        }
    }

    if let Some(key) = matched_key {
        let mut remove_after_send = false;
        let key_str = key.as_str(&incoming_sub_id);

        if let Some(entry) = subs.get_mut(key_str) {
            entry.last_event_time_ms = Some(event_time_ms);
            let is_start_ready = subscription_start_ready(&event);

            match &event {
                _ if is_start_ready => {
                    clear_startup_deadline(entry);
                    if let Some(result_tx) = entry.pending_result_tx.take() {
                        let _ = result_tx.send(Ok((entry.generation, entry.options.from)));
                    }
                },
                ChangeEvent::Error { code, message, .. } => {
                    clear_startup_deadline(entry);
                    if let Some(result_tx) = entry.pending_result_tx.take() {
                        let _ = result_tx.send(Err(KalamLinkError::WebSocketError(format!(
                            "Subscription failed ({}): {}",
                            code, message
                        ))));
                        remove_after_send = true;
                    }
                },
                _ => {},
            }

            if !is_start_ready {
                if entry.is_loading {
                    refresh_startup_deadline(entry, timeouts);
                } else if entry.reconnect_resubscribe_pending {
                    clear_startup_deadline(entry);
                }
            }

            if !remove_after_send && entry.event_tx.send(Ok(event)).await.is_err() {
                log::debug!("Subscription {} receiver dropped", incoming_sub_id);
            }
        }

        if remove_after_send {
            if let Some(entry) = subs.remove(key_str) {
                cache_entry_seq(seq_id_cache, key_str, &entry);
            }
        }
    } else {
        log::debug!("No subscription found for id: {}", incoming_sub_id);
    }
}
