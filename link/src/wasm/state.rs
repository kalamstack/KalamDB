use crate::models::SubscriptionOptions;
use crate::models::{ChangeEvent, ChangeTypeRaw, ServerMessage};
use crate::seq_id::SeqId;
use crate::seq_tracking;
use crate::subscription::{LiveRowsConfig, LiveRowsMaterializer};

#[derive(Clone)]
pub(crate) enum SubscriptionCallbackMode {
    RawEvents,
    LiveRows { materializer: LiveRowsMaterializer },
}

impl SubscriptionCallbackMode {
    pub(crate) fn raw() -> Self {
        Self::RawEvents
    }

    pub(crate) fn live_rows(config: LiveRowsConfig) -> Self {
        Self::LiveRows {
            materializer: LiveRowsMaterializer::new(config),
        }
    }
}

/// Stored subscription info for reconnection
#[derive(Clone)]
pub(crate) struct SubscriptionState {
    /// The SQL query for this subscription
    pub(crate) sql: String,
    /// Original subscription options
    pub(crate) options: SubscriptionOptions,
    /// JavaScript callback function
    pub(crate) callback: js_sys::Function,
    /// Last received seq_id for resumption
    pub(crate) last_seq_id: Option<SeqId>,
    /// Promise resolver for an in-flight subscribe request waiting for ack.
    pub(crate) pending_subscribe_resolve: Option<js_sys::Function>,
    /// Promise rejector for an in-flight subscribe request waiting for ack.
    pub(crate) pending_subscribe_reject: Option<js_sys::Function>,
    /// True until the server responds with subscription_ack or error.
    pub(crate) awaiting_initial_response: bool,
    /// Callback behavior for this subscription.
    pub(crate) callback_mode: SubscriptionCallbackMode,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum WasmLiveRowsEvent {
    Rows {
        subscription_id: String,
        rows: Vec<crate::models::RowData>,
    },
    Error {
        subscription_id: String,
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub(crate) struct WasmLiveRowsOptions {
    pub(crate) limit: Option<usize>,
    pub(crate) key_columns: Option<Vec<String>>,
    pub(crate) subscription_options: Option<SubscriptionOptions>,
}

fn change_event_to_server_message(event: &ChangeEvent) -> ServerMessage {
    match event {
        ChangeEvent::Ack {
            subscription_id,
            total_rows,
            batch_control,
            schema,
        } => ServerMessage::SubscriptionAck {
            subscription_id: subscription_id.clone(),
            total_rows: *total_rows,
            batch_control: batch_control.clone(),
            schema: schema.clone(),
        },
        ChangeEvent::InitialDataBatch {
            subscription_id,
            rows,
            batch_control,
        } => ServerMessage::InitialDataBatch {
            subscription_id: subscription_id.clone(),
            rows: rows.clone(),
            batch_control: batch_control.clone(),
        },
        ChangeEvent::Insert {
            subscription_id,
            rows,
        } => ServerMessage::Change {
            subscription_id: subscription_id.clone(),
            change_type: ChangeTypeRaw::Insert,
            rows: Some(rows.clone()),
            old_values: None,
        },
        ChangeEvent::Update {
            subscription_id,
            rows,
            old_rows,
        } => ServerMessage::Change {
            subscription_id: subscription_id.clone(),
            change_type: ChangeTypeRaw::Update,
            rows: Some(rows.clone()),
            old_values: Some(old_rows.clone()),
        },
        ChangeEvent::Delete {
            subscription_id,
            old_rows,
        } => ServerMessage::Change {
            subscription_id: subscription_id.clone(),
            change_type: ChangeTypeRaw::Delete,
            rows: None,
            old_values: Some(old_rows.clone()),
        },
        ChangeEvent::Error {
            subscription_id,
            code,
            message,
        } => ServerMessage::Error {
            subscription_id: subscription_id.clone(),
            code: code.clone(),
            message: message.clone(),
        },
        ChangeEvent::Unknown { .. } => ServerMessage::Error {
            subscription_id: String::new(),
            code: "unknown".to_string(),
            message: "Unknown subscription event".to_string(),
        },
    }
}

pub(crate) fn track_subscription_checkpoint(last_seq_id: &mut Option<SeqId>, event: &ChangeEvent) {
    match event {
        ChangeEvent::Ack { batch_control, .. } => {
            if let Some(seq_id) = batch_control.last_seq_id {
                seq_tracking::advance_seq(last_seq_id, seq_id);
            }
        },
        ChangeEvent::InitialDataBatch {
            rows,
            batch_control,
            ..
        } => {
            if let Some(seq_id) = batch_control.last_seq_id {
                seq_tracking::advance_seq(last_seq_id, seq_id);
            }
            seq_tracking::track_rows(last_seq_id, rows);
        },
        ChangeEvent::Insert { rows, .. } => {
            seq_tracking::track_rows(last_seq_id, rows);
        },
        ChangeEvent::Update { rows, old_rows, .. } => {
            seq_tracking::track_rows(last_seq_id, rows);
            seq_tracking::track_rows(last_seq_id, old_rows);
        },
        ChangeEvent::Delete { old_rows, .. } => {
            seq_tracking::track_rows(last_seq_id, old_rows);
        },
        ChangeEvent::Error { .. } | ChangeEvent::Unknown { .. } => {},
    }
}

pub(crate) fn filter_subscription_event(
    options: &SubscriptionOptions,
    event: &ServerMessage,
) -> Option<ChangeEvent> {
    let change_event = server_message_to_change_event(event)?;
    crate::subscription::filter_replayed_event(change_event, options.from)
}

pub(crate) fn callback_payload(
    mode: &mut SubscriptionCallbackMode,
    event: &ChangeEvent,
) -> Option<String> {
    match mode {
        SubscriptionCallbackMode::RawEvents => {
            serde_json::to_string(&change_event_to_server_message(event)).ok()
        },
        SubscriptionCallbackMode::LiveRows { materializer } => {
            let update = materializer.apply(event.clone())?;
            let wasm_event = match update {
                crate::subscription::LiveRowsEvent::Rows {
                    subscription_id,
                    rows,
                } => WasmLiveRowsEvent::Rows {
                    subscription_id,
                    rows,
                },
                crate::subscription::LiveRowsEvent::Error {
                    subscription_id,
                    code,
                    message,
                } => WasmLiveRowsEvent::Error {
                    subscription_id,
                    code,
                    message,
                },
            };
            serde_json::to_string(&wasm_event).ok()
        },
    }
}

fn server_message_to_change_event(event: &ServerMessage) -> Option<crate::models::ChangeEvent> {
    match event {
        ServerMessage::SubscriptionAck {
            subscription_id,
            total_rows,
            batch_control,
            schema,
        } => Some(crate::models::ChangeEvent::Ack {
            subscription_id: subscription_id.clone(),
            total_rows: *total_rows,
            batch_control: batch_control.clone(),
            schema: schema.clone(),
        }),
        ServerMessage::InitialDataBatch {
            subscription_id,
            rows,
            batch_control,
        } => Some(crate::models::ChangeEvent::InitialDataBatch {
            subscription_id: subscription_id.clone(),
            rows: rows.clone(),
            batch_control: batch_control.clone(),
        }),
        ServerMessage::Change {
            subscription_id,
            change_type,
            rows,
            old_values,
        } => Some(match change_type {
            crate::models::ChangeTypeRaw::Insert => crate::models::ChangeEvent::Insert {
                subscription_id: subscription_id.clone(),
                rows: rows.clone().unwrap_or_default(),
            },
            crate::models::ChangeTypeRaw::Update => crate::models::ChangeEvent::Update {
                subscription_id: subscription_id.clone(),
                rows: rows.clone().unwrap_or_default(),
                old_rows: old_values.clone().unwrap_or_default(),
            },
            crate::models::ChangeTypeRaw::Delete => crate::models::ChangeEvent::Delete {
                subscription_id: subscription_id.clone(),
                old_rows: old_values.clone().unwrap_or_default(),
            },
        }),
        ServerMessage::Error {
            subscription_id,
            code,
            message,
        } => Some(crate::models::ChangeEvent::Error {
            subscription_id: subscription_id.clone(),
            code: code.clone(),
            message: message.clone(),
        }),
        _ => None,
    }
}
