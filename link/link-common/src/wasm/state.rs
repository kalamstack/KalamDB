use crate::{
    models::{ChangeEvent, ServerMessage, SubscriptionOptions},
    seq_id::SeqId,
    seq_tracking,
    subscription::{LiveRowsConfig, LiveRowsMaterializer},
};

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

#[inline]
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

#[inline]
pub(crate) fn filter_subscription_event(
    options: &SubscriptionOptions,
    event: &ServerMessage,
) -> Option<ChangeEvent> {
    let change_event = ChangeEvent::from_server_message(event.clone())?;
    crate::subscription::filter_replayed_event(change_event, options.from)
}

#[inline]
pub(crate) fn callback_payload(
    mode: &mut SubscriptionCallbackMode,
    event: &ChangeEvent,
) -> Option<String> {
    match mode {
        SubscriptionCallbackMode::RawEvents => {
            serde_json::to_string(&event.to_server_message()).ok()
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
