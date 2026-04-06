use crate::{
    connection::FAR_FUTURE,
    error::Result,
    models::{ChangeEvent, SubscriptionInfo, SubscriptionOptions},
    seq_id::SeqId,
    seq_tracking,
    subscription::final_resume_seq,
    timeouts::KalamLinkTimeouts,
};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant as TokioInstant;

#[inline]
pub(super) fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

pub(super) fn snapshot_subscriptions(
    subs: &HashMap<String, SubEntry>,
    seq_id_cache: &HashMap<String, SeqId>,
) -> Vec<SubscriptionInfo> {
    let mut out: Vec<SubscriptionInfo> = subs
        .iter()
        .map(|(id, entry)| SubscriptionInfo {
            id: id.clone(),
            query: entry.sql.clone(),
            last_seq_id: effective_entry_seq(entry),
            last_event_time_ms: entry.last_event_time_ms,
            created_at_ms: entry.created_at_ms,
            closed: false,
        })
        .collect();

    for (id, &seq) in seq_id_cache {
        if !subs.contains_key(id) {
            out.push(SubscriptionInfo {
                id: id.clone(),
                query: String::new(),
                last_seq_id: Some(seq),
                last_event_time_ms: None,
                created_at_ms: 0,
                closed: true,
            });
        }
    }

    out
}

pub(super) fn effective_entry_seq(entry: &SubEntry) -> Option<SeqId> {
    final_resume_seq(entry.last_seq_id, entry.consumed_seq_id)
}

pub(super) fn cache_entry_seq(
    seq_id_cache: &mut HashMap<String, SeqId>,
    id: impl Into<String>,
    entry: &SubEntry,
) {
    if let Some(seq) = effective_entry_seq(entry) {
        seq_id_cache.insert(id.into(), seq);
    }
}

pub(super) fn merge_resume_from(
    options: &mut SubscriptionOptions,
    inherited_seq: Option<SeqId>,
) -> Option<SeqId> {
    let effective_from = match (options.from, inherited_seq) {
        (Some(explicit), Some(cached)) => Some(explicit.max(cached)),
        (explicit, cached) => explicit.or(cached),
    };
    options.from = effective_from;
    effective_from
}

pub(super) fn should_send_subscription_options(
    request_initial_data: bool,
    options: &SubscriptionOptions,
) -> bool {
    request_initial_data
        || options.batch_size.is_some()
        || options.last_rows.is_some()
        || options.from.is_some()
        || options.snapshot_end_seq.is_some()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn register_subscription_entry(
    subs: &mut HashMap<String, SubEntry>,
    seq_id_cache: &mut HashMap<String, SeqId>,
    next_generation: &mut u64,
    timeouts: &KalamLinkTimeouts,
    id: String,
    sql: String,
    mut options: SubscriptionOptions,
    request_initial_data: bool,
    event_tx: mpsc::Sender<Result<ChangeEvent>>,
    result_tx: oneshot::Sender<Result<(u64, Option<SeqId>)>>,
) -> (u64, Option<SeqId>) {
    let effective_from = merge_resume_from(&mut options, seq_id_cache.remove(&id));
    let generation = *next_generation;
    *next_generation += 1;

    subs.insert(
        id,
        SubEntry {
            sql,
            options,
            request_initial_data,
            event_tx,
            last_seq_id: effective_from,
            consumed_seq_id: effective_from,
            batch_seq_id: None,
            snapshot_end_seq: None,
            is_loading: true,
            generation,
            created_at_ms: now_ms(),
            last_event_time_ms: None,
            pending_result_tx: Some(result_tx),
            ready_deadline: startup_deadline(timeouts),
            reconnect_resubscribe_pending: false,
        },
    );

    (generation, effective_from)
}

pub(super) fn remove_subscription_entry(
    subs: &mut HashMap<String, SubEntry>,
    seq_id_cache: &mut HashMap<String, SeqId>,
    id: &str,
    generation: Option<u64>,
) -> Option<SubEntry> {
    let should_remove = match generation {
        Some(expected_generation) => {
            subs.get(id).is_some_and(|entry| entry.generation == expected_generation)
        },
        None => true,
    };
    if !should_remove {
        return None;
    }

    subs.remove(id)
        .inspect(|entry| cache_entry_seq(seq_id_cache, id.to_string(), entry))
}

pub(super) fn advance_entry_progress(
    entry: &mut SubEntry,
    generation: u64,
    seq_id: SeqId,
    advance_resume: bool,
) {
    if entry.generation != generation {
        return;
    }

    seq_tracking::advance_seq(&mut entry.consumed_seq_id, seq_id);
    if advance_resume {
        seq_tracking::advance_seq(&mut entry.last_seq_id, seq_id);
    }
    entry.last_event_time_ms = Some(now_ms());
}

pub(super) fn startup_deadline(timeouts: &KalamLinkTimeouts) -> Option<TokioInstant> {
    if KalamLinkTimeouts::is_no_timeout(timeouts.initial_data_timeout) {
        None
    } else {
        Some(TokioInstant::now() + timeouts.initial_data_timeout)
    }
}

pub(super) fn reset_startup_deadline(
    entry: &mut SubEntry,
    timeouts: &KalamLinkTimeouts,
    is_resume: bool,
) {
    entry.ready_deadline = startup_deadline(timeouts);
    entry.reconnect_resubscribe_pending = is_resume;
}

pub(super) fn refresh_startup_deadline(entry: &mut SubEntry, timeouts: &KalamLinkTimeouts) {
    if entry.ready_deadline.is_some() {
        entry.ready_deadline = startup_deadline(timeouts);
    }
}

pub(super) fn clear_startup_deadline(entry: &mut SubEntry) {
    entry.ready_deadline = None;
    entry.reconnect_resubscribe_pending = false;
}

pub(super) fn next_startup_deadline(subs: &HashMap<String, SubEntry>) -> TokioInstant {
    subs.values()
        .filter_map(|entry| entry.ready_deadline)
        .min()
        .unwrap_or_else(|| TokioInstant::now() + FAR_FUTURE)
}

pub(super) fn resolve_subscription_key(
    sub_id: &str,
    subs: &HashMap<String, SubEntry>,
) -> Option<String> {
    if subs.contains_key(sub_id) {
        Some(sub_id.to_string())
    } else {
        subs.keys().find(|client_id| sub_id.ends_with(client_id.as_str())).cloned()
    }
}

pub(super) enum ConnCmd {
    Subscribe {
        id: String,
        sql: String,
        options: SubscriptionOptions,
        request_initial_data: bool,
        event_tx: mpsc::Sender<Result<ChangeEvent>>,
        result_tx: oneshot::Sender<Result<(u64, Option<SeqId>)>>,
    },
    Unsubscribe {
        id: String,
        generation: Option<u64>,
    },
    Progress {
        id: String,
        generation: u64,
        seq_id: SeqId,
        advance_resume: bool,
    },
    ListSubscriptions {
        result_tx: oneshot::Sender<Vec<SubscriptionInfo>>,
    },
    Shutdown,
}

pub(super) struct SubEntry {
    pub(super) sql: String,
    pub(super) options: SubscriptionOptions,
    pub(super) request_initial_data: bool,
    pub(super) event_tx: mpsc::Sender<Result<ChangeEvent>>,
    pub(super) last_seq_id: Option<SeqId>,
    pub(super) consumed_seq_id: Option<SeqId>,
    pub(super) batch_seq_id: Option<SeqId>,
    pub(super) snapshot_end_seq: Option<SeqId>,
    pub(super) is_loading: bool,
    pub(super) generation: u64,
    pub(super) created_at_ms: u64,
    pub(super) last_event_time_ms: Option<u64>,
    pub(super) pending_result_tx: Option<oneshot::Sender<Result<(u64, Option<SeqId>)>>>,
    pub(super) ready_deadline: Option<TokioInstant>,
    pub(super) reconnect_resubscribe_pending: bool,
}
