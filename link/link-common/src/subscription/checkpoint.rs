#[cfg(any(feature = "tokio-runtime", test))]
use std::collections::VecDeque;

#[cfg(any(feature = "tokio-runtime", test))]
use crate::models::BatchStatus;
#[cfg(any(feature = "tokio-runtime", feature = "wasm", test))]
use crate::{models::ChangeEvent, seq_id::SeqId, seq_tracking};

#[cfg(any(feature = "tokio-runtime", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EventProgress {
    pub(crate) seq_id: SeqId,
    pub(crate) advance_resume: bool,
}

#[cfg(any(feature = "tokio-runtime", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchEnvelope {
    pub(crate) status: BatchStatus,
    pub(crate) has_more: bool,
    pub(crate) last_seq_id: Option<SeqId>,
}

#[cfg(any(feature = "tokio-runtime", test))]
pub(crate) fn batch_envelope(event: &ChangeEvent) -> Option<BatchEnvelope> {
    match event {
        ChangeEvent::Ack { batch_control, .. }
        | ChangeEvent::InitialDataBatch { batch_control, .. } => Some(BatchEnvelope {
            status: batch_control.status,
            has_more: batch_control.has_more,
            last_seq_id: batch_control.last_seq_id,
        }),
        _ => None,
    }
}

#[cfg(any(feature = "tokio-runtime", test))]
pub(crate) fn subscription_start_ready(event: &ChangeEvent) -> bool {
    batch_envelope(event).is_some_and(|batch| batch.status == BatchStatus::Ready)
}

#[cfg(any(feature = "tokio-runtime", test))]
pub(crate) fn event_progress(event: &ChangeEvent) -> Option<EventProgress> {
    match event {
        ChangeEvent::InitialDataBatch { rows, .. } if subscription_start_ready(event) => {
            let batch = batch_envelope(event);
            let seq_id = seq_tracking::extract_max_seq(rows)
                .or_else(|| batch.and_then(|batch| batch.last_seq_id))?;
            Some(EventProgress {
                seq_id,
                advance_resume: false,
            })
        },
        ChangeEvent::Insert { rows, .. } | ChangeEvent::Update { rows, .. } => {
            seq_tracking::extract_max_seq(rows).map(|seq_id| EventProgress {
                seq_id,
                advance_resume: true,
            })
        },
        ChangeEvent::Delete { old_rows, .. } => {
            seq_tracking::extract_max_seq(old_rows).map(|seq_id| EventProgress {
                seq_id,
                advance_resume: true,
            })
        },
        _ => None,
    }
}

#[cfg(any(feature = "tokio-runtime", test))]
pub(crate) fn final_resume_seq(
    requested_from: Option<SeqId>,
    consumed_seq_id: Option<SeqId>,
) -> Option<SeqId> {
    match (requested_from, consumed_seq_id) {
        (Some(requested), Some(consumed)) => Some(requested.max(consumed)),
        (requested, consumed) => requested.or(consumed),
    }
}

#[cfg(any(feature = "tokio-runtime", feature = "wasm", test))]
pub(crate) fn filter_replayed_event(
    event: ChangeEvent,
    resume_from: Option<SeqId>,
) -> Option<ChangeEvent> {
    let Some(from) = resume_from else {
        return Some(event);
    };

    match event {
        ChangeEvent::InitialDataBatch {
            subscription_id,
            mut rows,
            batch_control,
        } => {
            let removed = seq_tracking::retain_rows_after(&mut rows, from);
            if removed > 0 {
                log::debug!(
                    "[kalam-sdk] [{}] Filtered {} stale initial row(s) at from={}",
                    subscription_id,
                    removed,
                    from
                );
            }

            Some(ChangeEvent::InitialDataBatch {
                subscription_id,
                rows,
                batch_control,
            })
        },
        ChangeEvent::Insert {
            subscription_id,
            mut rows,
        } => {
            let removed = seq_tracking::retain_rows_after(&mut rows, from);
            if removed > 0 {
                log::debug!(
                    "[kalam-sdk] [{}] Filtered {} stale insert row(s) at from={}",
                    subscription_id,
                    removed,
                    from
                );
            }
            if rows.is_empty() {
                None
            } else {
                Some(ChangeEvent::Insert {
                    subscription_id,
                    rows,
                })
            }
        },
        ChangeEvent::Update {
            subscription_id,
            mut rows,
            mut old_rows,
        } => {
            let removed_new = seq_tracking::retain_rows_after(&mut rows, from);
            let removed_old = seq_tracking::retain_rows_after(&mut old_rows, from);
            let removed = removed_new.max(removed_old);
            if removed > 0 {
                log::debug!(
                    "[kalam-sdk] [{}] Filtered {} stale update row(s) at from={}",
                    subscription_id,
                    removed,
                    from
                );
            }
            if rows.is_empty() && old_rows.is_empty() {
                None
            } else {
                Some(ChangeEvent::Update {
                    subscription_id,
                    rows,
                    old_rows,
                })
            }
        },
        ChangeEvent::Delete {
            subscription_id,
            mut old_rows,
        } => {
            let removed = seq_tracking::retain_rows_after(&mut old_rows, from);
            if removed > 0 {
                log::debug!(
                    "[kalam-sdk] [{}] Filtered {} stale delete row(s) at from={}",
                    subscription_id,
                    removed,
                    from
                );
            }
            if old_rows.is_empty() {
                None
            } else {
                Some(ChangeEvent::Delete {
                    subscription_id,
                    old_rows,
                })
            }
        },
        _ => Some(event),
    }
}

#[cfg(any(feature = "tokio-runtime", test))]
pub(crate) fn buffer_event(
    event_queue: &mut VecDeque<ChangeEvent>,
    buffered_changes: &mut Vec<ChangeEvent>,
    is_loading: &mut bool,
    resume_from: Option<SeqId>,
    event: ChangeEvent,
) {
    let Some(event) = filter_replayed_event(event, resume_from) else {
        return;
    };

    if let Some(batch) = batch_envelope(&event) {
        *is_loading = batch.status != BatchStatus::Ready;
        event_queue.push_back(event);
        if !*is_loading {
            for buffered in buffered_changes.drain(..) {
                event_queue.push_back(buffered);
            }
        }
        return;
    }

    match event {
        ChangeEvent::Insert { .. } | ChangeEvent::Update { .. } | ChangeEvent::Delete { .. } => {
            if *is_loading {
                buffered_changes.push(event);
            } else {
                event_queue.push_back(event);
            }
        },
        _ => event_queue.push_back(event),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{BatchControl, KalamCellValue, RowData};

    fn batch_control(status: BatchStatus) -> BatchControl {
        BatchControl {
            batch_num: 1,
            has_more: false,
            status,
            last_seq_id: None,
        }
    }

    fn row(id: &str, seq: i64) -> RowData {
        let mut row = RowData::new();
        row.insert("id".to_string(), KalamCellValue::text(id));
        row.insert("_seq".to_string(), KalamCellValue::text(seq.to_string()));
        row
    }

    #[test]
    fn filters_resumed_delete_with_empty_result() {
        let event = ChangeEvent::Delete {
            subscription_id: "sub-1".to_string(),
            old_rows: vec![row("1", 10)],
        };

        assert!(filter_replayed_event(event, Some(SeqId::from_i64(10))).is_none());
    }

    #[test]
    fn progress_marks_ready_initial_batch_without_advancing_resume() {
        let event = ChangeEvent::InitialDataBatch {
            subscription_id: "sub-1".to_string(),
            rows: vec![row("1", 11)],
            batch_control: batch_control(BatchStatus::Ready),
        };

        assert_eq!(
            event_progress(&event),
            Some(EventProgress {
                seq_id: SeqId::from_i64(11),
                advance_resume: false,
            })
        );
    }

    #[test]
    fn buffering_flushes_live_changes_after_ready_snapshot() {
        let mut event_queue = VecDeque::new();
        let mut buffered = Vec::new();
        let mut is_loading = true;

        buffer_event(
            &mut event_queue,
            &mut buffered,
            &mut is_loading,
            None,
            ChangeEvent::Insert {
                subscription_id: "sub-1".to_string(),
                rows: vec![row("live", 12)],
            },
        );
        assert!(event_queue.is_empty());
        assert_eq!(buffered.len(), 1);

        buffer_event(
            &mut event_queue,
            &mut buffered,
            &mut is_loading,
            None,
            ChangeEvent::InitialDataBatch {
                subscription_id: "sub-1".to_string(),
                rows: vec![row("snap", 11)],
                batch_control: batch_control(BatchStatus::Ready),
            },
        );

        assert_eq!(event_queue.len(), 2);
        assert!(buffered.is_empty());
        assert!(!is_loading);
    }
}
