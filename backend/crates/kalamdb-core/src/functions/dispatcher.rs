//! Durable topic trigger delivery.

use std::sync::Arc;

use datafusion::scalar::ScalarValue;
use kalamdb_commons::{
    models::{ConsumerGroupId, TriggerAttemptId, UserId},
    Role,
};
use kalamdb_functions::RoutineValue;
use kalamdb_system::CatalogTriggerAttempt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::{call_types::FunctionCallOrigin, executor::FunctionService};
use crate::{app_context::AppContext, error::KalamDbError, sql::context::ExecutionContext};

const LEASE_MS: i64 = 30_000;

pub struct TriggerDispatcherRuntime {
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl TriggerDispatcherRuntime {
    pub fn start(app: Arc<AppContext>) -> Self {
        let cancel = CancellationToken::new();
        let handle = start_trigger_dispatcher(Arc::clone(&app), cancel.clone());
        Self { cancel, handle }
    }

    pub async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.handle.await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryAction {
    Deliver { next_attempt: u32 },
    AckSucceeded,
    AckDlq,
    Skip,
}

/// Background loop that delivers topic messages to enabled triggers.
pub fn start_trigger_dispatcher(
    app: Arc<AppContext>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = dispatch_once(&app).await {
                        tracing::warn!("trigger dispatcher: {error}");
                    }
                }
            }
        }
    })
}

pub async fn dispatch_once(app: &Arc<AppContext>) -> Result<usize, KalamDbError> {
    let triggers = app
        .system_tables()
        .catalog_stores()
        .list_triggers()
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    let mut delivered = 0;
    for trigger in triggers.into_iter().filter(|trigger| trigger.enabled) {
        delivered += dispatch_trigger(app, &trigger).await?;
    }
    Ok(delivered)
}

pub(crate) fn delivery_action(
    previous: Option<&CatalogTriggerAttempt>,
    now_ms: i64,
    retry_backoff_ms: i64,
    local_owner: &str,
) -> DeliveryAction {
    let Some(previous) = previous else {
        return DeliveryAction::Deliver { next_attempt: 1 };
    };
    match previous.status.as_str() {
        "succeeded" => DeliveryAction::AckSucceeded,
        "dlq" => DeliveryAction::AckDlq,
        "running" => {
            let lease_valid = previous.lease_expires_at.unwrap_or(0) > now_ms;
            let foreign_owner =
                previous.lease_owner.as_deref().is_some_and(|owner| owner != local_owner);
            if lease_valid && foreign_owner {
                DeliveryAction::Skip
            } else {
                DeliveryAction::Deliver {
                    next_attempt: previous.attempt as u32 + 1,
                }
            }
        },
        "retry" => {
            if now_ms < previous.updated_at.saturating_add(retry_backoff_ms) {
                DeliveryAction::Skip
            } else {
                DeliveryAction::Deliver {
                    next_attempt: previous.attempt as u32 + 1,
                }
            }
        },
        _ => DeliveryAction::Deliver {
            next_attempt: previous.attempt as u32 + 1,
        },
    }
}

fn lease_owner() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .unwrap_or_else(|| "local".to_string())
}

pub(crate) fn trigger_group_id(trigger_id: &str) -> ConsumerGroupId {
    ConsumerGroupId::new(format!("trigger:{trigger_id}"))
}

async fn dispatch_trigger(
    app: &Arc<AppContext>,
    trigger: &kalamdb_system::CatalogTrigger,
) -> Result<usize, KalamDbError> {
    let topic = app
        .system_tables()
        .topics()
        .get_topic_by_id(&trigger.topic_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    let Some(topic) = topic else {
        return Ok(0);
    };
    let mut delivered = 0;
    for partition_id in 0..topic.partitions {
        if process_partition(app, trigger, partition_id).await? {
            delivered += 1;
        }
    }
    Ok(delivered)
}

async fn process_partition(
    app: &Arc<AppContext>,
    trigger: &kalamdb_system::CatalogTrigger,
    partition_id: u32,
) -> Result<bool, KalamDbError> {
    let publisher = app.topic_publisher();
    let group_id = trigger_group_id(trigger.trigger_id.as_str());
    let next_offset = next_offset_for(app, trigger, partition_id, &group_id)?;
    let messages = match publisher.fetch_messages(&trigger.topic_id, partition_id, next_offset, 1) {
        Ok(messages) => messages,
        Err(error) if error.to_string().contains("OffsetOutOfRange") => {
            let earliest = publisher
                .earliest_available_offset(&trigger.topic_id, partition_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            publisher
                .reset_group_offset(&trigger.topic_id, &group_id, partition_id, earliest)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            publisher
                .fetch_messages(&trigger.topic_id, partition_id, earliest, 1)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        },
        Err(error) => return Err(KalamDbError::ExecutionError(error.to_string())),
    };
    let Some(message) = messages.into_iter().next() else {
        return Ok(false);
    };

    let event_id = format!("{}:{partition_id}:{}", trigger.topic_id.as_str(), message.offset);
    let stores = app.system_tables().catalog_stores();
    let previous = stores
        .list_trigger_attempts()
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .into_iter()
        .filter(|attempt| {
            attempt.trigger_id == trigger.trigger_id
                && attempt.partition_id == partition_id as i32
                && attempt.offset == message.offset as i64
        })
        .max_by_key(|attempt| attempt.attempt);

    let now = chrono::Utc::now().timestamp_millis();
    let owner = lease_owner();
    match delivery_action(previous.as_ref(), now, trigger.retry_backoff_ms, &owner) {
        DeliveryAction::Skip => return Ok(false),
        DeliveryAction::AckSucceeded | DeliveryAction::AckDlq => {
            publisher
                .ack_offset(&trigger.topic_id, &group_id, partition_id, message.offset)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            return Ok(false);
        },
        DeliveryAction::Deliver { next_attempt } => {
            deliver_attempt(
                app,
                trigger,
                partition_id,
                message.offset,
                &message.payload,
                &event_id,
                next_attempt,
                previous.as_ref().map(|row| row.created_at).unwrap_or(now),
                &owner,
            )
            .await
        },
    }
}

async fn deliver_attempt(
    app: &Arc<AppContext>,
    trigger: &kalamdb_system::CatalogTrigger,
    partition_id: u32,
    offset: u64,
    payload_bytes: &[u8],
    event_id: &str,
    attempt_no: u32,
    created_at: i64,
    owner: &str,
) -> Result<bool, KalamDbError> {
    let publisher = app.topic_publisher();
    let group_id = trigger_group_id(trigger.trigger_id.as_str());
    let stores = app.system_tables().catalog_stores();
    let now = chrono::Utc::now().timestamp_millis();
    let attempt_id = TriggerAttemptId::new(&trigger.trigger_id, partition_id, offset, attempt_no)
        .map_err(KalamDbError::InvalidSql)?;
    stores
        .upsert_trigger_attempt(CatalogTriggerAttempt {
            attempt_id: attempt_id.clone(),
            trigger_id: trigger.trigger_id.clone(),
            topic_id: trigger.topic_id.clone(),
            partition_id: partition_id as i32,
            offset: offset as i64,
            event_id: event_id.to_string(),
            attempt: attempt_no as i32,
            status: "running".to_string(),
            lease_owner: Some(owner.to_string()),
            lease_expires_at: Some(now + LEASE_MS),
            error: None,
            created_at,
            updated_at: now,
        })
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;

    let payload = decode_payload(payload_bytes)?;
    let principal_role = principal_role(app, &trigger.principal_user_id)?;
    let exec_ctx = ExecutionContext::new(
        trigger.principal_user_id.clone(),
        principal_role,
        app.base_session_context(),
    );
    let invoke = FunctionService::invoke(
        Arc::clone(app),
        &exec_ctx,
        FunctionCallOrigin::Topic {
            topic_name: trigger.topic_id.as_str().to_string(),
            event_id: event_id.to_string(),
            partition: partition_id,
            offset,
            attempt: attempt_no,
        },
        trigger.routine_id.clone(),
        vec![payload],
    )
    .await;

    let now = chrono::Utc::now().timestamp_millis();
    match invoke {
        Ok(_) => {
            stores
                .upsert_trigger_attempt(CatalogTriggerAttempt {
                    attempt_id,
                    trigger_id: trigger.trigger_id.clone(),
                    topic_id: trigger.topic_id.clone(),
                    partition_id: partition_id as i32,
                    offset: offset as i64,
                    event_id: event_id.to_string(),
                    attempt: attempt_no as i32,
                    status: "succeeded".to_string(),
                    lease_owner: None,
                    lease_expires_at: None,
                    error: None,
                    created_at,
                    updated_at: now,
                })
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            publisher
                .ack_offset(&trigger.topic_id, &group_id, partition_id, offset)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            Ok(true)
        },
        Err(error) => {
            let poison =
                attempt_no as i32 >= trigger.retries && trigger.retries > 0 || trigger.retries == 0;
            let status = if poison { "dlq" } else { "retry" };
            stores
                .upsert_trigger_attempt(CatalogTriggerAttempt {
                    attempt_id,
                    trigger_id: trigger.trigger_id.clone(),
                    topic_id: trigger.topic_id.clone(),
                    partition_id: partition_id as i32,
                    offset: offset as i64,
                    event_id: event_id.to_string(),
                    attempt: attempt_no as i32,
                    status: status.to_string(),
                    lease_owner: None,
                    lease_expires_at: None,
                    error: Some(error.to_string()),
                    created_at,
                    updated_at: now,
                })
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            if poison {
                publisher
                    .ack_offset(&trigger.topic_id, &group_id, partition_id, offset)
                    .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            }
            Ok(false)
        },
    }
}

fn next_offset_for(
    app: &AppContext,
    trigger: &kalamdb_system::CatalogTrigger,
    partition_id: u32,
    group_id: &ConsumerGroupId,
) -> Result<u64, KalamDbError> {
    let offsets = app
        .topic_publisher()
        .get_group_offsets(&trigger.topic_id, group_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    if let Some(offset) = offsets.iter().find(|row| row.partition_id == partition_id) {
        return Ok(offset.last_acked_offset.saturating_add(1));
    }
    Ok(0)
}

fn decode_payload(bytes: &[u8]) -> Result<RoutineValue, KalamDbError> {
    let json = match kalamdb_serialization::decode_object::<Value>(bytes) {
        Ok(json) => json,
        Err(_) => serde_json::from_slice(bytes).map_err(|error| {
            KalamDbError::ExecutionError(format!("failed to decode trigger payload: {error}"))
        })?,
    };
    Ok(RoutineValue::json(ScalarValue::Utf8(Some(json.to_string()))))
}

fn principal_role(app: &AppContext, user_id: &UserId) -> Result<Role, KalamDbError> {
    if *user_id == UserId::system() {
        return Ok(Role::System);
    }
    let user = app
        .system_tables()
        .users()
        .get_user_by_id(user_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    Ok(user.map(|user| user.role).unwrap_or(Role::User))
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::{TopicId, TriggerId};
    use kalamdb_system::CatalogTriggerAttempt;

    use super::*;

    fn attempt(
        status: &str,
        attempt: i32,
        updated_at: i64,
        lease_owner: Option<&str>,
        lease_expires_at: Option<i64>,
    ) -> CatalogTriggerAttempt {
        CatalogTriggerAttempt {
            attempt_id: TriggerAttemptId::from("chat.process:0:1:1"),
            trigger_id: TriggerId::from("chat.process"),
            topic_id: TopicId::new("chat.events"),
            partition_id: 0,
            offset: 1,
            event_id: "chat.events:0:1".to_string(),
            attempt,
            status: status.to_string(),
            lease_owner: lease_owner.map(str::to_string),
            lease_expires_at,
            error: None,
            created_at: 0,
            updated_at,
        }
    }

    #[test]
    fn crash_after_commit_acks_without_redelivery() {
        let previous = attempt("succeeded", 1, 10, None, None);
        assert_eq!(
            delivery_action(Some(&previous), 1_000, 1_000, "local"),
            DeliveryAction::AckSucceeded
        );
    }

    #[test]
    fn poison_dlq_is_acked() {
        let previous = attempt("dlq", 5, 10, None, None);
        assert_eq!(delivery_action(Some(&previous), 1_000, 1_000, "local"), DeliveryAction::AckDlq);
    }

    #[test]
    fn foreign_lease_skips_until_expiry() {
        let previous = attempt("running", 1, 10, Some("other-node"), Some(5_000));
        assert_eq!(delivery_action(Some(&previous), 1_000, 1_000, "local"), DeliveryAction::Skip);
        assert_eq!(
            delivery_action(Some(&previous), 6_000, 1_000, "local"),
            DeliveryAction::Deliver { next_attempt: 2 }
        );
    }

    #[test]
    fn same_owner_steals_running_lease_after_crash() {
        let previous = attempt("running", 1, 10, Some("local"), Some(5_000));
        assert_eq!(
            delivery_action(Some(&previous), 1_000, 1_000, "local"),
            DeliveryAction::Deliver { next_attempt: 2 }
        );
    }

    #[test]
    fn retry_honors_backoff() {
        let previous = attempt("retry", 2, 1_000, None, None);
        assert_eq!(delivery_action(Some(&previous), 1_500, 1_000, "local"), DeliveryAction::Skip);
        assert_eq!(
            delivery_action(Some(&previous), 2_000, 1_000, "local"),
            DeliveryAction::Deliver { next_attempt: 3 }
        );
    }

    #[test]
    fn first_delivery_is_attempt_one() {
        assert_eq!(
            delivery_action(None, 0, 1_000, "local"),
            DeliveryAction::Deliver { next_attempt: 1 }
        );
    }

    #[test]
    fn typed_topic_payload_is_json_object_for_v8() {
        let encoded =
            kalamdb_serialization::encode_object(&serde_json::json!({ "id": 7 })).unwrap();
        let value = decode_payload(encoded.as_slice()).expect("decode payload");
        assert!(value.json_sql, "trigger payloads must JSON.parse in V8 so input.id works");
        let ScalarValue::Utf8(Some(text)) = value.value else {
            panic!("expected utf8 json payload, got {:?}", value.value);
        };
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("json payload");
        assert_eq!(parsed["id"], 7);
    }
}
