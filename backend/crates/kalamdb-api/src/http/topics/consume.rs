//! Topic consume handler
//!
//! POST /v1/api/topics/consume - Consume messages from a topic
//!
//! Uses the standard topic_message_schema for consistent field structure
//! across SQL CONSUME and HTTP API responses.

use actix_web::{post, web, HttpResponse, Responder};
use kalamdb_auth::AuthSessionExtractor;
use kalamdb_commons::Role;
use kalamdb_core::app_context::AppContext;
use kalamdb_session::AuthSession;
use std::sync::Arc;

use super::models::{
    ConsumeRequest, ConsumeResponse, StartPosition, TopicErrorResponse, TopicMessage,
};

/// Check if role is allowed to consume/ack topics
/// Must be service, dba, or system role (NOT user)
fn is_topic_authorized(session: &AuthSession) -> bool {
    matches!(session.role(), Role::Service | Role::Dba | Role::System)
}

/// POST /v1/api/topics/consume - Consume messages from a topic
///
/// Long polling endpoint that waits for messages or timeout.
///
/// # Schema
/// Response messages follow the standard `topic_message_schema()` field structure:
/// - topic: Utf8 (NOT NULL) - Topic name
/// - partition: Int32 (NOT NULL) - Partition ID  
/// - offset: Int64 (NOT NULL) - Message offset
/// - key: Utf8 (NULLABLE) - Optional message key
/// - payload: Binary (NOT NULL) - Message payload bytes (base64-encoded in JSON)
/// - timestamp_ms: Int64 (NOT NULL) - Message timestamp in milliseconds
///
/// # Authentication
/// Requires Bearer token authentication.
///
/// # Authorization
/// Role must be `service`, `dba`, or `system` (NOT `user`).
#[post("/consume")]
pub async fn consume_handler(
    extractor: AuthSessionExtractor,
    body: web::Json<ConsumeRequest>,
    app_context: web::Data<Arc<AppContext>>,
) -> impl Responder {
    let session: AuthSession = extractor.into();

    // Authorization check
    if !is_topic_authorized(&session) {
        return HttpResponse::Forbidden().json(TopicErrorResponse::forbidden(
            "Topic consumption requires service, dba, or system role",
        ));
    }

    let topic_id = &body.topic_id;
    let group_id = &body.group_id;

    // Verify topic exists
    let topics_provider = app_context.system_tables().topics();
    let topic = match topics_provider.get_topic_by_id_async(topic_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return HttpResponse::NotFound().json(TopicErrorResponse::not_found(&format!(
                "Topic '{}' does not exist",
                topic_id
            )));
        },
        Err(e) => {
            return HttpResponse::InternalServerError().json(TopicErrorResponse::internal_error(
                &format!("Failed to lookup topic: {}", e),
            ));
        },
    };

    let topic_publisher = app_context.topic_publisher();

    // Determine start offset based on position.
    //
    // All positions first check the consumer group's committed offset.
    // If a committed offset exists, we resume from there (last_acked + 1).
    // The position only matters when no offset has been committed yet:
    //   - Earliest: start from offset 0 (replay all history)
    //   - Latest: start from high-water mark (last offset + 1)
    //   - Offset: start from the explicit offset
    let committed_offset =
        topic_publisher.get_group_offsets(topic_id, group_id).ok().and_then(|offsets| {
            offsets
                .iter()
                .find(|o| o.partition_id == body.partition_id)
                .map(|o| o.last_acked_offset + 1)
        });

    let start_offset = match committed_offset {
        Some(committed) => committed,
        None => match &body.start {
            StartPosition::Offset { offset } => *offset,
            StartPosition::Earliest => 0,
            StartPosition::Latest => {
                match topic_publisher.latest_offset(topic_id, body.partition_id) {
                    Ok(Some(last_offset)) => last_offset + 1,
                    Ok(None) => 0,
                    Err(e) => {
                        return HttpResponse::InternalServerError().json(
                            TopicErrorResponse::internal_error(&format!(
                                "Failed to resolve latest offset: {}",
                                e
                            )),
                        );
                    },
                }
            },
        },
    };

    // Fetch messages
    let messages = match topic_publisher.fetch_messages_for_group(
        topic_id,
        group_id,
        body.partition_id,
        start_offset,
        body.limit as usize,
    ) {
        Ok(msgs) => msgs,
        Err(e) => {
            return HttpResponse::InternalServerError().json(TopicErrorResponse::internal_error(
                &format!("Failed to fetch messages: {}", e),
            ));
        },
    };

    // Convert to response format (matching topic_message_schema fields)
    // Schema fields: topic, partition, offset, key, payload, timestamp_ms
    // Cache user_id → username lookups to avoid redundant RocksDB reads within a single batch.
    let mut user_cache: std::collections::HashMap<kalamdb_commons::models::UserId, Option<String>> =
        std::collections::HashMap::new();

    // Clone topic_id once for the entire response batch.
    let batch_topic_id = topic.topic_id.clone();
    // Pre-create the base64 engine reference once.
    let b64_engine = &base64::engine::general_purpose::STANDARD;

    let response_messages: Vec<TopicMessage> = messages
        .iter()
        .map(|msg| {
            use base64::Engine;
            // Resolve user_id to username for the consumer (cached)
            let username = msg.user_id.as_ref().and_then(|uid| {
                user_cache
                    .entry(uid.clone())
                    .or_insert_with(|| {
                        app_context
                            .system_tables()
                            .users()
                            .get_user_by_id(uid)
                            .ok()
                            .flatten()
                            .map(|u| u.username.into_string())
                    })
                    .clone()
            });
            TopicMessage {
                topic_id: batch_topic_id.clone(),
                partition_id: msg.partition_id,
                offset: msg.offset,
                payload: b64_engine.encode(&msg.payload),
                key: msg.key.clone(),
                timestamp_ms: msg.timestamp_ms,
                username,
                op: match msg.op {
                    kalamdb_commons::models::TopicOp::Insert => "Insert".to_owned(),
                    kalamdb_commons::models::TopicOp::Update => "Update".to_owned(),
                    kalamdb_commons::models::TopicOp::Delete => "Delete".to_owned(),
                },
            }
        })
        .collect();

    let next_offset = messages.last().map(|m| m.offset + 1).unwrap_or(start_offset);
    let has_more = messages.len() == body.limit as usize;

    HttpResponse::Ok().json(ConsumeResponse {
        messages: response_messages,
        next_offset,
        has_more,
    })
}
