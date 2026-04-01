//! Topic ack handler
//!
//! POST /v1/api/topics/ack - Acknowledge offset for consumer group

use actix_web::{post, web, HttpResponse, Responder};
use kalamdb_auth::AuthSessionExtractor;
use kalamdb_commons::Role;
use kalamdb_core::app_context::AppContext;
use kalamdb_session::AuthSession;
use std::sync::Arc;

use super::models::{AckRequest, AckResponse, TopicErrorResponse};

/// Check if role is allowed to consume/ack topics
/// Must be service, dba, or system role (NOT user)
fn is_topic_authorized(session: &AuthSession) -> bool {
    matches!(session.role(), Role::Service | Role::Dba | Role::System)
}

/// POST /v1/api/topics/ack - Acknowledge offset for consumer group
///
/// # Authentication
/// Requires Bearer token authentication.
///
/// # Authorization
/// Role must be `service`, `dba`, or `system` (NOT `user`).
#[post("/ack")]
pub async fn ack_handler(
    extractor: AuthSessionExtractor,
    body: web::Json<AckRequest>,
    app_context: web::Data<Arc<AppContext>>,
) -> impl Responder {
    let session: AuthSession = extractor.into();

    // Authorization check
    if !is_topic_authorized(&session) {
        return HttpResponse::Forbidden().json(TopicErrorResponse::forbidden(
            "Topic acknowledgment requires service, dba, or system role",
        ));
    }

    let topic_id = &body.topic_id;
    let group_id = &body.group_id;

    // Verify topic exists
    let topics_provider = app_context.system_tables().topics();
    match topics_provider.get_topic_by_id_async(topic_id).await {
        Ok(Some(_)) => {},
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

    // Commit the offset
    let topic_publisher = app_context.topic_publisher();
    if let Err(e) =
        topic_publisher.ack_offset(topic_id, group_id, body.partition_id, body.upto_offset)
    {
        return HttpResponse::InternalServerError().json(TopicErrorResponse::internal_error(
            &format!("Failed to acknowledge offset: {}", e),
        ));
    }

    HttpResponse::Ok().json(AckResponse {
        success: true,
        acknowledged_offset: body.upto_offset,
    })
}
