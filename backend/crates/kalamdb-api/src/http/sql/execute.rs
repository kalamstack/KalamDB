//! SQL execution handler for the `/v1/api/sql` REST API endpoint
//!
//! The main handler [`execute_sql_v1`] is decomposed into focused parsing and
//! execution helpers to keep each function small and testable:
//!
//! - [`parse_execute_statement`] – EXECUTE AS USER envelope parsing
//! - [`parse_incoming_payload`] – JSON / multipart payload extraction
//! - [`validate_sql_length`] – length guard
//! - [`split_and_prepare_statements`] – split → parse → classify pipeline
//! - [`classify_sql`] – single-statement classification with error mapping
//! - [`execute_file_upload_path`] – file-upload specific execution branch
//! - [`execute_batch_path`] – normal (possibly multi-statement) execution branch
//!
//! ## Performance notes
//!
//! - `extract_file_placeholders` is called **once** in the handler; the result is passed into
//!   `execute_file_upload_path` to avoid rescanning.
//! - `req_for_forward` (clones `sql` + `params`) is built lazily — only when forwarding is actually
//!   needed.
//! - In the batch loop, `params` is **moved** on the last iteration instead of cloned, eliminating
//!   one allocation per single-statement request (>90% of traffic).
//! - Content-type detection uses ASCII-case-insensitive comparison without allocating a lowercase
//!   copy.
//! - `EXECUTE AS USER` prefix detection uses a fixed-length slice comparison instead of uppercasing
//!   the entire input string.

use std::{sync::Arc, time::Instant};

use actix_web::{post, web, HttpRequest, HttpResponse, Responder};
use kalamdb_auth::AuthSessionExtractor;
use kalamdb_commons::models::NamespaceId;
use kalamdb_core::{
    app_context::AppContext,
    sql::{context::ExecutionContext, executor::SqlExecutor, SqlImpersonationService},
};
use kalamdb_jobs::health_monitor::record_activity_now;
use kalamdb_raft::GroupId;
use kalamdb_session::AuthSession;
use uuid::Uuid;

use super::{
    execution_paths::{execute_batch_path, execute_file_upload_path},
    file_utils::extract_file_placeholders,
    forward::forward_sql_if_follower,
    helpers::parse_scalar_params,
    models::{ErrorCode, QueryRequest, SqlResponse},
    request::{parse_incoming_payload, took_ms, validate_sql_length},
    statements::{
        authorized_username, split_and_prepare_statements, PreparedApiExecutionStatement,
    },
};
use crate::limiter::RateLimiter;

#[inline]
fn batch_requires_request_id(prepared_statements: &[PreparedApiExecutionStatement]) -> bool {
    prepared_statements.iter().any(|statement| {
        statement
            .prepared_statement
            .classified_statement
            .as_ref()
            .is_some_and(|classified| {
                matches!(
                    classified.kind(),
                    &kalamdb_sql::classifier::SqlStatementKind::BeginTransaction
                        | &kalamdb_sql::classifier::SqlStatementKind::CommitTransaction
                        | &kalamdb_sql::classifier::SqlStatementKind::RollbackTransaction
                )
            })
    })
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

/// POST /v1/api/sql - Execute SQL statement(s)
///
/// Accepts either JSON or multipart/form-data payloads.
///
/// - JSON: `sql` plus optional `params` and `namespace_id`.
/// - Multipart: `sql`, optional `params` (JSON array), optional `namespace_id`, and file parts
///   named `file:<placeholder>` for FILE("name") placeholders.
///
/// Multiple statements can be separated by semicolons and will be executed sequentially.
/// File uploads require a single SQL statement.
///
/// # Authentication
/// Requires authentication via Authorization header with Bearer token.
/// Basic auth is not supported for this endpoint - use tokens only.
#[post("/sql")]
pub async fn execute_sql_v1(
    extractor: AuthSessionExtractor,
    http_req: HttpRequest,
    payload: web::Payload,
    app_context: web::Data<Arc<AppContext>>,
    sql_executor: web::Data<Arc<SqlExecutor>>,
    rate_limiter: Option<web::Data<Arc<RateLimiter>>>,
) -> impl Responder {
    record_activity_now();
    let start_time = Instant::now();
    let session: AuthSession = extractor.into();

    // 1. Rate limiting
    if let Some(ref limiter) = rate_limiter {
        if !limiter.check_query_rate(session.user_id()) {
            log::warn!(
                "Rate limit exceeded for user: {} (queries per second)",
                session.user_id().as_str()
            );
            return HttpResponse::TooManyRequests().json(SqlResponse::error(
                ErrorCode::RateLimitExceeded,
                "Too many queries per second. Please slow down.",
                took_ms(start_time),
            ));
        }
    }

    // 2. Parse payload (JSON or multipart)
    let parsed_payload =
        match parse_incoming_payload(&http_req, payload, app_context.get_ref(), start_time).await {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    let sql = parsed_payload.sql;
    let params_json = parsed_payload.params;
    let namespace_id = parsed_payload.namespace_id;
    let files = parsed_payload.files;
    let is_multipart = parsed_payload.is_multipart;
    let files_present = files.as_ref().is_some_and(|f| !f.is_empty());

    // 3. Validate SQL length
    if let Err(resp) = validate_sql_length(&sql, start_time) {
        return resp;
    }

    // 4. Build execution context
    let default_namespace = namespace_id.clone().unwrap_or_else(|| NamespaceId::new("default"));
    let base_session = app_context.base_session_context();
    let mut exec_ctx = ExecutionContext::from_session(session, Arc::clone(&base_session))
        .with_namespace_id(default_namespace.clone());
    let is_meta_leader = app_context.executor().is_leader(GroupId::Meta).await;

    // 5. File uploads must go to the leader
    if files_present && !is_meta_leader {
        return HttpResponse::ServiceUnavailable().json(SqlResponse::error(
            ErrorCode::NotLeader,
            "File uploads must be sent to the current leader",
            took_ms(start_time),
        ));
    }

    // 6. Forward to leader if this node is a follower (non-file path).
    if !files_present && !is_meta_leader {
        if exec_ctx.request_id().is_none() {
            exec_ctx = exec_ctx.with_request_id(Uuid::now_v7().to_string());
        }

        let req_for_forward = QueryRequest {
            sql: sql.clone(),
            params: params_json.clone(),
            namespace_id: namespace_id.clone(),
        };
        if let Some(response) = forward_sql_if_follower(
            &http_req,
            &req_for_forward,
            app_context.get_ref(),
            &default_namespace,
            exec_ctx.user_id(),
            exec_ctx.request_id(),
        )
        .await
        {
            return response;
        }
    }

    // 7. Parse query parameters
    let params = match parse_scalar_params(&params_json) {
        Ok(p) => p,
        Err(err) => {
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidParameter,
                &err,
                took_ms(start_time),
            ));
        },
    };

    // 8. Split, parse, and classify SQL statements
    let prepared_statements =
        match split_and_prepare_statements(&sql, &exec_ctx, sql_executor.get_ref(), start_time) {
            Ok(stmts) => stmts,
            Err(resp) => return resp,
        };

    if exec_ctx.request_id().is_none() && batch_requires_request_id(&prepared_statements) {
        exec_ctx = exec_ctx.with_request_id(Uuid::now_v7().to_string());
    }

    let auth_username = authorized_username(&exec_ctx);
    let impersonation_service = SqlImpersonationService::new(Arc::clone(app_context.get_ref()));

    // 9. Reject params with multi-statement batches
    if !params.is_empty() && prepared_statements.len() > 1 {
        return HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::ParamsWithBatch,
            "Parameters not supported with multi-statement batches",
            took_ms(start_time),
        ));
    }

    // 10. Dispatch to file-upload or batch execution path.
    let required_files = extract_file_placeholders(&sql);
    if !required_files.is_empty() || files_present {
        let schema_registry = app_context.schema_registry();
        return execute_file_upload_path(
            is_multipart,
            files,
            &required_files,
            &prepared_statements,
            app_context.get_ref(),
            sql_executor.get_ref(),
            &exec_ctx,
            &impersonation_service,
            &auth_username,
            &default_namespace,
            params,
            &schema_registry,
            start_time,
        )
        .await;
    }

    // Build the forwarding request for the batch path (needed for
    // NOT_LEADER auto-forward during execution).  We already know we
    // did NOT forward above, so we only allocate here and only for
    // the non-file path.
    let req_for_forward = QueryRequest {
        sql,
        params: params_json,
        namespace_id,
    };

    execute_batch_path(
        &prepared_statements,
        app_context.get_ref(),
        sql_executor.get_ref(),
        &exec_ctx,
        &impersonation_service,
        &auth_username,
        params,
        &http_req,
        &req_for_forward,
        start_time,
    )
    .await
}
