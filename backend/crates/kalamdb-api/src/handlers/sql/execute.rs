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
//! - `extract_file_placeholders` is called **once** in the handler; the result
//!   is passed into `execute_file_upload_path` to avoid rescanning.
//! - `req_for_forward` (clones `sql` + `params`) is built lazily — only when
//!   forwarding is actually needed.
//! - In the batch loop, `params` is **moved** on the last iteration instead of
//!   cloned, eliminating one allocation per single-statement request (>90% of
//!   traffic).
//! - Content-type detection uses ASCII-case-insensitive comparison without
//!   allocating a lowercase copy.
//! - `EXECUTE AS USER` prefix detection uses a fixed-length slice comparison
//!   instead of uppercasing the entire input string.

use actix_multipart::Multipart;
use actix_web::{post, web, Either, FromRequest, HttpRequest, HttpResponse, Responder};
use bytes::Bytes;
use kalamdb_auth::AuthSessionExtractor;
use kalamdb_commons::models::{NamespaceId, TableId, UserId, Username};
use kalamdb_commons::schemas::TableType;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::schema_registry::SchemaRegistry;
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_core::sql::executor::{PreparedExecutionStatement, ScalarValue, SqlExecutor};
use kalamdb_core::sql::SqlImpersonationService;
use kalamdb_raft::GroupId;
use kalamdb_session::AuthSession;
use kalamdb_system::FileSubfolderState;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::file_utils::{
    extract_file_placeholders, parse_sql_payload, stage_and_finalize_files,
    substitute_file_placeholders,
};
use super::forward::{forward_sql_if_follower, handle_not_leader_error};
use super::helpers::{
    cleanup_files, execute_single_statement, execute_single_statement_raw,
    execution_result_to_query_result, parse_scalar_params, stream_sql_rows_response,
};
use super::models::{ErrorCode, QueryRequest, QueryResult, SqlResponse};
use crate::limiter::RateLimiter;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ParsedExecutionStatement {
    sql: String,
    execute_as_username: Option<Username>,
}

#[derive(Debug)]
struct PreparedApiExecutionStatement {
    execute_as_username: Option<Username>,
    prepared_statement: PreparedExecutionStatement,
}

// ---------------------------------------------------------------------------
// Tiny helpers
// ---------------------------------------------------------------------------

/// Compute elapsed milliseconds since `start_time`.
#[inline]
fn took_ms(start_time: Instant) -> f64 {
    start_time.elapsed().as_secs_f64() * 1000.0
}

fn authorized_username(exec_ctx: &ExecutionContext) -> Username {
    if let Some(username) = &exec_ctx.user_context().username {
        return username.clone();
    }
    Username::from(exec_ctx.user_id().as_str())
}

#[inline]
fn resolve_result_username(
    authorized_username: &Username,
    execute_as_username: Option<&Username>,
) -> Username {
    execute_as_username.cloned().unwrap_or_else(|| authorized_username.clone())
}

async fn resolve_execute_as_user(
    statement: &PreparedApiExecutionStatement,
    impersonation_service: &SqlImpersonationService,
    exec_ctx: &ExecutionContext,
) -> Result<Option<UserId>, String> {
    match statement.execute_as_username.as_ref() {
        Some(target_username) => impersonation_service
            .resolve_execute_as_user(
                exec_ctx.user_id(),
                exec_ctx.user_role(),
                target_username.as_str(),
            )
            .await
            .map(Some)
            .map_err(|err| err.to_string()),
        None => Ok(None),
    }
}

/// Check whether `content_type` starts with "multipart/form-data" using
/// ASCII-case-insensitive comparison, avoiding the allocation that
/// `to_ascii_lowercase()` would cause.
#[inline]
fn is_multipart_content_type(content_type: &str) -> bool {
    const PREFIX: &[u8] = b"multipart/form-data";
    content_type.len() >= PREFIX.len()
        && content_type.as_bytes()[..PREFIX.len()].eq_ignore_ascii_case(PREFIX)
}

// ---------------------------------------------------------------------------
// EXECUTE AS USER parsing
// ---------------------------------------------------------------------------

/// Extract the username and parenthesised body from an `EXECUTE AS USER`
/// envelope.  Delegates to the canonical parser in `kalamdb_sql::execute_as`.
fn parse_execute_statement(statement: &str) -> Result<ParsedExecutionStatement, String> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err("Empty SQL statement".to_string());
    }

    match kalamdb_sql::execute_as::parse_execute_as(statement)? {
        Some(envelope) => Ok(ParsedExecutionStatement {
            sql: envelope.inner_sql,
            execute_as_username: Some(
                Username::try_new(&envelope.username)
                    .map_err(|e| format!("Invalid execute-as username: {}", e))?,
            ),
        }),
        None => Ok(ParsedExecutionStatement {
            sql: trimmed.to_string(),
            execute_as_username: None,
        }),
    }
}

// ---------------------------------------------------------------------------
// Payload extraction
// ---------------------------------------------------------------------------

/// Parse the incoming HTTP payload (JSON **or** multipart/form-data) into a
/// unified [`super::models::ParsedSqlPayload`].
///
/// Returns `Err(HttpResponse)` with the appropriate status on parse failure.
async fn parse_incoming_payload(
    http_req: &HttpRequest,
    payload: web::Payload,
    app_context: &AppContext,
    start_time: Instant,
) -> Result<super::models::ParsedSqlPayload, HttpResponse> {
    let content_type = http_req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let is_multipart = is_multipart_content_type(content_type);

    let mut payload = payload.into_inner();

    if is_multipart {
        let multipart = Multipart::new(http_req.headers(), payload);
        parse_sql_payload(Either::Right(multipart), &app_context.config().files)
            .await
            .map_err(|e| {
                HttpResponse::BadRequest().json(SqlResponse::error(
                    e.code,
                    &e.message,
                    took_ms(start_time),
                ))
            })
    } else {
        let json =
            web::Json::<QueryRequest>::from_request(http_req, &mut payload)
                .await
                .map_err(|e| {
                    HttpResponse::BadRequest().json(SqlResponse::error(
                        ErrorCode::InvalidInput,
                        &format!("Invalid JSON payload: {}", e),
                        took_ms(start_time),
                    ))
                })?;
        parse_sql_payload(Either::Left(json), &app_context.config().files)
            .await
            .map_err(|e| {
                HttpResponse::BadRequest().json(SqlResponse::error(
                    e.code,
                    &e.message,
                    took_ms(start_time),
                ))
            })
    }
}

// ---------------------------------------------------------------------------
// SQL length validation
// ---------------------------------------------------------------------------

/// Reject SQL payloads exceeding the configured maximum length.
#[inline]
fn validate_sql_length(sql: &str, start_time: Instant) -> Result<(), HttpResponse> {
    let max = kalamdb_commons::constants::MAX_SQL_QUERY_LENGTH;
    if sql.len() > max {
        log::warn!("SQL query rejected: length {} bytes exceeds maximum {} bytes", sql.len(), max,);
        return Err(HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::InvalidSql,
            &format!("SQL query too long: {} bytes (maximum {} bytes)", sql.len(), max),
            took_ms(start_time),
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Statement classification helper
// ---------------------------------------------------------------------------

/// Classify a single SQL string, mapping classification errors to
/// `HttpResponse` (403 for unauthorized, 400 for invalid SQL).
fn classify_sql(
    sql: &str,
    default_namespace: &NamespaceId,
    exec_ctx: &ExecutionContext,
    start_time: Instant,
) -> Result<kalamdb_sql::classifier::SqlStatement, HttpResponse> {
    kalamdb_sql::classifier::SqlStatement::classify_and_parse(
        sql,
        default_namespace,
        exec_ctx.user_role(),
    )
    .map_err(|err| match err {
        kalamdb_sql::classifier::StatementClassificationError::Unauthorized(msg) => {
            HttpResponse::Forbidden().json(SqlResponse::error(
                ErrorCode::PermissionDenied,
                &msg,
                took_ms(start_time),
            ))
        },
        kalamdb_sql::classifier::StatementClassificationError::InvalidSql { message, .. } => {
            HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidSql,
                &message,
                took_ms(start_time),
            ))
        },
    })
}

// ---------------------------------------------------------------------------
// Split + prepare pipeline
// ---------------------------------------------------------------------------

/// Split the raw SQL text into individual statements, then for each one:
///   1. Parse the EXECUTE AS USER wrapper (if any).
///   2. Parse the inner SQL via `sqlparser`.
///   3. Extract DML table metadata for table-type lookup.
///   4. Classify the statement (DDL / DML / query / etc.).
///
/// Resolved table types are cached across statements in the same batch to
/// avoid repeated registry lookups.
fn split_and_prepare_statements(
    sql: &str,
    default_namespace: &NamespaceId,
    exec_ctx: &ExecutionContext,
    schema_registry: &SchemaRegistry,
    start_time: Instant,
) -> Result<Vec<PreparedApiExecutionStatement>, HttpResponse> {
    let raw_statements = kalamdb_sql::split_statements(sql).map_err(|err| {
        HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::BatchParseError,
            &format!("Failed to parse SQL batch: {}", err),
            took_ms(start_time),
        ))
    })?;

    if raw_statements.is_empty() {
        return Err(HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::EmptySql,
            "No SQL statements provided",
            took_ms(start_time),
        )));
    }

    let mut table_type_cache: HashMap<TableId, Option<TableType>> = HashMap::new();
    let mut prepared = Vec::with_capacity(raw_statements.len());

    for raw_statement in &raw_statements {
        let parsed = parse_execute_statement(raw_statement).map_err(|err| {
            HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidInput,
                &err,
                took_ms(start_time),
            ))
        })?;

        // parse_single_statement uses sqlparser which doesn't understand
        // custom DDL (CREATE NAMESPACE, SHOW TABLES, etc.).  When it fails we
        // fall through with None — the classifier and executor handle these
        // statements via their own tokeniser.
        let parsed_sql_statement = kalamdb_sql::parse_single_statement(&parsed.sql).ok().flatten();

        let table_id = parsed_sql_statement.as_ref().and_then(|stmt| {
            kalamdb_sql::extract_dml_table_id_from_statement(stmt, default_namespace.as_str())
        });

        let table_type = table_id.as_ref().and_then(|tid| {
            if let Some(cached) = table_type_cache.get(tid) {
                return *cached;
            }
            let resolved = schema_registry.get(tid).map(|c| c.table_entry().table_type);
            table_type_cache.insert(tid.clone(), resolved);
            resolved
        });

        let classified = classify_sql(&parsed.sql, default_namespace, exec_ctx, start_time)?;

        let prepared_statement = PreparedExecutionStatement::new(
            parsed.sql, // move, not clone
            table_id,
            table_type,
            parsed_sql_statement,
            Some(classified),
        );
        prepared.push(PreparedApiExecutionStatement {
            execute_as_username: parsed.execute_as_username,
            prepared_statement,
        });
    }

    Ok(prepared)
}

// ---------------------------------------------------------------------------
// File-upload execution path
// ---------------------------------------------------------------------------

/// Handle the file-upload branch: validate the single statement, stage files,
/// substitute FILE() placeholders, re-parse, execute, and clean up on failure.
///
/// `required_files` is the pre-extracted list from [`extract_file_placeholders`]
/// so we don't scan the SQL a second time.
#[allow(clippy::too_many_arguments)]
async fn execute_file_upload_path(
    is_multipart: bool,
    mut files: Option<HashMap<String, (String, Bytes, Option<String>)>>,
    required_files: &[String],
    prepared_statements: &[PreparedApiExecutionStatement],
    app_context: &Arc<AppContext>,
    sql_executor: &Arc<SqlExecutor>,
    exec_ctx: &ExecutionContext,
    impersonation_service: &SqlImpersonationService,
    authorized_username: &Username,
    default_namespace: &NamespaceId,
    params: Vec<ScalarValue>,
    schema_registry: &SchemaRegistry,
    start_time: Instant,
) -> HttpResponse {
    if !is_multipart {
        return HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::InvalidInput,
            "FILE placeholders require multipart/form-data",
            took_ms(start_time),
        ));
    }

    if prepared_statements.len() != 1 {
        return HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::InvalidInput,
            "File uploads require a single SQL statement",
            took_ms(start_time),
        ));
    }

    let stmt = &prepared_statements[0];
    let execute_as_user = match resolve_execute_as_user(stmt, impersonation_service, exec_ctx).await {
        Ok(uid) => uid,
        Err(err) => {
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::SqlExecutionError,
                &err,
                took_ms(start_time),
            ));
        },
    };

    let mut files_map = files.take().unwrap_or_default();
    if !required_files.is_empty() {
        files_map = files_map.into_iter().filter(|(key, _)| required_files.contains(key)).collect();
    }

    let table_id = match stmt.prepared_statement.table_id.clone() {
        Some(tid) => tid,
        None => {
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidInput,
                "Could not determine target table from SQL. Use fully qualified table name (namespace.table).",
                took_ms(start_time),
            ));
        },
    };

    let table_entry = match schema_registry.get(&table_id) {
        Some(cached) => cached.table_entry(),
        None => {
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::TableNotFound,
                &format!("Table '{}' not found", table_id),
                took_ms(start_time),
            ));
        },
    };

    let storage_id = table_entry.storage_id.clone();
    let table_type = table_entry.table_type;

    if execute_as_user.is_some() && table_type == TableType::Shared {
        return HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::SqlExecutionError,
            &format!(
                "EXECUTE AS USER is not allowed on SHARED tables (table '{}'). \
                 AS USER impersonation is only supported for USER tables.",
                table_id
            ),
            took_ms(start_time),
        ));
    }

    let user_id = match table_type {
        TableType::User => execute_as_user.clone().or_else(|| Some(exec_ctx.user_id().clone())),
        TableType::Shared => None,
        TableType::Stream | TableType::System => {
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidInput,
                "File uploads are not supported for stream or system tables",
                took_ms(start_time),
            ));
        },
    };

    let manifest_service = app_context.manifest_service();
    let mut subfolder_state = match manifest_service.get_file_subfolder_state(&table_id) {
        Ok(Some(state)) => state,
        Ok(None) => FileSubfolderState::new(),
        Err(e) => {
            log::warn!("Failed to get subfolder state for {}: {}", table_id, e);
            FileSubfolderState::new()
        },
    };

    let file_service = app_context.file_storage_service();
    let file_refs = if files_map.is_empty() {
        HashMap::new()
    } else {
        match stage_and_finalize_files(
            file_service.as_ref(),
            &files_map,
            &storage_id,
            table_type,
            &table_id,
            user_id.as_ref(),
            &mut subfolder_state,
            None,
        )
        .await
        {
            Ok(refs) => refs,
            Err(e) => {
                return HttpResponse::InternalServerError().json(SqlResponse::error(
                    e.code,
                    &e.message,
                    took_ms(start_time),
                ));
            },
        }
    };

    let modified_sql = substitute_file_placeholders(&stmt.prepared_statement.sql, &file_refs);

    let modified_parsed = match kalamdb_sql::parse_single_statement(&modified_sql) {
        Ok(Some(s)) => Some(s),
        Ok(None) => {
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidSql,
                "Expected exactly one SQL statement after FILE() substitution",
                took_ms(start_time),
            ));
        },
        Err(err) => {
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidSql,
                &format!("Failed to parse SQL statement after FILE() substitution: {}", err),
                took_ms(start_time),
            ));
        },
    };

    let modified_classified =
        match classify_sql(&modified_sql, default_namespace, exec_ctx, start_time) {
            Ok(c) => c,
            Err(resp) => return resp,
        };

    let modified_metadata = PreparedExecutionStatement::new(
        modified_sql.clone(),
        Some(table_id.clone()),
        Some(table_type),
        modified_parsed,
        Some(modified_classified),
    );

    let effective_username =
        resolve_result_username(authorized_username, stmt.execute_as_username.as_ref());

    match execute_single_statement(
        &modified_metadata,
        app_context,
        sql_executor,
        exec_ctx,
        execute_as_user,
        params,
    )
    .await
    {
        Ok(result) => {
            let result = result.with_as_user(effective_username);
            if let Err(e) = manifest_service.update_file_subfolder_state(&table_id, subfolder_state)
            {
                log::warn!("Failed to update subfolder state for {}: {}", table_id, e);
            }
            HttpResponse::Ok().json(SqlResponse::success(vec![result], took_ms(start_time)))
        },
        Err(err) => {
            cleanup_files(
                &file_refs,
                &storage_id,
                table_type,
                &table_id,
                user_id.as_ref(),
                app_context,
            )
            .await;
            HttpResponse::BadRequest().json(SqlResponse::error_with_details(
                ErrorCode::SqlExecutionError,
                &format!("Statement 1 failed: {}", err),
                &modified_sql,
                took_ms(start_time),
            ))
        },
    }
}

// ---------------------------------------------------------------------------
// Batch execution path
// ---------------------------------------------------------------------------

/// Execute one or more prepared statements sequentially, accumulating DML row
/// counts for multi-statement batches and logging per-statement timing.
///
/// `params` are moved on the last iteration to avoid a redundant clone for the
/// common single-statement case.
#[allow(clippy::too_many_arguments)]
async fn execute_batch_path(
    prepared_statements: &[PreparedApiExecutionStatement],
    app_context: &Arc<AppContext>,
    sql_executor: &Arc<SqlExecutor>,
    exec_ctx: &ExecutionContext,
    impersonation_service: &SqlImpersonationService,
    authorized_username: &Username,
    params: Vec<ScalarValue>,
    http_req: &HttpRequest,
    req_for_forward: &QueryRequest,
    start_time: Instant,
) -> HttpResponse {
    let is_batch = prepared_statements.len() > 1;
    let stmt_count = prepared_statements.len();
    let mut results = Vec::with_capacity(stmt_count);
    let mut total_inserted = 0usize;
    let mut total_updated = 0usize;
    let mut total_deleted = 0usize;
    // Keep params in an Option so we can move on the last iteration.
    let mut params_remaining = Some(params);

    for (idx, stmt) in prepared_statements.iter().enumerate() {
        let is_last = idx + 1 == stmt_count;

        let execute_as_user = match resolve_execute_as_user(stmt, impersonation_service, exec_ctx).await {
            Ok(uid) => uid,
            Err(err) => {
                return HttpResponse::BadRequest().json(SqlResponse::error(
                    ErrorCode::SqlExecutionError,
                    &err,
                    took_ms(start_time),
                ));
            },
        };

        // Reject EXECUTE AS USER on SHARED tables — impersonation is only
        // meaningful for USER tables (row-level security).
        if execute_as_user.is_some()
            && stmt.prepared_statement.table_type == Some(TableType::Shared)
        {
            if let Some(table_id) = stmt.prepared_statement.table_id.as_ref() {
                return HttpResponse::BadRequest().json(SqlResponse::error(
                    ErrorCode::SqlExecutionError,
                    &format!(
                        "EXECUTE AS USER is not allowed on SHARED tables (table '{}'). \
                         AS USER impersonation is only supported for USER tables.",
                        table_id
                    ),
                    took_ms(start_time),
                ));
            }
        }

        let stmt_start = Instant::now();
        let effective_username =
            resolve_result_username(authorized_username, stmt.execute_as_username.as_ref());

        // Move params on the last iteration, clone on earlier ones.
        let stmt_params = if is_last {
            params_remaining.take().unwrap_or_default()
        } else {
            params_remaining.as_ref().cloned().unwrap_or_default()
        };

        match execute_single_statement_raw(
            &stmt.prepared_statement,
            sql_executor,
            exec_ctx,
            execute_as_user.clone(),
            stmt_params,
        )
        .await
        {
            Ok(exec_result) => {
                let stmt_duration_secs = stmt_start.elapsed().as_secs_f64();
                let stmt_duration_ms = stmt_duration_secs * 1000.0;
                let row_count = exec_result.affected_rows();

                // SECURITY: Redact sensitive data before logging
                let safe_sql = kalamdb_commons::helpers::security::redact_sensitive_sql(
                    &stmt.prepared_statement.sql,
                );
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!(
                        target: "sql::exec",
                        "✅ SQL executed | sql='{}' | user='{}' | role='{:?}' | rows={} | took={:.3}ms",
                        safe_sql,
                        exec_ctx.user_id().as_str(),
                        exec_ctx.user_role(),
                        row_count,
                        stmt_duration_ms
                    );
                }

                app_context.slow_query_logger().log_if_slow(
                    safe_sql,
                    stmt_duration_secs,
                    row_count,
                    exec_ctx.user_id().clone(),
                    kalamdb_core::schema_registry::TableType::User,
                    None,
                );

                if !is_batch {
                    if let kalamdb_core::sql::ExecutionResult::Rows {
                        batches,
                        row_count,
                        schema,
                    } = exec_result
                    {
                        let effective_role = if execute_as_user.is_some() {
                            Some(kalamdb_commons::Role::User)
                        } else {
                            Some(exec_ctx.user_role())
                        };
                        return match stream_sql_rows_response(
                            batches,
                            schema,
                            effective_role,
                            effective_username,
                            row_count,
                            took_ms(start_time),
                        ) {
                            Ok(response) => response,
                            Err(err) => {
                                HttpResponse::InternalServerError().json(SqlResponse::error(
                                    ErrorCode::InternalError,
                                    &format!("Failed to stream SQL response: {}", err),
                                    took_ms(start_time),
                                ))
                            },
                        };
                    }
                }

                let effective_role = if execute_as_user.is_some() {
                    Some(kalamdb_commons::Role::User)
                } else {
                    Some(exec_ctx.user_role())
                };
                let result = match execution_result_to_query_result(exec_result, effective_role) {
                    Ok(result) => result.with_as_user(effective_username),
                    Err(err) => {
                        return HttpResponse::InternalServerError().json(SqlResponse::error(
                            ErrorCode::InternalError,
                            &format!("Failed to serialize SQL result: {}", err),
                            took_ms(start_time),
                        ));
                    },
                };

                // Accumulate DML row counts for multi-statement batches
                if is_batch {
                    if let Some(ref msg) = result.message {
                        if msg.contains("Inserted") {
                            total_inserted += result.row_count;
                            continue;
                        } else if msg.contains("Updated") {
                            total_updated += result.row_count;
                            continue;
                        } else if msg.contains("Deleted") {
                            total_deleted += result.row_count;
                            continue;
                        }
                    }
                }

                results.push(result);
            },
            Err(err) => {
                // Check if NOT_LEADER error and auto-forward to leader
                if let Some(kalamdb_err) = err.downcast_ref::<kalamdb_core::error::KalamDbError>() {
                    if let Some(response) = handle_not_leader_error(
                        kalamdb_err,
                        http_req,
                        req_for_forward,
                        app_context,
                        start_time,
                    )
                    .await
                    {
                        return response;
                    }
                }

                return HttpResponse::BadRequest().json(SqlResponse::error_with_details(
                    ErrorCode::SqlExecutionError,
                    &format!("Statement {} failed: {}", idx + 1, err),
                    &stmt.prepared_statement.sql,
                    took_ms(start_time),
                ));
            },
        }
    }

    // Append accumulated DML summaries for multi-statement batches
    if is_batch {
        if total_inserted > 0 {
            results.push(
                QueryResult::with_affected_rows(
                    total_inserted,
                    Some(format!("Inserted {} row(s)", total_inserted)),
                )
                .with_as_user(authorized_username.clone()),
            );
        }
        if total_updated > 0 {
            results.push(
                QueryResult::with_affected_rows(
                    total_updated,
                    Some(format!("Updated {} row(s)", total_updated)),
                )
                .with_as_user(authorized_username.clone()),
            );
        }
        if total_deleted > 0 {
            results.push(
                QueryResult::with_affected_rows(
                    total_deleted,
                    Some(format!("Deleted {} row(s)", total_deleted)),
                )
                .with_as_user(authorized_username.clone()),
            );
        }
    }

    HttpResponse::Ok().json(SqlResponse::success(results, took_ms(start_time)))
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

/// POST /v1/api/sql - Execute SQL statement(s)
///
/// Accepts either JSON or multipart/form-data payloads.
///
/// - JSON: `sql` plus optional `params` and `namespace_id`.
/// - Multipart: `sql`, optional `params` (JSON array), optional `namespace_id`,
///   and file parts named `file:<placeholder>` for FILE("name") placeholders.
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

    // 3. Validate SQL length
    if let Err(resp) = validate_sql_length(&sql, start_time) {
        return resp;
    }

    // 4. Build execution context
    let default_namespace = namespace_id.clone().unwrap_or_else(|| NamespaceId::new("default"));
    let base_session = app_context.base_session_context();
    let exec_ctx = ExecutionContext::from_session(session, Arc::clone(&base_session))
        .with_namespace_id(default_namespace.clone());
    let auth_username = authorized_username(&exec_ctx);
    let impersonation_service = SqlImpersonationService::new(Arc::clone(app_context.get_ref()));

    // 5. File uploads must go to the leader
    let files_present = files.as_ref().map_or(false, |f| !f.is_empty());
    if files_present {
        let executor = app_context.executor();
        if !executor.is_leader(GroupId::Meta).await {
            return HttpResponse::ServiceUnavailable().json(SqlResponse::error(
                ErrorCode::NotLeader,
                "File uploads must be sent to the current leader",
                took_ms(start_time),
            ));
        }
    }

    // 6. Forward to leader if this node is a follower (non-file path).
    //    Build the forwarding request lazily — only allocates when we
    //    actually need to forward.
    if !files_present {
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
    let schema_registry = app_context.schema_registry();
    let prepared_statements = match split_and_prepare_statements(
        &sql,
        &default_namespace,
        &exec_ctx,
        &schema_registry,
        start_time,
    ) {
        Ok(stmts) => stmts,
        Err(resp) => return resp,
    };

    // 9. Reject params with multi-statement batches
    if !params.is_empty() && prepared_statements.len() > 1 {
        return HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::ParamsWithBatch,
            "Parameters not supported with multi-statement batches",
            took_ms(start_time),
        ));
    }

    // 10. Dispatch to file-upload or batch execution path.
    //     `extract_file_placeholders` is called once here; the result is
    //     passed into `execute_file_upload_path` to avoid a second scan.
    let required_files = extract_file_placeholders(&sql);
    if !required_files.is_empty() || files_present {
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

#[cfg(test)]
mod tests {
    use super::{is_multipart_content_type, parse_execute_statement, resolve_result_username};
    use kalamdb_commons::models::Username;

    // -----------------------------------------------------------------------
    // EXECUTE AS USER — quoted
    // -----------------------------------------------------------------------

    #[test]
    fn parse_execute_as_user_wrapper() {
        let parsed = parse_execute_statement(
            "EXECUTE AS USER 'alice' (SELECT * FROM default.todos WHERE id = 1);",
        )
        .expect("wrapper should parse");

        assert_eq!(parsed.execute_as_username, Some(Username::from("alice")));
        assert_eq!(parsed.sql, "SELECT * FROM default.todos WHERE id = 1");
    }

    #[test]
    fn reject_multi_statement_inside_wrapper() {
        let err = parse_execute_statement("EXECUTE AS USER 'alice' (SELECT 1; SELECT 2)")
            .expect_err("multiple statements should be rejected");
        assert!(err.contains("single SQL statement"));
    }

    // -----------------------------------------------------------------------
    // EXECUTE AS USER — **bare** (unquoted)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_execute_as_user_bare_username() {
        let parsed = parse_execute_statement(
            "EXECUTE AS USER alice (SELECT * FROM default.todos WHERE id = 1);",
        )
        .expect("bare username should parse");

        assert_eq!(parsed.execute_as_username, Some(Username::from("alice")));
        assert_eq!(parsed.sql, "SELECT * FROM default.todos WHERE id = 1");
    }

    #[test]
    fn parse_execute_as_user_bare_case_insensitive() {
        let parsed =
            parse_execute_statement("execute as user bob (INSERT INTO default.t VALUES (1))")
                .expect("case-insensitive bare username should parse");

        assert_eq!(parsed.execute_as_username, Some(Username::from("bob")));
        assert_eq!(parsed.sql, "INSERT INTO default.t VALUES (1)");
    }

    #[test]
    fn parse_execute_as_user_bare_no_space_before_paren() {
        // `alice(SELECT ...)` — the username is `alice`, paren starts body.
        let parsed = parse_execute_statement("EXECUTE AS USER alice(SELECT 1)")
            .expect("bare username immediately followed by '(' should parse");

        assert_eq!(parsed.execute_as_username, Some(Username::from("alice")));
        assert_eq!(parsed.sql, "SELECT 1");
    }

    // -----------------------------------------------------------------------
    // Passthrough & helpers
    // -----------------------------------------------------------------------

    #[test]
    fn passthrough_non_wrapper_statement() {
        let parsed = parse_execute_statement("SELECT * FROM default.todos WHERE id = 10")
            .expect("statement should pass through");
        assert!(parsed.execute_as_username.is_none());
        assert_eq!(parsed.sql, "SELECT * FROM default.todos WHERE id = 10");
    }

    #[test]
    fn resolve_result_username_uses_authorized_when_no_execute_as() {
        let authorized = Username::from("admin_user");
        let actual = resolve_result_username(&authorized, None);
        assert_eq!(actual, authorized);
    }

    #[test]
    fn resolve_result_username_uses_execute_as_when_present() {
        let authorized = Username::from("admin_user");
        let execute_as = Username::from("alice");
        let actual = resolve_result_username(&authorized, Some(&execute_as));
        assert_eq!(actual, execute_as);
    }

    // -----------------------------------------------------------------------
    // Content-type helper
    // -----------------------------------------------------------------------

    #[test]
    fn multipart_detection_case_insensitive() {
        assert!(is_multipart_content_type("multipart/form-data"));
        assert!(is_multipart_content_type("Multipart/Form-Data; boundary=abc"));
        assert!(is_multipart_content_type("MULTIPART/FORM-DATA"));
        assert!(!is_multipart_content_type("application/json"));
        assert!(!is_multipart_content_type(""));
    }
}
