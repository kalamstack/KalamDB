use actix_web::{HttpRequest, HttpResponse};
use bytes::Bytes;
use kalamdb_commons::models::{NamespaceId, Username};
use kalamdb_commons::schemas::TableType;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::schema_registry::SchemaRegistry;
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_core::sql::executor::request_transaction_state::RequestTransactionState;
use kalamdb_core::sql::executor::{PreparedExecutionStatement, ScalarValue, SqlExecutor};
use kalamdb_core::sql::SqlImpersonationService;
use kalamdb_sql::classifier::SqlStatementKind;
use kalamdb_system::FileSubfolderState;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::file_utils::{stage_and_finalize_files, substitute_file_placeholders};
use super::forward::handle_not_leader_error;
use super::helpers::{
    cleanup_files, execute_single_statement, execute_single_statement_raw,
    execution_result_to_query_result, stream_sql_rows_response,
};
use super::models::{ErrorCode, QueryRequest, QueryResult, SqlResponse};
use super::request::took_ms;
use super::statements::{
    classify_sql, resolve_execute_as_user, resolve_result_username, PreparedApiExecutionStatement,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_file_upload_path(
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
    let execute_as_user = match resolve_execute_as_user(stmt, impersonation_service, exec_ctx).await
    {
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

    match kalamdb_sql::parse_single_statement(&modified_sql) {
        Ok(Some(_)) => {},
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_batch_path(
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
    let mut params_remaining = Some(params);
    let mut request_transaction_state =
        match RequestTransactionState::from_execution_context(exec_ctx) {
            Ok(state) => state,
            Err(err) => {
                return HttpResponse::BadRequest().json(SqlResponse::error(
                    ErrorCode::SqlExecutionError,
                    &err.to_string(),
                    took_ms(start_time),
                ));
            },
        };
    if let Some(state) = request_transaction_state.as_mut() {
        state.sync_from_coordinator(app_context);
    }

    let mut idx = 0;
    while idx < stmt_count {
        let stmt = &prepared_statements[idx];

        // ── Transaction batch INSERT path ───────────────────────────────
        // When an explicit transaction is active and we see consecutive INSERT
        // statements targeting the same table (no EXECUTE AS USER, no params),
        // collect them and process through the transaction batch insert path.
        if let Some(state) = request_transaction_state.as_ref() {
            if let Some(transaction_id) = state.active_transaction_id() {
                if is_batchable_insert(stmt) {
                    let batch_table_id = stmt.prepared_statement.table_id.as_ref();
                    let mut batch_end = idx + 1;
                    while batch_end < stmt_count
                        && is_batchable_insert(&prepared_statements[batch_end])
                        && prepared_statements[batch_end].prepared_statement.table_id.as_ref()
                            == batch_table_id
                    {
                        batch_end += 1;
                    }
                    let batch_len = batch_end - idx;

                    if batch_len > 1 {
                        let batch_stmts: Vec<&PreparedExecutionStatement> = prepared_statements
                            [idx..batch_end]
                            .iter()
                            .map(|s| &s.prepared_statement)
                            .collect();
                        let batch_start = Instant::now();

                        match sql_executor.try_batch_insert_in_transaction(
                            &batch_stmts,
                            exec_ctx,
                            transaction_id,
                        ) {
                            Ok(Some(results)) => {
                                let batch_rows: usize =
                                    results.iter().map(|r| r.affected_rows()).sum();
                                let batch_ms = batch_start.elapsed().as_secs_f64() * 1000.0;
                                log::debug!(
                                    target: "sql::exec",
                                    "✅ Batch INSERT ({} stmts, {} rows) | took={:.3}ms",
                                    batch_len,
                                    batch_rows,
                                    batch_ms,
                                );
                                total_inserted += batch_rows;
                                idx = batch_end;
                                if let Some(state) = request_transaction_state.as_mut() {
                                    state.sync_from_coordinator(app_context);
                                }
                                continue;
                            },
                            Ok(None) => { /* fast path not applicable, fall through */ },
                            Err(err) => {
                                if let Some(state) = request_transaction_state.as_mut() {
                                    let _ = state.rollback_if_active(app_context);
                                }
                                return HttpResponse::BadRequest().json(
                                    SqlResponse::error_with_details(
                                        ErrorCode::SqlExecutionError,
                                        &format!("Statement {} failed: {}", idx + 1, err),
                                        &prepared_statements[idx].prepared_statement.sql,
                                        took_ms(start_time),
                                    ),
                                );
                            },
                        }
                    }
                }
            }
        }

        // ── Per-statement execution (original path) ────────────────────
        let execute_as_user =
            match resolve_execute_as_user(stmt, impersonation_service, exec_ctx).await {
                Ok(uid) => uid,
                Err(err) => {
                    return HttpResponse::BadRequest().json(SqlResponse::error(
                        ErrorCode::SqlExecutionError,
                        &err,
                        took_ms(start_time),
                    ));
                },
            };

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

        let is_last = idx + 1 == stmt_count;

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

                if is_batch {
                    if let Some(ref msg) = result.message {
                        if msg.contains("Inserted") {
                            total_inserted += result.row_count;
                            if let Some(state) = request_transaction_state.as_mut() {
                                state.sync_from_coordinator(app_context);
                            }
                            idx += 1;
                            continue;
                        } else if msg.contains("Updated") {
                            total_updated += result.row_count;
                            if let Some(state) = request_transaction_state.as_mut() {
                                state.sync_from_coordinator(app_context);
                            }
                            idx += 1;
                            continue;
                        } else if msg.contains("Deleted") {
                            total_deleted += result.row_count;
                            if let Some(state) = request_transaction_state.as_mut() {
                                state.sync_from_coordinator(app_context);
                            }
                            idx += 1;
                            continue;
                        }
                    }
                }

                results.push(result);
            },
            Err(err) => {
                if let Some(state) = request_transaction_state.as_mut() {
                    let _ = state.rollback_if_active(app_context);
                }

                if let Some(kalamdb_err) = err.downcast_ref::<kalamdb_core::error::KalamDbError>() {
                    if let Some(response) = handle_not_leader_error(
                        kalamdb_err,
                        http_req,
                        req_for_forward,
                        app_context,
                        exec_ctx.request_id(),
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

        if let Some(state) = request_transaction_state.as_mut() {
            state.sync_from_coordinator(app_context);
        }
        idx += 1;
    }

    if let Some(state) = request_transaction_state.as_mut() {
        if state.is_active() {
            let _ = state.rollback_if_active(app_context);
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::SqlExecutionError,
                "Request completed with an open explicit transaction; rolled back automatically",
                took_ms(start_time),
            ));
        }
    }

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

/// Check if a prepared statement is a simple INSERT eligible for batching:
/// no EXECUTE AS USER, has a table_id and table_type, and is classified as INSERT.
fn is_batchable_insert(stmt: &PreparedApiExecutionStatement) -> bool {
    if stmt.execute_as_username.is_some() {
        return false;
    }
    if stmt.prepared_statement.table_id.is_none() || stmt.prepared_statement.table_type.is_none() {
        return false;
    }
    matches!(
        stmt.prepared_statement.classified_statement.as_ref().map(|c| c.kind()),
        Some(SqlStatementKind::Insert(_))
    )
}
