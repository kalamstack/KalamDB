use actix_web::HttpResponse;
use kalamdb_commons::models::{NamespaceId, UserId, Username};
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_core::sql::executor::{PreparedExecutionStatement, SqlExecutor};
use kalamdb_core::sql::SqlImpersonationService;
use std::time::Instant;

use super::models::{ErrorCode, SqlResponse};
use super::request::took_ms;

#[derive(Debug)]
pub(super) struct ParsedExecutionStatement {
    pub(super) sql: String,
    pub(super) execute_as_username: Option<Username>,
}

#[derive(Debug)]
pub(super) struct PreparedApiExecutionStatement {
    pub(super) execute_as_username: Option<Username>,
    pub(super) prepared_statement: PreparedExecutionStatement,
}

pub(super) fn authorized_username(exec_ctx: &ExecutionContext) -> Username {
    if let Some(username) = &exec_ctx.user_context().username {
        return username.clone();
    }
    Username::from(exec_ctx.user_id().as_str())
}

#[inline]
pub(super) fn resolve_result_username(
    authorized_username: &Username,
    execute_as_username: Option<&Username>,
) -> Username {
    execute_as_username.cloned().unwrap_or_else(|| authorized_username.clone())
}

pub(super) async fn resolve_execute_as_user(
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

pub(super) fn parse_execute_statement(statement: &str) -> Result<ParsedExecutionStatement, String> {
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

pub(super) fn classify_sql(
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

pub(super) fn split_and_prepare_statements(
    sql: &str,
    exec_ctx: &ExecutionContext,
    sql_executor: &SqlExecutor,
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

    let mut prepared = Vec::with_capacity(raw_statements.len());

    for raw_statement in &raw_statements {
        let parsed = parse_execute_statement(raw_statement).map_err(|err| {
            HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidInput,
                &err,
                took_ms(start_time),
            ))
        })?;

        let prepared_statement = sql_executor
            .prepare_statement_metadata(&parsed.sql, exec_ctx)
            .map_err(|err| match err {
                kalamdb_sql::classifier::StatementClassificationError::Unauthorized(msg) => {
                    HttpResponse::Forbidden().json(SqlResponse::error(
                        ErrorCode::PermissionDenied,
                        &msg,
                        took_ms(start_time),
                    ))
                },
                kalamdb_sql::classifier::StatementClassificationError::InvalidSql {
                    message,
                    ..
                } => HttpResponse::BadRequest().json(SqlResponse::error(
                    ErrorCode::InvalidSql,
                    &message,
                    took_ms(start_time),
                )),
            })?;

        prepared.push(PreparedApiExecutionStatement {
            execute_as_username: parsed.execute_as_username,
            prepared_statement,
        });
    }

    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use super::{parse_execute_statement, resolve_result_username};
    use kalamdb_commons::models::Username;

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
        let parsed = parse_execute_statement("EXECUTE AS USER alice(SELECT 1)")
            .expect("bare username immediately followed by '(' should parse");

        assert_eq!(parsed.execute_as_username, Some(Username::from("alice")));
        assert_eq!(parsed.sql, "SELECT 1");
    }

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
}
