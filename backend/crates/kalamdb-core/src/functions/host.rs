//! V8 host callbacks implemented by core (nested SQL, CALL, topics, HTTP).

use std::{collections::HashMap, sync::Arc};

use kalamdb_commons::{models::RoutineId, NamespaceId, Role, RoutineSecurityMode, UserId};
use kalamdb_functions::{
    FunctionHost, FunctionsError, HostFuture, InvocationMetadata, InvocationScope, RoutineValue,
};
use tokio::runtime::Handle;

use super::{
    call_types::{FunctionCallOrigin, ProcedureFrame},
    executor,
};
use crate::{app_context::AppContext, sql::context::ExecutionContext};

#[derive(Clone)]
pub(super) struct CoreFunctionHost {
    pub app:      Arc<AppContext>,
    pub handle:   Handle,
    pub session:  Arc<HostSession>,
    pub origin:   FunctionCallOrigin,
    pub scope:    InvocationScope,
    pub sql_gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone)]
pub(super) struct HostSession {
    pub exec_ctx: ExecutionContext,
    pub stack:    Vec<ProcedureFrame>,
}

impl CoreFunctionHost {
    pub(super) fn current_exec_ctx(&self) -> ExecutionContext {
        let session = self.session.as_ref();
        let Some(frame) = session.stack.last() else {
            return session.exec_ctx.clone();
        };
        session
            .exec_ctx
            .with_effective_identity(frame.principal_user.clone(), frame.principal_role)
    }

    pub(super) fn stack_label(&self) -> String {
        let session = self.session.as_ref();
        session
            .stack
            .iter()
            .map(ProcedureFrame::stack_label)
            .collect::<Vec<_>>()
            .join(" -> ")
    }
    async fn run_sql(
        &self,
        sql: String,
        params: Vec<RoutineValue>,
        rows: bool,
    ) -> kalamdb_functions::Result<RoutineValue> {
        self.scope.check()?;
        let _gate = self.sql_gate.lock().await;
        self.scope.check()?;
        let ctx = self.current_exec_ctx();
        let executor = self.app.sql_executor();
        let metadata = executor
            .prepare_statement_metadata(&sql, &ctx)
            .map_err(|e| FunctionsError::Invalid(e.to_string()))?;
        // Procedure transactions are owned by their root invocation.
        if metadata.classified_statement.as_ref().is_some_and(|statement| {
            matches!(
                statement.kind(),
                kalamdb_sql::SqlStatementKind::BeginTransaction
                    | kalamdb_sql::SqlStatementKind::CommitTransaction
                    | kalamdb_sql::SqlStatementKind::RollbackTransaction
            )
        }) {
            return Err(FunctionsError::Invalid(
                "transaction control is not allowed inside a procedure".into(),
            ));
        }
        let result = executor.execute_with_metadata(
            &metadata,
            &ctx,
            params.into_iter().map(|v| v.value).collect(),
        );
        let result = tokio::select! {
            biased;
            _ = self.scope.cancel.cancelled() => return Err(FunctionsError::Cancelled),
            _ = tokio::time::sleep_until(self.scope.deadline.into()) => return Err(FunctionsError::Timeout),
            result = result => result.map_err(map_core)?,
        };
        if rows {
            super::convert::execution_result_to_rows(result).map_err(map_core)
        } else {
            super::convert::execution_result_to_routine(result).map_err(map_core)
        }
    }
}

impl FunctionHost for CoreFunctionHost {
    fn sql(&self, sql: &str) -> kalamdb_functions::Result<RoutineValue> {
        self.handle.block_on(self.run_sql(sql.to_string(), Vec::new(), false))
    }

    fn query(&self, sql: String, params: Vec<RoutineValue>) -> HostFuture<'_, RoutineValue> {
        Box::pin(self.run_sql(sql, params, true))
    }

    fn execute(&self, sql: String, params: Vec<RoutineValue>) -> HostFuture<'_, RoutineValue> {
        Box::pin(self.run_sql(sql, params, false))
    }

    fn call_async(
        &self,
        procedure: String,
        args: Vec<RoutineValue>,
    ) -> HostFuture<'_, RoutineValue> {
        Box::pin(async move {
            let id = resolve_routine_id(&procedure, &self.session.exec_ctx.default_namespace());
            executor::invoke_nested(self, id, &args).await.map_err(map_core)
        })
    }

    fn metadata(&self) -> Option<InvocationMetadata> {
        let ctx = self.current_exec_ctx();
        Some(InvocationMetadata {
            caller:         self.session.exec_ctx.user_id().clone(),
            effective_user: ctx.user_id().clone(),
            role:           ctx.user_role(),
            namespace:      ctx.default_namespace(),
            request_id:     ctx.request_id().unwrap_or_default().to_string(),
        })
    }

    fn log(&self, level: &str, message: &str) -> kalamdb_functions::Result<()> {
        log::info!(target: "kalamdb::functions", "procedure={} level={} {}", self.stack_label(), level, message);
        Ok(())
    }

    fn call(
        &self,
        procedure: &str,
        args: &[RoutineValue],
    ) -> kalamdb_functions::Result<RoutineValue> {
        let default_ns = {
            let session = self.session.as_ref();
            session.exec_ctx.default_namespace()
        };
        let routine_id = resolve_routine_id(procedure, &default_ns);
        match self.handle.block_on(executor::invoke_nested(self, routine_id, args)) {
            Ok(value) => Ok(value),
            Err(error) => Err(annotate(self.stack_label(), error.to_string())),
        }
    }

    fn publish(&self, topic: &str, payload: &RoutineValue) -> kalamdb_functions::Result<()> {
        let exec_ctx = self.current_exec_ctx();
        executor::stage_topic_publish(&self.app, &exec_ctx, topic, payload).map_err(map_core)
    }

    fn http_request_header(&self, name: &str) -> kalamdb_functions::Result<Option<String>> {
        match &self.origin {
            FunctionCallOrigin::Http { headers, .. } => Ok(header_lookup(headers, name).cloned()),
            FunctionCallOrigin::Sql | FunctionCallOrigin::Topic { .. } => Ok(None),
        }
    }

    fn http_set_status(&self, status: i32) -> kalamdb_functions::Result<()> {
        let FunctionCallOrigin::Http { response, .. } = &self.origin else {
            return Err(FunctionsError::Invalid(
                "ctx.http.status is only available on HTTP-root invocations".to_string(),
            ));
        };
        if !self.is_http_root() {
            return Err(FunctionsError::Invalid(
                "nested procedures cannot mutate ctx.http".to_string(),
            ));
        }
        if !(100..=599).contains(&status) {
            return Err(FunctionsError::Invalid(format!("invalid http status {status}")));
        }
        response.lock().status = Some(status as u16);
        Ok(())
    }

    fn http_set_header(&self, name: &str, value: &str) -> kalamdb_functions::Result<()> {
        let FunctionCallOrigin::Http { response, .. } = &self.origin else {
            return Err(FunctionsError::Invalid(
                "ctx.http.header is only available on HTTP-root invocations".to_string(),
            ));
        };
        if !self.is_http_root() {
            return Err(FunctionsError::Invalid(
                "nested procedures cannot mutate ctx.http".to_string(),
            ));
        }
        response.lock().headers.insert(name.to_string(), value.to_string());
        Ok(())
    }

    fn is_http_root(&self) -> bool {
        matches!(self.origin, FunctionCallOrigin::Http { .. })
            && self.session.as_ref().stack.len() <= 1
    }

    fn invocation_source(&self) -> kalamdb_functions::InvocationSource {
        match &self.origin {
            FunctionCallOrigin::Topic {
                topic_name,
                event_id,
                partition,
                offset,
                attempt,
            } => kalamdb_functions::InvocationSource::Topic {
                topic_name: topic_name.clone(),
                event_id:   event_id.clone(),
                partition:  *partition,
                offset:     *offset,
                attempt:    *attempt,
            },
            FunctionCallOrigin::Sql | FunctionCallOrigin::Http { .. } => {
                kalamdb_functions::InvocationSource::Call
            },
        }
    }

    fn parent_procedure(&self) -> Option<String> {
        let session = self.session.as_ref();
        let len = session.stack.len();
        if len < 2 {
            return None;
        }
        session.stack.get(len - 2).map(ProcedureFrame::stack_label)
    }
}

pub(super) fn resolve_routine_id(name: &str, default_ns: &NamespaceId) -> RoutineId {
    let trimmed = name.trim();
    if let Some((schema, rest)) = trimmed.split_once('.') {
        RoutineId::from_parts(Some(&NamespaceId::new(schema)), rest)
    } else {
        RoutineId::from_parts(Some(default_ns), trimmed)
    }
}

fn header_lookup<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a String> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

fn map_core(error: crate::error::KalamDbError) -> FunctionsError {
    FunctionsError::Invalid(error.to_string())
}

fn annotate(stack: String, message: String) -> FunctionsError {
    if stack.is_empty() {
        FunctionsError::Invalid(message)
    } else {
        FunctionsError::Invalid(format!("{stack}: {message}"))
    }
}

pub(super) fn frame_principal(
    security: RoutineSecurityMode,
    caller_user: UserId,
    caller_role: Role,
    owner: UserId,
    owner_role: Role,
) -> (UserId, Role) {
    match security {
        RoutineSecurityMode::Invoker => (caller_user, caller_role),
        RoutineSecurityMode::Definer => (owner, owner_role),
    }
}
