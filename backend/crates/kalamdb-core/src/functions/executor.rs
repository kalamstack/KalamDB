//! Root and nested procedure invocation.

use std::sync::Arc;

use arrow::{
    array::RecordBatch,
    datatypes::{Field, Schema},
};
use datafusion::scalar::ScalarValue;
use kalamdb_commons::{
    conversions::arrow_json_conversion::scalar_value_to_json,
    models::{RoutineCall, RoutineId, TopicId, TransactionId},
    Role, UserId,
};
use kalamdb_filestore::StorageCached;
use kalamdb_functions::{
    FunctionActivation, FunctionsError, Invocation, InvocationScope, ModuleRevision, RoutineValue,
};
use kalamdb_sql::ddl::CallStatement;
use kalamdb_system::CatalogRoutine;
use kalamdb_transactions::RequestTransactionState;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    acl,
    call_types::{FunctionCallOrigin, FunctionCallResult, ProcedureFrame},
    convert::bind_call_arguments,
    host::{frame_principal, CoreFunctionHost, HostSession},
    runtime_state::StagedTopicPublish,
};
use crate::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult},
        executor::request_transaction_state::{
            map_request_transaction_error, AppContextRequestTransactionCoordinator,
        },
    },
};

pub struct FunctionService;

impl FunctionService {
    pub async fn execute_call(
        app: Arc<AppContext>,
        exec_ctx: &ExecutionContext,
        statement: &CallStatement,
        params: &[ScalarValue],
    ) -> Result<ExecutionResult, KalamDbError> {
        let result = Self::invoke_sql_call(app, exec_ctx, &statement.call, params).await?;
        routine_value_to_execution_result(&result.value)
    }

    pub async fn execute_routine_call(
        app: Arc<AppContext>,
        exec_ctx: &ExecutionContext,
        call: &RoutineCall,
    ) -> Result<ScalarValue, KalamDbError> {
        if call.has_placeholder() {
            return Err(KalamDbError::InvalidSql(
                "DEFAULT procedure arguments cannot use placeholders".to_string(),
            ));
        }
        let result = Self::invoke_sql_call(app, exec_ctx, call, &[]).await?;
        Ok(result.value.value)
    }

    async fn invoke_sql_call(
        app: Arc<AppContext>,
        exec_ctx: &ExecutionContext,
        call: &RoutineCall,
        params: &[ScalarValue],
    ) -> Result<FunctionCallResult, KalamDbError> {
        let args = bind_call_arguments(&call.arguments, params)?;
        Self::invoke(app, exec_ctx, FunctionCallOrigin::Sql, call.routine_id.clone(), args).await
    }

    pub async fn invoke(
        app: Arc<AppContext>,
        exec_ctx: &ExecutionContext,
        origin: FunctionCallOrigin,
        routine_id: RoutineId,
        args: Vec<RoutineValue>,
    ) -> Result<FunctionCallResult, KalamDbError> {
        let request_id = exec_ctx
            .request_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        let exec_ctx = exec_ctx.clone().with_request_id(request_id.clone());

        let coordinator = AppContextRequestTransactionCoordinator::new(app.as_ref());
        let mut request_state = RequestTransactionState::from_request_id(Some(request_id.as_str()))
            .expect("request id is present");
        request_state.sync(&coordinator);
        let owned_tx = !request_state.is_active();
        if owned_tx {
            request_state.begin(&coordinator).map_err(map_request_transaction_error)?;
        }

        kalamdb_observability::begin_function_run();
        let started = std::time::Instant::now();
        let invoke_result = invoke_root(Arc::clone(&app), exec_ctx, origin, routine_id, args).await;
        kalamdb_observability::finish_function_run(started.elapsed(), invoke_result.is_err());

        match invoke_result {
            Ok(result) => {
                if owned_tx {
                    request_state
                        .commit(&coordinator)
                        .await
                        .map_err(map_request_transaction_error)?;
                }
                Ok(result)
            },
            Err(error) => {
                if owned_tx {
                    let _ = request_state.rollback(&coordinator);
                }
                Err(error)
            },
        }
    }
}

async fn invoke_root(
    app: Arc<AppContext>,
    exec_ctx: ExecutionContext,
    origin: FunctionCallOrigin,
    routine_id: RoutineId,
    args: Vec<RoutineValue>,
) -> Result<FunctionCallResult, KalamDbError> {
    let host = CoreFunctionHost {
        app:      Arc::clone(&app),
        handle:   tokio::runtime::Handle::current(),
        session:  Arc::new(HostSession {
            exec_ctx,
            stack: Vec::new(),
        }),
        origin:   origin.clone(),
        scope:    InvocationScope {
            deadline: std::time::Instant::now()
                + app.function_runtime().engine().map_err(map_functions)?.config().timeout,
            cancel:   CancellationToken::new(),
            depth:    0,
        },
        sql_gate: Arc::new(tokio::sync::Mutex::new(())),
    };
    let value = invoke_on_host(&host, routine_id, &args).await?;

    let (http_status, http_headers) = match origin {
        FunctionCallOrigin::Http { response, .. } => {
            let overrides = response.lock().clone();
            (overrides.status, overrides.headers)
        },
        FunctionCallOrigin::Sql | FunctionCallOrigin::Topic { .. } => {
            (None, std::collections::HashMap::new())
        },
    };
    Ok(FunctionCallResult {
        value,
        http_status,
        http_headers,
    })
}

pub(super) async fn invoke_nested(
    host: &CoreFunctionHost,
    routine_id: RoutineId,
    args: &[RoutineValue],
) -> Result<RoutineValue, KalamDbError> {
    let mut child = host.clone();
    child.scope = host
        .scope
        .child(host.app.function_runtime().engine().map_err(map_functions)?.config().max_depth)
        .map_err(map_functions)?;
    invoke_on_host(&child, routine_id, args).await
}

async fn invoke_on_host(
    host: &CoreFunctionHost,
    routine_id: RoutineId,
    args: &[RoutineValue],
) -> Result<RoutineValue, KalamDbError> {
    let stores = host.app.system_tables().catalog_stores();
    let routine = stores.get_routine(&routine_id).map_err(|error| {
        KalamDbError::ExecutionError(format!("failed to load procedure {routine_id}: {error}"))
    })?;
    let Some(routine) = routine else {
        return Err(KalamDbError::NotFound(format!("procedure {routine_id} not found")));
    };

    let (caller_user, caller_role) = {
        let session = host.session.as_ref();
        match session.stack.last() {
            Some(frame) => (frame.principal_user.clone(), frame.principal_role),
            None => (session.exec_ctx.user_id().clone(), session.exec_ctx.user_role()),
        }
    };
    acl::require_execute(&stores, &routine, &caller_user, caller_role)?;

    let owner_role = owner_role(&host.app, &routine.owner)?;
    let (principal_user, principal_role) = frame_principal(
        routine.security,
        caller_user,
        caller_role,
        routine.owner.clone(),
        owner_role,
    );

    let revision = load_active_revision(host, &routine).await?;
    let frame = ProcedureFrame {
        routine_id: routine.routine_id.clone(),
        revision_id: revision.revision_id.clone(),
        principal_user,
        principal_role,
        security: routine.security,
    };

    let mut child = host.clone();
    let mut session = (*host.session).clone();
    session.stack.push(frame);
    child.session = Arc::new(session);
    let engine = host.app.function_runtime().engine().map_err(map_functions)?;
    engine
        .invoke(
            Invocation {
                routine_id,
                revision,
                args: args.to_vec(),
                scope: host.scope.clone(),
                return_template: None,
            },
            Arc::new(child),
        )
        .await
        .map_err(map_functions)
}

async fn load_active_revision(
    host: &CoreFunctionHost,
    routine: &CatalogRoutine,
) -> Result<Arc<ModuleRevision>, KalamDbError> {
    let language = routine.language.as_deref().unwrap_or("");
    if is_sql_language(language) {
        return Err(KalamDbError::InvalidOperation(format!(
            "CALL of LANGUAGE SQL procedure {} is not supported",
            routine.routine_id
        )));
    }
    let stores = host.app.system_tables().catalog_stores();
    let storage = function_storage(&host.app)?;
    let activation = FunctionActivation::new(stores);
    let module_id = acl::module_id_for(&routine.routine_id);
    let revision_id = activation
        .active_module(&module_id)
        .map_err(map_functions)?
        .and_then(|module| module.active_revision_id)
        .ok_or_else(|| KalamDbError::NotFound(format!("no active function module {module_id}")))?;
    host.app
        .function_runtime()
        .engine()
        .map_err(map_functions)?
        .load_revision(revision_id.clone(), activation.load_revision(&storage, &revision_id))
        .await
        .map_err(map_functions)
}

pub fn function_storage(app: &AppContext) -> Result<Arc<StorageCached>, KalamDbError> {
    let registry = app.storage_registry();
    let storages = registry.list_storages().map_err(|error| {
        KalamDbError::ExecutionError(format!("failed to list storages: {error}"))
    })?;
    let preferred = storages
        .iter()
        .find(|storage| storage.storage_id.as_str() == "local")
        .or_else(|| storages.first())
        .ok_or_else(|| {
            KalamDbError::NotFound("no storage configured for function artifacts".to_string())
        })?;
    registry
        .get_cached(&preferred.storage_id)
        .map_err(|error| KalamDbError::ExecutionError(format!("failed to load storage: {error}")))?
        .ok_or_else(|| {
            KalamDbError::NotFound(format!("storage {} is not cached", preferred.storage_id))
        })
}

fn owner_role(app: &AppContext, owner: &UserId) -> Result<Role, KalamDbError> {
    let user = app.system_tables().users().get_user_by_id(owner).map_err(|error| {
        KalamDbError::ExecutionError(format!("failed to load procedure owner: {error}"))
    })?;
    Ok(user.map(|user| user.role).unwrap_or(Role::User))
}

fn is_sql_language(language: &str) -> bool {
    language.eq_ignore_ascii_case("SQL")
}

pub(super) fn stage_topic_publish(
    app: &AppContext,
    exec_ctx: &ExecutionContext,
    topic: &str,
    payload: &RoutineValue,
) -> Result<(), KalamDbError> {
    let request_id = exec_ctx.request_id().ok_or_else(|| {
        KalamDbError::InvalidOperation(
            "topic publish requires an active request transaction".to_string(),
        )
    })?;
    let coordinator = AppContextRequestTransactionCoordinator::new(app);
    let mut request_state =
        RequestTransactionState::from_request_id(Some(request_id)).expect("request id is present");
    request_state.sync(&coordinator);
    let transaction_id = request_state.active_transaction_id().cloned().ok_or_else(|| {
        KalamDbError::InvalidOperation(
            "topic publish requires an active request transaction".to_string(),
        )
    })?;

    let json = scalar_value_to_json(&payload.value).map_err(|error| {
        KalamDbError::ExecutionError(format!("failed to encode topic payload: {error}"))
    })?;
    let encoded = kalamdb_serialization::encode_object(&json.0).map_err(|error| {
        KalamDbError::ExecutionError(format!("failed to encode topic payload: {error}"))
    })?;
    let topic_id = TopicId::new(topic.to_ascii_lowercase());
    if !app.topic_publisher().topic_exists(&topic_id) {
        return Err(KalamDbError::NotFound(format!("topic {topic} not found")));
    }
    app.function_runtime().stage(
        transaction_id,
        StagedTopicPublish {
            topic_id,
            payload: encoded.into_bytes(),
            user_id: Some(exec_ctx.user_id().clone()),
        },
    );
    Ok(())
}

pub(crate) fn flush_staged_publishes(
    app: &AppContext,
    transaction_id: &TransactionId,
) -> Result<(), KalamDbError> {
    let staged = app.function_runtime().take(transaction_id);
    for publish in staged {
        app.topic_publisher()
            .publish_typed(&publish.topic_id, publish.payload, publish.user_id.as_ref())
            .map_err(|error| {
                KalamDbError::ExecutionError(format!(
                    "failed to flush typed topic publish: {error}"
                ))
            })?;
    }
    Ok(())
}

pub(crate) fn drop_staged_publishes(app: &AppContext, transaction_id: &TransactionId) {
    let _ = app.function_runtime().take(transaction_id);
}

fn routine_value_to_execution_result(
    value: &RoutineValue,
) -> Result<ExecutionResult, KalamDbError> {
    let array = value
        .value
        .to_array()
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    let field = Field::new("result", array.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![array])
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    Ok(ExecutionResult::Rows {
        row_count: batch.num_rows(),
        batches:   vec![batch],
        schema:    None,
    })
}

fn map_functions(error: FunctionsError) -> KalamDbError {
    match error {
        FunctionsError::UnknownProcedure(name) => {
            KalamDbError::NotFound(format!("procedure {name} not found"))
        },
        FunctionsError::Timeout => {
            KalamDbError::ExecutionError("procedure invocation timed out".to_string())
        },
        FunctionsError::Cancelled => {
            KalamDbError::ExecutionError("procedure invocation cancelled".to_string())
        },
        FunctionsError::MemoryLimit => {
            KalamDbError::ExecutionError("procedure memory limit exceeded".to_string())
        },
        other => KalamDbError::ExecutionError(other.to_string()),
    }
}
