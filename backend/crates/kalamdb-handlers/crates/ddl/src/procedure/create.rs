//! Typed handler for CREATE PROCEDURE.

use std::sync::Arc;

use kalamdb_commons::{
    models::{RoutineParameterId, UserId},
    FunctionModuleId, FunctionRuntime,
};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_functions::{hash_artifact_bytes, wrap_procedure_source, FunctionActivation};
use kalamdb_sql::ddl::CreateProcedureStatement;
use kalamdb_system::{CatalogRoutine, CatalogRoutineParameter};

use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct CreateProcedureHandler {
    app_context: Arc<AppContext>,
}

impl CreateProcedureHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<CreateProcedureStatement> for CreateProcedureHandler {
    async fn execute(
        &self,
        statement: CreateProcedureStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "create procedure")?;
        let app = Arc::clone(&self.app_context);
        let owner = context.user_id().clone();
        let routine = catalog_routine(&statement, owner);
        let parameters = catalog_parameters(&statement)?;
        let existing = {
            let stores = app.system_tables().catalog_stores();
            stores
                .get_routine(&statement.routine_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        };
        if existing.is_some() && !statement.or_replace {
            return Err(KalamDbError::AlreadyExists(format!(
                "procedure {} already exists",
                statement.routine_id
            )));
        }

        let routine_id = statement.routine_id.clone();
        let app_for_catalog = Arc::clone(&app);
        let routine_for_catalog = routine.clone();
        let parameters_for_catalog = parameters.clone();
        run_blocking(move || {
            let stores = app_for_catalog.system_tables().catalog_stores();
            stores
                .upsert_routine(routine_for_catalog)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            stores
                .replace_parameters(&routine_id, parameters_for_catalog)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            Ok::<(), KalamDbError>(())
        })
        .await?;

        if should_activate_javascript(statement.language.as_deref()) {
            activate_javascript(&app, &statement).await?;
        }

        Ok(ExecutionResult::Success {
            message: format!("Procedure {} created", statement.routine_id),
        })
    }
}

fn catalog_routine(statement: &CreateProcedureStatement, owner: UserId) -> CatalogRoutine {
    CatalogRoutine {
        routine_id: statement.routine_id.clone(),
        namespace_id: statement.namespace_id.clone(),
        name: statement.name.clone(),
        owner,
        security: statement.security,
        language: statement.language.clone(),
        body: statement.body.clone(),
        return_type_id: statement
            .return_type
            .as_ref()
            .and_then(|ty| ty.resolved_type_id(&statement.namespace_id)),
        return_type_name: statement
            .return_type
            .as_ref()
            .map(|ty| ty.resolved_type_name(&statement.namespace_id)),
        return_is_array: statement.return_type.as_ref().is_some_and(|ty| ty.is_array),
        return_not_null: statement.return_type.as_ref().is_some_and(|ty| ty.not_null),
        comment: None,
        return_data_type: statement.return_type.as_ref().and_then(|ty| ty.builtin_data_type()),
    }
}

fn catalog_parameters(
    statement: &CreateProcedureStatement,
) -> Result<Vec<CatalogRoutineParameter>, KalamDbError> {
    let mut parameters = Vec::with_capacity(statement.parameters.len());
    for (index, parameter) in statement.parameters.iter().enumerate() {
        let ordinal = (index + 1) as i32;
        parameters.push(CatalogRoutineParameter {
            parameter_id: RoutineParameterId::new(&statement.routine_id, ordinal)
                .map_err(|error| KalamDbError::InvalidSql(error))?,
            routine_id: statement.routine_id.clone(),
            name: parameter.name.clone(),
            ordinal,
            type_id: parameter.type_ref.resolved_type_id(&statement.namespace_id),
            type_name: parameter.type_ref.resolved_type_name(&statement.namespace_id),
            is_array: parameter.type_ref.is_array,
            not_null: parameter.type_ref.not_null,
            nonempty: parameter.type_ref.nonempty,
            data_type: parameter.type_ref.builtin_data_type(),
        });
    }
    Ok(parameters)
}

fn should_activate_javascript(language: Option<&str>) -> bool {
    matches!(
        language.map(|value| value.to_ascii_uppercase()).as_deref(),
        Some("JAVASCRIPT" | "JS" | "TYPESCRIPT" | "TS")
    )
}

async fn activate_javascript(
    app: &Arc<AppContext>,
    statement: &CreateProcedureStatement,
) -> Result<(), KalamDbError> {
    let body = statement.body.as_deref().ok_or_else(|| {
        KalamDbError::InvalidSql(format!(
            "javascript procedure {} requires an AS $$ ... $$ body",
            statement.routine_id
        ))
    })?;
    let source = wrap_procedure_source(body);
    let storage = kalamdb_core::functions::function_storage(app)?;
    let stores = app.system_tables().catalog_stores();
    let activation = FunctionActivation::new(stores);
    let artifact = activation
        .upload(storage.as_ref(), source.as_bytes(), FunctionRuntime::Typescript)
        .await
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    let contract_hash = hash_artifact_bytes(source.as_bytes()).as_str().to_string();
    let (module, revision, artifact) = FunctionActivation::prepared_activation(
        FunctionModuleId::new(statement.routine_id.as_str()),
        artifact,
        contract_hash,
    );
    let expected_revision_id = activation
        .active_module(&module.module_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .and_then(|module| module.active_revision_id);
    let cmd = kalamdb_raft::MetaCommand::ActivateFunctionRevision {
        module,
        revision,
        artifact,
        expected_revision_id,
    };
    app.executor().execute_meta(cmd).await.map(|_| ()).map_err(|error| {
        KalamDbError::ExecutionError(format!(
            "failed to activate procedure {}: {error}",
            statement.routine_id
        ))
    })
}
