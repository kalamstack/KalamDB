//! Typed handler for CREATE TYPE.

use std::sync::Arc;

use kalamdb_commons::{
    models::{CatalogTypeKind, NamespaceId, TableId, TypeId},
    schemas::{TableDefinition, TableName},
};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::{CreateTypeBody, CreateTypeStatement, TypeReference};
use kalamdb_system::{CatalogStores, CatalogType, CatalogTypeField};

use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct CreateTypeHandler {
    app_context: Arc<AppContext>,
}

impl CreateTypeHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<CreateTypeStatement> for CreateTypeHandler {
    async fn execute(
        &self,
        statement: CreateTypeStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "create type")?;
        let app = Arc::clone(&self.app_context);
        run_blocking(move || persist_create_type(&app, statement)).await
    }
}

fn persist_create_type(
    app: &AppContext,
    statement: CreateTypeStatement,
) -> Result<ExecutionResult, KalamDbError> {
    let stores = app.system_tables().catalog_stores();
    if stores
        .get_type(&statement.type_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .is_some()
    {
        if statement.if_not_exists {
            return Ok(ExecutionResult::Success {
                message: format!("Type {} already exists, skipping", statement.type_id),
            });
        }
        return Err(KalamDbError::AlreadyExists(format!(
            "type {} already exists",
            statement.type_id
        )));
    }

    match &statement.body {
        CreateTypeBody::Composite { fields } => {
            upsert_named_type(&stores, &statement, CatalogTypeKind::Composite)?;
            let catalog_fields = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    catalog_field(
                        &statement.type_id,
                        &statement.namespace_id,
                        field.name.clone(),
                        field.type_ref.clone(),
                        (index + 1) as i32,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            stores
                .replace_type_fields(&statement.type_id, catalog_fields)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
        },
        CreateTypeBody::Enum { labels } => {
            upsert_named_type(&stores, &statement, CatalogTypeKind::Enum)?;
            let catalog_fields = labels
                .iter()
                .enumerate()
                .map(|(index, label)| {
                    CatalogTypeField::new(
                        statement.type_id.clone(),
                        label.clone(),
                        (index + 1) as i32,
                        None,
                        None,
                        label.clone(),
                        false,
                        true,
                        false,
                    )
                    .map_err(KalamDbError::InvalidSql)
                })
                .collect::<Result<Vec<_>, _>>()?;
            stores
                .replace_type_fields(&statement.type_id, catalog_fields)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
        },
        CreateTypeBody::FromTable {
            table_namespace_id,
            table_name,
        } => {
            persist_from_table(
                app,
                &stores,
                &statement,
                table_namespace_id.clone(),
                table_name.clone(),
            )?;
        },
    }

    Ok(ExecutionResult::Success {
        message: format!("Type {} created", statement.type_id),
    })
}

fn upsert_named_type(
    stores: &CatalogStores,
    statement: &CreateTypeStatement,
    kind: CatalogTypeKind,
) -> Result<(), KalamDbError> {
    stores
        .upsert_type(CatalogType {
            type_id: statement.type_id.clone(),
            namespace_id: statement.namespace_id.clone(),
            name: statement.name.clone(),
            kind,
            table_id: None,
            source_type_id: None,
            comment: None,
        })
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))
}

fn persist_from_table(
    app: &AppContext,
    stores: &CatalogStores,
    statement: &CreateTypeStatement,
    table_namespace_id: Option<NamespaceId>,
    table_name: String,
) -> Result<(), KalamDbError> {
    let table_schema = table_namespace_id.unwrap_or_else(|| statement.namespace_id.clone());
    if table_schema != statement.namespace_id {
        return Err(KalamDbError::InvalidSql(
            "FROM TABLE alias and table must live in the same schema".to_string(),
        ));
    }
    let table_id = TableId::new(table_schema, TableName::new(table_name));
    let Some(cached) = app.schema_registry().get(&table_id) else {
        return Err(KalamDbError::NotFound(format!("table {table_id} not found")));
    };

    let implicit_id = ensure_implicit_row_type(stores, &table_id, &cached.table)?;
    if statement.type_id == implicit_id {
        return Ok(());
    }

    for existing in stores
        .list_types()
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
    {
        if existing.kind == CatalogTypeKind::RowAlias
            && existing.source_type_id.as_ref() == Some(&implicit_id)
            && existing.type_id != statement.type_id
        {
            return Err(KalamDbError::AlreadyExists(format!(
                "table {table_id} already has row alias {}",
                existing.type_id
            )));
        }
    }

    stores
        .upsert_type(CatalogType {
            type_id:        statement.type_id.clone(),
            namespace_id:   statement.namespace_id.clone(),
            name:           statement.name.clone(),
            kind:           CatalogTypeKind::RowAlias,
            table_id:       None,
            source_type_id: Some(implicit_id),
            comment:        None,
        })
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))
}

pub fn ensure_implicit_row_type(
    stores: &CatalogStores,
    table_id: &TableId,
    table_def: &TableDefinition,
) -> Result<TypeId, KalamDbError> {
    let type_id = TypeId::from_parts(Some(table_id.namespace_id()), table_id.table_name().as_str());
    if let Some(existing) = stores
        .get_type(&type_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
    {
        if existing.kind != CatalogTypeKind::ImplicitTableRow {
            return Err(KalamDbError::AlreadyExists(format!("type {type_id} already exists")));
        }
        return Ok(type_id);
    }

    stores
        .upsert_type(CatalogType {
            type_id:        type_id.clone(),
            namespace_id:   table_id.namespace_id().clone(),
            name:           table_id.table_name().as_str().to_string(),
            kind:           CatalogTypeKind::ImplicitTableRow,
            table_id:       Some(table_id.clone()),
            source_type_id: None,
            comment:        None,
        })
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;

    let fields = table_def
        .columns
        .iter()
        .map(|column| {
            CatalogTypeField::from_column(&type_id, column).map_err(KalamDbError::InvalidSql)
        })
        .collect::<Result<Vec<_>, _>>()?;
    stores
        .replace_type_fields(&type_id, fields)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    Ok(type_id)
}

pub(super) fn catalog_field(
    type_id: &TypeId,
    current_schema: &NamespaceId,
    name: String,
    type_ref: TypeReference,
    ordinal: i32,
) -> Result<CatalogTypeField, KalamDbError> {
    let (type_name, field_type_id) = if let Some(data_type) = type_ref.builtin_data_type() {
        (data_type.sql_name(), None)
    } else {
        let nested = type_ref.resolved_type_id(current_schema).expect("named types have a type id");
        (nested.to_string(), Some(nested))
    };
    CatalogTypeField::new(
        type_id.clone(),
        name,
        ordinal,
        field_type_id,
        type_ref.builtin_data_type(),
        type_name,
        type_ref.is_array,
        type_ref.not_null,
        type_ref.nonempty,
    )
    .map_err(KalamDbError::InvalidSql)
}
