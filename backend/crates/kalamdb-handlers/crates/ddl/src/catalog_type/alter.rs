//! Typed handler for ALTER TYPE.

use std::sync::Arc;

use kalamdb_commons::models::{CatalogTypeKind, NamespaceId, TypeId};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::{AlterTypeOperation, AlterTypeStatement};
use kalamdb_system::{CatalogStores, CatalogType};

use super::create::catalog_field;
use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct AlterTypeHandler {
    app_context: Arc<AppContext>,
}

impl AlterTypeHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<AlterTypeStatement> for AlterTypeHandler {
    async fn execute(
        &self,
        statement: AlterTypeStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "alter type")?;
        let app = Arc::clone(&self.app_context);
        run_blocking(move || persist_alter_type(&app.system_tables().catalog_stores(), statement))
            .await
    }
}

fn persist_alter_type(
    stores: &CatalogStores,
    statement: AlterTypeStatement,
) -> Result<ExecutionResult, KalamDbError> {
    let catalog_type = stores
        .get_type(&statement.type_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .ok_or_else(|| KalamDbError::NotFound(format!("type {} not found", statement.type_id)))?;

    match statement.operation {
        AlterTypeOperation::SetSchema { schema } => {
            persist_set_schema(stores, catalog_type, schema)
        },
        other => persist_attribute_op(stores, catalog_type, other),
    }
}

fn persist_attribute_op(
    stores: &CatalogStores,
    catalog_type: CatalogType,
    operation: AlterTypeOperation,
) -> Result<ExecutionResult, KalamDbError> {
    if catalog_type.kind != CatalogTypeKind::Composite {
        return Err(KalamDbError::InvalidSql(format!(
            "ALTER TYPE {} attribute operations require a composite type",
            catalog_type.type_id
        )));
    }
    let mut fields = stores
        .list_type_fields(&catalog_type.type_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    match operation {
        AlterTypeOperation::AddAttribute { field, type_ref } => {
            if fields.iter().any(|existing| existing.name == field) {
                return Err(KalamDbError::AlreadyExists(format!(
                    "attribute {field} already exists on {}",
                    catalog_type.type_id
                )));
            }
            let ordinal = fields.iter().map(|existing| existing.ordinal).max().unwrap_or(0) + 1;
            fields.push(catalog_field(
                &catalog_type.type_id,
                &catalog_type.namespace_id,
                field,
                type_ref,
                ordinal,
            )?);
        },
        AlterTypeOperation::DropAttribute { field, cascade } => {
            if cascade {
                return Err(KalamDbError::InvalidSql(
                    "DROP ATTRIBUTE CASCADE is not supported; drop dependents first".to_string(),
                ));
            }
            let before = fields.len();
            fields.retain(|existing| existing.name != field);
            if fields.len() == before {
                return Err(KalamDbError::NotFound(format!(
                    "attribute {field} not found on {}",
                    catalog_type.type_id
                )));
            }
        },
        AlterTypeOperation::RenameAttribute { from, to } => {
            if !fields.iter().any(|field| field.name == from) {
                return Err(KalamDbError::NotFound(format!(
                    "attribute {from} not found on {}",
                    catalog_type.type_id
                )));
            }
            if fields.iter().any(|field| field.name == to) {
                return Err(KalamDbError::AlreadyExists(format!(
                    "attribute {to} already exists on {}",
                    catalog_type.type_id
                )));
            }
            for existing in &mut fields {
                if existing.name == from {
                    existing.name = to;
                    existing.type_field_id = kalamdb_commons::models::TypeFieldId::new(
                        &catalog_type.type_id,
                        &existing.name,
                    )
                    .map_err(KalamDbError::InvalidSql)?;
                    break;
                }
            }
        },
        AlterTypeOperation::AlterAttributeType { field, type_ref } => {
            let Some(existing) = fields.iter_mut().find(|item| item.name == field) else {
                return Err(KalamDbError::NotFound(format!(
                    "attribute {field} not found on {}",
                    catalog_type.type_id
                )));
            };
            let rebuilt = catalog_field(
                &catalog_type.type_id,
                &catalog_type.namespace_id,
                field,
                type_ref,
                existing.ordinal,
            )?;
            *existing = rebuilt;
        },
        AlterTypeOperation::SetSchema { .. } => unreachable!("handled separately"),
    }
    stores
        .replace_type_fields(&catalog_type.type_id, fields)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    Ok(ExecutionResult::Success {
        message: format!("Type {} altered", catalog_type.type_id),
    })
}

fn persist_set_schema(
    stores: &CatalogStores,
    catalog_type: CatalogType,
    namespace_id: NamespaceId,
) -> Result<ExecutionResult, KalamDbError> {
    if matches!(catalog_type.kind, CatalogTypeKind::ImplicitTableRow | CatalogTypeKind::RowAlias) {
        return Err(KalamDbError::InvalidSql(format!(
            "cannot SET SCHEMA on {} type {}",
            catalog_type.kind.as_str(),
            catalog_type.type_id
        )));
    }
    let new_id = TypeId::from_parts(Some(&namespace_id), &catalog_type.name);
    if new_id == catalog_type.type_id {
        return Ok(ExecutionResult::Success {
            message: format!("Type {} already in schema {}", catalog_type.type_id, namespace_id),
        });
    }
    if stores
        .get_type(&new_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .is_some()
    {
        return Err(KalamDbError::AlreadyExists(format!("type {new_id} already exists")));
    }
    let fields = stores
        .list_type_fields(&catalog_type.type_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    let mut moved = catalog_type.clone();
    moved.type_id = new_id.clone();
    moved.namespace_id = namespace_id;
    stores
        .upsert_type(moved)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    let moved_fields = fields
        .into_iter()
        .map(|mut field| {
            field.type_id = new_id.clone();
            field.type_field_id = kalamdb_commons::models::TypeFieldId::new(&new_id, &field.name)
                .map_err(KalamDbError::InvalidSql)?;
            Ok(field)
        })
        .collect::<Result<Vec<_>, KalamDbError>>()?;
    stores
        .replace_type_fields(&new_id, moved_fields)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    stores
        .drop_type(&catalog_type.type_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    Ok(ExecutionResult::Success {
        message: format!("Type {} moved to {new_id}", catalog_type.type_id),
    })
}
