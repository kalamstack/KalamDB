//! Resolve contract types to Arrow DataTypes.

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field};
use kalamdb_commons::{
    models::{datatypes::ToArrowType, NamespaceId},
    KalamDataType,
};

use super::{ContractError, ContractTypeKind};
use crate::ddl::TypeReference;

pub fn builtin_arrow_type(name: &str) -> Option<DataType> {
    KalamDataType::from_sql_name(name)?.to_arrow_type().ok()
}

pub fn is_builtin_type(name: &str) -> bool {
    KalamDataType::from_sql_name(name).is_some()
}

pub fn resolve_arrow_type(
    type_ref: &TypeReference,
    types: &HashMap<String, (ContractTypeKind, Option<DataType>)>,
    visiting: &mut Vec<String>,
) -> Result<DataType, ContractError> {
    let inner = if let Some(data_type) = type_ref.data_type {
        data_type
            .to_arrow_type()
            .map_err(|error| ContractError::new(error.to_string()))?
    } else {
        resolve_named_or_builtin(
            &type_ref.name,
            type_ref.namespace_id.as_ref().map(NamespaceId::as_str),
            types,
            visiting,
        )?
    };
    if type_ref.is_array {
        Ok(DataType::List(std::sync::Arc::new(Field::new(
            "item",
            inner,
            !type_ref.not_null,
        ))))
    } else {
        Ok(inner)
    }
}

fn resolve_named_or_builtin(
    name: &str,
    schema: Option<&str>,
    types: &HashMap<String, (ContractTypeKind, Option<DataType>)>,
    visiting: &mut Vec<String>,
) -> Result<DataType, ContractError> {
    if schema.is_none() {
        if let Some(builtin) = builtin_arrow_type(name) {
            return Ok(builtin);
        }
    }
    let key = match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    };
    if visiting.iter().any(|id| id == &key) {
        return Err(ContractError::new(format!("cycle in type references involving '{key}'")));
    }
    let Some((kind, cached)) = types.get(&key) else {
        return Err(ContractError::new(format!("unknown type '{key}'")));
    };
    if let Some(arrow) = cached {
        return Ok(arrow.clone());
    }
    visiting.push(key.clone());
    let arrow = match kind {
        ContractTypeKind::Enum { .. } => DataType::Utf8,
        ContractTypeKind::RowAlias { source } => {
            resolve_named_or_builtin(source.as_str(), None, types, visiting)?
        },
        ContractTypeKind::ImplicitTableRow { fields, .. }
        | ContractTypeKind::Composite { fields } => {
            let mut arrow_fields = Vec::with_capacity(fields.len());
            for field in fields {
                let mut nested = TypeReference {
                    namespace_id: None,
                    name:         field.type_name.clone(),
                    data_type:    field.data_type,
                    is_array:     field.is_array,
                    not_null:     field.not_null,
                    nonempty:     field.nonempty,
                };
                if let Some(type_id) = &field.type_id {
                    let (schema, name) = split_type_id(type_id.as_str());
                    nested.namespace_id = schema.map(NamespaceId::new);
                    nested.name = name;
                    nested.data_type = None;
                }
                let dt = resolve_arrow_type(&nested, types, visiting)?;
                arrow_fields.push(Field::new(&field.name, dt, !field.not_null));
            }
            DataType::Struct(arrow_fields.into())
        },
    };
    visiting.pop();
    Ok(arrow)
}

fn split_type_id(id: &str) -> (Option<String>, String) {
    match id.rsplit_once('.') {
        Some((schema, name)) => (Some(schema.to_string()), name.to_string()),
        None => (None, id.to_string()),
    }
}
