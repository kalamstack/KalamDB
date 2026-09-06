//! Direct V8 ↔ ScalarValue conversion without JSON stringify except for JSON SQL.

use std::sync::Arc;

use arrow::{
    array::{Array, StructArray},
    datatypes::{DataType, Field, TimeUnit},
};
use datafusion_common::ScalarValue;
use v8::{self, Local, PinScope};

use crate::{
    error::{FunctionsError, Result},
    value::RoutineValue,
};

pub fn routine_to_v8<'s>(
    scope: &PinScope<'s, '_>,
    value: &RoutineValue,
) -> Result<Local<'s, v8::Value>> {
    if value.json_sql {
        return json_sql_to_v8(scope, &value.value);
    }
    scalar_to_v8(scope, &value.value)
}

pub fn v8_to_routine<'s>(
    scope: &PinScope<'s, '_>,
    value: Local<'s, v8::Value>,
    template: &RoutineValue,
) -> Result<RoutineValue> {
    if template.json_sql {
        let parsed = v8_to_json_sql(scope, value)?;
        return Ok(RoutineValue {
            type_id:  template.type_id.clone(),
            value:    parsed,
            json_sql: true,
        });
    }
    let scalar = v8_to_scalar(scope, value, &template.value.data_type())?;
    Ok(RoutineValue {
        type_id:  template.type_id.clone(),
        value:    scalar,
        json_sql: false,
    })
}

fn json_sql_to_v8<'s>(
    scope: &PinScope<'s, '_>,
    value: &ScalarValue,
) -> Result<Local<'s, v8::Value>> {
    let text = match value {
        ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) => text.as_str(),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => {
            return Ok(v8::null(scope).into());
        },
        other => {
            return Err(FunctionsError::Invalid(format!(
                "json sql value must be utf8, got {other:?}"
            )));
        },
    };
    let source = v8::String::new(scope, text)
        .ok_or_else(|| FunctionsError::Invalid("json text too large".to_string()))?;
    v8::json::parse(scope, source)
        .ok_or_else(|| FunctionsError::Invalid("invalid json sql value".to_string()))
}

fn v8_to_json_sql<'s>(
    scope: &PinScope<'s, '_>,
    value: Local<'s, v8::Value>,
) -> Result<ScalarValue> {
    if value.is_null_or_undefined() {
        return Ok(ScalarValue::Utf8(None));
    }
    let text = v8::json::stringify(scope, value)
        .ok_or_else(|| FunctionsError::Invalid("failed to stringify json sql value".to_string()))?;
    Ok(ScalarValue::Utf8(Some(text.to_rust_string_lossy(scope))))
}

fn scalar_to_v8<'s>(scope: &PinScope<'s, '_>, value: &ScalarValue) -> Result<Local<'s, v8::Value>> {
    Ok(match value {
        ScalarValue::Null => v8::null(scope).into(),
        ScalarValue::Boolean(Some(flag)) => v8::Boolean::new(scope, *flag).into(),
        ScalarValue::Boolean(None) => v8::null(scope).into(),
        ScalarValue::Int8(Some(n)) => v8::Number::new(scope, *n as f64).into(),
        ScalarValue::Int16(Some(n)) => v8::Number::new(scope, *n as f64).into(),
        ScalarValue::Int32(Some(n)) => v8::Integer::new(scope, *n).into(),
        ScalarValue::Int64(Some(n)) => i64_to_v8(scope, *n),
        ScalarValue::UInt8(Some(n)) => v8::Number::new(scope, *n as f64).into(),
        ScalarValue::UInt16(Some(n)) => v8::Number::new(scope, *n as f64).into(),
        ScalarValue::UInt32(Some(n)) => v8::Number::new(scope, *n as f64).into(),
        ScalarValue::UInt64(Some(n)) => i64_to_v8(scope, *n as i64),
        ScalarValue::Float32(Some(n)) => v8::Number::new(scope, *n as f64).into(),
        ScalarValue::Float64(Some(n)) => v8::Number::new(scope, *n).into(),
        ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) => {
            v8::String::new(scope, text)
                .ok_or_else(|| FunctionsError::Invalid("utf8 value too large".to_string()))?
                .into()
        },
        ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None) => v8::null(scope).into(),
        ScalarValue::Struct(array) => struct_to_v8(scope, array)?,
        ScalarValue::List(array) => list_to_v8(scope, array.as_ref())?,
        other => {
            return Err(FunctionsError::Invalid(format!("unsupported function value: {other:?}")));
        },
    })
}

fn i64_to_v8<'s>(scope: &PinScope<'s, '_>, value: i64) -> Local<'s, v8::Value> {
    if value.abs() <= (1i64 << 53) {
        v8::Number::new(scope, value as f64).into()
    } else {
        v8::BigInt::new_from_i64(scope, value).into()
    }
}

fn struct_to_v8<'s>(scope: &PinScope<'s, '_>, array: &StructArray) -> Result<Local<'s, v8::Value>> {
    if array.len() == 0 || array.is_null(0) {
        return Ok(v8::null(scope).into());
    }
    let object = v8::Object::new(scope);
    for (index, field) in array.fields().iter().enumerate() {
        let column = array.column(index);
        let scalar = ScalarValue::try_from_array(column, 0).map_err(|error| {
            FunctionsError::Invalid(format!("struct field '{}': {error}", field.name()))
        })?;
        let js_value = scalar_to_v8(scope, &scalar)?;
        let key = v8::String::new(scope, field.name())
            .ok_or_else(|| FunctionsError::Invalid("struct field name too large".to_string()))?;
        object
            .set(scope, key.into(), js_value)
            .ok_or_else(|| FunctionsError::Invalid("failed to set struct field".to_string()))?;
    }
    Ok(object.into())
}

fn list_to_v8<'s>(scope: &PinScope<'s, '_>, array: &dyn Array) -> Result<Local<'s, v8::Value>> {
    let length = if array.len() == 0 { 0 } else { array.len() };
    // List ScalarValue stores a ListArray of length 1 whose values are the list items.
    if let Some(list) = array.as_any().downcast_ref::<arrow::array::ListArray>() {
        if list.len() == 0 || list.is_null(0) {
            return Ok(v8::null(scope).into());
        }
        let values = list.value(0);
        let js_array = v8::Array::new(scope, values.len() as i32);
        for index in 0..values.len() {
            let scalar = ScalarValue::try_from_array(values.as_ref(), index).map_err(|error| {
                FunctionsError::Invalid(format!("list element {index}: {error}"))
            })?;
            let js_value = scalar_to_v8(scope, &scalar)?;
            js_array
                .set_index(scope, index as u32, js_value)
                .ok_or_else(|| FunctionsError::Invalid("failed to set list element".to_string()))?;
        }
        return Ok(js_array.into());
    }
    let js_array = v8::Array::new(scope, length as i32);
    Ok(js_array.into())
}

fn v8_to_scalar<'s>(
    scope: &PinScope<'s, '_>,
    value: Local<'s, v8::Value>,
    data_type: &DataType,
) -> Result<ScalarValue> {
    if value.is_null_or_undefined() {
        return ScalarValue::try_from(data_type)
            .map_err(|error| FunctionsError::Invalid(error.to_string()));
    }
    match data_type {
        DataType::Boolean => Ok(ScalarValue::Boolean(Some(value.boolean_value(scope)))),
        DataType::Int8 => Ok(ScalarValue::Int8(Some(value.int32_value(scope).unwrap_or(0) as i8))),
        DataType::Int16 => {
            Ok(ScalarValue::Int16(Some(value.int32_value(scope).unwrap_or(0) as i16)))
        },
        DataType::Int32 => Ok(ScalarValue::Int32(Some(value.int32_value(scope).unwrap_or(0)))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(js_to_i64(scope, value)))),
        DataType::UInt8 => {
            Ok(ScalarValue::UInt8(Some(value.uint32_value(scope).unwrap_or(0) as u8)))
        },
        DataType::UInt16 => {
            Ok(ScalarValue::UInt16(Some(value.uint32_value(scope).unwrap_or(0) as u16)))
        },
        DataType::UInt32 => Ok(ScalarValue::UInt32(Some(value.uint32_value(scope).unwrap_or(0)))),
        DataType::UInt64 => Ok(ScalarValue::UInt64(Some(js_to_i64(scope, value) as u64))),
        DataType::Float32 => {
            Ok(ScalarValue::Float32(Some(value.number_value(scope).unwrap_or(0.0) as f32)))
        },
        DataType::Float64 => {
            Ok(ScalarValue::Float64(Some(value.number_value(scope).unwrap_or(0.0))))
        },
        DataType::Utf8 => {
            let text = value
                .to_string(scope)
                .map(|s| s.to_rust_string_lossy(scope))
                .unwrap_or_default();
            Ok(ScalarValue::Utf8(Some(text)))
        },
        DataType::Struct(fields) => v8_object_to_struct(scope, value, fields),
        DataType::List(field) => v8_array_to_list(scope, value, field),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            Ok(ScalarValue::TimestampMicrosecond(Some(js_to_i64(scope, value)), tz.clone()))
        },
        DataType::Null => infer_v8_value(scope, value),
        other => Err(FunctionsError::Invalid(format!("unsupported function return type: {other}"))),
    }
}

pub fn infer_v8_value<'s>(
    scope: &PinScope<'s, '_>,
    value: Local<'s, v8::Value>,
) -> Result<ScalarValue> {
    if value.is_null_or_undefined() {
        return Ok(ScalarValue::Null);
    }
    if value.is_boolean() {
        return Ok(ScalarValue::Boolean(Some(value.boolean_value(scope))));
    }
    if value.is_int32() {
        return Ok(ScalarValue::Int32(Some(value.int32_value(scope).unwrap_or(0))));
    }
    if value.is_number() {
        let number = value.number_value(scope).unwrap_or(0.0);
        if number.fract() == 0.0 && number.abs() <= i64::MAX as f64 {
            return Ok(ScalarValue::Int64(Some(number as i64)));
        }
        return Ok(ScalarValue::Float64(Some(number)));
    }
    if value.is_string() {
        let text = value
            .to_string(scope)
            .map(|s| s.to_rust_string_lossy(scope))
            .unwrap_or_default();
        return Ok(ScalarValue::Utf8(Some(text)));
    }
    if value.is_array() {
        let array = v8::Local::<v8::Array>::try_from(value)
            .map_err(|_| FunctionsError::Invalid("expected array".to_string()))?;
        let len = array.length();
        let mut items = Vec::with_capacity(len as usize);
        for index in 0..len {
            let element = array.get_index(scope, index).unwrap_or_else(|| v8::null(scope).into());
            items.push(infer_v8_value(scope, element)?);
        }
        let item_type = items.first().map(|item| item.data_type()).unwrap_or(DataType::Utf8);
        return Ok(ScalarValue::List(ScalarValue::new_list(&items, &item_type, true)));
    }
    if value.is_object() {
        return infer_v8_object(scope, value);
    }
    let text = value
        .to_string(scope)
        .map(|s| s.to_rust_string_lossy(scope))
        .unwrap_or_default();
    Ok(ScalarValue::Utf8(Some(text)))
}

fn infer_v8_object<'s>(
    scope: &PinScope<'s, '_>,
    value: Local<'s, v8::Value>,
) -> Result<ScalarValue> {
    let object = value
        .to_object(scope)
        .ok_or_else(|| FunctionsError::Invalid("expected object".to_string()))?;
    let names = object
        .get_own_property_names(scope, v8::GetPropertyNamesArgsBuilder::new().build())
        .ok_or_else(|| FunctionsError::Invalid("failed to list object keys".to_string()))?;
    let mut columns: Vec<(Arc<Field>, arrow::array::ArrayRef)> =
        Vec::with_capacity(names.length() as usize);
    for index in 0..names.length() {
        let key_value = names
            .get_index(scope, index)
            .ok_or_else(|| FunctionsError::Invalid("missing object key".to_string()))?;
        let key = key_value
            .to_string(scope)
            .map(|s| s.to_rust_string_lossy(scope))
            .unwrap_or_default();
        let property = object.get(scope, key_value).unwrap_or_else(|| v8::null(scope).into());
        let scalar = infer_v8_value(scope, property)?;
        let field = Arc::new(Field::new(&key, scalar.data_type(), true));
        let array = scalar
            .to_array()
            .map_err(|error| FunctionsError::Invalid(format!("object field '{key}': {error}")))?;
        columns.push((field, array));
    }
    Ok(ScalarValue::Struct(Arc::new(StructArray::from(columns))))
}

fn js_to_i64(scope: &PinScope<'_, '_>, value: Local<'_, v8::Value>) -> i64 {
    if value.is_big_int() {
        return value.to_big_int(scope).map(|n| n.i64_value().0).unwrap_or(0);
    }
    value.number_value(scope).unwrap_or(0.0) as i64
}

fn v8_object_to_struct<'s>(
    scope: &PinScope<'s, '_>,
    value: Local<'s, v8::Value>,
    fields: &arrow::datatypes::Fields,
) -> Result<ScalarValue> {
    let object = value
        .to_object(scope)
        .ok_or_else(|| FunctionsError::Invalid("expected object for struct".to_string()))?;
    let mut columns: Vec<(Arc<Field>, arrow::array::ArrayRef)> = Vec::with_capacity(fields.len());
    for field in fields.iter() {
        let key = v8::String::new(scope, field.name())
            .ok_or_else(|| FunctionsError::Invalid("struct field name too large".to_string()))?;
        let property = object.get(scope, key.into()).unwrap_or_else(|| v8::null(scope).into());
        let scalar = v8_to_scalar(scope, property, field.data_type())?;
        let array = scalar.to_array().map_err(|error| {
            FunctionsError::Invalid(format!("struct field '{}': {error}", field.name()))
        })?;
        columns.push((Arc::clone(field), array));
    }
    Ok(ScalarValue::Struct(Arc::new(StructArray::from(columns))))
}

fn v8_array_to_list<'s>(
    scope: &PinScope<'s, '_>,
    value: Local<'s, v8::Value>,
    field: &Arc<Field>,
) -> Result<ScalarValue> {
    let array = v8::Local::<v8::Array>::try_from(value)
        .map_err(|_| FunctionsError::Invalid("expected array for list".to_string()))?;
    let len = array.length();
    let mut items = Vec::with_capacity(len as usize);
    for index in 0..len {
        let element = array.get_index(scope, index).unwrap_or_else(|| v8::null(scope).into());
        items.push(v8_to_scalar(scope, element, field.data_type())?);
    }
    Ok(ScalarValue::List(ScalarValue::new_list(&items, field.data_type(), true)))
}
