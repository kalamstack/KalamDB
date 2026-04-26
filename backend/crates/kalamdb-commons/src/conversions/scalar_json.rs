use datafusion_common::ScalarValue;
use serde_json::{Number, Value};

use crate::models::datatypes::KalamDataType;

pub fn json_value_to_scalar_for_column(
    value: &Value,
    data_type: &KalamDataType,
) -> Result<ScalarValue, String> {
    if value.is_null() {
        return Ok(ScalarValue::Null);
    }

    let scalar = match data_type {
        KalamDataType::Boolean => ScalarValue::Boolean(Some(value.as_bool().unwrap_or(false))),
        KalamDataType::SmallInt => {
            let num = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
                .unwrap_or(0);
            ScalarValue::Int16(Some(num as i16))
        },
        KalamDataType::Int => {
            let num = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
                .unwrap_or(0);
            ScalarValue::Int32(Some(num as i32))
        },
        KalamDataType::BigInt
        | KalamDataType::Timestamp
        | KalamDataType::DateTime
        | KalamDataType::Time => {
            let num = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
                .unwrap_or(0);
            ScalarValue::Int64(Some(num))
        },
        KalamDataType::Date => {
            let num = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
                .unwrap_or(0);
            ScalarValue::Date32(Some(num as i32))
        },
        KalamDataType::Double => ScalarValue::Float64(Some(value.as_f64().unwrap_or(0.0))),
        KalamDataType::Float => ScalarValue::Float32(Some(value.as_f64().unwrap_or(0.0) as f32)),
        KalamDataType::Text | KalamDataType::Uuid => {
            let text = value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string());
            ScalarValue::Utf8(Some(text))
        },
        KalamDataType::Json | KalamDataType::File => match value {
            Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
            Value::Object(_) | Value::Array(_) => {
                // Store JSON objects/arrays as Utf8 JSON strings so DataFusion
                // JSON functions (datafusion-functions-json) can operate on them
                // directly without a binary↔string conversion layer.
                let json_str = serde_json::to_string(value)
                    .map_err(|e| format!("json field encode failed: {e}"))?;
                ScalarValue::Utf8(Some(json_str))
            },
            _ => ScalarValue::Utf8(Some(value.to_string())),
        },
        KalamDataType::Bytes => match value {
            Value::String(s) => ScalarValue::Binary(Some(s.as_bytes().to_vec())),
            Value::Array(arr) => {
                let bytes: Vec<u8> = arr
                    .iter()
                    .filter_map(|v| v.as_u64().and_then(|u| u8::try_from(u).ok()))
                    .collect();
                ScalarValue::Binary(Some(bytes))
            },
            _ => ScalarValue::Binary(Some(value.to_string().into_bytes())),
        },
        KalamDataType::Decimal { precision, scale } => {
            let parsed = match value {
                Value::String(s) => parse_decimal_value(s, *scale)?,
                Value::Number(n) => parse_decimal_value(&n.to_string(), *scale)?,
                _ => 0,
            };
            ScalarValue::Decimal128(Some(parsed), *precision, *scale as i8)
        },
        KalamDataType::Embedding(_) => ScalarValue::Utf8(Some(value.to_string())),
    };

    Ok(scalar)
}

pub fn scalar_to_json_for_column(
    scalar: &ScalarValue,
    data_type: &KalamDataType,
) -> Result<Value, String> {
    if matches!(scalar, ScalarValue::Null) {
        return Ok(Value::Null);
    }

    let json = match data_type {
        KalamDataType::Boolean => Value::Bool(extract_bool(scalar).unwrap_or(false)),
        KalamDataType::SmallInt
        | KalamDataType::Int
        | KalamDataType::BigInt
        | KalamDataType::Timestamp
        | KalamDataType::DateTime
        | KalamDataType::Time
        | KalamDataType::Date => {
            let value = extract_i64(scalar).unwrap_or(0);
            Value::Number(Number::from(value))
        },
        KalamDataType::Double | KalamDataType::Float => {
            let value = extract_f64(scalar).unwrap_or(0.0);
            Value::Number(
                Number::from_f64(value)
                    .ok_or_else(|| "invalid floating point value".to_string())?,
            )
        },
        KalamDataType::Text | KalamDataType::Uuid => {
            Value::String(extract_string(scalar).unwrap_or_default())
        },
        KalamDataType::Json | KalamDataType::File => match scalar {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                // Try to parse as structured JSON (objects, arrays, etc.) so
                // that system models with Vec / struct fields deserialize
                // correctly.  Plain strings that happen to look like JSON text
                // (e.g. a string value `"hello"`) fall back to Value::String.
                serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone()))
            },
            ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => Value::Null,
            _ => Value::String(extract_string(scalar).unwrap_or_default()),
        },
        KalamDataType::Bytes => match scalar {
            ScalarValue::Binary(Some(bytes)) | ScalarValue::LargeBinary(Some(bytes)) => {
                Value::Array(bytes.iter().map(|b| Value::Number(Number::from(*b))).collect())
            },
            ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Value::Null,
            _ => Value::String(extract_string(scalar).unwrap_or_default()),
        },
        KalamDataType::Decimal { scale, .. } => match scalar {
            ScalarValue::Decimal128(Some(value), _, _) => {
                Value::String(format_decimal_value(*value, *scale))
            },
            ScalarValue::Decimal128(None, _, _) => Value::Null,
            _ => Value::String(extract_string(scalar).unwrap_or_default()),
        },
        KalamDataType::Embedding(_) => Value::String(extract_string(scalar).unwrap_or_default()),
    };

    Ok(json)
}

fn parse_decimal_value(value: &str, scale: u8) -> Result<i128, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }

    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);
    let mut parts = unsigned.splitn(2, '.');
    let integer_part = parts.next().unwrap_or("0");
    let fractional_part = parts.next().unwrap_or("");

    let mut normalized_fraction = fractional_part.to_string();
    while normalized_fraction.len() < scale as usize {
        normalized_fraction.push('0');
    }
    if normalized_fraction.len() > scale as usize {
        normalized_fraction.truncate(scale as usize);
    }

    let joined = format!("{integer_part}{normalized_fraction}");
    let mut parsed = joined
        .parse::<i128>()
        .map_err(|e| format!("decimal parse failed for '{value}': {e}"))?;
    if negative {
        parsed = -parsed;
    }
    Ok(parsed)
}

fn format_decimal_value(value: i128, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }

    let divisor = 10_i128.pow(scale as u32);
    let integer = value / divisor;
    let fraction = (value % divisor).abs();
    format!("{integer}.{fraction:0width$}", width = scale as usize)
}

fn extract_bool(scalar: &ScalarValue) -> Option<bool> {
    match scalar {
        ScalarValue::Boolean(v) => *v,
        _ => None,
    }
}

fn extract_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int64(v) => *v,
        ScalarValue::Int32(v) => v.map(i64::from),
        ScalarValue::Int16(v) => v.map(i64::from),
        ScalarValue::Int8(v) => v.map(i64::from),
        ScalarValue::UInt64(v) => v.and_then(|i| i64::try_from(i).ok()),
        ScalarValue::UInt32(v) => v.map(i64::from),
        ScalarValue::UInt16(v) => v.map(i64::from),
        ScalarValue::UInt8(v) => v.map(i64::from),
        ScalarValue::Date32(v) => v.map(i64::from),
        ScalarValue::Time64Microsecond(v) => *v,
        ScalarValue::TimestampMillisecond(v, _) => *v,
        ScalarValue::TimestampMicrosecond(v, _) => *v,
        ScalarValue::TimestampNanosecond(v, _) => *v,
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn extract_f64(scalar: &ScalarValue) -> Option<f64> {
    match scalar {
        ScalarValue::Float64(v) => *v,
        ScalarValue::Float32(v) => v.map(f64::from),
        ScalarValue::Int64(v) => v.map(|i| i as f64),
        ScalarValue::Int32(v) => v.map(f64::from),
        ScalarValue::Int16(v) => v.map(f64::from),
        ScalarValue::Int8(v) => v.map(f64::from),
        ScalarValue::UInt64(v) => v.map(|i| i as f64),
        ScalarValue::UInt32(v) => v.map(f64::from),
        ScalarValue::UInt16(v) => v.map(f64::from),
        ScalarValue::UInt8(v) => v.map(f64::from),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn extract_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => None,
        ScalarValue::Int64(v) => v.map(|i| i.to_string()),
        ScalarValue::UInt64(v) => v.map(|i| i.to_string()),
        ScalarValue::Int32(v) => v.map(|i| i.to_string()),
        ScalarValue::UInt32(v) => v.map(|i| i.to_string()),
        ScalarValue::Int16(v) => v.map(|i| i.to_string()),
        ScalarValue::UInt16(v) => v.map(|i| i.to_string()),
        ScalarValue::Int8(v) => v.map(|i| i.to_string()),
        ScalarValue::UInt8(v) => v.map(|i| i.to_string()),
        ScalarValue::Boolean(v) => v.map(|b| b.to_string()),
        ScalarValue::Float64(v) => v.map(|f| f.to_string()),
        ScalarValue::Float32(v) => v.map(|f| f.to_string()),
        ScalarValue::TimestampMillisecond(v, _) => v.map(|t| t.to_string()),
        ScalarValue::TimestampMicrosecond(v, _) => v.map(|t| t.to_string()),
        ScalarValue::TimestampNanosecond(v, _) => v.map(|t| t.to_string()),
        ScalarValue::Binary(Some(bytes)) | ScalarValue::LargeBinary(Some(bytes)) => {
            Some(String::from_utf8_lossy(bytes).to_string())
        },
        _ => Some(format!("{scalar:?}")),
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use serde_json::json;

    use super::{json_value_to_scalar_for_column, scalar_to_json_for_column};
    use crate::models::datatypes::KalamDataType;

    #[test]
    fn test_uuid_roundtrip_as_text_scalar() {
        let value = json!("550e8400-e29b-41d4-a716-446655440000");
        let scalar =
            json_value_to_scalar_for_column(&value, &KalamDataType::Uuid).expect("uuid to scalar");

        match scalar {
            ScalarValue::Utf8(Some(ref v)) => {
                assert_eq!(v, "550e8400-e29b-41d4-a716-446655440000");
            },
            other => panic!("expected Utf8 for UUID, got {other:?}"),
        }

        let json_back =
            scalar_to_json_for_column(&scalar, &KalamDataType::Uuid).expect("scalar to uuid json");
        assert_eq!(json_back, value);
    }

    #[test]
    fn test_json_object_stored_as_utf8_and_roundtrips() {
        let value = json!({"route": ["a", "b"], "enabled": true});
        let scalar =
            json_value_to_scalar_for_column(&value, &KalamDataType::Json).expect("json to scalar");

        assert!(matches!(scalar, ScalarValue::Utf8(Some(_))));

        let json_back =
            scalar_to_json_for_column(&scalar, &KalamDataType::Json).expect("scalar to json");
        assert_eq!(json_back, value);
    }

    #[test]
    fn test_bytes_array_roundtrip() {
        let value = json!([1, 2, 255]);
        let scalar = json_value_to_scalar_for_column(&value, &KalamDataType::Bytes)
            .expect("bytes to scalar");

        assert!(
            matches!(scalar, ScalarValue::Binary(Some(ref bytes)) if bytes == &vec![1, 2, 255])
        );

        let json_back =
            scalar_to_json_for_column(&scalar, &KalamDataType::Bytes).expect("scalar to bytes");
        assert_eq!(json_back, value);
    }

    #[test]
    fn test_decimal_string_roundtrip() {
        let value = json!("200.75");
        let dtype = KalamDataType::Decimal {
            precision: 10,
            scale: 2,
        };

        let scalar = json_value_to_scalar_for_column(&value, &dtype).expect("decimal to scalar");
        assert!(matches!(scalar, ScalarValue::Decimal128(Some(20075), 10, 2)));

        let json_back = scalar_to_json_for_column(&scalar, &dtype).expect("scalar to decimal");
        assert_eq!(json_back, value);
    }

    #[test]
    fn test_numeric_string_to_int64_via_json_conversion() {
        let value = json!("42");
        let scalar =
            json_value_to_scalar_for_column(&value, &KalamDataType::BigInt).expect("bigint parse");
        assert!(matches!(scalar, ScalarValue::Int64(Some(0))));
    }
}
