//! Parse DEFAULT expressions into the shared `CALL` models.

use kalamdb_commons::{
    models::{is_builtin_default_name, normalize_builtin_name, CallArgument, RoutineCall},
    schemas::ColumnDefault,
    KalamDataType, NamespaceId, RoutineId,
};
use serde_json::{Number, Value as JsonValue};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, ObjectNamePart,
    TypedString, UnaryOperator, Value,
};

use crate::{compatibility::map_sql_type_to_kalam, ddl::DdlResult, dialect::KalamDbDialect};

pub fn expr_to_column_default(
    expr: &Expr,
    default_namespace: &NamespaceId,
) -> DdlResult<ColumnDefault> {
    match expr {
        Expr::Function(func) => {
            let call = function_to_routine_call(func, default_namespace)?;
            Ok(ColumnDefault::FunctionCall(call))
        },
        Expr::Value(value) => Ok(value_to_literal_default(&value.value)),
        Expr::Identifier(identifier) => {
            let normalized = identifier.value.to_ascii_uppercase();
            match normalized.as_str() {
                "CURRENT_TIMESTAMP" => Ok(ColumnDefault::function("NOW", vec![])),
                "CURRENT_USER" => Ok(ColumnDefault::function("CURRENT_USER", vec![])),
                "NULL" => Ok(ColumnDefault::literal(JsonValue::Null)),
                _ => Ok(ColumnDefault::literal(JsonValue::String(identifier.value.clone()))),
            }
        },
        _ => {
            let literal = expr.to_string();
            let normalized = literal.to_ascii_uppercase();
            if normalized == "NULL" {
                Ok(ColumnDefault::literal(JsonValue::Null))
            } else if normalized == "CURRENT_TIMESTAMP" || normalized == "NOW()" {
                Ok(ColumnDefault::function("NOW", vec![]))
            } else {
                let val = literal.trim_matches('\'').to_string();
                Ok(ColumnDefault::literal(JsonValue::String(val)))
            }
        },
    }
}

pub(crate) fn expr_to_call_argument(expr: &Expr) -> DdlResult<CallArgument> {
    match expr {
        Expr::Nested(inner) => expr_to_call_argument(inner),
        Expr::Value(value) => value_to_call_argument(&value.value),
        Expr::Cast {
            expr, data_type, ..
        } => {
            let kalam_type = map_sql_type_to_kalam(data_type)?;
            if matches!(expr.as_ref(), Expr::Value(value) if matches!(value.value, Value::Null)) {
                return Ok(CallArgument::typed(kalam_type, JsonValue::Null));
            }
            let value = expr_to_json_literal(expr)?;
            let value = coerce_json_for_type(&kalam_type, value)?;
            Ok(CallArgument::typed(kalam_type, value))
        },
        Expr::TypedString(typed) => typed_string_to_call_argument(typed),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => negate_numeric_argument(expr),
        Expr::Array(array) => {
            let mut items = Vec::with_capacity(array.elem.len());
            for item in &array.elem {
                items.push(expr_to_json_literal(item)?);
            }
            Ok(CallArgument::json(JsonValue::Array(items)))
        },
        _ => Err(format!(
            "unsupported function argument '{expr}'; use a literal or CAST(literal AS type)"
        )),
    }
}

fn function_to_routine_call(
    func: &Function,
    default_namespace: &NamespaceId,
) -> DdlResult<RoutineCall> {
    let arguments = function_args_to_call_arguments(&func.args)?;
    if arguments.iter().any(CallArgument::is_placeholder) {
        return Err("DEFAULT procedure arguments cannot use placeholders".to_string());
    }
    let routine_id = routine_id_from_function_name(&func.name, default_namespace)?;
    Ok(RoutineCall::new(routine_id, arguments))
}

fn routine_id_from_function_name(
    name: &ObjectName,
    default_namespace: &NamespaceId,
) -> DdlResult<RoutineId> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => ident.value.to_ascii_lowercase(),
            other => other.to_string().trim_matches('"').to_ascii_lowercase(),
        })
        .collect();
    match parts.as_slice() {
        [unqualified] if is_builtin_default_name(unqualified) => {
            Ok(RoutineId::new(normalize_builtin_name(unqualified)))
        },
        [unqualified] => Ok(RoutineId::from_parts(Some(default_namespace), unqualified)),
        [schema, unqualified] => {
            Ok(RoutineId::from_parts(Some(&NamespaceId::new(schema)), unqualified))
        },
        _ => Err(format!("invalid DEFAULT function name '{name}'")),
    }
}

fn function_args_to_call_arguments(args: &FunctionArguments) -> DdlResult<Vec<CallArgument>> {
    match args {
        FunctionArguments::None => Ok(Vec::new()),
        FunctionArguments::List(list) => {
            let mut arguments = Vec::with_capacity(list.args.len());
            for arg in &list.args {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
                    return Err("DEFAULT function arguments must be unnamed literals".to_string());
                };
                arguments.push(expr_to_call_argument(expr)?);
            }
            Ok(arguments)
        },
        FunctionArguments::Subquery(_) => {
            Err("DEFAULT function arguments cannot be subqueries".to_string())
        },
    }
}

fn typed_string_to_call_argument(typed: &TypedString) -> DdlResult<CallArgument> {
    let kalam_type = map_sql_type_to_kalam(&typed.data_type)?;
    let value = value_to_json(&typed.value.value)?;
    let value = coerce_json_for_type(&kalam_type, value)?;
    Ok(CallArgument::typed(kalam_type, value))
}

fn negate_numeric_argument(expr: &Expr) -> DdlResult<CallArgument> {
    match expr_to_call_argument(expr)? {
        CallArgument::Typed {
            data_type: KalamDataType::BigInt,
            value,
        } => {
            let number = value
                .as_i64()
                .ok_or_else(|| "negated argument must be an integer".to_string())?;
            Ok(CallArgument::bigint(-number))
        },
        CallArgument::Typed {
            data_type: KalamDataType::Int,
            value,
        } => {
            let number = value
                .as_i64()
                .ok_or_else(|| "negated argument must be an integer".to_string())?;
            Ok(CallArgument::int(-i32::try_from(number).map_err(|_| "INT overflow")?))
        },
        CallArgument::Typed {
            data_type: KalamDataType::Double,
            value,
        } => {
            let number =
                value.as_f64().ok_or_else(|| "negated argument must be a float".to_string())?;
            CallArgument::double(-number)
        },
        other => Err(format!("cannot negate argument {}", other.to_sql())),
    }
}

fn value_to_call_argument(value: &Value) -> DdlResult<CallArgument> {
    match value {
        Value::Null => Ok(CallArgument::Null),
        Value::Boolean(flag) => Ok(CallArgument::boolean(*flag)),
        Value::Number(number, _) => parse_number_argument(number),
        Value::SingleQuotedString(text)
        | Value::DoubleQuotedString(text)
        | Value::NationalStringLiteral(text)
        | Value::EscapedStringLiteral(text)
        | Value::DollarQuotedString(sqlparser::ast::DollarQuotedString { value: text, .. }) => {
            Ok(CallArgument::text(text.clone()))
        },
        Value::HexStringLiteral(hex) => {
            Ok(CallArgument::typed(KalamDataType::Bytes, hex_to_json_bytes(hex)?))
        },
        Value::Placeholder(placeholder) => parse_placeholder(placeholder),
        other => Err(format!("unsupported function argument '{other}'")),
    }
}

fn parse_number_argument(number: &str) -> DdlResult<CallArgument> {
    if let Ok(int_value) = number.parse::<i64>() {
        Ok(CallArgument::bigint(int_value))
    } else if let Ok(float_value) = number.parse::<f64>() {
        CallArgument::double(float_value)
    } else {
        Err(format!("invalid numeric argument '{number}'"))
    }
}

fn parse_placeholder(placeholder: &str) -> DdlResult<CallArgument> {
    let index = placeholder
        .trim_start_matches('$')
        .parse::<usize>()
        .map_err(|_| format!("invalid CALL placeholder '{placeholder}'"))?;
    if index == 0 {
        return Err("CALL placeholders are 1-based".to_string());
    }
    Ok(CallArgument::Placeholder(index))
}

fn expr_to_json_literal(expr: &Expr) -> DdlResult<JsonValue> {
    match expr_to_call_argument(expr)? {
        CallArgument::Null => Ok(JsonValue::Null),
        CallArgument::Typed { value, .. } => Ok(value),
        CallArgument::Placeholder(_) => {
            Err("placeholders cannot be nested inside CAST".to_string())
        },
    }
}

fn value_to_json(value: &Value) -> DdlResult<JsonValue> {
    match value_to_call_argument(value)? {
        CallArgument::Null => Ok(JsonValue::Null),
        CallArgument::Typed { value, .. } => Ok(value),
        CallArgument::Placeholder(_) => {
            Err("placeholders cannot be nested inside a typed literal".to_string())
        },
    }
}

fn coerce_json_for_type(data_type: &KalamDataType, value: JsonValue) -> DdlResult<JsonValue> {
    if value.is_null() {
        return Ok(JsonValue::Null);
    }
    match data_type {
        KalamDataType::Json | KalamDataType::File => match value {
            JsonValue::String(text) => serde_json::from_str(&text)
                .map_err(|error| format!("invalid {data_type} argument '{text}': {error}")),
            other => Ok(other),
        },
        KalamDataType::Bytes => match value {
            JsonValue::String(text) if text.chars().all(|ch| ch.is_ascii_hexdigit()) => {
                hex_to_json_bytes(&text)
            },
            other => Ok(other),
        },
        _ => Ok(value),
    }
}

fn hex_to_json_bytes(hex: &str) -> DdlResult<JsonValue> {
    if hex.len() % 2 != 0 {
        return Err(format!("invalid hex literal '{hex}'"));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let chars: Vec<char> = hex.chars().collect();
    for chunk in chars.chunks(2) {
        let text: String = chunk.iter().collect();
        let byte =
            u8::from_str_radix(&text, 16).map_err(|_| format!("invalid hex literal '{hex}'"))?;
        bytes.push(JsonValue::Number(Number::from(byte)));
    }
    Ok(JsonValue::Array(bytes))
}

fn value_to_literal_default(value: &Value) -> ColumnDefault {
    match value {
        Value::Number(number, _) => {
            if let Ok(int_value) = number.parse::<i64>() {
                ColumnDefault::literal(JsonValue::Number(int_value.into()))
            } else if let Ok(float_value) = number.parse::<f64>() {
                ColumnDefault::literal(serde_json::json!(float_value))
            } else {
                ColumnDefault::literal(JsonValue::String(number.clone()))
            }
        },
        Value::SingleQuotedString(string_value)
        | Value::DoubleQuotedString(string_value)
        | Value::NationalStringLiteral(string_value) => {
            ColumnDefault::literal(JsonValue::String(string_value.clone()))
        },
        Value::Boolean(boolean_value) => ColumnDefault::literal(JsonValue::Bool(*boolean_value)),
        Value::Null => ColumnDefault::literal(JsonValue::Null),
        other => ColumnDefault::literal(JsonValue::String(other.to_string())),
    }
}

pub(crate) fn parse_call_argument_sql(input: &str) -> DdlResult<CallArgument> {
    let dialect = KalamDbDialect::default();
    let expr = crate::parser::utils::parse_sql_expression(input, &dialect)
        .map_err(|error| error.to_string())?;
    expr_to_call_argument(&expr)
}
