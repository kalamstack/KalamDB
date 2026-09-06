//! POST /v1/functions/{schema}/{procedure}

use std::{collections::HashMap, sync::Arc};

use actix_web::{web, HttpRequest, HttpResponse, Responder};
use kalamdb_auth::AuthSessionExtractor;
use kalamdb_commons::{
    conversions::arrow_json_conversion::scalar_value_to_json,
    models::{NamespaceId, RoutineId},
    KalamDataType,
};
use kalamdb_core::{
    app_context::AppContext,
    functions::{
        json_to_routine_value, FunctionCallOrigin, FunctionService, HttpResponseOverrides,
        RoutineValue,
    },
    sql::context::ExecutionContext,
};
use kalamdb_session::AuthSession;
use parking_lot::Mutex;
use serde_json::{json, Value};
use uuid::Uuid;

const REJECTED_CONTEXT_KEYS: &[&str] = &["context", "ctx", "source", "actor", "tx"];

pub async fn invoke_function_v1(
    extractor: AuthSessionExtractor,
    http_req: HttpRequest,
    path: web::Path<(String, String)>,
    body: web::Json<Value>,
    app_context: web::Data<Arc<AppContext>>,
) -> impl Responder {
    let session: AuthSession = extractor.into();
    let (schema, procedure) = path.into_inner();
    let namespace_id = NamespaceId::new(schema);
    let routine_id = RoutineId::from_parts(Some(&namespace_id), &procedure);

    if let Some(key) = rejected_context_key(&body) {
        return HttpResponse::BadRequest().json(json!({
            "status": "error",
            "message": format!("client-supplied context field '{key}' is not allowed"),
        }));
    }

    let stores = app_context.system_tables().catalog_stores();
    let parameters = match stores.list_parameters(&routine_id) {
        Ok(parameters) => parameters,
        Err(error) => {
            return HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": error.to_string(),
            }));
        },
    };
    let args = match bind_json_args(&body, &parameters) {
        Ok(args) => args,
        Err(message) => {
            return HttpResponse::BadRequest().json(json!({
                "status": "error",
                "message": message,
            }));
        },
    };

    let mut headers = HashMap::new();
    for (name, value) in http_req.headers() {
        if let Ok(value) = value.to_str() {
            headers.insert(name.as_str().to_string(), value.to_string());
        }
    }
    let response = Arc::new(Mutex::new(HttpResponseOverrides::default()));
    let origin = FunctionCallOrigin::Http {
        headers,
        response: Arc::clone(&response),
    };

    let exec_ctx =
        ExecutionContext::from_session(session, Arc::clone(&app_context.base_session_context()))
            .with_namespace_id(namespace_id)
            .with_request_id(Uuid::now_v7().to_string());

    match FunctionService::invoke(
        Arc::clone(app_context.get_ref()),
        &exec_ctx,
        origin,
        routine_id,
        args,
    )
    .await
    {
        Ok(result) => {
            let payload = match scalar_value_to_json(&result.value.value) {
                Ok(value) => value.0,
                Err(error) => {
                    return HttpResponse::InternalServerError().json(json!({
                        "status": "error",
                        "message": error.to_string(),
                    }));
                },
            };
            let status = result.http_status.unwrap_or(200);
            let mut builder = HttpResponse::build(
                actix_web::http::StatusCode::from_u16(status)
                    .unwrap_or(actix_web::http::StatusCode::OK),
            );
            for (name, value) in result.http_headers {
                builder.insert_header((name, value));
            }
            builder.json(json!({
                "status": "success",
                "result": payload,
            }))
        },
        Err(error) => {
            let message = error.to_string();
            let lower = message.to_ascii_lowercase();
            let mut response = if lower.contains("not found") {
                HttpResponse::NotFound()
            } else if lower.contains("denied") || lower.contains("unauthorized") {
                HttpResponse::Forbidden()
            } else {
                HttpResponse::BadRequest()
            };
            response.json(json!({
                "status": "error",
                "message": message,
            }))
        },
    }
}

fn rejected_context_key(body: &Value) -> Option<&str> {
    let Value::Object(map) = body else {
        return None;
    };
    REJECTED_CONTEXT_KEYS.iter().copied().find(|key| map.contains_key(*key))
}

fn bind_json_args(
    body: &Value,
    parameters: &[kalamdb_system::CatalogRoutineParameter],
) -> Result<Vec<RoutineValue>, String> {
    match body {
        Value::Null => Ok(Vec::new()),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, item)| bind_json_value(item, parameters.get(index)))
            .collect(),
        Value::Object(map) => {
            if parameters.is_empty() {
                return Ok(Vec::new());
            }
            let mut args = Vec::with_capacity(parameters.len());
            for parameter in parameters {
                let value = map
                    .get(&parameter.name)
                    .ok_or_else(|| format!("missing procedure argument '{}'", parameter.name))?;
                args.push(bind_json_value(value, Some(parameter))?);
            }
            Ok(args)
        },
        other => Err(format!("function body must be a JSON object or array, got {other}")),
    }
}

fn bind_json_value(
    value: &Value,
    parameter: Option<&kalamdb_system::CatalogRoutineParameter>,
) -> Result<RoutineValue, String> {
    json_to_routine_value(value, parameter_data_type(parameter).as_ref())
        .map_err(|error| error.to_string())
}

fn parameter_data_type(
    parameter: Option<&kalamdb_system::CatalogRoutineParameter>,
) -> Option<KalamDataType> {
    let parameter = parameter?;
    if parameter.is_array {
        return None;
    }
    parameter.builtin_data_type()
}
