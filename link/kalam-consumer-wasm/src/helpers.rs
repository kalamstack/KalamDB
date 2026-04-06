use js_sys::Reflect;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, RequestInit, RequestMode, Response};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = "fetch")]
    fn global_fetch_with_str_and_init(url: &str, init: &RequestInit) -> js_sys::Promise;
}

pub(crate) async fn fetch_json_response(
    url: &str,
    body: &str,
    auth_header: Option<&str>,
) -> Result<Response, JsValue> {
    let headers = Headers::new()?;
    headers.set("Content-Type", "application/json")?;
    if let Some(auth_header) = auth_header {
        headers.set("Authorization", auth_header)?;
    }

    let init = RequestInit::new();
    init.set_method("POST");
    init.set_mode(RequestMode::Cors);
    init.set_headers(&headers);
    init.set_body(&JsValue::from_str(body));

    let response = JsFuture::from(global_fetch_with_str_and_init(url, &init)).await?;
    response
        .dyn_into::<Response>()
        .map_err(|_| JsValue::from_str("fetch() did not resolve to a Response"))
}

pub(crate) async fn response_text(response: &Response) -> Result<String, JsValue> {
    let text = JsFuture::from(response.text()?).await?;
    Ok(text.as_string().unwrap_or_default())
}

pub(crate) fn js_value_to_json_string(value: &JsValue, context: &str) -> Result<String, JsValue> {
    let json = js_sys::JSON::stringify(value)
        .map_err(|_| JsValue::from_str(&format!("{} must be JSON-serializable", context)))?;
    json.as_string().ok_or_else(|| {
        JsValue::from_str(&format!("{} could not be converted to a JSON string", context))
    })
}

pub(crate) fn serialize_json_to_js_value<T: Serialize>(
    value: &T,
    context: &str,
) -> Result<JsValue, JsValue> {
    let json = serde_json::to_string(value).map_err(|error| {
        JsValue::from_str(&format!("Failed to serialize {}: {}", context, error))
    })?;
    js_sys::JSON::parse(&json).map_err(|_| {
        JsValue::from_str(&format!("Failed to convert {} into a JavaScript value", context))
    })
}

pub(crate) fn topic_request_error(status: u16, text: &str, fallback_prefix: &str) -> JsValue {
    let parsed = serde_json::from_str::<serde_json::Value>(text).ok();
    let code = parsed
        .as_ref()
        .and_then(|value| value.get("code"))
        .and_then(serde_json::Value::as_str);
    let message = parsed
        .as_ref()
        .and_then(|value| value.get("error"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if text.trim().is_empty() {
                format!("{}: HTTP {}", fallback_prefix, status)
            } else {
                text.to_string()
            }
        });

    let error = js_sys::Error::new(&message);
    let value = JsValue::from(error);
    let _ =
        Reflect::set(&value, &JsValue::from_str("name"), &JsValue::from_str("TopicRequestError"));
    let _ =
        Reflect::set(&value, &JsValue::from_str("status"), &JsValue::from_f64(f64::from(status)));
    if let Some(code) = code {
        let _ = Reflect::set(&value, &JsValue::from_str("code"), &JsValue::from_str(code));
    }
    value
}
