use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, MessageEvent, Request, RequestInit, RequestMode, Response};

use super::wasm_debug_log;
use crate::compression;
use crate::models::SerializationType;

#[inline]
pub(crate) fn ws_url_from_http_opts(
    base_url: &str,
    disable_compression: bool,
) -> Result<String, JsValue> {
    let ws_url = if base_url.starts_with("https://") {
        base_url.replacen("https://", "wss://", 1)
    } else if base_url.starts_with("http://") {
        base_url.replacen("http://", "ws://", 1)
    } else {
        return Err(JsValue::from_str("Base URL must start with http:// or https://"));
    };
    // Strip trailing slash before appending path
    let ws_url = ws_url.trim_end_matches('/');
    if disable_compression {
        Ok(format!("{}/v1/ws?compress=false", ws_url))
    } else {
        Ok(format!("{}/v1/ws", ws_url))
    }
}

/// Hash a SQL string to produce a deterministic subscription ID suffix.
///
/// Uses `DefaultHasher` (SipHash) — fast and collision-resistant enough for
/// subscription deduplication.  Not a cryptographic hash.
#[inline]
pub(crate) fn subscription_hash(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

/// Helper to create a Promise with resolve/reject functions
pub(crate) fn create_promise() -> (js_sys::Promise, js_sys::Function, js_sys::Function) {
    let mut resolve_fn: Option<js_sys::Function> = None;
    let mut reject_fn: Option<js_sys::Function> = None;

    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        resolve_fn = Some(resolve);
        reject_fn = Some(reject);
    });

    (promise, resolve_fn.unwrap(), reject_fn.unwrap())
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

// ── Cross-platform fetch ────────────────────────────────────────────────────
//
// `web_sys::window()` returns `None` in Node.js because the polyfilled
// `window` object fails the `instanceof Window` check in the wasm-bindgen
// glue code.  To support both browser and Node.js WASM targets we bind
// directly to the global `fetch()` function which is available in both
// environments (browsers natively, Node.js 18+ via `globalThis.fetch`).

#[wasm_bindgen]
extern "C" {
    /// Call the global `fetch(request)` — works in browsers AND Node.js 18+.
    #[wasm_bindgen(js_name = "fetch")]
    fn global_fetch_with_request(request: &Request) -> js_sys::Promise;

    /// Call `setTimeout(callback, delay)` from the global scope.
    #[wasm_bindgen(js_name = "setTimeout")]
    pub(crate) fn global_set_timeout(closure: &js_sys::Function, delay: i32) -> i32;

    /// Call `setInterval(callback, delay)` from the global scope.
    #[wasm_bindgen(js_name = "setInterval")]
    pub(crate) fn global_set_interval(closure: &js_sys::Function, delay: i32) -> i32;

    /// Call `clearInterval(id)` from the global scope.
    #[wasm_bindgen(js_name = "clearInterval")]
    pub(crate) fn global_clear_interval(id: i32);
}

/// Portable replacement for `window.fetch_with_request(req)`.
///
/// Returns a `js_sys::Promise` that resolves to a `Response`.
pub(crate) fn fetch_request(request: &Request) -> js_sys::Promise {
    global_fetch_with_request(request)
}

/// Common HTTP fetch helper for WASM client methods.
///
/// Builds a CORS request with the given method, optional JSON body, and
/// header list, executes `fetch()`, checks for HTTP errors, and returns the
/// response body text.
pub(crate) async fn wasm_fetch(
    url: &str,
    method: &str,
    body: Option<&str>,
    headers: &[(&str, &str)],
    error_prefix: &str,
) -> Result<String, JsValue> {
    let opts = RequestInit::new();
    opts.set_method(method);
    opts.set_mode(RequestMode::Cors);

    let h = Headers::new()?;
    for &(name, value) in headers {
        h.set(name, value)?;
    }
    opts.set_headers(&h);

    if let Some(b) = body {
        opts.set_body(&JsValue::from_str(b));
    }

    let request = Request::new_with_str_and_init(url, &opts)?;
    let resp_value = JsFuture::from(fetch_request(&request)).await?;
    let resp: Response = resp_value.dyn_into()?;

    if !resp.ok() {
        let status = resp.status();
        let text = JsFuture::from(resp.text()?).await?;
        let error_msg =
            text.as_string().unwrap_or_else(|| format!("{}: HTTP {}", error_prefix, status));
        return Err(JsValue::from_str(&error_msg));
    }

    let json = JsFuture::from(resp.text()?).await?;
    Ok(json.as_string().unwrap_or_default())
}

/// Decode a WebSocket `MessageEvent` into a UTF-8 `String`.
///
/// Handles text (`JsString`), binary (`ArrayBuffer` — decompressed via gzip),
/// `Blob` fallback, and unknown types. Returns `None` when the payload cannot
/// be decoded (with a diagnostic logged to the JS console).
///
/// Optimized to use `decompress_if_gzip` which returns `Cow<[u8]>` — avoiding
/// a heap allocation when the payload is not gzip-compressed.
#[inline]
pub(crate) fn decode_ws_message(e: &MessageEvent) -> Option<String> {
    let data = e.data();

    if let Ok(txt) = data.dyn_into::<js_sys::JsString>() {
        return Some(String::from(txt));
    }

    let data = e.data();
    if let Ok(array_buffer) = data.dyn_into::<js_sys::ArrayBuffer>() {
        let uint8_array = js_sys::Uint8Array::new(&array_buffer);
        let raw = uint8_array.to_vec();

        let decompressed = compression::decompress_if_gzip(&raw);
        return match std::str::from_utf8(&decompressed) {
            Ok(s) => Some(s.to_owned()),
            Err(_e) => {
                wasm_debug_log!(&format!("KalamClient: Invalid UTF-8 in message: {}", _e));
                None
            },
        };
    }

    let data = e.data();
    if data.is_instance_of::<web_sys::Blob>() {
        wasm_debug_log!(
            "KalamClient: Received Blob message - binary mode may be misconfigured. Attempting to read as text.",
        );
        return data.as_string();
    }

    // Unknown message type — log diagnostics
    let data = e.data();
    let _type_name = js_sys::Reflect::get(&data, &"constructor".into())
        .ok()
        .and_then(|c| js_sys::Reflect::get(&c, &"name".into()).ok())
        .and_then(|n| n.as_string())
        .unwrap_or_else(|| "unknown".to_string());
    let _typeof_str = data.js_typeof().as_string().unwrap_or_else(|| "?".to_string());
    let _data_preview = js_sys::JSON::stringify(&data)
        .ok()
        .and_then(|s| s.as_string())
        .unwrap_or_else(|| format!("{:?}", data));

    wasm_debug_log!(&format!(
        "KalamClient: Received unknown message type: constructor={}, typeof={}, preview={}",
        _type_name,
        _typeof_str,
        &_data_preview[.._data_preview.len().min(200)]
    ));
    None
}

/// Extract raw bytes from a binary WebSocket frame, decompressing gzip if needed.
///
/// Returns `None` if the frame is not a binary `ArrayBuffer`.
#[inline]
pub(crate) fn decode_ws_binary_payload(e: &MessageEvent) -> Option<Vec<u8>> {
    let data = e.data();
    if let Ok(array_buffer) = data.dyn_into::<js_sys::ArrayBuffer>() {
        let uint8_array = js_sys::Uint8Array::new(&array_buffer);
        let raw = uint8_array.to_vec();
        let decompressed = compression::decompress_if_gzip(&raw);
        Some(decompressed.into_owned())
    } else {
        None
    }
}

/// Send a `ClientMessage` using the negotiated serialization format.
///
/// JSON messages are sent as text frames; MessagePack messages as binary frames.
pub(crate) fn send_ws_message(
    ws: &web_sys::WebSocket,
    msg: &crate::models::ClientMessage,
    serialization: SerializationType,
) -> Result<(), JsValue> {
    match serialization {
        SerializationType::Json => {
            let json = serde_json::to_string(msg)
                .map_err(|e| JsValue::from_str(&format!("JSON serialization error: {}", e)))?;
            ws.send_with_str(&json)
        },
        SerializationType::MessagePack => {
            let bytes = rmp_serde::to_vec_named(msg).map_err(|e| {
                JsValue::from_str(&format!("MessagePack serialization error: {}", e))
            })?;
            ws.send_with_u8_array(&bytes)
        },
    }
}
