use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
};

use wasm_bindgen::{
    prelude::{Closure, JsValue},
    JsCast,
};
use wasm_bindgen_futures::JsFuture;
use web_sys::{ErrorEvent, MessageEvent, WebSocket};

use super::{
    auth::WasmAuthProvider,
    helpers::{create_promise, send_ws_message, ws_url_from_http_opts},
    state::SubscriptionState,
    wasm_debug_log,
};
use crate::models::{
    ClientMessage, ConnectionOptions, ProtocolOptions, SerializationType, ServerMessage,
    SubscriptionRequest,
};

/// Resolve a `WasmAuthProvider` from an optional JS async callback.
///
/// If `auth_provider_cb` is `Some`, the callback is invoked and the returned
/// Promise is awaited to produce the auth.  Otherwise `fallback` is returned.
async fn resolve_auth_for_reconnect(
    auth_provider_cb: &Option<js_sys::Function>,
    fallback: WasmAuthProvider,
) -> Result<WasmAuthProvider, JsValue> {
    if let Some(cb) = auth_provider_cb {
        let result = JsFuture::from(js_sys::Promise::resolve(
            &cb.call0(&JsValue::NULL)
                .map_err(|e| JsValue::from_str(&format!("authProvider threw: {:?}", e)))?,
        ))
        .await?;

        if result.is_null() || result.is_undefined() {
            return Ok(WasmAuthProvider::None);
        }

        let jwt_obj = js_sys::Reflect::get(&result, &"jwt".into()).ok();
        if let Some(jwt) = jwt_obj.filter(|v| !v.is_undefined() && !v.is_null()) {
            let token = js_sys::Reflect::get(&jwt, &"token".into())
                .ok()
                .and_then(|v| v.as_string())
                .ok_or_else(|| {
                    JsValue::from_str(
                        "authProvider result must have shape { jwt: { token: string } }",
                    )
                })?;
            return Ok(WasmAuthProvider::Jwt { token });
        }
        Ok(WasmAuthProvider::None)
    } else {
        Ok(fallback)
    }
}

/// Internal reconnection logic with auth provider support.
///
/// `auth_provider_cb` — if `Some`, is invoked asynchronously to obtain a fresh
/// `WasmAuthProvider` before connecting (supports refresh-token flows).
/// `disable_compression` — when `true`, appends `?compress=false` to the WS URL.
pub(crate) async fn reconnect_internal_with_auth(
    url: String,
    auth: WasmAuthProvider,
    auth_provider_cb: Option<js_sys::Function>,
    disable_compression: bool,
) -> Result<WebSocket, JsValue> {
    // Resolve auth (dynamic provider takes precedence).
    let resolved_auth = resolve_auth_for_reconnect(&auth_provider_cb, auth).await?;

    if matches!(resolved_auth, WasmAuthProvider::Basic { .. }) {
        return Err(JsValue::from_str(
            "WebSocket authentication requires a JWT token. Use KalamClient.withJwt, login first, \
             or set an authProvider.",
        ));
    }

    let ws_url = ws_url_from_http_opts(&url, disable_compression)?;

    let ws = WebSocket::new(&ws_url)?;

    // Set binaryType to arraybuffer so binary messages come as ArrayBuffer, not Blob
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    let (connect_promise, connect_resolve, connect_reject) = create_promise();
    let (auth_promise, auth_resolve, auth_reject) = create_promise();

    // Check if auth is required
    let requires_auth = !matches!(resolved_auth, WasmAuthProvider::None);
    let auth_message = resolved_auth.to_ws_auth_message(ProtocolOptions::default());
    let ws_clone = ws.clone();
    let auth_resolve_for_anon = auth_resolve.clone();

    let connect_resolve_clone = connect_resolve.clone();
    let onopen = Closure::wrap(Box::new(move || {
        if let Some(auth_msg) = &auth_message {
            if let Ok(json) = serde_json::to_string(&auth_msg) {
                let _ = ws_clone.send_with_str(&json);
            }
        } else {
            // No auth needed (anonymous), resolve auth immediately
            let _ = auth_resolve_for_anon.call0(&JsValue::NULL);
        }
        let _ = connect_resolve_clone.call0(&JsValue::NULL);
    }) as Box<dyn FnMut()>);
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    onopen.forget();

    let connect_reject_clone = connect_reject.clone();
    let auth_reject_clone = auth_reject.clone();
    let onerror = Closure::wrap(Box::new(move |_: ErrorEvent| {
        let error = JsValue::from_str("Reconnection failed");
        let _ = connect_reject_clone.call1(&JsValue::NULL, &error);
        let _ = auth_reject_clone.call1(&JsValue::NULL, &error);
    }) as Box<dyn FnMut(ErrorEvent)>);
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
    onerror.forget();

    let auth_resolve_clone = auth_resolve.clone();
    let auth_reject_clone2 = auth_reject.clone();
    let auth_handled = Rc::new(RefCell::new(!requires_auth));
    let auth_handled_clone = auth_handled.clone();

    let onmessage = Closure::wrap(Box::new(move |e: MessageEvent| {
        if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
            let message = String::from(txt);
            if let Ok(event) = serde_json::from_str::<ServerMessage>(&message) {
                if !*auth_handled_clone.borrow() {
                    match event {
                        ServerMessage::AuthSuccess { .. } => {
                            *auth_handled_clone.borrow_mut() = true;
                            let _ = auth_resolve_clone.call0(&JsValue::NULL);
                        },
                        ServerMessage::AuthError { message } => {
                            *auth_handled_clone.borrow_mut() = true;
                            let error = JsValue::from_str(&format!("Auth failed: {}", message));
                            let _ = auth_reject_clone2.call1(&JsValue::NULL, &error);
                        },
                        _ => {},
                    }
                }
            }
        }
    }) as Box<dyn FnMut(MessageEvent)>);
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    JsFuture::from(connect_promise).await?;
    JsFuture::from(auth_promise).await?;

    Ok(ws)
}

/// Restart the keepalive ping interval after a successful reconnection.
///
/// This mirrors `KalamClient::start_ping_timer` but operates on the shared
/// Rc fields passed through the reconnect closure chain.
pub(crate) fn restart_ping_timer(
    ws_ref: &Rc<RefCell<Option<WebSocket>>>,
    connection_options: &Rc<RefCell<ConnectionOptions>>,
    ping_interval_id: &Rc<RefCell<i32>>,
    negotiated_ser: &Rc<Cell<SerializationType>>,
) {
    // Stop any previous timer
    let old_id = *ping_interval_id.borrow();
    if old_id >= 0 {
        super::helpers::global_clear_interval(old_id);
        *ping_interval_id.borrow_mut() = -1;
    }

    let interval_ms = connection_options.borrow().ping_interval_ms;
    if interval_ms == 0 {
        return;
    }

    let ws_clone = Rc::clone(ws_ref);
    let ser = Rc::clone(negotiated_ser);
    let ping_cb = Closure::wrap(Box::new(move || {
        if let Some(ws) = ws_clone.borrow().as_ref() {
            if ws.ready_state() == WebSocket::OPEN {
                let _ = send_ws_message(ws, &ClientMessage::Ping, ser.get());
            }
        }
    }) as Box<dyn FnMut()>);

    let id =
        super::helpers::global_set_interval(ping_cb.as_ref().unchecked_ref(), interval_ms as i32);
    ping_cb.forget();
    *ping_interval_id.borrow_mut() = id;
}

/// Re-subscribe to all subscriptions after reconnection with last seq_id
pub(crate) async fn resubscribe_all(
    ws_ref: Rc<RefCell<Option<WebSocket>>>,
    subscription_state: Rc<RefCell<HashMap<String, SubscriptionState>>>,
    negotiated_ser: SerializationType,
) {
    let states: Vec<(String, SubscriptionState)> = {
        let mut subs = subscription_state.borrow_mut();
        subs.iter_mut()
            .map(|(id, state)| {
                if let Some(seq_id) = state.last_seq_id {
                    state.options.from = Some(seq_id);
                }
                (id.clone(), state.clone())
            })
            .collect()
    };

    for (subscription_id, state) in states {
        wasm_debug_log!(&format!(
            "KalamClient: Re-subscribing to {} with last_seq_id: {:?}",
            subscription_id,
            state.last_seq_id.map(|s| s.to_string())
        ));

        // Create options with from if we have a last seq_id
        let mut options = state.options.clone();
        if let Some(seq_id) = state.last_seq_id {
            options.from = Some(seq_id);
        }

        let subscribe_msg = ClientMessage::Subscribe {
            subscription: SubscriptionRequest {
                id: subscription_id.clone(),
                sql: state.sql.clone(),
                options: Some(options),
            },
        };

        if let Some(ws) = ws_ref.borrow().as_ref() {
            if let Err(_e) = send_ws_message(ws, &subscribe_msg, negotiated_ser) {
                wasm_debug_log!(&format!(
                    "KalamClient: Failed to re-subscribe to {}: {:?}",
                    subscription_id, _e
                ));
            }
        }
    }
}
