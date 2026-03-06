use wasm_bindgen::prelude::*;

mod auth;
mod client;
mod helpers;
mod reconnect;
mod state;
mod timestamp;
mod validation;

pub use client::KalamClient;
pub use timestamp::{parse_iso8601, timestamp_now, WasmTimestampFormatter};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

// Log helper for debugging connection state changes
pub(crate) fn console_log(message: &str) {
    #[cfg(target_arch = "wasm32")]
    log(message);

    #[cfg(not(target_arch = "wasm32"))]
    let _ = message;
}
