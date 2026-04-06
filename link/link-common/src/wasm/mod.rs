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

macro_rules! wasm_debug_log {
    (&format!($fmt:literal $(, $args:expr)* $(,)?)) => {{
        #[cfg(all(target_arch = "wasm32", debug_assertions))]
        $crate::wasm::log(&format!($fmt $(, $args)*));
    }};
    ($message:expr $(,)?) => {{
        #[cfg(all(target_arch = "wasm32", debug_assertions))]
        $crate::wasm::log($message);
    }};
}

pub(crate) use wasm_debug_log;

// Log helper for debugging connection state changes.
// Keep these logs out of release wasm builds so the format strings and
// console calls do not inflate the shipped bundle.
#[allow(dead_code)]
#[inline(always)]
pub(crate) fn console_log(message: &str) {
    #[cfg(all(target_arch = "wasm32", debug_assertions))]
    log(message);

    #[cfg(any(not(target_arch = "wasm32"), not(debug_assertions)))]
    let _ = message;
}
