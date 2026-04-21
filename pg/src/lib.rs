//! PostgreSQL extension entrypoint — remote-only mode.

#[cfg(any(not(test), feature = "pg_test"))]
mod remote_executor;
#[cfg(any(not(test), feature = "pg_test"))]
mod remote_state;
#[cfg(any(not(test), feature = "pg_test"))]
mod remote_server;
mod session_settings;

#[cfg(any(feature = "pg_test", feature = "e2e"))]
pub(crate) mod test_alloc {
    use serde::Serialize;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct TrackingAllocator;

    #[derive(Clone, Copy, Debug, Default, Serialize)]
    pub struct AllocationSnapshot {
        pub allocations: usize,
        pub deallocations: usize,
        pub allocated_bytes: usize,
        pub deallocated_bytes: usize,
        pub live_bytes: usize,
        pub peak_live_bytes: usize,
    }

    static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
    static DEALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
    static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
    static DEALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
    static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
    static PEAK_LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

    #[inline]
    fn record_alloc(bytes: usize) {
        ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
        ALLOCATED_BYTES.fetch_add(bytes, Ordering::SeqCst);
        let live_bytes = LIVE_BYTES.fetch_add(bytes, Ordering::SeqCst) + bytes;

        loop {
            let peak = PEAK_LIVE_BYTES.load(Ordering::SeqCst);
            if live_bytes <= peak {
                break;
            }
            if PEAK_LIVE_BYTES
                .compare_exchange(peak, live_bytes, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    #[inline]
    fn record_dealloc(bytes: usize) {
        DEALLOCATIONS.fetch_add(1, Ordering::SeqCst);
        DEALLOCATED_BYTES.fetch_add(bytes, Ordering::SeqCst);
        LIVE_BYTES.fetch_sub(bytes, Ordering::SeqCst);
    }

    unsafe impl GlobalAlloc for TrackingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                record_alloc(layout.size());
            }
            ptr
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc_zeroed(layout) };
            if !ptr.is_null() {
                record_alloc(layout.size());
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) };
            record_dealloc(layout.size());
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
            if !new_ptr.is_null() {
                record_dealloc(layout.size());
                record_alloc(new_size);
            }
            new_ptr
        }
    }

    pub fn reset() {
        ALLOCATIONS.store(0, Ordering::SeqCst);
        DEALLOCATIONS.store(0, Ordering::SeqCst);
        ALLOCATED_BYTES.store(0, Ordering::SeqCst);
        DEALLOCATED_BYTES.store(0, Ordering::SeqCst);
        LIVE_BYTES.store(0, Ordering::SeqCst);
        PEAK_LIVE_BYTES.store(0, Ordering::SeqCst);
    }

    pub fn snapshot() -> AllocationSnapshot {
        AllocationSnapshot {
            allocations: ALLOCATIONS.load(Ordering::SeqCst),
            deallocations: DEALLOCATIONS.load(Ordering::SeqCst),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::SeqCst),
            deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::SeqCst),
            live_bytes: LIVE_BYTES.load(Ordering::SeqCst),
            peak_live_bytes: PEAK_LIVE_BYTES.load(Ordering::SeqCst),
        }
    }
}

#[cfg(feature = "e2e")]
pub(crate) mod conversion_test_stats {
    use serde::Serialize;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy, Debug, Default, Serialize)]
    pub struct ConversionTestStats {
        pub text_to_pg_fast_path_calls: usize,
        pub text_to_pg_fast_path_bytes: usize,
        pub json_to_pg_input_calls: usize,
        pub json_to_pg_input_bytes: usize,
        pub jsonb_to_pg_input_calls: usize,
        pub jsonb_to_pg_input_bytes: usize,
        pub text_from_pg_fast_path_calls: usize,
        pub text_from_pg_fast_path_bytes: usize,
        pub json_from_pg_fast_path_calls: usize,
        pub json_from_pg_fast_path_bytes: usize,
        pub jsonb_from_pg_fast_path_calls: usize,
        pub jsonb_from_pg_fast_path_bytes: usize,
    }

    static TEXT_TO_PG_FAST_PATH_CALLS: AtomicUsize = AtomicUsize::new(0);
    static TEXT_TO_PG_FAST_PATH_BYTES: AtomicUsize = AtomicUsize::new(0);
    static JSON_TO_PG_INPUT_CALLS: AtomicUsize = AtomicUsize::new(0);
    static JSON_TO_PG_INPUT_BYTES: AtomicUsize = AtomicUsize::new(0);
    static JSONB_TO_PG_INPUT_CALLS: AtomicUsize = AtomicUsize::new(0);
    static JSONB_TO_PG_INPUT_BYTES: AtomicUsize = AtomicUsize::new(0);
    static TEXT_FROM_PG_FAST_PATH_CALLS: AtomicUsize = AtomicUsize::new(0);
    static TEXT_FROM_PG_FAST_PATH_BYTES: AtomicUsize = AtomicUsize::new(0);
    static JSON_FROM_PG_FAST_PATH_CALLS: AtomicUsize = AtomicUsize::new(0);
    static JSON_FROM_PG_FAST_PATH_BYTES: AtomicUsize = AtomicUsize::new(0);
    static JSONB_FROM_PG_FAST_PATH_CALLS: AtomicUsize = AtomicUsize::new(0);
    static JSONB_FROM_PG_FAST_PATH_BYTES: AtomicUsize = AtomicUsize::new(0);

    pub fn reset() {
        TEXT_TO_PG_FAST_PATH_CALLS.store(0, Ordering::SeqCst);
        TEXT_TO_PG_FAST_PATH_BYTES.store(0, Ordering::SeqCst);
        JSON_TO_PG_INPUT_CALLS.store(0, Ordering::SeqCst);
        JSON_TO_PG_INPUT_BYTES.store(0, Ordering::SeqCst);
        JSONB_TO_PG_INPUT_CALLS.store(0, Ordering::SeqCst);
        JSONB_TO_PG_INPUT_BYTES.store(0, Ordering::SeqCst);
        TEXT_FROM_PG_FAST_PATH_CALLS.store(0, Ordering::SeqCst);
        TEXT_FROM_PG_FAST_PATH_BYTES.store(0, Ordering::SeqCst);
        JSON_FROM_PG_FAST_PATH_CALLS.store(0, Ordering::SeqCst);
        JSON_FROM_PG_FAST_PATH_BYTES.store(0, Ordering::SeqCst);
        JSONB_FROM_PG_FAST_PATH_CALLS.store(0, Ordering::SeqCst);
        JSONB_FROM_PG_FAST_PATH_BYTES.store(0, Ordering::SeqCst);
    }

    pub fn record_text_to_pg_fast_path(bytes: usize) {
        TEXT_TO_PG_FAST_PATH_CALLS.fetch_add(1, Ordering::SeqCst);
        TEXT_TO_PG_FAST_PATH_BYTES.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn record_json_to_pg_input(bytes: usize) {
        JSON_TO_PG_INPUT_CALLS.fetch_add(1, Ordering::SeqCst);
        JSON_TO_PG_INPUT_BYTES.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn record_jsonb_to_pg_input(bytes: usize) {
        JSONB_TO_PG_INPUT_CALLS.fetch_add(1, Ordering::SeqCst);
        JSONB_TO_PG_INPUT_BYTES.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn record_text_from_pg_fast_path(bytes: usize) {
        TEXT_FROM_PG_FAST_PATH_CALLS.fetch_add(1, Ordering::SeqCst);
        TEXT_FROM_PG_FAST_PATH_BYTES.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn record_json_from_pg_fast_path(bytes: usize) {
        JSON_FROM_PG_FAST_PATH_CALLS.fetch_add(1, Ordering::SeqCst);
        JSON_FROM_PG_FAST_PATH_BYTES.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn record_jsonb_from_pg_fast_path(bytes: usize) {
        JSONB_FROM_PG_FAST_PATH_CALLS.fetch_add(1, Ordering::SeqCst);
        JSONB_FROM_PG_FAST_PATH_BYTES.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn snapshot() -> ConversionTestStats {
        ConversionTestStats {
            text_to_pg_fast_path_calls: TEXT_TO_PG_FAST_PATH_CALLS.load(Ordering::SeqCst),
            text_to_pg_fast_path_bytes: TEXT_TO_PG_FAST_PATH_BYTES.load(Ordering::SeqCst),
            json_to_pg_input_calls: JSON_TO_PG_INPUT_CALLS.load(Ordering::SeqCst),
            json_to_pg_input_bytes: JSON_TO_PG_INPUT_BYTES.load(Ordering::SeqCst),
            jsonb_to_pg_input_calls: JSONB_TO_PG_INPUT_CALLS.load(Ordering::SeqCst),
            jsonb_to_pg_input_bytes: JSONB_TO_PG_INPUT_BYTES.load(Ordering::SeqCst),
            text_from_pg_fast_path_calls: TEXT_FROM_PG_FAST_PATH_CALLS.load(Ordering::SeqCst),
            text_from_pg_fast_path_bytes: TEXT_FROM_PG_FAST_PATH_BYTES.load(Ordering::SeqCst),
            json_from_pg_fast_path_calls: JSON_FROM_PG_FAST_PATH_CALLS.load(Ordering::SeqCst),
            json_from_pg_fast_path_bytes: JSON_FROM_PG_FAST_PATH_BYTES.load(Ordering::SeqCst),
            jsonb_from_pg_fast_path_calls: JSONB_FROM_PG_FAST_PATH_CALLS.load(Ordering::SeqCst),
            jsonb_from_pg_fast_path_bytes: JSONB_FROM_PG_FAST_PATH_BYTES.load(Ordering::SeqCst),
        }
    }
}

#[cfg(any(feature = "pg_test", feature = "e2e"))]
#[global_allocator]
static TEST_ALLOCATOR: test_alloc::TrackingAllocator = test_alloc::TrackingAllocator;

// FDW callback layer
#[cfg(any(not(test), feature = "pg_test"))]
mod arrow_to_pg;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_ddl;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_handler;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_import;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_modify;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_options;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_scan;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_state;
#[cfg(any(not(test), feature = "pg_test"))]
mod fdw_xact;
#[cfg(any(not(test), feature = "pg_test"))]
mod pg_to_kalam;
#[cfg(any(not(test), feature = "pg_test"))]
mod relation_table_options;
#[cfg(any(not(test), feature = "pg_test"))]
mod write_buffer;

pub use session_settings::SessionSettings;

// Plain cargo test/nextest lib builds do not run inside a PostgreSQL backend.
// Keep that target limited to the pure-Rust surface so `--all-targets` does not
// try to link Postgres symbols. Normal builds and `cargo pgrx test` still compile
// the full extension surface.
#[cfg(any(not(test), feature = "pg_test"))]
include!("pgrx_entrypoint.rs");

#[cfg(all(test, not(feature = "pg_test")))]
pub fn kalam_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(all(test, not(feature = "pg_test")))]
pub fn kalam_compiled_mode() -> &'static str {
    "remote"
}

#[cfg(all(test, not(feature = "pg_test")))]
pub fn kalam_user_id_guc_name() -> &'static str {
    kalam_pg_common::USER_ID_GUC
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
