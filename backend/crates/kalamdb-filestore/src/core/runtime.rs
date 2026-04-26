use std::{future::Future, sync::OnceLock};

use crate::error::{FilestoreError, Result};

/// Run an async operation in a synchronous context.
///
/// This function handles the tricky case of needing to call async code from sync context.
///
/// **Strategy**:
/// - If we're in a tokio multi-thread runtime context, use `block_in_place` which allows blocking
///   while letting other tasks run on other threads
/// - If we're in a tokio current-thread runtime (common in tests), spawn the work on a background
///   thread that uses a shared runtime to avoid nested block_on calls
/// - If no runtime exists, use the shared runtime's block_on
///
/// **Why this matters for object_store**:
/// Remote backends (S3, GCS, Azure) use the tokio I/O driver for networking.
/// Calling `handle.block_on()` from within the runtime thread will deadlock because
/// the network I/O futures need the runtime thread to poll them.
pub fn run_blocking<F, Fut, T>(make_future: F) -> Result<T>
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T>> + Send,
    T: Send,
{
    fn shared_blocking_runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("Failed to create shared filestore runtime")
        })
    }

    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            // We're inside a tokio runtime. Check if we can use block_in_place.
            // block_in_place is only allowed in multi-thread runtime, not current_thread.
            match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    // Safe to use block_in_place - this lets us block while the runtime
                    // continues to service I/O on other threads
                    tokio::task::block_in_place(|| handle.block_on(make_future()))
                },
                tokio::runtime::RuntimeFlavor::CurrentThread => {
                    // Current-thread runtime (e.g., actix-rt): we cannot call block_on
                    // because we're already inside a block_on. Spawn a background thread
                    // that uses the shared multi-thread runtime.
                    std::thread::scope(|s| {
                        s.spawn(|| shared_blocking_runtime().block_on(make_future()))
                            .join()
                            .map_err(|_| {
                                FilestoreError::Other("Thread panicked in run_blocking".to_string())
                            })?
                    })
                },
                _ => {
                    // Unknown runtime flavor - use thread-based approach for safety
                    std::thread::scope(|s| {
                        s.spawn(|| shared_blocking_runtime().block_on(make_future()))
                            .join()
                            .map_err(|_| {
                                FilestoreError::Other("Thread panicked in run_blocking".to_string())
                            })?
                    })
                },
            }
        },
        Err(_) => {
            // No tokio runtime in current thread - safe to use block_on
            shared_blocking_runtime().block_on(make_future())
        },
    }
}
