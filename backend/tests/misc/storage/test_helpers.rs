//! Test helpers for storage tests.
//!
//! These are placeholder stubs since the actual tests using these are marked #[ignore]
//! and require kalamdb-core's internal test infrastructure.

// Re-export from kalamdb-core for ignored tests that might someday run
// For now, these tests are ignored and don't actually compile against kalamdb-core's
// internal test_app_context, so we just provide empty stubs.

#[allow(dead_code)]
pub fn test_app_context() {
    panic!(
        "test_app_context is not available in server integration tests - this test should be \
         #[ignore]"
    );
}
