// Aggregator for opt-in performance regression tests.
//
// These tests target a manually running server and are kept separate from the
// regular smoke suite so they can run in isolation.

mod common;

#[path = "performance/test_server_memory_regression.rs"]
mod test_server_memory_regression;