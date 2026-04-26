// Aggregator for connection tests to ensure Cargo picks them up
//
// Run these tests with:
//   cargo test --test connection
//
// Run individual test files:
//   cargo test --test connection connection_options
//   cargo test --test connection reconnection
//   cargo test --test connection resume_seqid
//   cargo test --test connection timeout
//   cargo test --test connection concurrent_ws
//
// Note: subscription_options_tests and live_connection_tests have been moved to the subscription
// category
//
// Unit tests (no server required):
mod common;
#[path = "connection/concurrent_ws_tests.rs"]
mod concurrent_ws_tests;
#[path = "connection/connection_options_tests.rs"]
mod connection_options_tests;
#[path = "connection/reconnection_tests.rs"]
mod reconnection_tests;
#[path = "connection/resume_seqid_tests.rs"]
mod resume_seqid_tests;
#[path = "connection/timeout_tests.rs"]
mod timeout_tests;
