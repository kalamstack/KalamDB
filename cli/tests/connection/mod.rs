//! Connection tests for kalam-client
//!
//! This directory contains tests for connection-related functionality:
//!
//! ## Unit Tests (no server required)
//!
//! - `connection_options_tests.rs`: ConnectionOptions defaults, builder pattern, presets, JSON serialization
//! - `subscription_options_tests.rs`: SubscriptionOptions defaults, batch_size, last_rows, from_seq_id
//! - `reconnection_tests.rs`: Reconnection workflow, attempt counting, seq_id tracking
//! - `resume_seqid_tests.rs`: SeqId resumption scenarios, tracking, edge cases
//! - `timeout_tests.rs`: Connection timeout to unreachable servers, connection refused
//!
//! ## Integration Tests (requires running KalamDB server)
//!
//! - `live_connection_tests.rs`: Real server connections with user tables:
//!   - test_live_subscription_default_options: Subscribe and receive events
//!   - test_live_subscription_with_batch_size: Test batch_size option
//!   - test_live_subscription_with_last_rows: Test last_rows option
//!   - test_live_subscription_seq_id_tracking: Track seq_id for resumption
//!   - test_live_multiple_subscriptions: Multiple concurrent subscriptions
//!   - test_live_subscription_change_event_order: Verify change events are received
//!   - test_connection_timeout_option: Validate ConnectionOptions configuration
//!
//! ## Running Tests
//!
//! All connection tests (requires server for live tests):
//! ```bash
//! cargo test --test connection -- --test-threads=1
//! ```
//!
//! Unit tests only (no server required):
//! ```bash
//! cargo test --test connection -- --skip live_
//! ```
//!
//! Live tests only:
//! ```bash
//! cargo test --test connection live_connection -- --test-threads=1
//! ```
//!
//! Filter by module name:
//! ```bash
//! cargo test --test connection reconnection
//! cargo test --test connection resume_seqid
//! ```
//!
//! ## Note
//!
//! These tests are aggregated by `cli/tests/connection.rs` which uses
//! the `#[path = "..."]` attribute to include each test file.
