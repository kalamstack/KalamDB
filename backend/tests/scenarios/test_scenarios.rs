//! Test driver for scenario-based end-to-end tests.
//!
//! Run with: cargo test --test test_scenarios

// Include the common test support
#[path = "../common/testserver/mod.rs"]
mod test_support;

// Include all scenario modules
pub mod helpers;

mod scenario_01_chat_app;
mod scenario_02_offline_sync;
mod scenario_03_shopping_cart;
mod scenario_04_iot_telemetry;
mod scenario_05_dashboards;
mod scenario_06_jobs;
mod scenario_07_collaborative;
mod scenario_08_burst;
mod scenario_09_ddl_while_active;
mod scenario_10_multi_tenant;
mod scenario_11_multi_storage;
mod scenario_12_performance;
mod scenario_13_soak_test;
mod scenario_14_vector_rag;
