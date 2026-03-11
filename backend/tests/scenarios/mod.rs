//! Scenario-based end-to-end tests for KalamDB.
//!
//! These tests validate KalamDB as a SQL-first, real-time database with:
//! - Table-per-user isolation (USER tables)
//! - Shared reference data (SHARED tables)
//! - Ephemeral streams with TTL (STREAM tables)
//! - Hot + cold tiers (RocksDB + Parquet) with flush jobs
//! - Live SQL subscriptions with initial snapshot batching
//! - RBAC and service-role AS USER writes
//! - Parallel usage under realistic workloads

pub mod helpers;

// Scenario tests - organized by scenario number
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
