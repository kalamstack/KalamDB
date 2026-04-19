// Aggregator for smoke tests to ensure Cargo picks them up
//
// Smoke tests are organized into subcategories:
//   - usecases: Combined/integration use cases
//   - impersonating: AS USER impersonation tests
//   - subscription: Live query and subscription tests
//   - cli: CLI command and cluster operation tests
//   - tables: User and shared table tests
//   - flushing: Flush operation tests
//   - storage: Storage template tests
//   - ddl: DDL and ALTER tests
//   - dml: DML operation tests
//   - query: Query performance tests
//   - system: System table tests
//
// Run these tests with:
//   cargo test --test smoke
//   cargo test --test smoke subscription
//   cargo test --test smoke flushing

mod common;

// Use case tests
#[path = "smoke/usecases/chat_ai_example_smoke.rs"]
mod chat_ai_example_smoke;
#[path = "smoke/usecases/smoke_test_all_datatypes.rs"]
mod smoke_test_all_datatypes;
#[path = "smoke/usecases/smoke_test_batch_control.rs"]
mod smoke_test_batch_control;
#[path = "smoke/usecases/smoke_test_core_operations.rs"]
mod smoke_test_core_operations;
#[path = "smoke/usecases/smoke_test_custom_functions.rs"]
mod smoke_test_custom_functions;
#[path = "smoke/usecases/smoke_test_file_datatype.rs"]
mod smoke_test_file_datatype;
#[path = "smoke/usecases/smoke_test_int64_precision.rs"]
mod smoke_test_int64_precision;
#[path = "smoke/usecases/smoke_test_schema_history.rs"]
mod smoke_test_schema_history;
#[path = "smoke/usecases/smoke_test_timing_output.rs"]
mod smoke_test_timing_output;
#[path = "smoke/usecases/smoke_test_websocket_capacity.rs"]
mod smoke_test_websocket_capacity;

// Impersonation tests
#[path = "smoke/impersonating/smoke_test_as_user_authorization.rs"]
mod smoke_test_as_user_authorization;
#[path = "smoke/impersonating/smoke_test_as_user_chat_impersonation.rs"]
mod smoke_test_as_user_chat_impersonation;
#[path = "smoke/impersonating/smoke_test_as_user_impersonation.rs"]
mod smoke_test_as_user_impersonation;

// Subscription tests
#[path = "smoke/subscription/smoke_test_shared_table_subscription.rs"]
mod smoke_test_shared_table_subscription;
#[path = "smoke/subscription/smoke_test_stream_subscription.rs"]
mod smoke_test_stream_subscription;
#[path = "smoke/subscription/smoke_test_subscription_advanced.rs"]
mod smoke_test_subscription_advanced;
#[path = "smoke/subscription/smoke_test_subscription_close.rs"]
mod smoke_test_subscription_close;
#[path = "smoke/subscription/smoke_test_subscription_delete.rs"]
mod smoke_test_subscription_delete;
#[path = "smoke/subscription/smoke_test_subscription_delta_updates.rs"]
mod smoke_test_subscription_delta_updates;
#[path = "smoke/subscription/smoke_test_subscription_listing.rs"]
mod smoke_test_subscription_listing;
#[path = "smoke/subscription/smoke_test_subscription_multi_reconnect.rs"]
mod smoke_test_subscription_multi_reconnect;
#[path = "smoke/subscription/smoke_test_subscription_reconnect_resume.rs"]
mod smoke_test_subscription_reconnect_resume;
#[path = "smoke/subscription/smoke_test_user_table_subscription.rs"]
mod smoke_test_user_table_subscription;

// Topic tests
#[path = "smoke/topics/smoke_test_topic_consumption.rs"]
mod smoke_test_topic_consumption;
#[path = "smoke/topics/smoke_test_topic_high_load.rs"]
mod smoke_test_topic_high_load;
#[path = "smoke/topics/smoke_test_topic_throughput.rs"]
mod smoke_test_topic_throughput;

// CLI tests
#[path = "smoke/cli/smoke_test_cli_commands.rs"]
mod smoke_test_cli_commands;
#[path = "smoke/cli/smoke_test_cluster_operations.rs"]
mod smoke_test_cluster_operations;

// Table tests
#[path = "smoke/tables/smoke_test_shared_table_crud.rs"]
mod smoke_test_shared_table_crud;
#[path = "smoke/tables/smoke_test_user_table_rls.rs"]
mod smoke_test_user_table_rls;

// Flushing tests
#[path = "smoke/flushing/smoke_test_flush_manifest.rs"]
mod smoke_test_flush_manifest;
#[path = "smoke/flushing/smoke_test_flush_operations.rs"]
mod smoke_test_flush_operations;
#[path = "smoke/flushing/smoke_test_flush_pk_integrity.rs"]
mod smoke_test_flush_pk_integrity;

// Storage tests
#[path = "smoke/storage/smoke_test_show_storages.rs"]
mod smoke_test_show_storages;
#[path = "smoke/storage/smoke_test_storage_compact.rs"]
mod smoke_test_storage_compact;
#[path = "smoke/storage/smoke_test_storage_health.rs"]
mod smoke_test_storage_health;
#[path = "smoke/storage/smoke_test_storage_templates.rs"]
mod smoke_test_storage_templates;

// DDL tests
#[path = "smoke/ddl/smoke_test_alter_with_data.rs"]
mod smoke_test_alter_with_data;
#[path = "smoke/ddl/smoke_test_backup_restore.rs"]
mod smoke_test_backup_restore;
#[path = "smoke/ddl/smoke_test_datatype_preservation.rs"]
mod smoke_test_datatype_preservation;
#[path = "smoke/ddl/smoke_test_ddl_alter.rs"]
mod smoke_test_ddl_alter;
#[path = "smoke/ddl/smoke_test_export_user_data.rs"]
mod smoke_test_export_user_data;

// DML tests
#[path = "smoke/dml/smoke_test_dml_extended.rs"]
mod smoke_test_dml_extended;
#[path = "smoke/dml/smoke_test_dml_watermark_optimization.rs"]
mod smoke_test_dml_watermark_optimization;
#[path = "smoke/dml/smoke_test_dml_wide_columns.rs"]
mod smoke_test_dml_wide_columns;
#[path = "smoke/dml/smoke_test_insert_returning.rs"]
mod smoke_test_insert_returning;
#[path = "smoke/dml/smoke_test_insert_throughput.rs"]
mod smoke_test_insert_throughput;

// Leader-only reads tests (Spec 021)
#[path = "smoke/leader_only_reads.rs"]
mod leader_only_reads;

// Query tests
#[path = "smoke/query/smoke_test_00_parallel_query_burst.rs"]
mod smoke_test_00_parallel_query_burst;
#[path = "smoke/query/smoke_test_json_operators.rs"]
mod smoke_test_json_operators;
#[path = "smoke/query/smoke_test_queries_benchmark.rs"]
mod smoke_test_queries_benchmark;

// System tests
#[path = "smoke/system/smoke_test_all_system_tables_schemas.rs"]
mod smoke_test_all_system_tables_schemas;
#[path = "smoke/system/smoke_test_cleanup_job.rs"]
mod smoke_test_cleanup_job;
#[path = "smoke/system/smoke_test_system_and_users.rs"]
mod smoke_test_system_and_users;
#[path = "smoke/system/smoke_test_system_tables_extended.rs"]
mod smoke_test_system_tables_extended;

// Security tests
#[path = "smoke/security/smoke_test_rpc_auth.rs"]
mod smoke_test_rpc_auth;
#[path = "smoke/security/smoke_test_security_access.rs"]
mod smoke_test_security_access;
