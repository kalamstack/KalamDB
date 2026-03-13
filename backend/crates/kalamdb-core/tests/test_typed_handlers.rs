//! Integration test demonstrating typed handler pattern
//!
//! Shows how the executor classifies SQL, parses once, and dispatches to typed handlers.

use chrono::Utc;
use kalamdb_commons::models::StorageId;
use kalamdb_commons::models::UserId;
use kalamdb_commons::NodeId;
use kalamdb_commons::Role;
use kalamdb_configs::ServerConfig;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_core::sql::context::ExecutionResult;
use kalamdb_core::sql::executor::SqlExecutor;
use kalamdb_store::test_utils::TestDb;
use kalamdb_system::providers::storages::models::StorageType;
use kalamdb_system::Storage;
use std::sync::Arc;

/// Helper to create AppContext with temporary RocksDB for testing
async fn create_test_app_context() -> (Arc<AppContext>, TestDb) {
    let test_db = TestDb::with_system_tables().expect("Failed to create test database");
    let storage_base_path = test_db.storage_dir().expect("Failed to create storage directory");
    let backend = test_db.backend();

    let app_context = AppContext::create_isolated(
        backend,
        NodeId::new(1),
        storage_base_path.to_string_lossy().into_owned(),
        ServerConfig::default(),
    );

    app_context.executor().start().await.expect("Failed to start Raft");
    app_context
        .executor()
        .initialize_cluster()
        .await
        .expect("Failed to initialize Raft cluster");
    app_context.wire_raft_appliers();

    let storages = app_context.system_tables().storages();
    let storage_id = StorageId::from("local");
    if storages.get_storage_by_id(&storage_id).unwrap().is_none() {
        storages
            .create_storage(Storage {
                storage_id,
                storage_name: "Local Storage".to_string(),
                description: Some("Default local storage for tests".to_string()),
                storage_type: StorageType::Filesystem,
                base_directory: storage_base_path.to_string_lossy().to_string(),
                credentials: None,
                config_json: None,
                shared_tables_template: "shared/{namespace}/{table}".to_string(),
                user_tables_template: "user/{namespace}/{table}/{userId}".to_string(),
                created_at: Utc::now().timestamp_millis(),
                updated_at: Utc::now().timestamp_millis(),
            })
            .expect("Failed to create default local storage");
    }

    (app_context, test_db)
}

#[tokio::test]
#[ignore = "Requires Raft for CREATE NAMESPACE"]
async fn test_typed_handler_create_namespace() {
    let (app_ctx, _test_db) = create_test_app_context().await;
    let executor = SqlExecutor::new(Arc::clone(&app_ctx), false);
    let exec_ctx =
        ExecutionContext::new(UserId::from("admin"), Role::Dba, app_ctx.base_session_context());

    // Test CREATE NAMESPACE with typed handler
    let namespace = format!("integration_test_ns_{}", std::process::id());
    let sql = format!("CREATE NAMESPACE {}", namespace);
    let result = executor.execute(&sql, &exec_ctx, vec![]).await;

    assert!(result.is_ok(), "CREATE NAMESPACE should succeed");

    match result.unwrap() {
        ExecutionResult::Success { message } => {
            assert!(message.contains(&namespace));
            assert!(message.contains("created successfully"));
        },
        _ => panic!("Expected Success result"),
    }
}

#[tokio::test]
async fn test_typed_handler_authorization() {
    let (app_ctx, _temp_dir) = create_test_app_context().await;
    let executor = SqlExecutor::new(Arc::clone(&app_ctx), false);
    let user_ctx = ExecutionContext::new(
        UserId::from("regular_user"),
        Role::User,
        app_ctx.base_session_context(),
    );

    // Regular users cannot create namespaces
    let sql = "CREATE NAMESPACE unauthorized_ns";
    let result = executor.execute(sql, &user_ctx, vec![]).await;

    assert!(result.is_err(), "Regular users should not create namespaces");
}

#[tokio::test]
async fn test_classifier_prioritizes_select() {
    // This test verifies that SELECT queries go through the fast path
    // without attempting DDL parsing
    let (app_ctx, _temp_dir) = create_test_app_context().await;
    let executor = SqlExecutor::new(Arc::clone(&app_ctx), false);
    let exec_ctx =
        ExecutionContext::new(UserId::from("user"), Role::User, app_ctx.base_session_context());

    // SELECT should hit the DataFusion path immediately
    let sql = "SELECT 1 as test";
    let result = executor.execute(sql, &exec_ctx, vec![]).await;

    // Should succeed (DataFusion can execute this)
    assert!(result.is_ok(), "SELECT should execute via DataFusion");
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn test_storage_flush_table_returns_noop_when_no_pending_writes() {
    let (app_ctx, _temp_dir) = create_test_app_context().await;
    let executor = SqlExecutor::new(Arc::clone(&app_ctx), false);
    let exec_ctx =
        ExecutionContext::new(UserId::from("admin"), Role::Dba, app_ctx.base_session_context());

    let create_namespace = executor.execute("CREATE NAMESPACE flush_test", &exec_ctx, vec![]).await;
    assert!(create_namespace.is_ok(), "CREATE NAMESPACE should succeed");

    let create_table = executor
        .execute(
            "CREATE SHARED TABLE flush_test.docs (id INT PRIMARY KEY, body TEXT)",
            &exec_ctx,
            vec![],
        )
        .await;
    assert!(create_table.is_ok(), "CREATE TABLE should succeed");

    let flush = executor
        .execute("STORAGE FLUSH TABLE flush_test.docs", &exec_ctx, vec![])
        .await
        .expect("flush with no pending writes should succeed");
    match flush {
        ExecutionResult::Success { message } => {
            assert!(message
                .contains("Storage flush skipped: no pending writes for table 'flush_test.docs'"));
        },
        other => panic!("Expected success result for no-op flush, got {:?}", other),
    }
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn test_storage_flush_table_returns_noop_when_flush_already_in_progress() {
    let (app_ctx, _temp_dir) = create_test_app_context().await;
    let executor = SqlExecutor::new(Arc::clone(&app_ctx), false);
    let exec_ctx =
        ExecutionContext::new(UserId::from("admin"), Role::Dba, app_ctx.base_session_context());

    let create_namespace = executor.execute("CREATE NAMESPACE flush_busy", &exec_ctx, vec![]).await;
    assert!(create_namespace.is_ok(), "CREATE NAMESPACE should succeed");

    let create_table = executor
        .execute(
            "CREATE SHARED TABLE flush_busy.docs (id INT PRIMARY KEY, body TEXT)",
            &exec_ctx,
            vec![],
        )
        .await;
    assert!(create_table.is_ok(), "CREATE TABLE should succeed");

    let insert = executor
        .execute("INSERT INTO flush_busy.docs (id, body) VALUES (1, 'hello')", &exec_ctx, vec![])
        .await;
    assert!(insert.is_ok(), "INSERT should succeed");

    let first_flush = executor
        .execute("STORAGE FLUSH TABLE flush_busy.docs", &exec_ctx, vec![])
        .await
        .expect("first flush should succeed");
    match first_flush {
        ExecutionResult::Success { message } => {
            assert!(message.contains("Storage flush started for table 'flush_busy.docs'"));
        },
        other => panic!("Expected success result for first flush, got {:?}", other),
    }

    let second_flush = executor
        .execute("STORAGE FLUSH TABLE flush_busy.docs", &exec_ctx, vec![])
        .await
        .expect("second flush should be treated as no-op success");
    match second_flush {
        ExecutionResult::Success { message } => {
            assert!(message.contains("Storage flush skipped: a flush is already queued or running for table 'flush_busy.docs'"));
        },
        other => panic!("Expected success result for second flush, got {:?}", other),
    }
}
