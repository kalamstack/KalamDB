//! Test helpers for kalamdb-core.
//!
//! All helpers are compiled when the `test-helpers` feature (or `cfg(test)`) is active.
//! They have no dependency on `kalamdb-jobs`, so they are safe to use from other crates'
//! test code via `kalamdb-core = { ..., features = ["test-helpers"] }` in dev-dependencies.

use crate::app_context::AppContext;
use datafusion::prelude::SessionContext;
use kalamdb_commons::models::{NodeId, StorageId};
use kalamdb_store::test_utils::TestDb;
use kalamdb_store::StorageBackend;
use kalamdb_system::{StoragePartition, SystemTable};
use std::sync::Arc;

// ── Imports needed by init_test_app_context / test_app_context ─────────────────
#[cfg(any(test, feature = "test-helpers"))]
use kalamdb_commons::models::NamespaceId;
#[cfg(any(test, feature = "test-helpers"))]
use once_cell::sync::OnceCell;
#[cfg(any(test, feature = "test-helpers"))]
use std::sync::Once;

#[cfg(any(test, feature = "test-helpers"))]
static TEST_DB: OnceCell<Arc<TestDb>> = OnceCell::new();
#[cfg(any(test, feature = "test-helpers"))]
static TEST_RUNTIME: OnceCell<Arc<tokio::runtime::Runtime>> = OnceCell::new();
#[cfg(any(test, feature = "test-helpers"))]
static TEST_APP_CONTEXT: OnceCell<Arc<AppContext>> = OnceCell::new();
#[cfg(any(test, feature = "test-helpers"))]
static INIT: Once = Once::new();
#[cfg(any(test, feature = "test-helpers"))]
static BOOTSTRAP_INIT: Once = Once::new();

// ── Full helpers (no kalamdb-jobs dep) ─────────────────────────────────────────

/// Initialize AppContext with minimal test dependencies.
///
/// This is used by unit tests inside `kalamdb-core/src/**` that run with `cfg(test)`.
#[cfg(any(test, feature = "test-helpers"))]
pub fn init_test_app_context() -> Arc<TestDb> {
    INIT.call_once(|| {
        let mut column_families: Vec<&'static str> = SystemTable::all_tables()
            .iter()
            .filter_map(|t| t.column_family_name())
            .collect();
        column_families.push(StoragePartition::InformationSchemaTables.name());
        column_families.push("shared_table:app:config");
        column_families.push("stream_table:app:events");

        let test_db = Arc::new(TestDb::new(&column_families).unwrap());

        TEST_DB.set(test_db.clone()).ok();

        let storage_backend: Arc<dyn StorageBackend> = test_db.backend();

        let mut test_config = kalamdb_configs::ServerConfig::default();
        test_config.storage.data_path = "data".to_string();
        test_config.execution.max_parameters = 50;
        test_config.execution.max_parameter_size_bytes = 512 * 1024;

        let app_ctx = AppContext::init(
            storage_backend,
            NodeId::new(1),
            "data/storage".to_string(),
            test_config,
        );
        TEST_APP_CONTEXT.set(app_ctx).expect("TEST_APP_CONTEXT already initialized");
    });

    // One-time bootstrap that matches server startup behavior closely:
    // - Start + initialize single-node Raft so meta operations have a leader
    // - Seed default namespace + default local storage so scans can resolve storage paths
    BOOTSTRAP_INIT.call_once(|| {
        let app_ctx = test_app_context();
        let executor = app_ctx.executor();

        // Keep a dedicated Tokio runtime alive for the lifetime of the test process.
        // Raft spawns background tasks that must keep running after init.
        let rt = TEST_RUNTIME
            .get_or_init(|| Arc::new(tokio::runtime::Runtime::new().expect("tokio runtime")))
            .clone();

        // Kick off raft start+bootstrap on the dedicated runtime and synchronously wait.
        let (tx, rx) = std::sync::mpsc::channel();
        rt.spawn(async move {
            let result = async {
                executor.start().await.map_err(|e| format!("start raft: {e}"))?;
                executor
                    .initialize_cluster()
                    .await
                    .map_err(|e| format!("initialize single-node raft: {e}"))?;
                Ok::<(), String>(())
            }
            .await;
            let _ = tx.send(result);
        });

        rx.recv()
            .expect("raft bootstrap result")
            .expect("raft bootstrap should succeed");

        let app_ctx = test_app_context();

        // Ensure default namespace exists
        let namespaces = app_ctx.system_tables().namespaces();
        let default_namespace = NamespaceId::default();
        if namespaces.get_namespace_by_id(&default_namespace).unwrap().is_none() {
            namespaces
                .create_namespace(kalamdb_system::Namespace {
                    namespace_id: default_namespace,
                    name: "default".to_string(),
                    created_at: chrono::Utc::now().timestamp_millis(),
                    options: Some("{}".to_string()),
                    table_count: 0,
                })
                .unwrap();
        }

        // Ensure default local storage exists
        let storages = app_ctx.system_tables().storages();
        let storage_id = StorageId::from("local");
        if storages.get_storage_by_id(&storage_id).unwrap().is_none() {
            storages
                .create_storage(kalamdb_system::Storage {
                    storage_id,
                    storage_name: "Local Storage".to_string(),
                    description: Some("Default local storage for tests".to_string()),
                    storage_type:
                        kalamdb_system::providers::storages::models::StorageType::Filesystem,
                    base_directory: "/tmp/kalamdb_test".to_string(),
                    credentials: None,
                    config_json: None,
                    shared_tables_template: "shared/{namespace}/{table}".to_string(),
                    user_tables_template: "user/{namespace}/{table}/{userId}".to_string(),
                    created_at: chrono::Utc::now().timestamp_millis(),
                    updated_at: chrono::Utc::now().timestamp_millis(),
                })
                .unwrap();
        }
    });

    TEST_DB.get().expect("TEST_DB should be initialized").clone()
}

#[cfg(any(test, feature = "test-helpers"))]
pub fn test_app_context() -> Arc<AppContext> {
    init_test_app_context();
    TEST_APP_CONTEXT.get().expect("TEST_APP_CONTEXT should be initialized").clone()
}

/// Returns an AppContext without starting Raft bootstrap.
///
/// Use this for unit tests that only need schema registry, system tables, etc.
/// but do NOT need Raft consensus or leader election.
///
/// This creates a fresh AppContext per call (no shared statics) and is much
/// faster than `test_app_context()` which starts a full Raft cluster.
pub fn test_app_context_simple() -> Arc<AppContext> {
    let mut column_families: Vec<&'static str> = SystemTable::all_tables()
        .iter()
        .filter_map(|t| t.column_family_name())
        .collect();
    column_families.push(StoragePartition::InformationSchemaTables.name());
    column_families.push("shared_table:app:config");
    column_families.push("stream_table:app:events");

    let test_db = TestDb::new(&column_families).expect("create test db");
    let storage_backend: Arc<dyn StorageBackend> = test_db.backend();

    let storage_base_path = test_db.storage_dir().expect("create storage base path");
    let data_path = test_db.path().to_path_buf();

    let mut test_config = kalamdb_configs::ServerConfig::default();
    test_config.storage.data_path = data_path.to_string_lossy().to_string();
    test_config.execution.max_parameters = 50;
    test_config.execution.max_parameter_size_bytes = 512 * 1024;

    let app_ctx = AppContext::init(
        storage_backend,
        NodeId::new(1),
        storage_base_path.to_string_lossy().to_string(),
        test_config,
    );

    // Ensure default local storage exists for simple test contexts.
    let storages = app_ctx.system_tables().storages();
    let storage_id = StorageId::from("local");
    if storages.get_storage_by_id(&storage_id).unwrap().is_none() {
        storages
            .create_storage(kalamdb_system::Storage {
                storage_id,
                storage_name: "Local Storage".to_string(),
                description: Some("Default local storage for tests".to_string()),
                storage_type: kalamdb_system::providers::storages::models::StorageType::Filesystem,
                base_directory: storage_base_path.to_string_lossy().to_string(),
                credentials: None,
                config_json: None,
                shared_tables_template: "shared/{namespace}/{table}".to_string(),
                user_tables_template: "user/{namespace}/{table}/{userId}".to_string(),
                created_at: chrono::Utc::now().timestamp_millis(),
                updated_at: chrono::Utc::now().timestamp_millis(),
            })
            .unwrap();
    }

    std::mem::forget(test_db);
    app_ctx
}

/// Creates a SessionContext using test_app_context_simple() (no Raft bootstrap).
pub fn create_test_session_simple() -> Arc<SessionContext> {
    let app_ctx = test_app_context_simple();
    Arc::new(app_ctx.session_factory().create_session())
}
