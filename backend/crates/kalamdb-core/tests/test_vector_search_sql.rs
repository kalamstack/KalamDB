use chrono::Utc;
use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{StorageId, TableId};
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::{NodeId, Role, UserId};
use kalamdb_configs::ServerConfig;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult};
use kalamdb_core::sql::executor::handler_registry::HandlerRegistry;
use kalamdb_core::sql::executor::SqlExecutor;
use kalamdb_core::vector::flush_shared_scope_vectors;
use kalamdb_jobs::executors::{
    BackupExecutor, CleanupExecutor, CompactExecutor, FlushExecutor, JobRegistry, RestoreExecutor,
    RetentionExecutor, StreamEvictionExecutor, UserCleanupExecutor, VectorIndexExecutor,
};
use kalamdb_jobs::JobsManager;
use kalamdb_store::test_utils::TestDb;
use kalamdb_store::EntityStore;
use kalamdb_store::Partition;
use kalamdb_system::providers::storages::models::StorageType;
use kalamdb_system::{Storage, VectorMetric};
use kalamdb_vector::{
    new_indexed_shared_vector_hot_store, shared_vector_ops_partition_name,
    shared_vector_pk_index_partition_name, SharedVectorHotOpId, VectorHotOp, VectorHotOpType,
};
use std::sync::Arc;

fn create_executor(app_context: Arc<AppContext>) -> SqlExecutor {
    let registry = Arc::new(HandlerRegistry::new());
    kalamdb_handlers::register_all_handlers(&registry, app_context.clone(), false);
    SqlExecutor::new(app_context, registry)
}

fn init_job_manager(app_context: &Arc<AppContext>) {
    let registry = Arc::new(JobRegistry::new());
    registry.register(Arc::new(FlushExecutor::new()));
    registry.register(Arc::new(CleanupExecutor::new()));
    registry.register(Arc::new(RetentionExecutor::new()));
    registry.register(Arc::new(StreamEvictionExecutor::new()));
    registry.register(Arc::new(UserCleanupExecutor::new()));
    registry.register(Arc::new(CompactExecutor::new()));
    registry.register(Arc::new(BackupExecutor::new()));
    registry.register(Arc::new(RestoreExecutor::new()));
    registry.register(Arc::new(VectorIndexExecutor::new()));
    let jobs_provider = app_context.system_tables().jobs();
    let job_nodes_provider = app_context.system_tables().job_nodes();
    let job_manager = Arc::new(JobsManager::new(
        jobs_provider,
        job_nodes_provider,
        registry,
        Arc::clone(app_context),
    ));
    app_context.set_job_manager(job_manager.clone(), job_manager);
}

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

    init_job_manager(&app_context);
    (app_context, test_db)
}

fn create_exec_context(app_context: Arc<AppContext>) -> ExecutionContext {
    ExecutionContext::new(UserId::new("u_admin"), Role::Dba, app_context.base_session_context())
}

fn extract_id_values(batches: &[RecordBatch]) -> Vec<i64> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            for idx in 0..arr.len() {
                if !arr.is_null(idx) {
                    values.push(arr.value(idx));
                }
            }
            continue;
        }

        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
            for idx in 0..arr.len() {
                if !arr.is_null(idx) {
                    values.push(arr.value(idx) as i64);
                }
            }
        }
    }
    values
}

fn stage_shared_vector_op(
    store: &kalamdb_vector::SharedVectorHotStore,
    table_id: &TableId,
    column_name: &str,
    seq: i64,
    pk: i64,
    op_type: VectorHotOpType,
    vector: Option<Vec<f32>>,
) {
    store
        .insert(
            &SharedVectorHotOpId::new(SeqId::from(seq), pk.to_string()),
            &VectorHotOp::new(
                table_id.clone(),
                column_name.to_string(),
                pk.to_string(),
                op_type,
                vector,
                None,
                3,
                VectorMetric::Cosine,
            ),
        )
        .expect("stage shared vector op");
}

fn embedding_for_id(id: i64) -> [f32; 3] {
    let theta = ((id - 1) as f32 / 100.0) * std::f32::consts::TAU;
    [theta.cos(), theta.sin(), (2.0 * theta).cos() * 0.5]
}

fn round_embedding(embedding: [f32; 3]) -> [f32; 3] {
    [
        (embedding[0] * 1_000_000.0).round() / 1_000_000.0,
        (embedding[1] * 1_000_000.0).round() / 1_000_000.0,
        (embedding[2] * 1_000_000.0).round() / 1_000_000.0,
    ]
}

fn embedding_literal(embedding: [f32; 3]) -> String {
    format!("[{:.6},{:.6},{:.6}]", embedding[0], embedding[1], embedding[2])
}

async fn execute_sql(
    executor: &SqlExecutor,
    exec_ctx: &ExecutionContext,
    sql: &str,
) -> ExecutionResult {
    executor
        .execute(sql, exec_ctx, vec![])
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {}\nSQL: {}", e, sql))
}

async fn setup_vector_table(executor: &SqlExecutor, exec_ctx: &ExecutionContext) {
    execute_sql(executor, exec_ctx, "CREATE NAMESPACE test_ns").await;
    execute_sql(
        executor,
        exec_ctx,
        "CREATE SHARED TABLE test_ns.documents (id INT PRIMARY KEY, title TEXT, embedding EMBEDDING(3), embedding_alt EMBEDDING(3))",
    )
    .await;

    let inserts = [
        "INSERT INTO test_ns.documents (id, title, embedding, embedding_alt) VALUES (1, 'Doc A', '[1.0,0.0,0.0]', '[0.0,1.0,0.0]')",
        "INSERT INTO test_ns.documents (id, title, embedding, embedding_alt) VALUES (2, 'Doc B', '[0.9,0.1,0.0]', '[0.2,0.8,0.0]')",
        "INSERT INTO test_ns.documents (id, title, embedding, embedding_alt) VALUES (3, 'Doc C', '[0.0,1.0,0.0]', '[1.0,0.0,0.0]')",
    ];

    for sql in inserts {
        execute_sql(executor, exec_ctx, sql).await;
    }
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn test_cosine_distance_order_by_syntax_on_table() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    setup_vector_table(&executor, &exec_ctx).await;

    let result = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.documents ORDER BY COSINE_DISTANCE(embedding, '[1.0,0.0,0.0]') LIMIT 2",
    )
    .await;

    match result {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 2);
            let ids = extract_id_values(&batches);
            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0], 1, "nearest vector should be id=1");
        },
        other => panic!("Expected row result, got {:?}", other),
    }
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn test_create_and_drop_vector_index_lifecycle() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    setup_vector_table(&executor, &exec_ctx).await;

    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.documents CREATE INDEX embedding USING COSINE",
    )
    .await;

    let backend = app_context.storage_backend();
    let table_id = TableId::from_strings("test_ns", "documents");
    let ops_partition = Partition::new(shared_vector_ops_partition_name(&table_id, "embedding"));
    let pk_partition =
        Partition::new(shared_vector_pk_index_partition_name(&table_id, "embedding"));
    let cached_table = app_context.schema_registry().get(&table_id).expect("cached table");
    let storage_cached = cached_table
        .storage_cached(&app_context.storage_registry())
        .expect("storage cached");
    let schema = app_context.schema_registry().get_arrow_schema(&table_id).expect("table schema");
    assert!(
        backend.partition_exists(&ops_partition),
        "vector ops partition should exist after CREATE INDEX"
    );
    assert!(
        backend.partition_exists(&pk_partition),
        "vector pk index partition should exist after CREATE INDEX"
    );

    let emb_store = new_indexed_shared_vector_hot_store(backend.clone(), &table_id, "embedding");
    emb_store
        .insert(
            &SharedVectorHotOpId::new(SeqId::from(10i64), "1".to_string()),
            &VectorHotOp::new(
                table_id.clone(),
                "embedding".to_string(),
                "1".to_string(),
                VectorHotOpType::Upsert,
                Some(vec![1.0, 0.0, 0.0]),
                None,
                3,
                VectorMetric::Cosine,
            ),
        )
        .expect("stage vector snapshot v1 op");
    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush vector snapshot v1");

    let manifest_after_flush1 = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after first vector flush");
    let snapshot_path_flush1 = manifest_after_flush1
        .vector_indexes
        .get("embedding")
        .and_then(|m| m.snapshot_path.clone())
        .expect("snapshot path should exist after first flush");

    let manifest_before_drop = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest before drop");
    let snapshot_path_before_drop = manifest_before_drop
        .vector_indexes
        .get("embedding")
        .and_then(|m| m.snapshot_path.clone())
        .expect("snapshot path should exist after flushes");
    assert_eq!(snapshot_path_flush1, snapshot_path_before_drop);

    assert!(
        storage_cached
            .exists(TableType::Shared, &table_id, None, snapshot_path_flush1.as_str(),)
            .await
            .expect("exists snapshot v1 path")
            .exists,
        "first snapshot should exist before DROP INDEX"
    );
    assert!(
        storage_cached
            .exists(TableType::Shared, &table_id, None, snapshot_path_before_drop.as_str(),)
            .await
            .expect("exists latest snapshot")
            .exists,
        "latest snapshot should exist before DROP INDEX"
    );

    let manifest_after_add = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after add index");
    let add_meta = manifest_after_add
        .vector_indexes
        .get("embedding")
        .expect("vector metadata exists after CREATE INDEX");
    assert!(add_meta.enabled);
    assert_eq!(add_meta.metric, VectorMetric::Cosine);

    execute_sql(&executor, &exec_ctx, "ALTER TABLE test_ns.documents DROP INDEX embedding").await;

    let manifest_after_drop = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after drop index");
    let drop_meta = manifest_after_drop
        .vector_indexes
        .get("embedding")
        .expect("vector metadata remains after DROP INDEX");
    assert!(!drop_meta.enabled);
    assert!(
        drop_meta.snapshot_path.is_none(),
        "snapshot pointer should be cleared after DROP INDEX"
    );
    assert!(
        !backend.partition_exists(&ops_partition),
        "vector ops partition should be dropped after DROP INDEX"
    );
    assert!(
        !backend.partition_exists(&pk_partition),
        "vector pk index partition should be dropped after DROP INDEX"
    );
    assert!(
        !storage_cached
            .exists(TableType::Shared, &table_id, None, snapshot_path_flush1.as_str(),)
            .await
            .expect("exists snapshot v1 path after drop")
            .exists,
        "first snapshot should be removed after DROP INDEX"
    );
    assert!(
        !storage_cached
            .exists(TableType::Shared, &table_id, None, snapshot_path_before_drop.as_str(),)
            .await
            .expect("exists latest snapshot after drop")
            .exists,
        "latest snapshot should be removed after DROP INDEX"
    );
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn test_multiple_vector_indexes_flush_and_selection() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    setup_vector_table(&executor, &exec_ctx).await;

    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.documents CREATE INDEX embedding USING COSINE",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.documents CREATE INDEX embedding_alt USING COSINE",
    )
    .await;

    let table_id = TableId::from_strings("test_ns", "documents");
    let backend = app_context.storage_backend();
    let emb_store = new_indexed_shared_vector_hot_store(backend.clone(), &table_id, "embedding");
    emb_store
        .insert(
            &SharedVectorHotOpId::new(SeqId::from(1i64), "1".to_string()),
            &VectorHotOp::new(
                table_id.clone(),
                "embedding".to_string(),
                "1".to_string(),
                VectorHotOpType::Upsert,
                Some(vec![1.0, 0.0, 0.0]),
                None,
                3,
                VectorMetric::Cosine,
            ),
        )
        .expect("stage shared embedding vector op");

    let emb_alt_store =
        new_indexed_shared_vector_hot_store(backend.clone(), &table_id, "embedding_alt");
    emb_alt_store
        .insert(
            &SharedVectorHotOpId::new(SeqId::from(2i64), "3".to_string()),
            &VectorHotOp::new(
                table_id.clone(),
                "embedding_alt".to_string(),
                "3".to_string(),
                VectorHotOpType::Upsert,
                Some(vec![1.0, 0.0, 0.0]),
                None,
                3,
                VectorMetric::Cosine,
            ),
        )
        .expect("stage shared embedding_alt vector op");

    execute_sql(&executor, &exec_ctx, "STORAGE FLUSH TABLE test_ns.documents").await;

    let manifest = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after flush");

    let emb_meta = manifest.vector_indexes.get("embedding").expect("embedding metadata exists");
    assert!(emb_meta.enabled);

    let emb_alt_meta = manifest
        .vector_indexes
        .get("embedding_alt")
        .expect("embedding_alt metadata exists");
    assert!(emb_alt_meta.enabled);

    let nearest_embedding = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.documents ORDER BY COSINE_DISTANCE(embedding, '[1.0,0.0,0.0]') LIMIT 1",
    )
    .await;
    match nearest_embedding {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 1);
            assert_eq!(extract_id_values(&batches), vec![1]);
        },
        other => panic!("Expected rows for embedding query, got {:?}", other),
    }

    let nearest_alt = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.documents ORDER BY COSINE_DISTANCE(embedding_alt, '[1.0,0.0,0.0]') LIMIT 1",
    )
    .await;
    match nearest_alt {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 1);
            assert_eq!(extract_id_values(&batches), vec![3]);
        },
        other => panic!("Expected rows for embedding_alt query, got {:?}", other),
    }
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_vector_search_with_hundred_rows_real_embeddings() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    execute_sql(&executor, &exec_ctx, "CREATE NAMESPACE test_ns").await;
    execute_sql(
        &executor,
        &exec_ctx,
        "CREATE SHARED TABLE test_ns.docs100 (id INT PRIMARY KEY, embedding EMBEDDING(3))",
    )
    .await;

    for id in 1..=100 {
        let vector = embedding_literal(round_embedding(embedding_for_id(id)));
        let sql =
            format!("INSERT INTO test_ns.docs100 (id, embedding) VALUES ({}, '{}')", id, vector);
        execute_sql(&executor, &exec_ctx, &sql).await;
    }

    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.docs100 CREATE INDEX embedding USING COSINE",
    )
    .await;

    let table_id = TableId::from_strings("test_ns", "docs100");
    let backend = app_context.storage_backend();
    let emb_store = new_indexed_shared_vector_hot_store(backend.clone(), &table_id, "embedding");
    let cached_table = app_context.schema_registry().get(&table_id).expect("cached docs100 table");
    let storage_cached = cached_table
        .storage_cached(&app_context.storage_registry())
        .expect("storage cached for docs100");
    let schema = app_context
        .schema_registry()
        .get_arrow_schema(&table_id)
        .expect("docs100 schema");
    for id in 1..=100 {
        let embedding = round_embedding(embedding_for_id(id));
        emb_store
            .insert(
                &SharedVectorHotOpId::new(SeqId::from(id), id.to_string()),
                &VectorHotOp::new(
                    table_id.clone(),
                    "embedding".to_string(),
                    id.to_string(),
                    VectorHotOpType::Upsert,
                    Some(embedding.to_vec()),
                    None,
                    3,
                    VectorMetric::Cosine,
                ),
            )
            .expect("stage docs100 vector op");
    }

    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush docs100 vector snapshot");

    let target_id = 37i64;
    let query_embedding = round_embedding(embedding_for_id(target_id));
    let query_vector = embedding_literal(query_embedding);

    let manifest = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after docs100 flush");
    let snapshot_path = manifest
        .vector_indexes
        .get("embedding")
        .and_then(|m| m.snapshot_path.clone())
        .expect("snapshot path should exist for docs100 index");
    assert!(
        storage_cached
            .exists(TableType::Shared, &table_id, None, snapshot_path.as_str())
            .await
            .expect("snapshot file exists")
            .exists,
        "vector snapshot should be created for docs100 after flush"
    );

    let cosine_sql = format!(
        "SELECT id FROM test_ns.docs100 ORDER BY COSINE_DISTANCE(embedding, '{}') LIMIT 5",
        query_vector
    );
    let cosine_result = execute_sql(&executor, &exec_ctx, &cosine_sql).await;
    match cosine_result {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 5);
            let ids = extract_id_values(&batches);
            let best = ids.first().copied().expect("at least one id");
            assert!(ids.contains(&target_id), "top-k should include the exact query vector id");
            assert!(
                (best - target_id).abs() <= 5,
                "best match should be in local neighborhood: best={}, target={}",
                best,
                target_id
            );
        },
        other => panic!("Expected rows for COSINE_DISTANCE query, got {:?}", other),
    }
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_cosine_distance_query_combines_hot_and_cold_rows_after_index_flush() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    execute_sql(&executor, &exec_ctx, "CREATE NAMESPACE test_ns").await;
    execute_sql(
        &executor,
        &exec_ctx,
        "CREATE SHARED TABLE test_ns.docs_overlay (id INT PRIMARY KEY, embedding EMBEDDING(3))",
    )
    .await;

    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.docs_overlay CREATE INDEX embedding USING COSINE",
    )
    .await;

    let table_id = TableId::from_strings("test_ns", "docs_overlay");
    let cached_table =
        app_context.schema_registry().get(&table_id).expect("cached docs_overlay table");
    let storage_cached = cached_table
        .storage_cached(&app_context.storage_registry())
        .expect("storage cached for docs_overlay");

    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_overlay (id, embedding) VALUES (1, '[1.0,0.0,0.0]')",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_overlay (id, embedding) VALUES (2, '[0.75,0.25,0.0]')",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_overlay (id, embedding) VALUES (3, '[0.50,0.50,0.0]')",
    )
    .await;

    execute_sql(&executor, &exec_ctx, "STORAGE FLUSH TABLE test_ns.docs_overlay").await;
    let schema = app_context
        .schema_registry()
        .get_arrow_schema(&table_id)
        .expect("docs_overlay schema");
    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush vector snapshot for docs_overlay");

    let manifest_after_cold_flush = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after docs_overlay cold flush");
    let cold_snapshot = manifest_after_cold_flush
        .vector_indexes
        .get("embedding")
        .and_then(|m| m.snapshot_path.clone())
        .expect("cold snapshot path should exist");
    assert!(
        storage_cached
            .exists(TableType::Shared, &table_id, None, cold_snapshot.as_str())
            .await
            .expect("check cold snapshot exists")
            .exists,
        "cold snapshot file should exist after table flush"
    );

    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_overlay (id, embedding) VALUES (4, '[0.999,0.001,0.0]')",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_overlay (id, embedding) VALUES (5, '[0.25,0.75,0.0]')",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_overlay (id, embedding) VALUES (6, '[0.0,1.0,0.0]')",
    )
    .await;

    let result = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.docs_overlay ORDER BY COSINE_DISTANCE(embedding, '[1.0,0.0,0.0]') LIMIT 3",
    )
    .await;

    match result {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 3);
            let ids = extract_id_values(&batches);
            assert_eq!(ids.first().copied(), Some(1));
            assert!(ids.contains(&1), "top-k should include a row already flushed to cold storage");
            assert!(ids.contains(&4), "top-k should include a newly inserted hot-storage row");
        },
        other => panic!("Expected row result, got {:?}", other),
    }
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_vector_index_multiple_flush_cycles_and_watermark() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    execute_sql(&executor, &exec_ctx, "CREATE NAMESPACE test_ns").await;
    execute_sql(
        &executor,
        &exec_ctx,
        "CREATE SHARED TABLE test_ns.docs_multi_flush (id INT PRIMARY KEY, embedding EMBEDDING(3))",
    )
    .await;

    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.docs_multi_flush CREATE INDEX embedding USING COSINE",
    )
    .await;

    let table_id = TableId::from_strings("test_ns", "docs_multi_flush");
    let backend = app_context.storage_backend();
    let emb_store = new_indexed_shared_vector_hot_store(backend.clone(), &table_id, "embedding");
    let cached_table = app_context
        .schema_registry()
        .get(&table_id)
        .expect("cached docs_multi_flush table");
    let storage_cached = cached_table
        .storage_cached(&app_context.storage_registry())
        .expect("storage cached for docs_multi_flush");
    let schema = app_context
        .schema_registry()
        .get_arrow_schema(&table_id)
        .expect("docs_multi_flush schema");

    stage_shared_vector_op(
        &emb_store,
        &table_id,
        "embedding",
        1,
        1,
        VectorHotOpType::Upsert,
        Some(vec![1.0, 0.0, 0.0]),
    );
    stage_shared_vector_op(
        &emb_store,
        &table_id,
        "embedding",
        2,
        2,
        VectorHotOpType::Upsert,
        Some(vec![0.8, 0.2, 0.0]),
    );
    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush cycle #1");
    let manifest_1 = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after cycle #1");
    let meta_1 = manifest_1
        .vector_indexes
        .get("embedding")
        .expect("embedding index metadata after cycle #1");
    let snapshot_1 =
        meta_1.snapshot_path.clone().expect("snapshot path after cycle #1 should exist");
    assert_eq!(meta_1.snapshot_version, 1);
    assert_eq!(meta_1.last_applied_seq.as_i64(), 2);
    assert_eq!(snapshot_1, "vec-embedding-snapshot-1.vix");

    stage_shared_vector_op(
        &emb_store,
        &table_id,
        "embedding",
        3,
        3,
        VectorHotOpType::Upsert,
        Some(vec![0.95, 0.05, 0.0]),
    );
    stage_shared_vector_op(
        &emb_store,
        &table_id,
        "embedding",
        4,
        4,
        VectorHotOpType::Upsert,
        Some(vec![0.2, 0.8, 0.0]),
    );
    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush cycle #2");
    let manifest_2 = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after cycle #2");
    let meta_2 = manifest_2
        .vector_indexes
        .get("embedding")
        .expect("embedding index metadata after cycle #2");
    let snapshot_2 =
        meta_2.snapshot_path.clone().expect("snapshot path after cycle #2 should exist");
    assert_eq!(meta_2.snapshot_version, 2);
    assert_eq!(meta_2.last_applied_seq.as_i64(), 4);
    assert_eq!(snapshot_2, "vec-embedding-snapshot-2.vix");
    assert_ne!(snapshot_1, snapshot_2, "snapshot path should rotate each flush");

    stage_shared_vector_op(&emb_store, &table_id, "embedding", 5, 1, VectorHotOpType::Delete, None);
    stage_shared_vector_op(
        &emb_store,
        &table_id,
        "embedding",
        6,
        5,
        VectorHotOpType::Upsert,
        Some(vec![1.0, 0.0, 0.0]),
    );
    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush cycle #3");
    let manifest_3 = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after cycle #3");
    let meta_3 = manifest_3
        .vector_indexes
        .get("embedding")
        .expect("embedding index metadata after cycle #3");
    let snapshot_3 =
        meta_3.snapshot_path.clone().expect("snapshot path after cycle #3 should exist");
    assert_eq!(meta_3.snapshot_version, 3);
    assert_eq!(meta_3.last_applied_seq.as_i64(), 6);
    assert_eq!(snapshot_3, "vec-embedding-snapshot-3.vix");

    for snapshot in [&snapshot_1, &snapshot_2, &snapshot_3] {
        assert!(
            storage_cached
                .exists(TableType::Shared, &table_id, None, snapshot.as_str())
                .await
                .expect("snapshot exists check")
                .exists,
            "snapshot should exist in cold storage: {}",
            snapshot
        );
    }

    let remaining_hot_ops =
        emb_store.scan_all_typed(Some(32), None, None).expect("scan remaining hot ops");
    assert!(remaining_hot_ops.is_empty(), "hot staging should be pruned after flush commits");
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_cosine_distance_query_combines_hot_and_cold_rows() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    execute_sql(&executor, &exec_ctx, "CREATE NAMESPACE test_ns").await;
    execute_sql(
        &executor,
        &exec_ctx,
        "CREATE SHARED TABLE test_ns.docs_tiered (id INT PRIMARY KEY, embedding EMBEDDING(3))",
    )
    .await;

    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_tiered (id, embedding) VALUES (1, '[1.0,0.0,0.0]')",
    )
    .await;
    execute_sql(&executor, &exec_ctx, "STORAGE FLUSH TABLE test_ns.docs_tiered").await;

    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_tiered (id, embedding) VALUES (2, '[0.999,0.001,0.0]')",
    )
    .await;

    let result = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.docs_tiered ORDER BY COSINE_DISTANCE(embedding, '[1.0,0.0,0.0]') LIMIT 2",
    )
    .await;

    match result {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 2);
            let ids = extract_id_values(&batches);
            assert_eq!(ids, vec![1, 2], "query should combine cold+hot tiers");
        },
        other => panic!("Expected row result, got {:?}", other),
    }
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_vector_delete_stages_hot_tombstone_and_flushes_cleanup() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    execute_sql(&executor, &exec_ctx, "CREATE NAMESPACE test_ns").await;
    execute_sql(
        &executor,
        &exec_ctx,
        "CREATE SHARED TABLE test_ns.docs_delete_cleanup (id INT PRIMARY KEY, title TEXT, embedding EMBEDDING(3))",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.docs_delete_cleanup CREATE INDEX embedding USING COSINE",
    )
    .await;

    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_delete_cleanup (id, title, embedding) VALUES (1, 'base', '[1.0,0.0,0.0]')",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_delete_cleanup (id, title, embedding) VALUES (2, 'near', '[0.95,0.05,0.0]')",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "INSERT INTO test_ns.docs_delete_cleanup (id, title, embedding) VALUES (3, 'far', '[0.0,1.0,0.0]')",
    )
    .await;

    execute_sql(&executor, &exec_ctx, "STORAGE FLUSH TABLE test_ns.docs_delete_cleanup").await;

    let table_id = TableId::from_strings("test_ns", "docs_delete_cleanup");
    let backend = app_context.storage_backend();
    let store = new_indexed_shared_vector_hot_store(backend, &table_id, "embedding");
    let cached_table = app_context
        .schema_registry()
        .get(&table_id)
        .expect("cached docs_delete_cleanup table");
    let storage_cached = cached_table
        .storage_cached(&app_context.storage_registry())
        .expect("storage cached docs_delete_cleanup");
    let schema = app_context
        .schema_registry()
        .get_arrow_schema(&table_id)
        .expect("docs_delete_cleanup schema");
    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush vector snapshot for initial docs_delete_cleanup");

    let manifest_after_initial_flush = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after initial flush");
    let initial_last_applied = manifest_after_initial_flush
        .vector_indexes
        .get("embedding")
        .expect("vector metadata after initial flush")
        .last_applied_seq
        .as_i64();
    let initial_snapshot = manifest_after_initial_flush
        .vector_indexes
        .get("embedding")
        .and_then(|m| m.snapshot_path.clone())
        .expect("snapshot path after initial flush");

    execute_sql(&executor, &exec_ctx, "DELETE FROM test_ns.docs_delete_cleanup WHERE id = 1").await;

    let staged = store
        .scan_all_typed(Some(128), None, None)
        .expect("scan hot vector ops after delete");
    assert!(
        staged
            .iter()
            .any(|(_, op)| op.pk == "1" && matches!(op.op_type, VectorHotOpType::Delete)),
        "delete should stage a vector tombstone op for pk=1"
    );

    let hot_query = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.docs_delete_cleanup ORDER BY COSINE_DISTANCE(embedding, '[1.0,0.0,0.0]') LIMIT 3",
    )
    .await;
    match hot_query {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 2, "deleted row should be invisible before flush");
            let ids = extract_id_values(&batches);
            assert!(!ids.contains(&1), "deleted row should be shadowed immediately");
        },
        other => panic!("Expected rows for hot query, got {:?}", other),
    }

    execute_sql(&executor, &exec_ctx, "STORAGE FLUSH TABLE test_ns.docs_delete_cleanup").await;
    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush vector snapshot after delete docs_delete_cleanup");

    let manifest_after_delete_flush = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest after delete flush");
    let delete_meta = manifest_after_delete_flush
        .vector_indexes
        .get("embedding")
        .expect("vector metadata after delete flush");
    assert!(
        delete_meta.last_applied_seq.as_i64() > initial_last_applied,
        "delete flush should advance last_applied_seq watermark"
    );
    let delete_snapshot =
        delete_meta.snapshot_path.as_ref().expect("snapshot path after delete flush");
    assert_ne!(
        delete_snapshot, &initial_snapshot,
        "flush after delete should rotate vector snapshot file"
    );

    let remaining = store
        .scan_all_typed(Some(64), None, None)
        .expect("scan remaining hot vector ops");
    assert!(remaining.is_empty(), "hot vector ops should be pruned after flush");

    let cold_query = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.docs_delete_cleanup ORDER BY COSINE_DISTANCE(embedding, '[1.0,0.0,0.0]') LIMIT 3",
    )
    .await;
    match cold_query {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 2);
            let ids = extract_id_values(&batches);
            assert!(!ids.contains(&1), "deleted row must stay absent after flush");
        },
        other => panic!("Expected rows for cold query, got {:?}", other),
    }
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_multiple_vector_indexes_with_mixed_rows_and_null_embeddings() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context(app_context.clone());
    let executor = create_executor(app_context.clone());

    execute_sql(&executor, &exec_ctx, "CREATE NAMESPACE test_ns").await;
    execute_sql(
        &executor,
        &exec_ctx,
        "CREATE SHARED TABLE test_ns.docs_mixed_vectors (id INT PRIMARY KEY, title TEXT, category TEXT, score DOUBLE, embedding EMBEDDING(3), embedding_alt EMBEDDING(3))",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.docs_mixed_vectors CREATE INDEX embedding USING COSINE",
    )
    .await;
    execute_sql(
        &executor,
        &exec_ctx,
        "ALTER TABLE test_ns.docs_mixed_vectors CREATE INDEX embedding_alt USING COSINE",
    )
    .await;

    let rows = [
        "INSERT INTO test_ns.docs_mixed_vectors (id, title, category, score, embedding, embedding_alt) VALUES (1, 'alpha', 'a', 10.5, '[1.0,0.0,0.0]', '[0.0,1.0,0.0]')",
        "INSERT INTO test_ns.docs_mixed_vectors (id, title, category, score, embedding, embedding_alt) VALUES (2, 'beta', 'b', 9.0, NULL, '[0.02,0.98,0.0]')",
        "INSERT INTO test_ns.docs_mixed_vectors (id, title, category, score, embedding, embedding_alt) VALUES (3, 'gamma', 'c', 8.5, '[0.8,0.2,0.0]', NULL)",
        "INSERT INTO test_ns.docs_mixed_vectors (id, title, category, score, embedding, embedding_alt) VALUES (4, 'delta', 'd', 7.5, '[0.0,1.0,0.0]', '[1.0,0.0,0.0]')",
    ];
    for sql in rows {
        execute_sql(&executor, &exec_ctx, sql).await;
    }

    execute_sql(&executor, &exec_ctx, "STORAGE FLUSH TABLE test_ns.docs_mixed_vectors").await;

    let table_id = TableId::from_strings("test_ns", "docs_mixed_vectors");
    let cached_table = app_context.schema_registry().get(&table_id).expect("cached table");
    let storage_cached = cached_table
        .storage_cached(&app_context.storage_registry())
        .expect("storage cached");
    let schema = app_context
        .schema_registry()
        .get_arrow_schema(&table_id)
        .expect("docs_mixed_vectors schema");
    flush_shared_scope_vectors(&app_context, &table_id, &schema, &storage_cached)
        .expect("flush vector snapshot for docs_mixed_vectors");
    let manifest = app_context
        .manifest_service()
        .ensure_manifest_initialized(&table_id, None)
        .expect("manifest for mixed vectors");
    for column in ["embedding", "embedding_alt"] {
        let meta = manifest
            .vector_indexes
            .get(column)
            .unwrap_or_else(|| panic!("missing vector metadata for {}", column));
        let snapshot = meta
            .snapshot_path
            .as_ref()
            .unwrap_or_else(|| panic!("missing snapshot path for {}", column));
        assert!(
            storage_cached
                .exists(TableType::Shared, &table_id, None, snapshot.as_str())
                .await
                .expect("snapshot exists")
                .exists,
            "snapshot must exist in cold storage for {}",
            column
        );
    }

    let result_primary = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.docs_mixed_vectors ORDER BY COSINE_DISTANCE(embedding, '[1.0,0.0,0.0]') LIMIT 3",
    )
    .await;
    match result_primary {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 3);
            let ids = extract_id_values(&batches);
            assert_eq!(ids.first().copied(), Some(1));
            assert!(!ids.contains(&2), "row with NULL embedding should not rank for embedding");
        },
        other => panic!("Expected rows for primary embedding query, got {:?}", other),
    }

    let result_alt = execute_sql(
        &executor,
        &exec_ctx,
        "SELECT id FROM test_ns.docs_mixed_vectors ORDER BY COSINE_DISTANCE(embedding_alt, '[0.0,1.0,0.0]') LIMIT 3",
    )
    .await;
    match result_alt {
        ExecutionResult::Rows {
            batches, row_count, ..
        } => {
            assert_eq!(row_count, 3);
            let ids = extract_id_values(&batches);
            assert_eq!(ids.first().copied(), Some(1));
            assert!(
                !ids.contains(&3),
                "row with NULL embedding_alt should not rank for embedding_alt"
            );
        },
        other => panic!("Expected rows for alt embedding query, got {:?}", other),
    }
}
