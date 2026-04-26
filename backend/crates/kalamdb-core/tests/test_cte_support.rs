//! Integration tests for Common Table Expressions (CTEs)
//!
//! Tests cover:
//! - Simple CTEs with single WITH clause
//! - Multiple CTEs (chained WITH clauses)
//! - Recursive CTEs
//! - CTEs with aggregations
//! - CTEs with JOINs
//! - CTEs with filtering

use std::sync::Arc;

use chrono::Utc;
use kalamdb_commons::{models::StorageId, NodeId, Role, UserId};
use kalamdb_configs::ServerConfig;
use kalamdb_core::{
    app_context::AppContext,
    sql::{
        context::{ExecutionContext, ExecutionResult},
        executor::{handler_registry::HandlerRegistry, SqlExecutor},
    },
};
use kalamdb_store::test_utils::TestDb;
use kalamdb_system::{providers::storages::models::StorageType, Storage};

/// Helper to create a fully-wired SqlExecutor with all handlers registered.
fn create_executor(app_context: Arc<AppContext>) -> SqlExecutor {
    let registry = Arc::new(HandlerRegistry::new());
    kalamdb_handlers::register_all_handlers(&registry, app_context.clone(), false);
    SqlExecutor::new(app_context, registry)
}

/// Helper to create AppContext with temporary RocksDB for testing
async fn create_test_app_context() -> (Arc<AppContext>, TestDb) {
    let test_db = TestDb::with_system_tables().expect("Failed to create test database");
    let storage_base_path = test_db.storage_dir().expect("Failed to create storage directory");
    let backend = test_db.backend();
    let config = ServerConfig::default();
    let node_id = NodeId::new(1);

    let app_context = AppContext::create_isolated(
        backend,
        node_id,
        storage_base_path.to_string_lossy().into_owned(),
        config,
    );

    // Initialize Raft for single-node mode (required for DDL operations)
    app_context.executor().start().await.expect("Failed to start Raft");
    app_context
        .executor()
        .initialize_cluster()
        .await
        .expect("Failed to initialize Raft cluster");
    app_context.wire_raft_appliers();

    // Ensure default local storage exists for table creation.
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

/// Helper to create ExecutionContext
fn create_exec_context_with_app_context(
    app_context: Arc<AppContext>,
    user_id: &str,
    role: Role,
) -> ExecutionContext {
    let user_id = UserId::new(user_id.to_string());
    let base_session = app_context.base_session_context();
    ExecutionContext::new(user_id, role, base_session)
}

/// Setup: Create a test table and insert sample data
async fn setup_test_table(
    executor: &SqlExecutor,
    exec_ctx: &ExecutionContext,
) -> Result<(), String> {
    // Create a namespace
    executor
        .execute("CREATE NAMESPACE test_ns", exec_ctx, vec![])
        .await
        .map_err(|e| format!("Failed to create namespace: {}", e))?;

    // Create a test table
    executor
        .execute(
            "CREATE USER TABLE test_ns.employees (id INT PRIMARY KEY, name TEXT, department TEXT, \
             salary INT)",
            exec_ctx,
            vec![],
        )
        .await
        .map_err(|e| format!("Failed to create table: {}", e))?;

    // Insert test data
    let insert_queries = vec![
        "INSERT INTO test_ns.employees (id, name, department, salary) VALUES (1, 'Alice', \
         'Engineering', 100000)",
        "INSERT INTO test_ns.employees (id, name, department, salary) VALUES (2, 'Bob', \
         'Engineering', 90000)",
        "INSERT INTO test_ns.employees (id, name, department, salary) VALUES (3, 'Charlie', \
         'Sales', 80000)",
        "INSERT INTO test_ns.employees (id, name, department, salary) VALUES (4, 'Diana', \
         'Sales', 85000)",
        "INSERT INTO test_ns.employees (id, name, department, salary) VALUES (5, 'Eve', \
         'Marketing', 75000)",
    ];

    for query in insert_queries {
        executor
            .execute(query, exec_ctx, vec![])
            .await
            .map_err(|e| format!("Failed to insert data: {}", e))?;
    }

    Ok(())
}

// ============================================================================
// SIMPLE CTE TESTS
// ============================================================================

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_simple_cte() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context_with_app_context(app_context.clone(), "u_admin", Role::Dba);
    let executor = create_executor(app_context.clone());

    // Setup test data
    setup_test_table(&executor, &exec_ctx).await.unwrap();

    // Test simple CTE
    let result = executor
        .execute(
            r#"
            WITH high_earners AS (
                SELECT name, salary 
                FROM test_ns.employees 
                WHERE salary > 80000
            )
            SELECT * FROM high_earners ORDER BY salary DESC
            "#,
            &exec_ctx,
            vec![],
        )
        .await;

    assert!(result.is_ok(), "CTE query should succeed: {:?}", result.err());
    let exec_result = result.unwrap();

    // Verify result has rows
    match exec_result {
        ExecutionResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3, "Should return 3 high earners (Alice, Bob, Diana)");
        },
        _ => panic!("Expected Rows result, got: {:?}", exec_result),
    }
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_cte_with_aggregation() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context_with_app_context(app_context.clone(), "u_admin", Role::Dba);
    let executor = create_executor(app_context.clone());

    // Setup test data
    setup_test_table(&executor, &exec_ctx).await.unwrap();

    // Test CTE with aggregation
    let result = executor
        .execute(
            r#"
            WITH dept_stats AS (
                SELECT 
                    department,
                    COUNT(*) as employee_count,
                    AVG(salary) as avg_salary
                FROM test_ns.employees 
                GROUP BY department
            )
            SELECT * FROM dept_stats ORDER BY avg_salary DESC
            "#,
            &exec_ctx,
            vec![],
        )
        .await;

    assert!(result.is_ok(), "CTE with aggregation should succeed: {:?}", result.err());
    let exec_result = result.unwrap();

    // Verify result has rows
    match exec_result {
        ExecutionResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3, "Should return 3 departments");
        },
        _ => panic!("Expected Rows result, got: {:?}", exec_result),
    }
}

// ============================================================================
// MULTIPLE CTE TESTS
// ============================================================================

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_multiple_ctes() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context_with_app_context(app_context.clone(), "u_admin", Role::Dba);
    let executor = create_executor(app_context.clone());

    // Setup test data
    setup_test_table(&executor, &exec_ctx).await.unwrap();

    // Test multiple CTEs
    let result = executor
        .execute(
            r#"
            WITH 
                engineering AS (
                    SELECT name, salary 
                    FROM test_ns.employees 
                    WHERE department = 'Engineering'
                ),
                sales AS (
                    SELECT name, salary 
                    FROM test_ns.employees 
                    WHERE department = 'Sales'
                )
            SELECT 'Engineering' as dept, name, salary FROM engineering
            UNION ALL
            SELECT 'Sales' as dept, name, salary FROM sales
            ORDER BY salary DESC
            "#,
            &exec_ctx,
            vec![],
        )
        .await;

    assert!(result.is_ok(), "Multiple CTEs should succeed: {:?}", result.err());
    let exec_result = result.unwrap();

    // Verify result has rows
    match exec_result {
        ExecutionResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 4, "Should return 4 employees (2 Engineering + 2 Sales)");
        },
        _ => panic!("Expected Rows result, got: {:?}", exec_result),
    }
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_chained_ctes() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context_with_app_context(app_context.clone(), "u_admin", Role::Dba);
    let executor = create_executor(app_context.clone());

    // Setup test data
    setup_test_table(&executor, &exec_ctx).await.unwrap();

    // Test chained CTEs (one CTE references another)
    let result = executor
        .execute(
            r#"
            WITH 
                all_employees AS (
                    SELECT * FROM test_ns.employees
                ),
                high_earners AS (
                    SELECT * FROM all_employees WHERE salary > 80000
                )
            SELECT name, department, salary 
            FROM high_earners 
            ORDER BY salary DESC
            "#,
            &exec_ctx,
            vec![],
        )
        .await;

    assert!(result.is_ok(), "Chained CTEs should succeed: {:?}", result.err());
    let exec_result = result.unwrap();

    // Verify result has rows
    match exec_result {
        ExecutionResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3, "Should return 3 high earners");
        },
        _ => panic!("Expected Rows result, got: {:?}", exec_result),
    }
}

// ============================================================================
// CTE WITH FILTERING AND ORDERING TESTS
// ============================================================================

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_cte_with_where_clause() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context_with_app_context(app_context.clone(), "u_admin", Role::Dba);
    let executor = create_executor(app_context.clone());

    // Setup test data
    setup_test_table(&executor, &exec_ctx).await.unwrap();

    // Test CTE with WHERE clause in both CTE and main query
    let result = executor
        .execute(
            r#"
            WITH dept_employees AS (
                SELECT name, salary, department
                FROM test_ns.employees
                WHERE department IN ('Engineering', 'Sales')
            )
            SELECT * FROM dept_employees 
            WHERE salary >= 85000
            ORDER BY salary DESC
            "#,
            &exec_ctx,
            vec![],
        )
        .await;

    assert!(result.is_ok(), "CTE with WHERE should succeed: {:?}", result.err());
    let exec_result = result.unwrap();

    // Verify result has rows
    match exec_result {
        ExecutionResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3, "Should return 3 employees (Alice, Bob, Diana)");
        },
        _ => panic!("Expected Rows result, got: {:?}", exec_result),
    }
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_cte_with_limit() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context_with_app_context(app_context.clone(), "u_admin", Role::Dba);
    let executor = create_executor(app_context.clone());

    // Setup test data
    setup_test_table(&executor, &exec_ctx).await.unwrap();

    // Test CTE with LIMIT
    let result = executor
        .execute(
            r#"
            WITH all_salaries AS (
                SELECT name, salary 
                FROM test_ns.employees
                ORDER BY salary DESC
            )
            SELECT * FROM all_salaries LIMIT 2
            "#,
            &exec_ctx,
            vec![],
        )
        .await;

    assert!(result.is_ok(), "CTE with LIMIT should succeed: {:?}", result.err());
    let exec_result = result.unwrap();

    // Verify result has rows
    match exec_result {
        ExecutionResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 2, "Should return top 2 earners");
        },
        _ => panic!("Expected Rows result, got: {:?}", exec_result),
    }
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_cte_syntax_error() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context_with_app_context(app_context.clone(), "u_admin", Role::Dba);
    let executor = create_executor(app_context.clone());

    // Setup test data
    setup_test_table(&executor, &exec_ctx).await.unwrap();

    // Test CTE with syntax error (missing AS keyword)
    let result = executor
        .execute(
            r#"
            WITH high_earners (
                SELECT name, salary 
                FROM test_ns.employees 
                WHERE salary > 80000
            )
            SELECT * FROM high_earners
            "#,
            &exec_ctx,
            vec![],
        )
        .await;

    assert!(result.is_err(), "Invalid CTE syntax should fail");
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_cte_undefined_table() {
    let (app_context, _temp_dir) = create_test_app_context().await;
    let exec_ctx = create_exec_context_with_app_context(app_context.clone(), "u_admin", Role::Dba);
    let executor = create_executor(app_context.clone());

    // No setup - testing undefined table

    // Test CTE referencing non-existent table
    let result = executor
        .execute(
            r#"
            WITH data AS (
                SELECT * FROM non_existent_table
            )
            SELECT * FROM data
            "#,
            &exec_ctx,
            vec![],
        )
        .await;

    assert!(result.is_err(), "CTE with undefined table should fail");
}
