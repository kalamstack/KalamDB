//! Integration tests for DataFusion 55 SQL features used by KalamDB.

use std::sync::Arc;

use datafusion::{
    arrow::array::{BooleanArray, Float32Array, Float64Array, Int64Array, ListArray, StringArray},
    prelude::SessionContext,
};
use kalamdb_commons::{Role, UserId};
use kalamdb_core::sql::{context::ExecutionContext, datafusion_session::DataFusionSessionFactory};

fn create_test_session() -> Arc<SessionContext> {
    let factory =
        DataFusionSessionFactory::new().expect("Failed to create DataFusionSessionFactory");
    let session = factory.create_session();
    DataFusionSessionFactory::ensure_extended_functions(&session);
    Arc::new(session)
}

fn exec_ctx() -> ExecutionContext {
    ExecutionContext::new(UserId::new("u_test"), Role::User, create_test_session())
}

async fn scalar_bool(session: &SessionContext, sql: &str) -> bool {
    let batches = session
        .sql(sql)
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean column")
        .value(0)
}

async fn scalar_f64(session: &SessionContext, sql: &str) -> f64 {
    let batches = session
        .sql(sql)
        .await
        .expect("query should plan")
        .collect()
        .await
        .expect("query should execute");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float64 column")
        .value(0)
}

#[tokio::test]
async fn test_json_arrow_operator_without_sql_rewrite() {
    let session = exec_ctx().create_session_with_user();

    let result = session
        .sql("SELECT doc->>'name' AS name FROM (SELECT '{\"name\":\"alice\"}' AS doc) docs")
        .await;
    assert!(result.is_ok(), "native JSON operator query failed: {:?}", result.err());

    let batches = result.unwrap().collect().await.unwrap();
    let name = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column")
        .value(0);
    assert_eq!(name, "alice");
}

#[tokio::test]
async fn test_json_operators_through_rewrite_layer() {
    use kalamdb_sql::rewrite_context_functions_for_datafusion;

    let session = exec_ctx().create_session_with_user();
    let queries = [
        (
            "SELECT doc->'profile' AS profile FROM (SELECT '{\"profile\":{\"city\":\"london\"}}' \
             AS doc) docs",
            "london",
        ),
        (
            "SELECT doc->'user'->'address'->>'zip' AS zip FROM (SELECT \
             '{\"user\":{\"address\":{\"zip\":\"90210\"}}}' AS doc) docs",
            "90210",
        ),
        (
            "SELECT doc->>'customer_id' AS customer_id FROM (SELECT \
             '{\"customer_id\":\"cust_123\"}' AS doc) docs WHERE doc ? 'customer_id'",
            "cust_123",
        ),
        (
            "SELECT doc->>'priority' AS p FROM (SELECT \
             '{\"status\":\"active\",\"priority\":\"1\"}' AS doc) docs WHERE doc->>'status' = \
             'active'",
            "1",
        ),
        (
            "SELECT doc->'items'->0 AS first_item FROM (SELECT '{\"items\":[{\"id\":1}]}' AS doc) \
             docs",
            "id",
        ),
    ];

    for (sql, expected) in queries {
        let rewritten = rewrite_context_functions_for_datafusion(sql);
        let result = session.sql(rewritten.as_ref()).await;
        assert!(result.is_ok(), "rewritten JSON query failed for {sql:?}: {:?}", result.err());

        let batches = result.unwrap().collect().await.unwrap();
        let value = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("result column")
            .value(0);
        assert!(
            value.contains(expected),
            "expected {expected} in result for {sql:?}, got {value}"
        );
    }
}

#[tokio::test]
async fn test_numeric_string_comparison_uses_numeric_coercion() {
    let session = exec_ctx().create_session_with_user();

    assert!(
        !scalar_bool(&session, "SELECT 5 > '100' AS cmp").await,
        "DF54 should compare numerically, so 5 > 100 is false"
    );
    assert!(
        scalar_bool(&session, "SELECT 5 > '1' AS cmp").await,
        "DF54 should compare numerically, so 5 > 1 is true"
    );
}

#[tokio::test]
async fn test_scalar_subquery_with_multiple_rows_errors() {
    let session = exec_ctx().create_session_with_user();

    let result = session.sql("SELECT (SELECT n FROM (VALUES (1), (2)) AS t(n)) AS value").await;
    assert!(result.is_ok(), "scalar subquery should plan: {:?}", result.err());

    let execution = result.unwrap().collect().await;
    assert!(
        execution.is_err(),
        "uncorrelated scalar subquery with multiple rows should fail at execution"
    );
    let message = execution.err().expect("execution error").to_string();
    assert!(
        message.contains("more than one row"),
        "expected scalar subquery cardinality error, got: {message}"
    );
}

#[tokio::test]
async fn test_native_cosine_distance_for_array_arguments() {
    let session = exec_ctx().create_session_with_user();

    let distance = scalar_f64(
        &session,
        "SELECT cosine_distance(arrow_cast([1.0, 0.0], 'FixedSizeList(2, Float64)'), \
         arrow_cast([0.0, 1.0], 'FixedSizeList(2, Float64)')) AS distance",
    )
    .await;
    assert!(
        (distance - 1.0).abs() < 1e-6,
        "orthogonal unit vectors should have cosine distance 1.0, got {distance}"
    );
}

#[tokio::test]
async fn test_cosine_distance_json_query_vector_uses_kdb_path() {
    let session = exec_ctx().create_session_with_user();

    let distance = session
        .sql(
            "SELECT cosine_distance(arrow_cast([1.0, 0.0], 'FixedSizeList(2, Float32)'), '[0.0, \
             1.0]') AS distance",
        )
        .await
        .expect("json query vector should plan")
        .collect()
        .await
        .expect("json query vector should execute");

    let value = distance[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("float32 distance")
        .value(0);
    assert!((value - 1.0).abs() < 1e-6, "expected distance 1.0, got {value}");
}

#[tokio::test]
async fn test_array_transform_lambda_multiplies_elements() {
    let session = exec_ctx().create_session_with_user();

    let result = session
        .sql("SELECT array_transform([1, 2, 3, 4, 5], x -> x * 10) AS scaled")
        .await;
    assert!(result.is_ok(), "array_transform query failed: {:?}", result.err());

    let batches = result.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("scaled list column");
    let scaled_values = list.value(0);
    let values = scaled_values.as_any().downcast_ref::<Int64Array>().expect("scaled values");
    assert_eq!(
        values.iter().map(|value| value.expect("scaled value")).collect::<Vec<_>>(),
        vec![10, 20, 30, 40, 50]
    );
}

#[tokio::test]
async fn test_array_filter_and_transform_lambda_compose() {
    let session = exec_ctx().create_session_with_user();

    let result = session
        .sql(
            "SELECT array_transform(array_filter([1, 2, 3, 4, 5], x -> x > 2), x -> x * 10) AS \
             filtered_scaled",
        )
        .await;
    assert!(result.is_ok(), "composed lambda array query failed: {:?}", result.err());

    let batches = result.unwrap().collect().await.unwrap();
    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("filtered list column");
    let filtered_values = list.value(0);
    let values = filtered_values.as_any().downcast_ref::<Int64Array>().expect("filtered values");
    assert_eq!(
        values.iter().map(|value| value.expect("filtered value")).collect::<Vec<_>>(),
        vec![30, 40, 50]
    );
}

#[tokio::test]
async fn test_array_any_match_lambda() {
    let session = exec_ctx().create_session_with_user();

    let result = session.sql("SELECT array_any_match([1, 2, 3], x -> x > 2) AS has_large").await;
    assert!(result.is_ok(), "array_any_match query failed: {:?}", result.err());

    let batches = result.unwrap().collect().await.unwrap();
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .expect("boolean result")
        .value(0);
    assert!(value);
}

#[tokio::test]
async fn postgres_explain_option_list_rewrites_to_runnable_sql() {
    use kalamdb_sql::rewrite_explain_for_datafusion;

    let session = exec_ctx().create_session_with_user();
    let original = "EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) SELECT 1";
    let rewritten = rewrite_explain_for_datafusion(original)
        .expect("postgres EXPLAIN should parse")
        .expect("parenthesized EXPLAIN");
    assert_eq!(rewritten.sql, "EXPLAIN (ANALYZE, FORMAT pgjson) SELECT 1");

    let result = session.sql(&rewritten.sql).await;
    assert!(
        result.is_ok(),
        "rewritten EXPLAIN should plan under DuckDB dialect: {:?}",
        result.err()
    );
    let batches = result.unwrap().collect().await.expect("EXPLAIN ANALYZE should run");
    assert!(!batches.is_empty(), "EXPLAIN ANALYZE should return a plan");
}

#[tokio::test]
async fn datafusion_explain_metrics_option_runs_under_duckdb_dialect() {
    use kalamdb_sql::rewrite_explain_for_datafusion;

    let session = exec_ctx().create_session_with_user();
    let native = "EXPLAIN (ANALYZE, FORMAT pgjson, METRICS 'rows', LEVEL summary) SELECT 1";
    let native_result = session.sql(native).await;
    assert!(
        native_result.is_ok(),
        "DataFusion-native EXPLAIN options should plan under DuckDB dialect: {:?}",
        native_result.err()
    );
    let native_batches = native_result
        .unwrap()
        .collect()
        .await
        .expect("native EXPLAIN ANALYZE should run");
    assert!(!native_batches.is_empty());

    let rewritten = rewrite_explain_for_datafusion(
        "EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS, METRICS 'rows', LEVEL summary) SELECT 1",
    )
    .expect("postgres EXPLAIN should parse")
    .expect("parenthesized EXPLAIN");
    assert_eq!(
        rewritten.sql,
        "EXPLAIN (ANALYZE, FORMAT pgjson, LEVEL summary, METRICS 'rows') SELECT 1"
    );

    let adapted = session.sql(&rewritten.sql).await;
    assert!(
        adapted.is_ok(),
        "adapted JDBC EXPLAIN with METRICS should plan: {:?}",
        adapted.err()
    );
    let adapted_batches = adapted.unwrap().collect().await.expect("adapted EXPLAIN should run");
    assert!(!adapted_batches.is_empty());
}
