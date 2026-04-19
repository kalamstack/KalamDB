mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion_common::ScalarValue;
use kalamdb_commons::conversions::arrow_json_conversion::record_batch_to_json_rows;
use kalamdb_commons::models::pg_operations::{InsertRequest, ScanRequest};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
use kalamdb_commons::models::{NamespaceId, TableId, TableName};
use kalamdb_commons::schemas::ColumnDefault;
use kalamdb_commons::{TableAccess, TableType};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::operations::service::OperationService;
use kalamdb_pg::OperationExecutor;
use support::create_cluster_app_context;

async fn create_shared_table(
    app_ctx: &Arc<AppContext>,
    namespace: &NamespaceId,
    table_name: &str,
) -> TableId {
    let table_id = TableId::new(namespace.clone(), TableName::new(table_name));
    let id_col = ColumnDefinition::new(
        1,
        "id".to_string(),
        1,
        kalamdb_commons::models::datatypes::KalamDataType::BigInt,
        false,
        true,
        false,
        ColumnDefault::None,
        None,
    );
    let name_col = ColumnDefinition::simple(
        2,
        "name",
        2,
        kalamdb_commons::models::datatypes::KalamDataType::Text,
    );

    let mut table_options = TableOptions::shared();
    if let TableOptions::Shared(options) = &mut table_options {
        options.access_level = Some(TableAccess::Public);
    }

    let mut table_def = TableDefinition::new(
        namespace.clone(),
        table_id.table_name().clone(),
        TableType::Shared,
        vec![id_col, name_col],
        table_options,
        None,
    )
    .expect("create shared table definition");
    app_ctx
        .system_columns_service()
        .add_system_columns(&mut table_def)
        .expect("add system columns");

    app_ctx
        .schema_registry()
        .register_table(table_def)
        .expect("register shared table");

    table_id
}

fn row(id: i64, name: &str) -> Row {
    Row::new(BTreeMap::from([
        ("id".to_string(), ScalarValue::Int64(Some(id))),
        ("name".to_string(), ScalarValue::Utf8(Some(name.to_string()))),
    ]))
}

async fn scan_names(
    service: &OperationService,
    table_id: &TableId,
    session_id: &str,
) -> Vec<String> {
    let result = service
        .execute_scan(ScanRequest {
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            session_id: Some(session_id.to_string()),
            columns: vec![],
            limit: None,
            user_id: None,
            filters: vec![],
        })
        .await
        .expect("scan succeeds");

    let mut names = Vec::new();
    for batch in result.batches {
        for row in record_batch_to_json_rows(&batch).expect("json rows") {
            if let Some(name) = row.get("name").and_then(|value| value.as_str()) {
                names.push(name.to_string());
            }
        }
    }
    names
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn snapshot_isolation_hides_later_commits_from_open_transaction() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let service = OperationService::new(Arc::clone(&app_ctx));
    let namespace = NamespaceId::new("snapshot_isolation_core");
    let table_id = create_shared_table(&app_ctx, &namespace, "items").await;

    let session_b = "pg-5101-1a2b";
    let session_a = "pg-5102-1a2c";
    let observer_session = "pg-5103-1a2d";

    let transaction_id = service
        .begin_transaction(session_b)
        .await
        .expect("begin transaction succeeds")
        .expect("transaction id returned");
    assert!(!transaction_id.is_empty());

    assert!(scan_names(&service, &table_id, session_b).await.is_empty());

    service
        .execute_insert(InsertRequest {
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            session_id: Some(session_a.to_string()),
            user_id: None,
            rows: vec![row(1, "later")],
        })
        .await
        .expect("autocommit insert succeeds");

    assert!(scan_names(&service, &table_id, session_b).await.is_empty());

    let observer_rows = scan_names(&service, &table_id, observer_session).await;
    assert_eq!(observer_rows, vec!["later".to_string()]);

    service
        .rollback_transaction(session_b, &transaction_id)
        .await
        .expect("rollback succeeds");
}
