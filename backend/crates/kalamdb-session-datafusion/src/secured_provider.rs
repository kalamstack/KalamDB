use crate::permissions::{check_system_table_access, session_error_to_datafusion};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use kalamdb_commons::models::TableId;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::Instrument;

pub struct SecuredSystemTableProvider {
    inner: Arc<dyn TableProvider>,
    table_id: TableId,
}

impl Debug for SecuredSystemTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecuredSystemTableProvider")
            .field("table_id", &self.table_id)
            .finish()
    }
}

impl SecuredSystemTableProvider {
    pub fn new(inner: Arc<dyn TableProvider>, table_id: TableId) -> Self {
        Self { inner, table_id }
    }

    pub fn table_id(&self) -> &TableId {
        &self.table_id
    }

    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

#[async_trait]
impl TableProvider for SecuredSystemTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let span = tracing::info_span!(
            "provider.scan",
            table_id = %self.table_id,
            filter_count = filters.len(),
            projection_count = projection.map_or(0, Vec::len),
            has_limit = limit.is_some(),
            limit = limit.unwrap_or(0)
        );

        async move {
            check_system_table_access(state, &self.table_id)
                .map_err(session_error_to_datafusion)?;
            tracing::debug!(table_id = %self.table_id, "System table access granted");
            self.inner.scan(state, projection, filters, limit).await
        }
        .instrument(span)
        .await
    }
}

pub fn secure_provider(
    inner: Arc<dyn TableProvider>,
    table_id: TableId,
) -> Arc<SecuredSystemTableProvider> {
    Arc::new(SecuredSystemTableProvider::new(inner, table_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use kalamdb_commons::{NamespaceId, TableName};

    fn create_mock_provider() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let array = StringArray::from(vec!["test_id"]);
        let batch =
            arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])
                .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    #[test]
    fn test_secured_provider_creation() {
        let inner = create_mock_provider();
        let table_id = TableId::new(NamespaceId::system(), TableName::new("test_table"));
        let secured = secure_provider(inner, table_id.clone());

        assert_eq!(secured.table_id().namespace_id().as_str(), "system");
        assert_eq!(secured.table_id().table_name().as_str(), "test_table");
    }

    #[test]
    fn test_secured_provider_schema() {
        let inner = create_mock_provider();
        let table_id = TableId::new(NamespaceId::system(), TableName::new("users"));
        let secured = secure_provider(inner, table_id);

        let schema = secured.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }
}
