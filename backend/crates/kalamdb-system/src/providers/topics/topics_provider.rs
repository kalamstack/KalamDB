//! System.topics table provider
//!
//! This module provides a DataFusion TableProvider implementation for the system.topics table.
//! Uses `IndexedEntityStore` for automatic secondary index management.

use crate::error::{SystemError, SystemResultExt};
use crate::providers::base::{system_rows_to_batch, IndexedProviderDefinition};
use crate::system_row_mapper::{model_to_system_row, system_row_to_model};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use kalamdb_commons::models::rows::SystemTableRow;
use kalamdb_commons::models::TopicId;
use kalamdb_commons::SystemTable;
use kalamdb_store::entity_store::EntityStore;
use kalamdb_store::{IndexedEntityStore, StorageBackend};
use std::sync::{Arc, OnceLock};

use super::models::Topic;

/// Type alias for the indexed topics store
pub type TopicsStore = IndexedEntityStore<TopicId, SystemTableRow>;

/// System.topics table provider using IndexedEntityStore for automatic index management.
///
/// All insert/update/delete operations automatically maintain secondary indexes
/// using RocksDB's atomic WriteBatch - no manual index management needed.
#[derive(Clone)]
pub struct TopicsTableProvider {
    store: TopicsStore,
}

impl TopicsTableProvider {
    /// Create a new topics table provider with automatic index management.
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new TopicsTableProvider instance with indexes configured
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            SystemTable::Topics.column_family_name().expect("Topics is a table, not a view"),
            Vec::new(), // No secondary indexes for MVP
        );
        Self { store }
    }

    /// Create a new topic entry.
    ///
    /// # Arguments
    /// * `topic` - Topic entity to create
    ///
    /// # Returns
    /// Success message or error
    pub fn create_topic(&self, topic: Topic) -> Result<String, SystemError> {
        let row = Self::encode_topic_row(&topic)?;
        self.store
            .insert(&topic.topic_id, &row)
            .into_system_error("insert topic error")?;
        Ok(format!("Topic {} created", topic.name))
    }

    /// Async version of `create_topic()`.
    pub async fn create_topic_async(&self, topic: Topic) -> Result<(), SystemError> {
        let topic_id = topic.topic_id.clone();
        let row = Self::encode_topic_row(&topic)?;
        self.store
            .insert_async(topic_id, row)
            .await
            .into_system_error("insert_async topic error")
    }

    /// Get a topic by ID
    pub fn get_topic_by_id(&self, topic_id: &TopicId) -> Result<Option<Topic>, SystemError> {
        let row = self.store.get(topic_id)?;
        row.map(|value| Self::decode_topic_row(&value)).transpose()
    }

    /// Async version of `get_topic_by_id()`.
    pub async fn get_topic_by_id_async(
        &self,
        topic_id: &TopicId,
    ) -> Result<Option<Topic>, SystemError> {
        let row = self
            .store
            .get_async(topic_id.clone())
            .await
            .into_system_error("get_async error")?;
        row.map(|value| Self::decode_topic_row(&value)).transpose()
    }

    /// Update an existing topic entry.
    ///
    /// Indexes are automatically maintained via `IndexedEntityStore`.
    pub fn update_topic(&self, topic: Topic) -> Result<(), SystemError> {
        // Check if topic exists
        let old_topic = self.store.get(&topic.topic_id)?;
        if old_topic.is_none() {
            return Err(SystemError::NotFound(format!("Topic not found: {}", topic.topic_id)));
        }
        let old_topic = Self::decode_topic_row(&old_topic.unwrap())?;
        let row = Self::encode_topic_row(&topic)?;

        // No secondary indexes configured for topics in this phase.
        self.store
            .update_with_old(&topic.topic_id, Some(&Self::encode_topic_row(&old_topic)?), &row)
            .into_system_error("update topic error")
    }

    /// Async version of `update_topic()`.
    pub async fn update_topic_async(&self, topic: Topic) -> Result<(), SystemError> {
        // Check if topic exists
        let old_topic = self
            .store
            .get_async(topic.topic_id.clone())
            .await
            .into_system_error("get_async error")?;

        if old_topic.is_none() {
            return Err(SystemError::NotFound(format!("Topic not found: {}", topic.topic_id)));
        }

        let row = Self::encode_topic_row(&topic)?;
        self.store
            .insert_async(topic.topic_id.clone(), row)
            .await
            .into_system_error("insert_async topic error")
    }

    /// Delete a topic by ID.
    pub async fn delete_topic_async(&self, topic_id: &TopicId) -> Result<(), SystemError> {
        self.store
            .delete_async(topic_id.clone())
            .await
            .into_system_error("delete_async topic error")
    }

    /// List all topics
    pub fn list_topics(&self) -> Result<Vec<Topic>, SystemError> {
        let rows = self.store.scan_all_typed(None, None, None)?;
        rows.into_iter().map(|(_, row)| Self::decode_topic_row(&row)).collect()
    }

    fn build_batch_from_pairs(
        &self,
        pairs: Vec<(TopicId, SystemTableRow)>,
    ) -> Result<RecordBatch, SystemError> {
        let rows = pairs.into_iter().map(|(_, row)| row).collect();
        system_rows_to_batch(&Self::schema(), rows)
    }

    fn encode_topic_row(topic: &Topic) -> Result<SystemTableRow, SystemError> {
        model_to_system_row(topic, &Topic::definition())
    }

    fn decode_topic_row(row: &SystemTableRow) -> Result<Topic, SystemError> {
        system_row_to_model(row, &Topic::definition())
    }
}

crate::impl_system_table_provider_metadata!(
    indexed,
    provider = TopicsTableProvider,
    key = TopicId,
    table_name = SystemTable::Topics.table_name(),
    primary_key_column = "topic_id",
    parse_key = |value| Some(TopicId::new(value)),
    schema = Topic::definition().to_arrow_schema().expect("failed to build topics schema")
);

crate::impl_indexed_system_table_provider!(
    provider = TopicsTableProvider,
    key = TopicId,
    value = SystemTableRow,
    store = store,
    definition = provider_definition,
    build_batch = build_batch_from_pairs
);

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_store::test_utils::InMemoryBackend;

    #[test]
    fn test_topics_provider_creation() {
        let backend = Arc::new(InMemoryBackend::new());
        let _provider = TopicsTableProvider::new(backend);
        // Provider created successfully
    }

    #[test]
    fn test_create_and_get_topic() {
        let backend = Arc::new(InMemoryBackend::new());
        let provider = TopicsTableProvider::new(backend);

        let topic = Topic::new(TopicId::new("topic_123"), "app.notifications".to_string());

        // Create topic
        let result = provider.create_topic(topic.clone());
        assert!(result.is_ok());

        // Get topic
        let retrieved = provider.get_topic_by_id(&topic.topic_id).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "app.notifications");
    }

    #[test]
    fn test_update_topic() {
        let backend = Arc::new(InMemoryBackend::new());
        let provider = TopicsTableProvider::new(backend);

        let mut topic = Topic::new(TopicId::new("topic_456"), "test.topic".to_string());
        provider.create_topic(topic.clone()).unwrap();

        // Update topic
        topic.partitions = 4;
        let result = provider.update_topic(topic.clone());
        assert!(result.is_ok());

        // Verify update
        let retrieved = provider.get_topic_by_id(&topic.topic_id).unwrap().unwrap();
        assert_eq!(retrieved.partitions, 4);
    }
}
