//! System.topic_offsets table provider
//!
//! This module provides a DataFusion TableProvider implementation for the system.topic_offsets table.
//! Uses `IndexedEntityStore` with composite primary key (topic_id, group_id, partition_id).

use crate::error::{SystemError, SystemResultExt};
use crate::providers::base::{
    extract_filter_value, system_rows_to_batch, SimpleProviderDefinition,
};
use crate::system_row_mapper::{model_to_system_row, system_row_to_model};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Expr;
use kalamdb_commons::models::rows::SystemTableRow;
use kalamdb_commons::models::{ConsumerGroupId, TopicId};
use kalamdb_commons::SystemTable;
use kalamdb_store::entity_store::EntityStore;
use kalamdb_store::{IndexedEntityStore, StorageBackend};
use std::sync::{Arc, OnceLock};

use super::models::TopicOffset;

/// Composite key for topic offsets as a String: "topic_id:group_id:partition_id"
pub type TopicOffsetKey = String;

/// Type alias for the indexed topic offsets store
pub type TopicOffsetsStore = IndexedEntityStore<TopicOffsetKey, SystemTableRow>;

/// System.topic_offsets table provider using IndexedEntityStore.
///
/// Uses a composite primary key (topic_id, group_id, partition_id) for
/// efficient offset tracking per consumer group and partition.
#[derive(Clone)]
pub struct TopicOffsetsTableProvider {
    store: TopicOffsetsStore,
}

impl TopicOffsetsTableProvider {
    /// Create a new topic offsets table provider.
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new TopicOffsetsTableProvider instance
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            SystemTable::TopicOffsets
                .column_family_name()
                .expect("TopicOffsets is a table, not a view"),
            Vec::new(), // No secondary indexes for MVP
        );
        Self { store }
    }

    /// Create a composite key from components
    fn make_key(
        topic_id: &TopicId,
        group_id: &ConsumerGroupId,
        partition_id: u32,
    ) -> TopicOffsetKey {
        format!("{}:{}:{}", topic_id.as_str(), group_id.as_str(), partition_id)
    }

    /// Create or update a topic offset entry.
    ///
    /// # Arguments
    /// * `offset` - TopicOffset entity to upsert
    pub fn upsert_offset(&self, offset: TopicOffset) -> Result<(), SystemError> {
        let key = Self::make_key(&offset.topic_id, &offset.group_id, offset.partition_id);
        let row = Self::encode_offset_row(&offset)?;
        self.store.insert(&key, &row).into_system_error("insert topic offset error")
    }

    /// Get a topic offset by composite key
    pub fn get_offset(
        &self,
        topic_id: &TopicId,
        group_id: &ConsumerGroupId,
        partition_id: u32,
    ) -> Result<Option<TopicOffset>, SystemError> {
        let key = Self::make_key(topic_id, group_id, partition_id);
        let row = self.store.get(&key)?;
        row.map(|value| Self::decode_offset_row(&value)).transpose()
    }

    /// Get all offsets for a topic and consumer group across all partitions.
    ///
    /// Uses a prefix scan on `topic_id:group_id:` for O(partitions) lookup
    /// instead of scanning the entire offsets table.
    pub fn get_group_offsets(
        &self,
        topic_id: &TopicId,
        group_id: &ConsumerGroupId,
    ) -> Result<Vec<TopicOffset>, SystemError> {
        let prefix = format!("{}:{}:", topic_id.as_str(), group_id.as_str());
        let entries = self
            .store
            .scan_with_raw_prefix(prefix.as_bytes(), None, 10_000)
            .into_system_error("prefix scan topic offsets")?;
        entries.into_iter().map(|(_, row)| Self::decode_offset_row(&row)).collect()
    }

    /// Get all offsets for a topic across all consumer groups.
    ///
    /// Uses a prefix scan on `topic_id:` for O(groups×partitions) lookup
    /// instead of scanning the entire offsets table.
    pub fn get_topic_offsets(&self, topic_id: &TopicId) -> Result<Vec<TopicOffset>, SystemError> {
        let prefix = format!("{}:", topic_id.as_str());
        let entries = self
            .store
            .scan_with_raw_prefix(prefix.as_bytes(), None, 10_000)
            .into_system_error("prefix scan topic offsets")?;
        entries.into_iter().map(|(_, row)| Self::decode_offset_row(&row)).collect()
    }

    /// Acknowledge consumption through a specific offset
    ///
    /// Updates last_acked_offset for the given topic, group, and partition.
    pub fn ack_offset(
        &self,
        topic_id: &TopicId,
        group_id: &ConsumerGroupId,
        partition_id: u32,
        offset: u64,
    ) -> Result<(), SystemError> {
        let key = Self::make_key(topic_id, group_id, partition_id);

        // Get existing offset or create new one
        let mut topic_offset = self
            .store
            .get(&key)?
            .map(|row| Self::decode_offset_row(&row))
            .transpose()?
            .unwrap_or_else(|| {
                TopicOffset::new(
                    topic_id.clone(),
                    group_id.clone(),
                    partition_id,
                    0,
                    chrono::Utc::now().timestamp_millis(),
                )
            });

        // Update offset
        topic_offset.ack(offset, chrono::Utc::now().timestamp_millis());

        // Save back
        let row = Self::encode_offset_row(&topic_offset)?;
        self.store.insert(&key, &row).into_system_error("update offset error")
    }

    /// Delete all offsets for a consumer group
    pub fn delete_group_offsets(
        &self,
        topic_id: &TopicId,
        group_id: &ConsumerGroupId,
    ) -> Result<usize, SystemError> {
        let offsets = self.get_group_offsets(topic_id, group_id)?;
        let count = offsets.len();

        for offset in offsets {
            let key = Self::make_key(&offset.topic_id, &offset.group_id, offset.partition_id);
            self.store.delete(&key).into_system_error("delete offset error")?;
        }

        Ok(count)
    }

    /// Delete all offsets for a topic (all consumer groups and partitions)
    pub fn delete_topic_offsets(&self, topic_id: &TopicId) -> Result<usize, SystemError> {
        let offsets = self.get_topic_offsets(topic_id)?;
        let count = offsets.len();

        for offset in offsets {
            let key = Self::make_key(&offset.topic_id, &offset.group_id, offset.partition_id);
            self.store.delete(&key).into_system_error("delete offset error")?;
        }

        Ok(count)
    }

    /// List all topic offsets
    pub fn list_offsets(&self) -> Result<Vec<TopicOffset>, SystemError> {
        let rows = self.store.scan_all_typed(None, None, None)?;
        rows.into_iter().map(|(_, row)| Self::decode_offset_row(&row)).collect()
    }

    /// Load all topic offsets as a single RecordBatch for DataFusion
    fn load_batch_internal(&self) -> Result<RecordBatch, SystemError> {
        let rows = self
            .store
            .scan_all_typed(None, None, None)?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        system_rows_to_batch(&Self::schema(), rows)
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        let topic_id_filter = extract_filter_value(filters, "topic_id");
        let group_id_filter = extract_filter_value(filters, "group_id");
        let partition_id_filter =
            extract_filter_value(filters, "partition_id").and_then(|v| v.parse::<u32>().ok());

        let iter = self.store.scan_iterator(None, None)?;
        let effective_limit = limit.unwrap_or(100_000);
        let mut rows = Vec::with_capacity(effective_limit.min(1000));

        for item in iter {
            let (_, row) = item?;
            let offset = Self::decode_offset_row(&row)?;
            if topic_id_filter
                .as_deref()
                .is_some_and(|topic_id| offset.topic_id.as_str() != topic_id)
            {
                continue;
            }
            if group_id_filter
                .as_deref()
                .is_some_and(|group_id| offset.group_id.as_str() != group_id)
            {
                continue;
            }
            if partition_id_filter.is_some_and(|partition_id| offset.partition_id != partition_id) {
                continue;
            }

            rows.push(row);
            if rows.len() >= effective_limit {
                break;
            }
        }

        system_rows_to_batch(&Self::schema(), rows)
    }

    fn encode_offset_row(offset: &TopicOffset) -> Result<SystemTableRow, SystemError> {
        model_to_system_row(offset, &TopicOffset::definition())
    }

    fn decode_offset_row(row: &SystemTableRow) -> Result<TopicOffset, SystemError> {
        system_row_to_model(row, &TopicOffset::definition())
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = TopicOffsetsTableProvider,
    table_name = SystemTable::TopicOffsets.table_name(),
    schema = TopicOffset::definition()
        .to_arrow_schema()
        .expect("failed to build topic_offsets schema")
);

crate::impl_simple_system_table_provider!(
    provider = TopicOffsetsTableProvider,
    key = TopicOffsetKey,
    value = SystemTableRow,
    definition = provider_definition,
    scan_all = load_batch_internal,
    scan_filtered = scan_to_batch_filtered
);

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_store::test_utils::InMemoryBackend;

    #[test]
    fn test_topic_offsets_provider_creation() {
        let backend = Arc::new(InMemoryBackend::new());
        let _provider = TopicOffsetsTableProvider::new(backend);
        // Provider created successfully
    }

    #[test]
    fn test_upsert_and_get_offset() {
        let backend = Arc::new(InMemoryBackend::new());
        let provider = TopicOffsetsTableProvider::new(backend);

        let offset = TopicOffset::new(
            TopicId::new("topic_123"),
            ConsumerGroupId::new("group_1"),
            0,
            0,                                     // last_acked_offset
            chrono::Utc::now().timestamp_millis(), // updated_at
        );

        // Upsert offset
        provider.upsert_offset(offset.clone()).unwrap();

        // Get offset
        let retrieved = provider
            .get_offset(&offset.topic_id, &offset.group_id, offset.partition_id)
            .unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().last_acked_offset, 0);
    }

    #[test]
    fn test_ack_offset() {
        let backend = Arc::new(InMemoryBackend::new());
        let provider = TopicOffsetsTableProvider::new(backend);

        let topic_id = TopicId::new("topic_456");
        let group_id = ConsumerGroupId::new("service_1");

        // Ack offset (creates entry if not exists)
        provider.ack_offset(&topic_id, &group_id, 0, 42).unwrap();

        // Verify
        let offset = provider.get_offset(&topic_id, &group_id, 0).unwrap().unwrap();
        assert_eq!(offset.last_acked_offset, 42);
        assert_eq!(offset.next_offset(), 43);
    }

    #[test]
    fn test_get_group_offsets() {
        let backend = Arc::new(InMemoryBackend::new());
        let provider = TopicOffsetsTableProvider::new(backend);

        let topic_id = TopicId::new("topic_789");
        let group_id = ConsumerGroupId::new("analytics");

        // Create offsets for multiple partitions
        provider.ack_offset(&topic_id, &group_id, 0, 10).unwrap();
        provider.ack_offset(&topic_id, &group_id, 1, 20).unwrap();
        provider.ack_offset(&topic_id, &group_id, 2, 30).unwrap();

        // Get all offsets for the group
        let offsets = provider.get_group_offsets(&topic_id, &group_id).unwrap();
        assert_eq!(offsets.len(), 3);
    }
}
