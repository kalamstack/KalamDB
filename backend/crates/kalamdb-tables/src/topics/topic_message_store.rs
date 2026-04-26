//! Topic message store implementation using EntityStore pattern
//!
//! This module provides EntityStore-based storage for pub/sub topic messages.
//! Messages are stored with composite keys for efficient range queries.
//!
//! **Storage Architecture**:
//! - TopicMessageId: Composite key (topic_id, partition_id, offset)
//! - TopicMessage: Message envelope with payload, key, timestamp
//! - Storage key format: composite encoding for efficient filtering
//! - Efficient range scans for message fetching

use std::sync::Arc;

use kalamdb_commons::{models::TopicId, storage::Partition};
use kalamdb_store::{EntityStore, StorageBackend};

use crate::topics::topic_message_models::{TopicMessage, TopicMessageId};

/// Store for topic messages (append-only message log)
///
/// Uses composite TopicMessageId keys for efficient range queries.
/// Messages are immutable once written.
#[derive(Clone)]
pub struct TopicMessageStore {
    backend: Arc<dyn StorageBackend>,
    partition: Partition,
}

impl TopicMessageStore {
    /// Create a new topic message store
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    /// * `partition` - Partition name (e.g., "topic_messages")
    pub fn new(backend: Arc<dyn StorageBackend>, partition: impl Into<Partition>) -> Self {
        Self {
            backend,
            partition: partition.into(),
        }
    }

    /// Publish a message to a topic partition, returning the assigned offset
    ///
    /// This method:
    /// 1. Gets the next offset from counter
    /// 2. Creates the message with assigned offset
    /// 3. Stores the message
    ///
    /// Note: Offset management should be coordinated externally
    pub fn publish(
        &self,
        topic_id: &TopicId,
        partition_id: u32,
        offset: u64,
        payload: Vec<u8>,
        key: Option<String>,
        timestamp_ms: i64,
    ) -> kalamdb_store::storage_trait::Result<()> {
        let payload_bytes = payload.len();
        let span = tracing::debug_span!(
            "topic.store_publish",
            topic_name = topic_id.as_str(),
            partition_id = partition_id,
            offset = offset,
            payload_bytes = payload_bytes,
            has_key = key.is_some()
        );
        let _span_guard = span.entered();
        let message = TopicMessage::new(
            topic_id.clone(),
            partition_id,
            offset,
            payload,
            key,
            timestamp_ms,
            Default::default(),
        );
        let msg_id = message.id();
        tracing::trace!("Persisting topic message");
        self.put(&msg_id, &message)
    }

    /// Fetch messages from a partition starting at `offset`, up to `limit` messages
    ///
    /// Returns messages in order: [offset, offset+1, offset+2, ...]
    pub fn fetch_messages(
        &self,
        topic_id: &TopicId,
        partition_id: u32,
        offset: u64,
        limit: usize,
    ) -> kalamdb_store::storage_trait::Result<Vec<TopicMessage>> {
        let prefix = TopicMessageId::prefix_for_partition(topic_id, partition_id);
        let start_key = TopicMessageId::start_key_for_partition(topic_id, partition_id, offset);
        let results = self.scan_with_raw_prefix(&prefix, Some(&start_key), limit)?;
        Ok(results.into_iter().map(|(_, msg)| msg).collect())
    }

    /// Get the latest offset for a topic partition.
    ///
    /// Returns `None` when the partition has no messages.
    pub fn latest_offset(
        &self,
        topic_id: &TopicId,
        partition_id: u32,
    ) -> kalamdb_store::storage_trait::Result<Option<u64>> {
        let messages = self.fetch_messages(topic_id, partition_id, 0, usize::MAX)?;
        Ok(messages.last().map(|message| message.offset))
    }

    /// Delete all messages for a specific topic
    ///
    /// This method scans all messages for the topic and deletes them.
    /// Returns the count of deleted messages.
    pub fn delete_topic_messages(
        &self,
        topic_id: &TopicId,
    ) -> kalamdb_store::storage_trait::Result<usize> {
        // Use topic_id as prefix to scan all partitions.
        // Delete using raw keys to avoid deserializing large/corrupt values.
        let prefix = kalamdb_commons::encode_prefix(&(topic_id.as_str(),));
        let partition = self.partition();
        let iter = self.backend().scan(&partition, Some(&prefix), None, None)?;

        let mut count: usize = 0;
        for (key_bytes, _) in iter {
            self.backend().delete(&partition, &key_bytes)?;
            count += 1;
        }

        Ok(count)
    }

    /// Delete all messages for a specific partition within a topic
    ///
    /// Returns the count of deleted messages.
    pub fn delete_partition_messages(
        &self,
        topic_id: &TopicId,
        partition_id: u32,
    ) -> kalamdb_store::storage_trait::Result<usize> {
        let prefix = TopicMessageId::prefix_for_partition(topic_id, partition_id);
        let partition = self.partition();
        let iter = self.backend().scan(&partition, Some(&prefix), None, None)?;

        let mut count: usize = 0;
        for (key_bytes, _) in iter {
            self.backend().delete(&partition, &key_bytes)?;
            count += 1;
        }

        Ok(count)
    }

    /// Batch-write pre-encoded message key-value pairs directly to storage.
    ///
    /// Unlike `batch_put()` on EntityStore, this skips per-entry serialization
    /// and partition cloning because callers have already encoded the keys and
    /// values. This is designed for the publish hot-path where messages are
    /// serialized *outside* the partition write lock to minimise lock hold time.
    pub fn batch_put_raw(
        &self,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> kalamdb_store::storage_trait::Result<()> {
        use kalamdb_commons::storage::Operation;

        let partition = self.partition();
        let operations: Vec<Operation> = entries
            .into_iter()
            .map(|(key, value)| Operation::Put {
                partition: partition.clone(),
                key,
                value,
            })
            .collect();

        self.backend().batch(operations)
    }
}

/// Implement EntityStore trait for typed CRUD operations
impl EntityStore<TopicMessageId, TopicMessage> for TopicMessageStore {
    fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    fn partition(&self) -> Partition {
        self.partition.clone()
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::StorageKey;

    use super::*;
    use crate::utils::test_backend::RecordingBackend;

    fn setup_test_store() -> TopicMessageStore {
        let backend = Arc::new(RecordingBackend::new());
        let partition = Partition::new("test_topic_messages");
        TopicMessageStore::new(backend, partition)
    }

    #[test]
    fn test_publish_and_fetch() {
        let store = setup_test_store();
        let topic_id = TopicId::from("test_topic");
        let partition_id = 0;

        // Publish messages
        store
            .publish(&topic_id, partition_id, 0, b"message1".to_vec(), None, 1000)
            .unwrap();
        store
            .publish(
                &topic_id,
                partition_id,
                1,
                b"message2".to_vec(),
                Some("key1".to_string()),
                2000,
            )
            .unwrap();

        // Fetch messages
        let messages = store.fetch_messages(&topic_id, partition_id, 0, 10).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].payload, b"message1");
        assert_eq!(messages[1].payload, b"message2");
        assert_eq!(messages[1].key, Some("key1".to_string()));
    }

    #[test]
    fn test_get_message() {
        let store = setup_test_store();
        let topic_id = TopicId::from("test_topic");
        let partition_id = 0;

        store.publish(&topic_id, partition_id, 0, b"test".to_vec(), None, 1000).unwrap();

        let msg_id = TopicMessageId::new(topic_id.clone(), partition_id, 0);
        let message = store.get(&msg_id).unwrap();
        assert!(message.is_some());
        assert_eq!(message.unwrap().payload, b"test");

        // Non-existent message
        let missing_id = TopicMessageId::new(topic_id, partition_id, 999);
        let missing = store.get(&missing_id).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_multiple_partitions() {
        let store = setup_test_store();
        let topic_id = TopicId::from("test_topic");

        // Publish to partition 0
        store.publish(&topic_id, 0, 0, b"p0_msg1".to_vec(), None, 1000).unwrap();

        // Publish to partition 1
        store.publish(&topic_id, 1, 0, b"p1_msg1".to_vec(), None, 2000).unwrap();

        // Each partition has independent messages
        let messages0 = store.fetch_messages(&topic_id, 0, 0, 10).unwrap();
        let messages1 = store.fetch_messages(&topic_id, 1, 0, 10).unwrap();

        assert_eq!(messages0.len(), 1);
        assert_eq!(messages1.len(), 1);
        assert_eq!(messages0[0].payload, b"p0_msg1");
        assert_eq!(messages1[0].payload, b"p1_msg1");
    }

    #[test]
    fn test_fetch_with_limit() {
        let store = setup_test_store();
        let topic_id = TopicId::from("test_topic");
        let partition_id = 0;

        // Publish 5 messages
        for i in 0..5 {
            store
                .publish(
                    &topic_id,
                    partition_id,
                    i,
                    format!("message{}", i).into_bytes(),
                    None,
                    (1000 + i * 100) as i64,
                )
                .unwrap();
        }

        // Fetch with limit
        let messages = store.fetch_messages(&topic_id, partition_id, 0, 3).unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].payload, b"message0");
        assert_eq!(messages[2].payload, b"message2");

        // Fetch from middle
        let messages = store.fetch_messages(&topic_id, partition_id, 2, 10).unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].payload, b"message2");
        assert_eq!(messages[2].payload, b"message4");
    }

    #[test]
    fn test_storage_key_roundtrip() {
        let msg_id = TopicMessageId::new(TopicId::from("my_topic"), 3, 42);
        let key = msg_id.storage_key();
        let decoded = TopicMessageId::from_storage_key(&key).unwrap();
        assert_eq!(decoded, msg_id);
    }

    #[test]
    fn test_fetch_messages_uses_prefix_scan() {
        let backend = Arc::new(RecordingBackend::new());
        let partition = Partition::new("test_topic_messages_scan");
        let store = TopicMessageStore::new(backend.clone(), partition);

        let topic_id = TopicId::from("scan_topic");
        for i in 0..5 {
            store.publish(&topic_id, 0, i, vec![i as u8], None, 1000 + i as i64).unwrap();
        }

        let _ = store.fetch_messages(&topic_id, 0, 2, 3).unwrap();

        assert_eq!(backend.scan_calls(), 1);
        let last = backend.last_scan().expect("scan call missing");
        assert_eq!(last.prefix, Some(TopicMessageId::prefix_for_partition(&topic_id, 0)));
        assert_eq!(last.start_key, Some(TopicMessageId::start_key_for_partition(&topic_id, 0, 2)));
        assert_eq!(last.limit, Some(3));
    }
}
