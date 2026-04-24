//! Topic Publisher Service — unified service for all topic operations.
//!
//! Responsibilities:
//! - Maintain in-memory registry of topics and their routes
//! - Route table mutations to matching topics
//! - Publish messages to topic message store
//! - Track consumer group offsets
//! - Provide fast TableId → Topics lookup

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use kalamdb_commons::{
    errors::{CommonError, Result},
    models::{rows::Row, ConsumerGroupId, TableId, TopicId, TopicOp, UserId},
    storage::Partition,
};
use kalamdb_store::{EntityStore, StorageBackend};
use kalamdb_system::providers::{
    topic_offsets::{TopicOffset, TopicOffsetsTableProvider},
    topics::Topic,
};
use kalamdb_tables::{TopicMessage, TopicMessageStore};

use crate::models::TopicCacheStats;
use crate::offset::OffsetAllocator;
use crate::payload;
use crate::routing::RouteCache;

/// Lookup primary-key columns for a table so topic keys can be derived from
/// stable row identity instead of the full row payload.
pub trait TopicPrimaryKeyLookup: Send + Sync {
    fn primary_key_columns(&self, table_id: &TableId) -> Result<Vec<String>>;
}

/// Default visibility timeout for pending claims.
///
/// If a consumer fetches messages but does not ack within this window, the
/// claimed range is released so another consumer can re-deliver it.
const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(60);

/// Tracks per-(topic, group, partition) claim state for consumer groups.
///
/// The cursor prevents multiple consumers from receiving the same offset range.
/// Pending claims provide crash resilience: if a consumer dies without acking,
/// the lease expires and the cursor resets so another consumer re-delivers.
#[derive(Debug)]
struct ClaimState {
    /// Next offset to hand out.
    cursor: u64,
    /// Pending (unacked) claims with their expiry information.
    pending: Vec<PendingClaim>,
}

#[derive(Debug)]
struct PendingClaim {
    start: u64,
    /// Exclusive upper bound of the claimed range.
    end_exclusive: u64,
    /// When the claim was issued.
    claimed_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TopicPartitionKey {
    topic_id: TopicId,
    partition_id: u32,
}

impl TopicPartitionKey {
    #[inline]
    fn new(topic_id: &TopicId, partition_id: u32) -> Self {
        Self {
            topic_id: topic_id.clone(),
            partition_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupPartitionKey {
    topic_id: TopicId,
    group_id: ConsumerGroupId,
    partition_id: u32,
}

impl GroupPartitionKey {
    #[inline]
    fn new(topic_id: &TopicId, group_id: &ConsumerGroupId, partition_id: u32) -> Self {
        Self {
            topic_id: topic_id.clone(),
            group_id: group_id.clone(),
            partition_id,
        }
    }
}

impl ClaimState {
    fn new(cursor: u64) -> Self {
        Self {
            cursor,
            pending: Vec::new(),
        }
    }

    /// Expire stale pending claims and reset cursor to the earliest expired start.
    ///
    /// This ensures messages claimed by a consumer that crashed (or is too slow)
    /// are eventually re-delivered by another consumer.
    fn expire_stale_claims(&mut self, now: Instant, timeout: Duration) {
        let mut earliest_expired: Option<u64> = None;
        self.pending.retain(|claim| {
            if now.duration_since(claim.claimed_at) > timeout {
                earliest_expired =
                    Some(earliest_expired.map_or(claim.start, |e: u64| e.min(claim.start)));
                false // remove expired
            } else {
                true
            }
        });

        if let Some(reset_to) = earliest_expired {
            if reset_to < self.cursor {
                log::warn!(
                    "Resetting group cursor from {} to {} due to expired claims",
                    self.cursor,
                    reset_to
                );
                self.cursor = reset_to;
            }
        }
    }

    /// Remove pending claims fully covered by the acknowledged offset.
    fn ack_up_to(&mut self, acked_offset_inclusive: u64) {
        self.pending.retain(|claim| claim.end_exclusive > acked_offset_inclusive + 1);
        let next = acked_offset_inclusive + 1;
        if self.cursor < next {
            self.cursor = next;
        }
    }
}

/// Topic Publisher Service — unified service for all topic operations.
///
/// Thread-safe. Wrap in `Arc` for shared ownership.
pub struct TopicPublisherService {
    /// Persistent storage for topic messages.
    message_store: Arc<TopicMessageStore>,
    /// System table provider for consumer group offsets.
    offset_store: Arc<TopicOffsetsTableProvider>,
    /// In-memory route cache: TableId → routes.
    route_cache: RouteCache,
    /// Schema-backed lookup for deriving stable topic keys from table primary keys.
    primary_key_lookup: Option<Arc<dyn TopicPrimaryKeyLookup>>,
    /// Atomic per-topic-partition offset counters.
    offset_allocator: OffsetAllocator,
    /// In-memory per-(topic, group, partition) claim state used to avoid
    /// duplicate delivery and to expire stale claims from crashed consumers.
    group_claim_state: DashMap<GroupPartitionKey, ClaimState>,
    /// Per-(topic, partition) write locks that serialize offset allocation +
    /// RocksDB write to guarantee messages are stored in offset order.
    partition_write_locks: DashMap<TopicPartitionKey, Arc<Mutex<()>>>,
    /// How long a consumer claim stays valid before re-delivery.
    visibility_timeout: Duration,
}

impl TopicPublisherService {
    /// Create a new TopicPublisherService with stores backed by the given storage.
    pub fn new(storage_backend: Arc<dyn StorageBackend>) -> Self {
        Self::with_visibility_timeout_and_primary_key_lookup(
            storage_backend,
            DEFAULT_VISIBILITY_TIMEOUT,
            None,
        )
    }

    /// Create a new TopicPublisherService with a custom visibility timeout.
    pub fn with_visibility_timeout(
        storage_backend: Arc<dyn StorageBackend>,
        visibility_timeout: Duration,
    ) -> Self {
        Self::with_visibility_timeout_and_primary_key_lookup(
            storage_backend,
            visibility_timeout,
            None,
        )
    }

    /// Create a new TopicPublisherService with a custom visibility timeout and
    /// an optional primary-key lookup for deriving stable topic keys.
    pub fn with_visibility_timeout_and_primary_key_lookup(
        storage_backend: Arc<dyn StorageBackend>,
        visibility_timeout: Duration,
        primary_key_lookup: Option<Arc<dyn TopicPrimaryKeyLookup>>,
    ) -> Self {
        // Ensure the topic message partition exists.
        // Consumer offsets live in system.topic_offsets (system_topic_offsets CF),
        // so creating a separate topic_offsets CF here only adds permanent idle overhead.
        let messages_partition = Partition::new("topic_messages");
        let _ = storage_backend.create_partition(&messages_partition);

        let message_store =
            Arc::new(TopicMessageStore::new(storage_backend.clone(), messages_partition));
        let offset_store = Arc::new(TopicOffsetsTableProvider::new(storage_backend));

        Self {
            message_store,
            offset_store,
            route_cache: RouteCache::new(),
            primary_key_lookup,
            offset_allocator: OffsetAllocator::new(),
            group_claim_state: DashMap::new(),
            partition_write_locks: DashMap::new(),
            visibility_timeout,
        }
    }

    fn primary_key_columns_for(&self, table_id: &TableId) -> Result<Vec<String>> {
        match &self.primary_key_lookup {
            Some(lookup) => lookup.primary_key_columns(table_id),
            None => Ok(Vec::new()),
        }
    }

    // ===== Registry Methods =====

    /// Check if any topics are configured for a given table.
    #[inline]
    pub fn has_topics_for_table(&self, table_id: &TableId) -> bool {
        self.route_cache.has_topics_for_table(table_id)
    }

    /// Check if any topics are configured for a table with a specific operation.
    #[inline]
    pub fn has_topics_for_table_op(&self, table_id: &TableId, operation: &TopicOp) -> bool {
        self.route_cache.has_topics_for_table_op(table_id, operation)
    }

    /// Check if a topic exists.
    pub fn topic_exists(&self, topic_id: &TopicId) -> bool {
        self.route_cache.topic_exists(topic_id)
    }

    /// Get a topic by ID.
    pub fn get_topic(&self, topic_id: &TopicId) -> Option<Topic> {
        self.route_cache.get_topic(topic_id)
    }

    /// Get all topic IDs for a table.
    pub fn get_topic_ids_for_table(&self, table_id: &TableId) -> Vec<TopicId> {
        self.route_cache.get_topic_ids_for_table(table_id)
    }

    /// Refresh the topics cache from a list of topics.
    pub fn refresh_topics_cache(&self, topics: Vec<Topic>) {
        self.route_cache.refresh(topics);
    }

    /// Add a single topic to the cache.
    pub fn add_topic(&self, topic: Topic) {
        self.route_cache.add_topic(topic);
    }

    /// Remove a topic from the cache.
    pub fn remove_topic(&self, topic_id: &TopicId) {
        self.route_cache.remove_topic(topic_id);
    }

    /// Update a topic in the cache (removes old routes, adds new ones).
    pub fn update_topic(&self, topic: Topic) {
        self.route_cache.update_topic(topic);
    }

    /// Clear all caches.
    pub fn clear_cache(&self) {
        self.route_cache.clear();
        self.offset_allocator.clear();
        self.group_claim_state.clear();
        self.partition_write_locks.clear();
    }

    // ===== Publishing Methods =====

    /// Publish a single row change to matching topics.
    ///
    /// Message-centric design: one Row = one message.
    /// Called synchronously from table providers.
    ///
    /// # Returns
    /// Number of messages published across all matching topics.
    pub fn publish_message(
        &self,
        table_id: &TableId,
        operation: TopicOp,
        row: &Row,
        user_id: Option<&UserId>,
    ) -> Result<usize> {
        let span = tracing::debug_span!(
            "topic.publish",
            table_id = %table_id,
            operation = ?operation,
            has_user_id = user_id.is_some(),
            row_value_count = row.values.len(),
            published_count = tracing::field::Empty
        );
        let _span_guard = span.entered();

        // Fast path: get matching routes for this table + operation.
        let matching = self.route_cache.get_matching_routes(table_id, &operation);
        if matching.is_empty() {
            return Ok(0);
        }
        let primary_key_columns = self.primary_key_columns_for(table_id)?;

        let mut total_published = 0;

        for entry in matching {
            let topic_span = tracing::debug_span!(
                "publish_to_topic",
                topic_name = entry.topic_id.as_str(),
                topic_partitions = entry.topic_partitions,
                operation = ?entry.route.op
            );
            let _topic_span_guard = topic_span.entered();

            // Extract payload based on route's payload mode.
            let payload_bytes = payload::extract_payload(&entry.route, row, table_id)?;

            // Extract message key (optional).
            let key = payload::extract_key(row, &primary_key_columns)?;

            // Select partition.
            let partition_id = if let Some(ref k) = key {
                (payload::hash_key(k) % entry.topic_partitions as u64) as u32
            } else {
                (payload::hash_row(row) % entry.topic_partitions as u64) as u32
            };

            // Allocate offset and write message under a per-partition lock.
            // This ensures messages are stored in offset order even with
            // concurrent publishers, so consumers never skip gaps.
            let partition_lock_key = TopicPartitionKey::new(&entry.topic_id, partition_id);
            let lock = self
                .partition_write_locks
                .entry(partition_lock_key)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone();
            // Drop the DashMap ref before acquiring the mutex to avoid
            // holding two locks simultaneously.
            let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());

            let offset = self.offset_allocator.next_offset(&entry.topic_id, partition_id);

            // Create and persist message.
            let timestamp_ms = chrono::Utc::now().timestamp_millis();
            let message = TopicMessage::new_with_user(
                entry.topic_id.clone(),
                partition_id,
                offset,
                payload_bytes,
                key,
                timestamp_ms,
                user_id.cloned(),
                operation.clone(),
            );

            self.message_store.put(&message.id(), &message).map_err(|e| {
                CommonError::Internal(format!("Failed to store topic message: {}", e))
            })?;

            tracing::debug!(
                topic_name = entry.topic_id.as_str(),
                partition_id = partition_id,
                offset = offset,
                payload_bytes = message.payload.len(),
                "Published message to topic"
            );

            total_published += 1;
        }

        tracing::Span::current().record("published_count", total_published);
        Ok(total_published)
    }

    /// Publish a batch of row changes to matching topics.
    ///
    /// This is significantly faster than calling `publish_message()` in a loop
    /// because it:
    /// 1. Acquires the partition write lock once per partition (not per message)
    /// 2. Allocates a contiguous offset range atomically
    /// 3. Writes all messages in a single RocksDB WriteBatch
    /// 4. Serializes each row's JSON only once via `PreparedRow`
    /// 5. Pre-encodes Full/Diff payloads with `_table` injected at construction
    /// 6. Pre-computes partition hash to avoid redundant hashing
    ///
    /// # Returns
    /// Number of messages published across all matching topics.
    pub fn publish_batch(
        &self,
        table_id: &TableId,
        operation: TopicOp,
        rows: &[Row],
        user_id: Option<&UserId>,
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let span = tracing::debug_span!(
            "topic.publish_batch",
            table_id = %table_id,
            operation = ?operation,
            row_count = rows.len(),
            published_count = tracing::field::Empty
        );
        let _span_guard = span.entered();

        // Fast path: get matching routes for this table + operation.
        let matching = self.route_cache.get_matching_routes(table_id, &operation);
        if matching.is_empty() {
            return Ok(0);
        }
        let primary_key_columns = self.primary_key_columns_for(table_id)?;

        // Check if any route uses Full/Diff mode (needs _table injection).
        let needs_full_payload = matching.iter().any(|e| {
            matches!(
                e.route.payload_mode,
                kalamdb_commons::models::PayloadMode::Full
                    | kalamdb_commons::models::PayloadMode::Diff
            )
        });

        // Pre-compute row JSON once per row. If any route needs Full/Diff, inject
        // _table at construction time to avoid per-message HashMap clone.
        let prepared: Vec<payload::PreparedRow> = if needs_full_payload {
            rows.iter()
                .map(|row| payload::PreparedRow::from_row_with_table(row, table_id))
                .collect::<Result<Vec<_>>>()?
        } else {
            rows.iter()
                .map(|row| payload::PreparedRow::from_row(row))
                .collect::<Result<Vec<_>>>()?
        };

        let prepared_keys: Vec<Option<String>> = prepared
            .iter()
            .map(|prep| prep.extract_key(&primary_key_columns))
            .collect::<Result<Vec<_>>>()?;

        let mut total_published = 0;
        let timestamp_ms = chrono::Utc::now().timestamp_millis();

        for entry in &matching {
            // Group rows by partition using pre-computed hashes.
            let mut partition_groups: std::collections::HashMap<u32, Vec<usize>> =
                std::collections::HashMap::new();

            for (idx, prep) in prepared.iter().enumerate() {
                let partition_hash = match prepared_keys[idx].as_deref() {
                    Some(key) => payload::hash_key(key),
                    None => prep.hash_row(),
                };
                let partition_id = (partition_hash % entry.topic_partitions as u64) as u32;
                partition_groups.entry(partition_id).or_default().push(idx);
            }

            // Borrow topic_id once for the entire entry loop.
            // Write each partition group with a single lock + single WriteBatch.
            for (partition_id, row_indices) in &partition_groups {
                let count = row_indices.len() as u64;

                // Pre-extract payloads and keys OUTSIDE the lock to minimize
                // lock hold time. Serialization is the expensive part.
                let mut pre_encoded: Vec<(Vec<u8>, Option<String>)> =
                    Vec::with_capacity(row_indices.len());
                for &row_idx in row_indices {
                    let prep = &prepared[row_idx];
                    let payload_bytes = prep.extract_payload(&entry.route, table_id)?;
                    let key = prepared_keys[row_idx].clone();

                    // We'll fill in the actual offset inside the lock.
                    // For now, pre-encode everything except offset-dependent fields.
                    // Store (payload_bytes, key) temporarily.
                    pre_encoded.push((payload_bytes, key));
                }

                // Acquire partition lock only for offset allocation + RocksDB write.
                let partition_lock_key = TopicPartitionKey::new(&entry.topic_id, *partition_id);
                let lock = self
                    .partition_write_locks
                    .entry(partition_lock_key)
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone();
                let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());

                // Allocate contiguous offset range atomically.
                let start_offset =
                    self.offset_allocator.next_n_offsets(&entry.topic_id, *partition_id, count);

                // Now build messages with real offsets and serialize them.
                let mut raw_entries = Vec::with_capacity(pre_encoded.len());
                for (i, (payload_bytes, key)) in pre_encoded.into_iter().enumerate() {
                    let offset = start_offset + i as u64;

                    let message = TopicMessage::new_with_user(
                        entry.topic_id.clone(),
                        *partition_id,
                        offset,
                        payload_bytes,
                        key,
                        timestamp_ms,
                        user_id.cloned(),
                        operation.clone(),
                    );
                    let msg_id = message.id();

                    //TODO: Use the store to serialize the message directly to avoid redundant serialization in TopicMessage::new and TopicMessageStore::put. This would require refactoring TopicMessage to separate the in-memory model from the serialized form, or adding a method to get the pre-encoded bytes without going through the full struct construction.
                    let key_encoded = kalamdb_commons::StorageKey::storage_key(&msg_id);
                    let value_encoded =
                        kalamdb_commons::KSerializable::encode(&message).map_err(|e| {
                            CommonError::Internal(format!(
                                "Failed to serialize topic message: {}",
                                e
                            ))
                        })?;
                    raw_entries.push((key_encoded, value_encoded));
                }

                self.message_store.batch_put_raw(raw_entries).map_err(|e| {
                    CommonError::Internal(format!("Failed to batch store topic messages: {}", e))
                })?;

                total_published += row_indices.len();
            }
        }

        tracing::Span::current().record("published_count", total_published);
        Ok(total_published)
    }

    // ===== Message Consumption Methods =====

    /// Fetch messages from a topic partition.
    pub fn fetch_messages(
        &self,
        topic_id: &TopicId,
        partition_id: u32,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<TopicMessage>> {
        self.message_store
            .fetch_messages(topic_id, partition_id, offset, limit)
            .map_err(|e| CommonError::Internal(format!("Failed to fetch messages: {}", e)))
    }

    /// Fetch messages for a consumer group while claiming offsets in-memory.
    ///
    /// Guarantees:
    /// - Concurrent consumers in the same group and partition never receive
    ///   overlapping offset ranges (serialized via DashMap entry lock).
    /// - If a consumer does not ack within [`VISIBILITY_TIMEOUT`], the
    ///   claimed range expires and is re-delivered to the next consumer.
    pub fn fetch_messages_for_group(
        &self,
        topic_id: &TopicId,
        group_id: &ConsumerGroupId,
        partition_id: u32,
        start_offset: u64,
        limit: usize,
    ) -> Result<Vec<TopicMessage>> {
        let cursor_key = GroupPartitionKey::new(topic_id, group_id, partition_id);
        let mut state = self
            .group_claim_state
            .entry(cursor_key)
            .or_insert_with(|| ClaimState::new(start_offset));

        // Expire stale claims so crashed consumers don't block delivery.
        state.expire_stale_claims(Instant::now(), self.visibility_timeout);

        let effective_start = state.cursor.max(start_offset);

        let messages = self
            .message_store
            .fetch_messages(topic_id, partition_id, effective_start, limit)
            .map_err(|e| CommonError::Internal(format!("Failed to fetch messages: {}", e)))?;

        if !messages.is_empty() {
            let end_exclusive = messages.last().unwrap().offset + 1;
            state.cursor = end_exclusive;
            state.pending.push(PendingClaim {
                start: effective_start,
                end_exclusive,
                claimed_at: Instant::now(),
            });
        }

        Ok(messages)
    }

    /// Get the latest offset for a topic partition.
    ///
    /// Returns `None` when the partition is empty.
    pub fn latest_offset(&self, topic_id: &TopicId, partition_id: u32) -> Result<Option<u64>> {
        let next_offset = self.offset_allocator.peek_next_offset(topic_id, partition_id);

        Ok(next_offset.and_then(|next| next.checked_sub(1)))
    }

    // ===== Offset Management Methods =====

    /// Acknowledge (commit) an offset for a consumer group.
    ///
    /// Persists the committed offset and clears any pending claims up to this
    /// offset so they are not re-delivered on expiry.
    pub fn ack_offset(
        &self,
        topic_id: &TopicId,
        group_id: &ConsumerGroupId,
        partition_id: u32,
        offset: u64,
    ) -> Result<()> {
        self.offset_store
            .ack_offset(topic_id, group_id, partition_id, offset)
            .map_err(|e| CommonError::Internal(format!("Failed to ack offset: {}", e)))?;

        let cursor_key = GroupPartitionKey::new(topic_id, group_id, partition_id);
        if let Some(mut state) = self.group_claim_state.get_mut(&cursor_key) {
            state.ack_up_to(offset);
        } else {
            // No claim state yet — seed one from the acked offset.
            self.group_claim_state.insert(cursor_key, ClaimState::new(offset + 1));
        }

        Ok(())
    }

    /// Get all committed offsets for a consumer group on a topic.
    pub fn get_group_offsets(
        &self,
        topic_id: &TopicId,
        group_id: &ConsumerGroupId,
    ) -> Result<Vec<TopicOffset>> {
        self.offset_store
            .get_group_offsets(topic_id, group_id)
            .map_err(|e| CommonError::Internal(format!("Failed to get offsets: {}", e)))
    }

    // ===== Accessors =====

    /// Get the underlying message store.
    pub fn message_store(&self) -> Arc<TopicMessageStore> {
        self.message_store.clone()
    }

    /// Get the underlying offset store.
    pub fn offset_store(&self) -> Arc<TopicOffsetsTableProvider> {
        self.offset_store.clone()
    }

    /// Get the configured visibility timeout for consumer claims.
    pub fn visibility_timeout(&self) -> Duration {
        self.visibility_timeout
    }

    /// Get statistics about the topic cache.
    pub fn cache_stats(&self) -> TopicCacheStats {
        TopicCacheStats {
            topic_count: self.route_cache.topic_count(),
            table_route_count: self.route_cache.table_route_count(),
            total_routes: self.route_cache.total_routes(),
        }
    }

    /// Restore in-memory offset counters by scanning persisted messages.
    ///
    /// After a server restart the counters are empty, which would cause new
    /// messages to start at offset 0 — potentially overwriting data. This
    /// method scans each cached topic/partition for the highest existing offset
    /// and seeds the counter to `max_offset + 1`.
    pub fn restore_offset_counters(&self) {
        for entry in self.route_cache.iter_topics() {
            let topic = entry.value();
            for partition_id in 0..topic.partitions {
                match self.message_store.fetch_messages(
                    &topic.topic_id,
                    partition_id,
                    0,
                    usize::MAX,
                ) {
                    Ok(msgs) => {
                        if let Some(last) = msgs.last() {
                            let next = last.offset + 1;
                            self.offset_allocator.seed(&topic.topic_id, partition_id, next);
                            log::info!(
                                "Restored offset counter for topic={} partition={}: next_offset={}",
                                topic.topic_id.as_str(),
                                partition_id,
                                next,
                            );
                        }
                    },
                    Err(e) => {
                        log::warn!(
                            "Failed to restore offset for topic={} partition={}: {}",
                            topic.topic_id.as_str(),
                            partition_id,
                            e,
                        );
                    },
                }
            }
        }
    }
}

// ===== TopicPublisher trait implementation =====

impl kalamdb_system::TopicPublisher for TopicPublisherService {
    fn has_topics_for_table(&self, table_id: &TableId) -> bool {
        self.route_cache.has_topics_for_table(table_id)
    }

    fn publish_for_table(
        &self,
        table_id: &TableId,
        operation: TopicOp,
        row: &Row,
        user_id: Option<&UserId>,
    ) -> std::result::Result<usize, String> {
        self.publish_message(table_id, operation, row, user_id)
            .map_err(|e| e.to_string())
    }

    fn publish_batch_for_table(
        &self,
        table_id: &TableId,
        operation: TopicOp,
        rows: &[Row],
        user_id: Option<&UserId>,
    ) -> std::result::Result<usize, String> {
        self.publish_batch(table_id, operation, rows, user_id)
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::models::{NamespaceId, PayloadMode, TableName};
    use kalamdb_store::test_utils::InMemoryBackend;
    use kalamdb_system::providers::topics::TopicRoute;

    struct FixedPrimaryKeyLookup {
        columns: Vec<String>,
    }

    impl TopicPrimaryKeyLookup for FixedPrimaryKeyLookup {
        fn primary_key_columns(&self, _table_id: &TableId) -> Result<Vec<String>> {
            Ok(self.columns.clone())
        }
    }

    fn create_test_row(id: i32, name: &str) -> Row {
        let mut values = std::collections::BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int32(Some(id)));
        values.insert("name".to_string(), ScalarValue::Utf8(Some(name.to_string())));
        Row { values }
    }

    fn create_test_topic(topic_id: TopicId, table_id: TableId, op: TopicOp) -> Topic {
        create_test_topic_with_partitions(topic_id, table_id, op, 2)
    }

    fn create_test_topic_with_partitions(
        topic_id: TopicId,
        table_id: TableId,
        op: TopicOp,
        partitions: u32,
    ) -> Topic {
        Topic {
            topic_id: topic_id.clone(),
            name: format!("topic_{}", topic_id.as_str()),
            alias: None,
            partitions,
            retention_seconds: None,
            retention_max_bytes: None,
            routes: vec![TopicRoute {
                table_id,
                op,
                payload_mode: PayloadMode::Full,
                filter_expr: None,
                partition_key_expr: None,
            }],
            created_at: 0,
            updated_at: 0,
        }
    }

    fn service_with_primary_key(columns: &[&str]) -> TopicPublisherService {
        let backend = Arc::new(InMemoryBackend::new());
        let lookup: Arc<dyn TopicPrimaryKeyLookup> = Arc::new(FixedPrimaryKeyLookup {
            columns: columns.iter().map(|column| (*column).to_string()).collect(),
        });
        TopicPublisherService::with_visibility_timeout_and_primary_key_lookup(
            backend,
            Duration::from_secs(60),
            Some(lookup),
        )
    }

    #[test]
    fn test_service_creation() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);
        assert_eq!(service.cache_stats().topic_count, 0);
        assert_eq!(service.cache_stats().table_route_count, 0);
    }

    #[test]
    fn test_has_topics_for_table() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("users"));
        let topic_id = TopicId::new("user_events");

        assert!(!service.has_topics_for_table(&table_id));

        let topic = create_test_topic(topic_id, table_id.clone(), TopicOp::Insert);
        service.add_topic(topic);

        assert!(service.has_topics_for_table(&table_id));
        assert!(service.has_topics_for_table_op(&table_id, &TopicOp::Insert));
        assert!(!service.has_topics_for_table_op(&table_id, &TopicOp::Delete));
    }

    #[test]
    fn test_add_and_remove_topic() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("users"));
        let topic_id = TopicId::new("user_events");

        let topic = create_test_topic(topic_id.clone(), table_id.clone(), TopicOp::Insert);
        service.add_topic(topic);

        assert!(service.topic_exists(&topic_id));
        assert_eq!(service.cache_stats().topic_count, 1);

        service.remove_topic(&topic_id);

        assert!(!service.topic_exists(&topic_id));
        assert!(!service.has_topics_for_table(&table_id));
        assert_eq!(service.cache_stats().topic_count, 0);
    }

    #[test]
    fn test_route_and_publish() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("users"));
        let topic_id = TopicId::new("user_events");

        let topic = create_test_topic(topic_id.clone(), table_id.clone(), TopicOp::Insert);
        service.add_topic(topic);

        let rows = vec![
            create_test_row(1, "Alice"),
            create_test_row(2, "Bob"),
            create_test_row(3, "Charlie"),
        ];

        let mut total_count = 0;
        for row in &rows {
            let count = service.publish_message(&table_id, TopicOp::Insert, row, None).unwrap();
            total_count += count;
        }

        assert_eq!(total_count, 3);

        let mut all_messages = Vec::new();
        for partition_id in 0..2 {
            let messages = service.fetch_messages(&topic_id, partition_id, 0, 10).unwrap();
            all_messages.extend(messages);
        }
        assert_eq!(all_messages.len(), 3);
    }

    #[test]
    fn test_publish_uses_primary_key_as_message_key() {
        let service = service_with_primary_key(&["id"]);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("users"));
        let topic_id = TopicId::new("pk_topic");

        let topic = create_test_topic(topic_id.clone(), table_id.clone(), TopicOp::Insert);
        service.add_topic(topic);

        let row = create_test_row(42, "Alice");
        let published = service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
        assert_eq!(published, 1);

        let mut messages = Vec::new();
        for partition_id in 0..2 {
            messages.extend(service.fetch_messages(&topic_id, partition_id, 0, 10).unwrap());
        }

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key.as_deref(), Some("42"));
    }

    #[test]
    fn test_batch_publish_same_primary_key_stays_in_one_partition() {
        let service = service_with_primary_key(&["id"]);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("users"));
        let topic_id = TopicId::new("pk_batch_topic");
        let partitions = 32;

        let topic =
            create_test_topic_with_partitions(topic_id.clone(), table_id.clone(), TopicOp::Insert, partitions);
        service.add_topic(topic);

        let first = create_test_row(7, "alpha");
        let second = (0..256)
            .map(|idx| create_test_row(7, &format!("variant_{}", idx)))
            .find(|candidate| {
                payload::hash_row(&first) % partitions as u64
                    != payload::hash_row(candidate) % partitions as u64
            })
            .expect("expected a same-PK row with a different full-row hash partition");

        let published = service
            .publish_batch(&table_id, TopicOp::Insert, &[first.clone(), second.clone()], None)
            .unwrap();
        assert_eq!(published, 2);

        let mut seen_partition_ids = HashSet::new();
        let mut matching_messages = Vec::new();
        for partition_id in 0..partitions {
            for message in service.fetch_messages(&topic_id, partition_id, 0, 10).unwrap() {
                matching_messages.push(message.clone());
                seen_partition_ids.insert(message.partition_id);
            }
        }

        assert_eq!(matching_messages.len(), 2);
        assert_eq!(seen_partition_ids.len(), 1, "same PK should hash to the same partition");
        assert!(matching_messages.iter().all(|message| message.key.as_deref() == Some("7")));
    }

    #[test]
    fn test_no_routes_returns_zero() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("no_routes"));

        let row = create_test_row(1, "Test");
        let count = service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();

        assert_eq!(count, 0);
    }

    #[test]
    fn test_offset_tracking() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let topic_id = TopicId::new("test_topic");
        let group_id = ConsumerGroupId::new("test_group");

        let offsets = service.get_group_offsets(&topic_id, &group_id).unwrap();
        assert!(offsets.is_empty());

        service.ack_offset(&topic_id, &group_id, 0, 42).unwrap();

        let offsets = service.get_group_offsets(&topic_id, &group_id).unwrap();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].last_acked_offset, 42);
    }

    #[test]
    fn test_fetch_messages_for_group_advances_claim_cursor() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("users"));
        let topic_id = TopicId::new("group_claim_topic");
        let group_id = ConsumerGroupId::new("test_group");

        let topic = create_test_topic(topic_id.clone(), table_id.clone(), TopicOp::Insert);
        service.add_topic(topic);

        for idx in 0..10 {
            let row = create_test_row(idx, &format!("user_{}", idx));
            service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
        }

        let first = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 4).unwrap();
        let second = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 4).unwrap();

        assert!(!first.is_empty());
        assert!(!second.is_empty());
        let first_last_offset = first.last().map(|message| message.offset).unwrap();
        let second_first_offset = second.first().map(|message| message.offset).unwrap();
        assert!(
            second_first_offset > first_last_offset,
            "second fetch should continue after first claimed range"
        );
    }

    #[test]
    fn test_out_of_order_ack_does_not_regress_offset() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let topic_id = TopicId::new("ack_order_topic");
        let group_id = ConsumerGroupId::new("ack_group");

        // Simulate: consumer B acks a higher offset first, then consumer A acks a lower one.
        service.ack_offset(&topic_id, &group_id, 0, 399).unwrap();
        service.ack_offset(&topic_id, &group_id, 0, 199).unwrap();

        let offsets = service.get_group_offsets(&topic_id, &group_id).unwrap();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].last_acked_offset, 399, "Committed offset must never regress");
    }

    #[test]
    fn test_concurrent_group_fetch_no_overlap() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("events"));
        let topic_id = TopicId::new("overlap_topic");
        let group_id = ConsumerGroupId::new("overlap_group");

        let topic = create_test_topic(topic_id.clone(), table_id.clone(), TopicOp::Insert);
        service.add_topic(topic);

        // Publish 100 messages
        for idx in 0..100 {
            let row = create_test_row(idx, &format!("event_{}", idx));
            service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
        }

        // Simulate two consumers fetching sequentially (serialized by lock)
        let mut all_offsets = Vec::new();
        for _ in 0..10 {
            let batch = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
            if batch.is_empty() {
                break;
            }
            for msg in &batch {
                all_offsets.push(msg.offset);
            }
        }

        // Verify: no duplicates, sorted, total count correct
        let unique: HashSet<u64> = all_offsets.iter().copied().collect();
        assert_eq!(
            unique.len(),
            all_offsets.len(),
            "Group fetch must never return duplicate offsets"
        );

        // Collect all messages across partitions for comparison
        let mut total_published = 0;
        for pid in 0..2 {
            let msgs = service.fetch_messages(&topic_id, pid, 0, 1000).unwrap();
            total_published += msgs.len();
        }

        // All messages from partition 0 should be consumed
        let p0_total = service.fetch_messages(&topic_id, 0, 0, 1000).unwrap().len();
        assert_eq!(all_offsets.len(), p0_total, "All partition-0 messages should be consumed");
        assert_eq!(total_published, 100);
    }

    #[test]
    fn test_ack_clears_pending_claims() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("events"));
        let topic_id = TopicId::new("ack_clear_topic");
        let group_id = ConsumerGroupId::new("ack_clear_group");

        let topic = create_test_topic(topic_id.clone(), table_id.clone(), TopicOp::Insert);
        service.add_topic(topic);

        for idx in 0..20 {
            let row = create_test_row(idx, &format!("e_{}", idx));
            service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
        }

        // Fetch a batch (creates a pending claim)
        let batch1 = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 5).unwrap();
        assert!(!batch1.is_empty());
        let last_offset = batch1.last().unwrap().offset;

        // Verify pending claim exists
        let cursor_key = GroupPartitionKey::new(&topic_id, &group_id, 0);
        {
            let state = service.group_claim_state.get(&cursor_key).unwrap();
            assert_eq!(state.pending.len(), 1, "Should have one pending claim before ack");
        }

        // Ack clears the pending claim
        service.ack_offset(&topic_id, &group_id, 0, last_offset).unwrap();
        {
            let state = service.group_claim_state.get(&cursor_key).unwrap();
            assert_eq!(state.pending.len(), 0, "Pending claim should be removed after ack");
        }
    }

    #[test]
    fn test_empty_partition_returns_empty() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let topic_id = TopicId::new("empty_topic");
        let group_id = ConsumerGroupId::new("empty_group");

        let result = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
        assert!(result.is_empty(), "Empty partition should return empty vec");

        // Cursor should stay at 0 (not advance past non-existent messages)
        let result2 = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
        assert!(result2.is_empty());
    }

    #[test]
    fn test_group_fetch_then_ack_then_fetch_continues() {
        let backend = Arc::new(InMemoryBackend::new());
        let service = TopicPublisherService::new(backend);

        let ns = NamespaceId::new("test_ns");
        let table_id = TableId::new(ns.clone(), TableName::from("events"));
        let topic_id = TopicId::new("resume_topic");
        let group_id = ConsumerGroupId::new("resume_group");

        let topic = create_test_topic(topic_id.clone(), table_id.clone(), TopicOp::Insert);
        service.add_topic(topic);

        for idx in 0..30 {
            let row = create_test_row(idx, &format!("msg_{}", idx));
            service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
        }

        // Fetch first batch
        let batch1 = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
        assert!(!batch1.is_empty());
        let last1 = batch1.last().unwrap().offset;

        // Ack first batch
        service.ack_offset(&topic_id, &group_id, 0, last1).unwrap();

        // Fetch second batch — should continue from after first
        let batch2 = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
        if !batch2.is_empty() {
            assert!(batch2[0].offset > last1, "Second batch should start after first acked offset");
        }
    }
}
