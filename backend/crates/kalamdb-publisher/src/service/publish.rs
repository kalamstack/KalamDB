use super::*;

impl TopicPublisherService {
    pub(super) fn add_retained_bytes(&self, topic_id: &TopicId, partition_id: u32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let key = TopicPartitionKey::new(topic_id, partition_id);
        self.retained_bytes
            .entry(key)
            .and_modify(|current| *current = current.saturating_add(bytes))
            .or_insert(bytes);
    }

    pub(super) fn subtract_retained_bytes(
        &self,
        topic_id: &TopicId,
        partition_id: u32,
        bytes: u64,
    ) {
        if bytes == 0 {
            return;
        }
        let key = TopicPartitionKey::new(topic_id, partition_id);
        self.retained_bytes
            .entry(key)
            .and_modify(|current| *current = current.saturating_sub(bytes))
            .or_insert(0);
    }

    pub(super) fn set_retained_bytes(&self, topic_id: &TopicId, partition_id: u32, bytes: u64) {
        self.retained_bytes
            .insert(TopicPartitionKey::new(topic_id, partition_id), bytes);
    }

    pub(super) fn retained_bytes_for_partition(
        &self,
        topic_id: &TopicId,
        partition_id: u32,
    ) -> Result<u64> {
        let key = TopicPartitionKey::new(topic_id, partition_id);
        if let Some(bytes) = self.retained_bytes.get(&key) {
            return Ok(*bytes);
        }

        let bytes = self
            .message_store
            .retained_bytes_for_partition(topic_id, partition_id)
            .map_err(|e| CommonError::Internal(format!("Failed to read retained bytes: {}", e)))?;
        self.retained_bytes.insert(key, bytes);
        Ok(bytes)
    }

    pub(super) fn register_consumer_group(&self, topic_id: &TopicId, group_id: &ConsumerGroupId) {
        self.consumer_groups.insert(ConsumerGroupKey::new(topic_id, group_id), ());
    }

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

        let matching = self.route_cache.get_matching_routes(table_id, &operation);
        if matching.is_empty() {
            return Ok(0);
        }
        let primary_key_columns = self.primary_key_columns_for(table_id)?;

        let mut total_published = 0;

        for entry in matching {
            if !Self::route_matches_row(&entry, row) {
                continue;
            }

            let topic_span = tracing::debug_span!(
                "publish_to_topic",
                topic_name = entry.topic_id.as_str(),
                topic_partitions = entry.topic_partitions,
                operation = ?entry.route.op
            );
            let _topic_span_guard = topic_span.entered();

            let payload_bytes = payload::extract_payload(&entry.route, row, table_id)?;
            let key = payload::extract_key(row, &primary_key_columns)?;

            let partition_id = if let Some(ref key) = key {
                (payload::hash_key(key) % entry.topic_partitions as u64) as u32
            } else {
                (payload::hash_row(row) % entry.topic_partitions as u64) as u32
            };

            let lock = self.partition_write_lock(&entry.topic_id, partition_id);
            let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());

            let offset = self.offset_allocator.next_offset(&entry.topic_id, partition_id);

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

            let message_bytes =
                self.message_store.put_message_with_retention_index(&message).map_err(|e| {
                    CommonError::Internal(format!("Failed to store topic message: {}", e))
                })?;
            self.add_retained_bytes(&entry.topic_id, partition_id, message_bytes);
            record_pubsub_messages_published(1, message_bytes);

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

        let matching = self.route_cache.get_matching_routes(table_id, &operation);
        if matching.is_empty() {
            return Ok(0);
        }
        let primary_key_columns = self.primary_key_columns_for(table_id)?;

        let needs_full_payload = matching.iter().any(|entry| {
            matches!(
                entry.route.payload_mode,
                kalamdb_commons::models::PayloadMode::Full
                    | kalamdb_commons::models::PayloadMode::Diff
            )
        });

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
            let mut partition_groups: std::collections::HashMap<u32, Vec<usize>> =
                std::collections::HashMap::new();

            for (idx, prep) in prepared.iter().enumerate() {
                if !Self::route_matches_row(entry, &rows[idx]) {
                    continue;
                }

                let partition_hash = match prepared_keys[idx].as_deref() {
                    Some(key) => payload::hash_key(key),
                    None => prep.hash_row(),
                };
                let partition_id = (partition_hash % entry.topic_partitions as u64) as u32;
                partition_groups.entry(partition_id).or_default().push(idx);
            }

            if partition_groups.is_empty() {
                continue;
            }

            for (partition_id, row_indices) in &partition_groups {
                let count = row_indices.len() as u64;

                let mut pre_encoded: Vec<(Vec<u8>, Option<String>)> =
                    Vec::with_capacity(row_indices.len());
                for &row_idx in row_indices {
                    let prep = &prepared[row_idx];
                    let payload_bytes = prep.extract_payload(&entry.route, table_id)?;
                    let key = prepared_keys[row_idx].clone();
                    pre_encoded.push((payload_bytes, key));
                }

                let lock = self.partition_write_lock(&entry.topic_id, *partition_id);
                let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());

                let start_offset =
                    self.offset_allocator.next_n_offsets(&entry.topic_id, *partition_id, count);

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

                    let key_encoded = kalamdb_commons::StorageKey::storage_key(&msg_id);
                    let value_encoded = kalamdb_store::encode_entity(&message).map_err(|e| {
                        CommonError::Internal(format!("Failed to serialize topic message: {}", e))
                    })?;
                    let retention_entry = kalamdb_tables::TopicRetentionIndexEntry::new_raw(
                        entry.topic_id.clone(),
                        *partition_id,
                        timestamp_ms,
                        offset,
                        value_encoded.len() as u64,
                    );
                    raw_entries.push((retention_entry, key_encoded, value_encoded));
                }

                let message_bytes =
                    self.message_store.batch_put_raw_with_retention(raw_entries).map_err(|e| {
                        CommonError::Internal(format!(
                            "Failed to batch store topic messages: {}",
                            e
                        ))
                    })?;
                self.add_retained_bytes(&entry.topic_id, *partition_id, message_bytes);
                record_pubsub_messages_published(row_indices.len() as u64, message_bytes);

                total_published += row_indices.len();
            }
        }

        tracing::Span::current().record("published_count", total_published);
        Ok(total_published)
    }

    /// Persist a typed procedure payload on an explicit topic.
    pub fn publish_typed(
        &self,
        topic_id: &TopicId,
        payload: Vec<u8>,
        user_id: Option<&UserId>,
    ) -> Result<u64> {
        if !self.topic_exists(topic_id) {
            return Err(CommonError::NotFound(format!("topic {topic_id} not found")));
        }
        let partition_id = 0u32;
        let lock = self.partition_write_lock(topic_id, partition_id);
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        let offset = self.offset_allocator.next_offset(topic_id, partition_id);
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let message = TopicMessage::new_with_user(
            topic_id.clone(),
            partition_id,
            offset,
            payload,
            None,
            timestamp_ms,
            user_id.cloned(),
            TopicOp::Insert,
        );
        let message_bytes =
            self.message_store.put_message_with_retention_index(&message).map_err(|e| {
                CommonError::Internal(format!("Failed to store typed topic message: {}", e))
            })?;
        self.add_retained_bytes(topic_id, partition_id, message_bytes);
        record_pubsub_messages_published(1, message_bytes);
        Ok(offset)
    }
}
