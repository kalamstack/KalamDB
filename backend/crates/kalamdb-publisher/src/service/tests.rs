use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Condvar, Mutex as StdMutex,
    },
    thread,
    time::Duration as StdDuration,
};

use datafusion_common::ScalarValue;
use kalamdb_commons::{
    models::{NamespaceId, PayloadMode, TableName},
    StorageKey,
};
use kalamdb_store::{
    storage_trait::{KvIterator, Operation, Partition, StorageBackend},
    test_utils::InMemoryBackend,
};
use kalamdb_system::providers::topics::TopicRoute;

use super::*;

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

fn create_task_row(id: i32, title: &str, cancelled: bool) -> Row {
    let mut values = std::collections::BTreeMap::new();
    values.insert("id".to_string(), ScalarValue::Int32(Some(id)));
    values.insert("title".to_string(), ScalarValue::Utf8(Some(title.to_string())));
    values.insert("cancelled".to_string(), ScalarValue::Boolean(Some(cancelled)));
    Row { values }
}

fn create_event_row(
    id: i32,
    status: &str,
    priority: i32,
    event_type: &str,
    archived: Option<bool>,
) -> Row {
    let mut values = std::collections::BTreeMap::new();
    values.insert("id".to_string(), ScalarValue::Int32(Some(id)));
    values.insert("status".to_string(), ScalarValue::Utf8(Some(status.to_string())));
    values.insert("priority".to_string(), ScalarValue::Int32(Some(priority)));
    values.insert("event_type".to_string(), ScalarValue::Utf8(Some(event_type.to_string())));
    if let Some(archived) = archived {
        values.insert("archived".to_string(), ScalarValue::Boolean(Some(archived)));
    }
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

fn create_test_topic_with_filter(
    topic_id: TopicId,
    table_id: TableId,
    op: TopicOp,
    partitions: u32,
    filter_expr: &str,
) -> Topic {
    let mut topic = create_test_topic_with_partitions(topic_id, table_id, op, partitions);
    topic.routes[0].filter_expr = Some(filter_expr.to_string());
    topic
}

fn create_test_topic_with_retention(
    topic_id: TopicId,
    table_id: TableId,
    op: TopicOp,
    partitions: u32,
    retention_seconds: Option<i64>,
    retention_max_bytes: Option<i64>,
) -> Topic {
    let mut topic = create_test_topic_with_partitions(topic_id, table_id, op, partitions);
    topic.retention_seconds = retention_seconds;
    topic.retention_max_bytes = retention_max_bytes;
    topic
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

fn append_retained_message(
    service: &TopicPublisherService,
    topic_id: &TopicId,
    partition_id: u32,
    offset: u64,
    payload: &[u8],
    timestamp_ms: i64,
) -> u64 {
    let message = TopicMessage::new(
        topic_id.clone(),
        partition_id,
        offset,
        payload.to_vec(),
        None,
        timestamp_ms,
        Default::default(),
    );
    let message_bytes = service.message_store.put_message_with_retention_index(&message).unwrap();
    service.add_retained_bytes(topic_id, partition_id, message_bytes);
    service.offset_allocator.seed(topic_id, partition_id, offset + 1);
    message_bytes
}

fn put_primary_only_message(
    backend: &Arc<InMemoryBackend>,
    topic_id: &TopicId,
    partition_id: u32,
    offset: u64,
    payload: &[u8],
    timestamp_ms: i64,
) {
    let message = TopicMessage::new(
        topic_id.clone(),
        partition_id,
        offset,
        payload.to_vec(),
        None,
        timestamp_ms,
        Default::default(),
    );
    backend
        .put(
            &Partition::new("topic_messages"),
            &message.id().storage_key(),
            &kalamdb_store::encode_entity(&message).unwrap(),
        )
        .unwrap();
}

struct PausingScanBackend {
    inner:           InMemoryBackend,
    pause_next_scan: AtomicBool,
    scan_started:    (StdMutex<bool>, Condvar),
    release_scan:    (StdMutex<bool>, Condvar),
}

impl PausingScanBackend {
    fn new() -> Self {
        Self {
            inner:           InMemoryBackend::new(),
            pause_next_scan: AtomicBool::new(false),
            scan_started:    (StdMutex::new(false), Condvar::new()),
            release_scan:    (StdMutex::new(false), Condvar::new()),
        }
    }

    fn pause_next_scan(&self) {
        self.pause_next_scan.store(true, Ordering::SeqCst);
        *self.scan_started.0.lock().unwrap() = false;
        *self.release_scan.0.lock().unwrap() = false;
    }

    fn wait_for_paused_scan(&self) {
        let (lock, cvar) = &self.scan_started;
        let started = lock.lock().unwrap();
        let (started, _) = cvar
            .wait_timeout_while(started, StdDuration::from_secs(1), |started| !*started)
            .unwrap();
        assert!(*started, "first consumer should enter the paused storage scan");
    }

    fn release_paused_scan(&self) {
        let (lock, cvar) = &self.release_scan;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }
}

impl StorageBackend for PausingScanBackend {
    fn get(
        &self,
        partition: &Partition,
        key: &[u8],
    ) -> kalamdb_store::storage_trait::Result<Option<Vec<u8>>> {
        self.inner.get(partition, key)
    }

    fn put(
        &self,
        partition: &Partition,
        key: &[u8],
        value: &[u8],
    ) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.put(partition, key, value)
    }

    fn delete(
        &self,
        partition: &Partition,
        key: &[u8],
    ) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.delete(partition, key)
    }

    fn batch(&self, operations: Vec<Operation>) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.batch(operations)
    }

    fn scan(
        &self,
        partition: &Partition,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> kalamdb_store::storage_trait::Result<KvIterator<'_>> {
        if self.pause_next_scan.swap(false, Ordering::SeqCst) {
            let (started_lock, started_cvar) = &self.scan_started;
            *started_lock.lock().unwrap() = true;
            started_cvar.notify_all();

            let (release_lock, release_cvar) = &self.release_scan;
            let released = release_lock.lock().unwrap();
            let (released, _) = release_cvar
                .wait_timeout_while(released, StdDuration::from_secs(2), |released| !*released)
                .unwrap();
            assert!(*released, "paused scan should be released by the test");
        }

        self.inner.scan(partition, prefix, start_key, limit)
    }

    fn partition_exists(&self, partition: &Partition) -> bool {
        self.inner.partition_exists(partition)
    }

    fn create_partition(&self, partition: &Partition) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.create_partition(partition)
    }

    fn list_partitions(&self) -> kalamdb_store::storage_trait::Result<Vec<Partition>> {
        self.inner.list_partitions()
    }

    fn drop_partition(&self, partition: &Partition) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.drop_partition(partition)
    }

    fn compact_partition(&self, partition: &Partition) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.compact_partition(partition)
    }

    fn stats(&self) -> kalamdb_store::storage_trait::StorageStats {
        self.inner.stats()
    }
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

    let topic = create_test_topic_with_partitions(
        topic_id.clone(),
        table_id.clone(),
        TopicOp::Insert,
        partitions,
    );
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
fn test_batch_publish_preserves_actor_user_id() {
    let service = service_with_primary_key(&["id"]);

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("shared_events"));
    let topic_id = TopicId::new("shared_actor_topic");
    let actor_user_id = UserId::from("actor_user_1");

    let topic = create_test_topic(topic_id.clone(), table_id.clone(), TopicOp::Insert);
    service.add_topic(topic);

    let rows = [create_test_row(1, "alpha"), create_test_row(2, "beta")];
    let published = service
        .publish_batch(&table_id, TopicOp::Insert, &rows, Some(&actor_user_id))
        .unwrap();
    assert_eq!(published, rows.len());

    let mut messages = Vec::new();
    for partition_id in 0..2 {
        messages.extend(service.fetch_messages(&topic_id, partition_id, 0, 10).unwrap());
    }

    assert_eq!(messages.len(), rows.len());
    assert!(
        messages.iter().all(|message| message.user_id.as_ref() == Some(&actor_user_id)),
        "every published message should retain the shared-table actor user"
    );
}

#[test]
fn test_publish_message_respects_route_filter_on_insert() {
    let service = service_with_primary_key(&["id"]);

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("tasks"));
    let topic_id = TopicId::new("task_cancellation_insert_topic");

    let topic = create_test_topic_with_filter(
        topic_id.clone(),
        table_id.clone(),
        TopicOp::Insert,
        1,
        "cancelled = true",
    );
    service.add_topic(topic);

    let matching_row = create_task_row(1, "cancel deployment", true);
    let non_matching_row = create_task_row(2, "keep deployment", false);

    assert_eq!(
        service
            .publish_message(&table_id, TopicOp::Insert, &matching_row, None)
            .unwrap(),
        1
    );
    assert_eq!(
        service
            .publish_message(&table_id, TopicOp::Insert, &non_matching_row, None)
            .unwrap(),
        0
    );

    let messages = service.fetch_messages(&topic_id, 0, 0, 10).unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].op, TopicOp::Insert);
    assert_eq!(messages[0].key.as_deref(), Some("1"));

    let payload: serde_json::Value =
        serde_json::from_slice(&messages[0].payload).expect("topic payload should be JSON");
    assert_eq!(payload["title"].as_str(), Some("cancel deployment"));
    assert_eq!(payload["cancelled"].as_bool(), Some(true));
}

#[test]
fn test_publish_message_respects_complex_route_filter_on_insert() {
    let service = service_with_primary_key(&["id"]);

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("events"));
    let topic_id = TopicId::new("complex_event_insert_topic");

    let topic = create_test_topic_with_filter(
        topic_id.clone(),
        table_id.clone(),
        TopicOp::Insert,
        1,
        "((status IN ('blocked', 'cancelled') AND priority BETWEEN 5 AND 10) OR event_type ILIKE \
         'deploy_%') AND archived IS NULL",
    );
    service.add_topic(topic);
    assert_eq!(service.cache_stats().total_routes, 1);

    let non_matching_row = create_event_row(1, "active", 1, "noop", None);
    let matching_status_row = create_event_row(2, "blocked", 7, "noop", None);
    let matching_event_type_row = create_event_row(3, "active", 1, "DEPLOY_START", None);
    let archived_row = create_event_row(4, "blocked", 7, "noop", Some(true));

    let routes = service.route_cache.get_matching_routes(&table_id, &TopicOp::Insert);
    let compiled_filter = routes[0].compiled_filter.as_ref().expect("route should compile filter");
    assert!(compiled_filter
        .matches(&matching_status_row)
        .expect("compiled route filter should evaluate"));
    assert!(compiled_filter
        .matches(&matching_event_type_row)
        .expect("compiled route filter should evaluate"));
    assert!(!compiled_filter
        .matches(&archived_row)
        .expect("compiled route filter should evaluate"));

    assert_eq!(
        service
            .publish_message(&table_id, TopicOp::Insert, &non_matching_row, None)
            .unwrap(),
        0
    );
    assert_eq!(
        service
            .publish_message(&table_id, TopicOp::Insert, &matching_status_row, None)
            .unwrap(),
        1
    );
    assert_eq!(
        service
            .publish_message(&table_id, TopicOp::Insert, &matching_event_type_row, None)
            .unwrap(),
        1
    );
    assert_eq!(
        service
            .publish_message(&table_id, TopicOp::Insert, &archived_row, None)
            .unwrap(),
        0
    );

    let messages = service.fetch_messages(&topic_id, 0, 0, 10).unwrap();
    let keys: HashSet<String> = messages.iter().filter_map(|message| message.key.clone()).collect();
    assert_eq!(keys, HashSet::from(["2".to_string(), "3".to_string()]));
}

#[test]
fn test_publish_batch_respects_route_filter_on_update() {
    let service = service_with_primary_key(&["id"]);

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("tasks"));
    let topic_id = TopicId::new("task_cancellation_update_topic");

    let topic = create_test_topic_with_filter(
        topic_id.clone(),
        table_id.clone(),
        TopicOp::Update,
        1,
        "cancelled = true",
    );
    service.add_topic(topic);

    let rows = vec![
        create_task_row(1, "still running", false),
        create_task_row(2, "cancel billing", true),
        create_task_row(3, "stop indexing", true),
    ];

    assert_eq!(service.publish_batch(&table_id, TopicOp::Update, &rows, None).unwrap(), 2);

    let messages = service.fetch_messages(&topic_id, 0, 0, 10).unwrap();
    assert_eq!(messages.len(), 2);
    assert!(messages.iter().all(|message| message.op == TopicOp::Update));

    let keys: HashSet<String> = messages.iter().filter_map(|message| message.key.clone()).collect();
    assert_eq!(keys, HashSet::from(["2".to_string(), "3".to_string()]));

    for message in &messages {
        let payload: serde_json::Value =
            serde_json::from_slice(&message.payload).expect("topic payload should be JSON");
        assert_eq!(payload["cancelled"].as_bool(), Some(true));
    }
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
fn test_restore_offset_counters_only_seeds_offsets_without_rebuilding_retention_index() {
    let backend = Arc::new(InMemoryBackend::new());
    let service = TopicPublisherService::new(backend.clone());

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("users"));
    let topic_id = TopicId::new("restore_retention_topic");

    let topic = create_test_topic(topic_id.clone(), table_id, TopicOp::Insert);
    service.add_topic(topic);

    put_primary_only_message(&backend, &topic_id, 0, 0, b"first", 1_000);
    put_primary_only_message(&backend, &topic_id, 0, 1, b"second", 2_000);

    assert!(
        service
            .message_store
            .retention_entries_for_partition(&topic_id, 0, 10)
            .unwrap()
            .is_empty(),
        "test precondition: retention index should be missing before restore"
    );

    service.restore_offset_counters();

    assert!(
        service
            .message_store
            .retention_entries_for_partition(&topic_id, 0, 10)
            .unwrap()
            .is_empty(),
        "startup restore should not rebuild retention index eagerly"
    );
    assert_eq!(service.latest_offset(&topic_id, 0).unwrap(), Some(1));
}

#[test]
fn test_time_retention_advances_earliest_offset_without_rewriting_latest() {
    let backend = Arc::new(InMemoryBackend::new());
    let service = TopicPublisherService::new(backend);

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("users"));
    let topic_id = TopicId::new("time_retention_topic");

    let topic = create_test_topic_with_retention(
        topic_id.clone(),
        table_id,
        TopicOp::Insert,
        1,
        Some(3600),
        None,
    );
    service.add_topic(topic.clone());

    append_retained_message(&service, &topic_id, 0, 0, b"oldest", 1_000);
    append_retained_message(&service, &topic_id, 0, 1, b"older", 2_000);
    append_retained_message(&service, &topic_id, 0, 2, b"fresh", 3_000);

    let stats = service.enforce_retention(&topic, 0, Some(2_500), None, 10).unwrap();

    assert_eq!(stats.messages_deleted, 2);
    assert_eq!(service.earliest_available_offset(&topic_id, 0).unwrap(), 2);
    assert_eq!(service.latest_offset(&topic_id, 0).unwrap(), Some(2));
    assert_eq!(service.fetch_messages(&topic_id, 0, 2, 10).unwrap().len(), 1);

    let err = service.fetch_messages(&topic_id, 0, 1, 10).unwrap_err();
    assert!(err.to_string().contains("OffsetOutOfRange"));
}

#[test]
fn test_byte_retention_prunes_oldest_messages_first() {
    let backend = Arc::new(InMemoryBackend::new());
    let service = TopicPublisherService::new(backend);

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("users"));
    let topic_id = TopicId::new("byte_retention_topic");

    let topic = create_test_topic_with_retention(
        topic_id.clone(),
        table_id,
        TopicOp::Insert,
        1,
        None,
        Some(1),
    );
    service.add_topic(topic.clone());

    let first_bytes = append_retained_message(&service, &topic_id, 0, 0, b"first", 1_000);
    let second_bytes = append_retained_message(&service, &topic_id, 0, 1, b"second", 2_000);
    let third_bytes = append_retained_message(&service, &topic_id, 0, 2, b"third", 3_000);

    let max_bytes = (second_bytes + third_bytes) as i64;
    let stats = service.enforce_retention(&topic, 0, None, Some(max_bytes), 10).unwrap();

    assert_eq!(stats.messages_deleted, 1);
    assert_eq!(stats.bytes_freed, first_bytes);
    assert_eq!(service.earliest_available_offset(&topic_id, 0).unwrap(), 1);
    assert_eq!(service.latest_offset(&topic_id, 0).unwrap(), Some(2));
    assert_eq!(
        service
            .fetch_messages(&topic_id, 0, 1, 10)
            .unwrap()
            .iter()
            .map(|message| message.offset)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let err = service.fetch_messages(&topic_id, 0, 0, 10).unwrap_err();
    assert!(err.to_string().contains("OffsetOutOfRange"));
}

#[test]
fn test_byte_retention_can_fully_cleanup_partition() {
    let backend = Arc::new(InMemoryBackend::new());
    let service = TopicPublisherService::new(backend);

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("users"));
    let topic_id = TopicId::new("byte_retention_full_cleanup_topic");

    let topic = create_test_topic_with_retention(
        topic_id.clone(),
        table_id,
        TopicOp::Insert,
        1,
        None,
        Some(1),
    );
    service.add_topic(topic.clone());

    append_retained_message(&service, &topic_id, 0, 0, b"first", 1_000);
    append_retained_message(&service, &topic_id, 0, 1, b"second", 2_000);
    append_retained_message(&service, &topic_id, 0, 2, b"third", 3_000);

    let stats = service.enforce_retention(&topic, 0, None, Some(1), 10).unwrap();

    assert_eq!(stats.messages_deleted, 3);
    assert_eq!(service.earliest_available_offset(&topic_id, 0).unwrap(), 3);
    assert_eq!(service.latest_offset(&topic_id, 0).unwrap(), Some(2));
    assert_eq!(service.retained_bytes_for_partition(&topic_id, 0).unwrap(), 0);
    assert!(service.fetch_messages(&topic_id, 0, 3, 10).unwrap().is_empty());
    assert!(service
        .message_store
        .retention_entries_for_partition(&topic_id, 0, 10)
        .unwrap()
        .is_empty());

    let err = service.fetch_messages(&topic_id, 0, 0, 10).unwrap_err();
    assert!(err.to_string().contains("OffsetOutOfRange"));
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

    let topic =
        create_test_topic_with_partitions(topic_id.clone(), table_id.clone(), TopicOp::Insert, 1);
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
fn test_group_fetch_does_not_hold_claim_state_during_storage_scan() {
    let backend = Arc::new(PausingScanBackend::new());
    let storage_backend: Arc<dyn StorageBackend> = backend.clone();
    let service = Arc::new(TopicPublisherService::new(storage_backend));

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("events"));
    let topic_id = TopicId::new("nonblocking_claim_topic");
    let group_id = ConsumerGroupId::new("nonblocking_claim_group");

    let topic =
        create_test_topic_with_partitions(topic_id.clone(), table_id.clone(), TopicOp::Insert, 1);
    service.add_topic(topic);

    for idx in 0..30 {
        let row = create_test_row(idx, &format!("event_{}", idx));
        service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
    }

    backend.pause_next_scan();

    let first_service = service.clone();
    let first_topic = topic_id.clone();
    let first_group = group_id.clone();
    let first_handle = thread::spawn(move || {
        first_service
            .fetch_messages_for_group(&first_topic, &first_group, 0, 0, 10)
            .unwrap()
    });

    backend.wait_for_paused_scan();

    let (tx, rx) = mpsc::channel();
    let second_service = service.clone();
    let second_topic = topic_id.clone();
    let second_group = group_id.clone();
    thread::spawn(move || {
        let batch = second_service
            .fetch_messages_for_group(&second_topic, &second_group, 0, 0, 10)
            .unwrap();
        let _ = tx.send(batch);
    });

    let second_batch = match rx.recv_timeout(StdDuration::from_millis(100)) {
        Ok(batch) => batch,
        Err(_) => {
            backend.release_paused_scan();
            let _ = first_handle.join();
            panic!("second consumer should not wait for the first consumer's storage scan");
        },
    };

    backend.release_paused_scan();
    let first_batch = first_handle.join().unwrap();

    let first_offsets: HashSet<u64> = first_batch.iter().map(|message| message.offset).collect();
    let second_offsets: HashSet<u64> = second_batch.iter().map(|message| message.offset).collect();

    assert_eq!(first_offsets.len(), 10);
    assert_eq!(second_offsets.len(), 10);
    assert!(
        first_offsets.is_disjoint(&second_offsets),
        "concurrent same-group fetches must reserve disjoint offsets"
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
fn test_partial_ack_trims_pending_claim_start() {
    let backend = Arc::new(InMemoryBackend::new());
    let service = TopicPublisherService::new(backend);

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("events"));
    let topic_id = TopicId::new("partial_ack_topic");
    let group_id = ConsumerGroupId::new("partial_ack_group");

    let topic =
        create_test_topic_with_partitions(topic_id.clone(), table_id.clone(), TopicOp::Insert, 1);
    service.add_topic(topic);

    for idx in 0..20 {
        let row = create_test_row(idx, &format!("e_{}", idx));
        service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
    }

    let batch = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
    assert_eq!(batch.first().map(|message| message.offset), Some(0));
    assert_eq!(batch.last().map(|message| message.offset), Some(9));

    service.ack_offset(&topic_id, &group_id, 0, 4).unwrap();

    let cursor_key = GroupPartitionKey::new(&topic_id, &group_id, 0);
    let state = service.group_claim_state.get(&cursor_key).unwrap();
    assert_eq!(state.pending.len(), 1, "Partially acked claim should stay pending");
    assert_eq!(
        state.pending[0].start, 5,
        "Expired claims must restart after the last acked offset"
    );
    assert_eq!(state.pending[0].end_exclusive, 10);
    assert_eq!(state.cursor, 10);
}

#[test]
fn test_expired_claim_redelivery_skips_still_pending_ranges() {
    let backend = Arc::new(InMemoryBackend::new());
    let service =
        TopicPublisherService::with_visibility_timeout(backend, StdDuration::from_millis(80));

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("events"));
    let topic_id = TopicId::new("partial_expiry_topic");
    let group_id = ConsumerGroupId::new("partial_expiry_group");

    let topic =
        create_test_topic_with_partitions(topic_id.clone(), table_id.clone(), TopicOp::Insert, 1);
    service.add_topic(topic);

    for idx in 0..30 {
        let row = create_test_row(idx, &format!("event_{}", idx));
        service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
    }

    let first = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
    assert_eq!(first.first().map(|message| message.offset), Some(0));

    thread::sleep(StdDuration::from_millis(50));

    let second = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
    assert_eq!(second.first().map(|message| message.offset), Some(10));

    thread::sleep(StdDuration::from_millis(50));

    let redelivered = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
    assert_eq!(redelivered.first().map(|message| message.offset), Some(0));

    let next = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 10).unwrap();
    assert_eq!(
        next.first().map(|message| message.offset),
        Some(20),
        "fetch should skip the still-pending 10..20 range after redelivering 0..10"
    );
}

#[test]
fn test_expired_claim_redelivery_uses_group_cursor_not_client_position() {
    let backend = Arc::new(InMemoryBackend::new());
    let service =
        TopicPublisherService::with_visibility_timeout(backend, StdDuration::from_millis(120));

    let ns = NamespaceId::new("test_ns");
    let table_id = TableId::new(ns.clone(), TableName::from("events"));
    let topic_id = TopicId::new("position_ahead_recovery_topic");
    let group_id = ConsumerGroupId::new("position_ahead_recovery_group");

    let topic =
        create_test_topic_with_partitions(topic_id.clone(), table_id.clone(), TopicOp::Insert, 1);
    service.add_topic(topic);

    for idx in 0..480 {
        let row = create_test_row(idx, &format!("event_{}", idx));
        service.publish_message(&table_id, TopicOp::Insert, &row, None).unwrap();
    }

    let crashed_claim = service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 160).unwrap();
    assert_eq!(crashed_claim.first().map(|message| message.offset), Some(0));
    assert_eq!(crashed_claim.last().map(|message| message.offset), Some(159));

    thread::sleep(StdDuration::from_millis(80));

    let active_tail_claim =
        service.fetch_messages_for_group(&topic_id, &group_id, 0, 0, 120).unwrap();
    assert_eq!(active_tail_claim.first().map(|message| message.offset), Some(160));
    assert_eq!(active_tail_claim.last().map(|message| message.offset), Some(279));

    thread::sleep(StdDuration::from_millis(60));

    let recovered_prefix =
        service.fetch_messages_for_group(&topic_id, &group_id, 0, 280, 120).unwrap();
    assert_eq!(recovered_prefix.first().map(|message| message.offset), Some(0));
    assert_eq!(recovered_prefix.last().map(|message| message.offset), Some(119));

    let recovered_gap =
        service.fetch_messages_for_group(&topic_id, &group_id, 0, 120, 120).unwrap();
    assert_eq!(recovered_gap.first().map(|message| message.offset), Some(120));
    assert_eq!(recovered_gap.last().map(|message| message.offset), Some(159));
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
