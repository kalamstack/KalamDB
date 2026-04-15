mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use kalamdb_commons::models::pg_operations::{InsertRequest, ScanRequest};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{NamespaceId, NodeId, StorageId, TableId, TableName};
use kalamdb_commons::TableType;
use kalamdb_configs::ServerConfig;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::operations::service::OperationService;
use kalamdb_pg::OperationExecutor;
use kalamdb_store::test_utils::TestDb;
use kalamdb_store::StorageBackend;
use kalamdb_system::providers::storages::models::StorageType;
use kalamdb_system::{Storage, StoragePartition, SystemTable};

use support::{create_cluster_app_context, create_shared_table, row, unique_namespace};

const VALID_IDLE_SESSION_ID: &str = "pg-7101-deadbeef";
const MAX_REGRESSION_RATIO: f64 = 1.05;
const WRITE_ROUNDS: usize = 7;
const WRITE_OPS_PER_ROUND: usize = 12;
const READ_ROUNDS: usize = 9;
const READ_OPS_PER_ROUND: usize = 12;
const READ_SEED_ROWS: usize = 192;
const ALLOCATION_ITERS: usize = 32;
const ALLOCATION_SAMPLE_ROUNDS: usize = 5;

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        record_allocation(!ptr.is_null(), layout.size());
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        record_allocation(!ptr.is_null(), layout.size());
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        record_allocation(!new_ptr.is_null(), new_size);
        new_ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AllocationSample {
    allocations: u64,
    bytes: u64,
}

impl AllocationSample {
    fn min_noise_floor(self, other: Self) -> Self {
        if other.allocations < self.allocations
            || (other.allocations == self.allocations && other.bytes < self.bytes)
        {
            other
        } else {
            self
        }
    }
}

struct AllocationGuard;

impl AllocationGuard {
    fn start() -> Self {
        ALLOCATION_COUNT.store(0, Ordering::SeqCst);
        ALLOCATION_BYTES.store(0, Ordering::SeqCst);
        COUNT_ALLOCATIONS.store(true, Ordering::SeqCst);
        Self
    }

    fn sample(&self) -> AllocationSample {
        AllocationSample {
            allocations: ALLOCATION_COUNT.load(Ordering::SeqCst),
            bytes: ALLOCATION_BYTES.load(Ordering::SeqCst),
        }
    }
}

impl Drop for AllocationGuard {
    fn drop(&mut self) {
        COUNT_ALLOCATIONS.store(false, Ordering::SeqCst);
    }
}

#[inline]
fn record_allocation(success: bool, size: usize) {
    if success && COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
        ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOCATION_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}

fn empty_row() -> Row {
    Row::new(Default::default())
}

fn create_simple_app_context() -> (Arc<AppContext>, TestDb) {
    let mut column_families: Vec<&'static str> = SystemTable::all_tables()
        .iter()
        .filter_map(|table| table.column_family_name())
        .collect();
    column_families.push(StoragePartition::InformationSchemaTables.name());
    column_families.push("shared_table:app:config");
    column_families.push("stream_table:app:events");

    let test_db = TestDb::new(&column_families).expect("create simple test db");
    let storage_backend: Arc<dyn StorageBackend> = test_db.backend();
    let storage_base_path = test_db.storage_dir().expect("storage dir");
    let data_path = test_db.path().to_path_buf();

    let mut test_config = ServerConfig::default();
    test_config.storage.data_path = data_path.to_string_lossy().to_string();
    test_config.execution.max_parameters = 50;
    test_config.execution.max_parameter_size_bytes = 512 * 1024;

    let app_ctx = AppContext::init(
        storage_backend,
        NodeId::new(1),
        storage_base_path.to_string_lossy().to_string(),
        test_config,
    );

    let storages = app_ctx.system_tables().storages();
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
                created_at: chrono::Utc::now().timestamp_millis(),
                updated_at: chrono::Utc::now().timestamp_millis(),
            })
            .expect("create default local storage");
    }

    (app_ctx, test_db)
}

fn make_shared_insert_request(
    table_id: &TableId,
    session_id: Option<&str>,
    id: i64,
) -> InsertRequest {
    InsertRequest {
        table_id: table_id.clone(),
        table_type: TableType::Shared,
        session_id: session_id.map(str::to_string),
        user_id: None,
        rows: vec![row(id, &format!("name_{id}"))],
    }
}

fn make_system_insert_request(session_id: Option<&str>) -> InsertRequest {
    InsertRequest {
        table_id: TableId::new(NamespaceId::new("system"), TableName::new("users")),
        table_type: TableType::System,
        session_id: session_id.map(str::to_string),
        user_id: None,
        rows: vec![empty_row()],
    }
}

fn make_scan_request(table_id: &TableId, session_id: Option<&str>) -> ScanRequest {
    ScanRequest {
        table_id: table_id.clone(),
        table_type: TableType::Shared,
        session_id: session_id.map(str::to_string),
        columns: vec![],
        limit: None,
        user_id: None,
    filters: vec![],
    }
}

async fn seed_shared_table(
    service: &OperationService,
    table_id: &TableId,
    start_id: i64,
    rows: usize,
) {
    for chunk_start in (0..rows).step_by(24) {
        let chunk_end = (chunk_start + 24).min(rows);
        let chunk_rows = (chunk_start..chunk_end)
            .map(|offset| {
                let id = start_id + offset as i64;
                row(id, &format!("seed_{id}"))
            })
            .collect::<Vec<_>>();

        service
            .execute_insert(InsertRequest {
                table_id: table_id.clone(),
                table_type: TableType::Shared,
                session_id: None,
                user_id: None,
                rows: chunk_rows,
            })
            .await
            .expect("seed insert succeeds");
    }
}

async fn measure_insert_round(
    service: &OperationService,
    table_id: &TableId,
    session_id: Option<&str>,
    start_id: i64,
    ops: usize,
) -> u128 {
    let requests = (0..ops)
        .map(|offset| make_shared_insert_request(table_id, session_id, start_id + offset as i64))
        .collect::<Vec<_>>();
    let start = Instant::now();
    for request in requests {
        service.execute_insert(request).await.expect("autocommit insert succeeds");
    }
    start.elapsed().as_nanos() / ops as u128
}

async fn measure_scan_round(
    service: &OperationService,
    table_id: &TableId,
    session_id: Option<&str>,
    ops: usize,
) -> u128 {
    let requests = (0..ops).map(|_| make_scan_request(table_id, session_id)).collect::<Vec<_>>();
    let start = Instant::now();
    for request in requests {
        service.execute_scan(request).await.expect("autocommit scan succeeds");
    }
    start.elapsed().as_nanos() / ops as u128
}

fn median_nanos(samples: &[u128]) -> u128 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

fn assert_regression(label: &str, baseline_ns: u128, candidate_ns: u128) {
    let ratio = candidate_ns as f64 / baseline_ns as f64;
    assert!(
        ratio <= MAX_REGRESSION_RATIO,
        "{label} autocommit regression exceeded 5%: baseline={}ns candidate={}ns ratio={:.3}",
        baseline_ns,
        candidate_ns,
        ratio
    );
}

async fn measure_allocation_floor<F, Fut>(mut measure: F) -> AllocationSample
where
    F: FnMut() -> Fut,
    Fut: Future<Output = AllocationSample>,
{
    // The counting allocator is process-global, so unrelated background work can
    // only add noise to a sample. Take the minimum across several identical
    // steady-state rounds to recover the intrinsic allocation floor of the path.
    let mut best = measure().await;
    for _ in 1..ALLOCATION_SAMPLE_ROUNDS {
        best = best.min_noise_floor(measure().await);
    }
    best
}

async fn measure_allocations_for_scan(
    service: &OperationService,
    table_id: &TableId,
    session_id: Option<&str>,
) -> AllocationSample {
    let requests = (0..ALLOCATION_ITERS)
        .map(|_| make_scan_request(table_id, session_id))
        .collect::<Vec<_>>();
    let guard = AllocationGuard::start();
    for request in requests {
        service
            .execute_scan(request)
            .await
            .expect("scan succeeds during allocation capture");
    }
    let sample = guard.sample();
    drop(guard);
    sample
}

async fn measure_allocations_for_rejected_write(
    service: &OperationService,
    session_id: Option<&str>,
) -> AllocationSample {
    let requests = (0..ALLOCATION_ITERS)
        .map(|_| make_system_insert_request(session_id))
        .collect::<Vec<_>>();
    let guard = AllocationGuard::start();
    for request in requests {
        let error = service
            .execute_insert(request)
            .await
            .expect_err("system-table insert remains rejected");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
    let sample = guard.sample();
    drop(guard);
    sample
}

#[tokio::test(flavor = "current_thread")]
#[ntest::timeout(8000)]
async fn idle_autocommit_transaction_checks_add_no_extra_allocations() {
    let (app_ctx, _test_db) = create_simple_app_context();
    let service = OperationService::new(Arc::clone(&app_ctx));
    let scan_table =
        create_shared_table(&app_ctx, &unique_namespace("autocommit_alloc"), "items").await;

    service
        .execute_scan(make_scan_request(&scan_table, None))
        .await
        .expect("warmup scan succeeds");
    service
        .execute_scan(make_scan_request(&scan_table, Some(VALID_IDLE_SESSION_ID)))
        .await
        .expect("warmup scan with session succeeds");
    let warmup_error = service
        .execute_insert(make_system_insert_request(None))
        .await
        .expect_err("warmup rejected write stays rejected");
    assert_eq!(warmup_error.code(), tonic::Code::PermissionDenied);
    let warmup_error = service
        .execute_insert(make_system_insert_request(Some(VALID_IDLE_SESSION_ID)))
        .await
        .expect_err("warmup rejected write with session stays rejected");
    assert_eq!(warmup_error.code(), tonic::Code::PermissionDenied);

    let scan_without_session =
        measure_allocation_floor(|| measure_allocations_for_scan(&service, &scan_table, None))
            .await;
    let scan_with_idle_session = measure_allocation_floor(|| {
        measure_allocations_for_scan(&service, &scan_table, Some(VALID_IDLE_SESSION_ID))
    })
    .await;
    assert_eq!(
        scan_with_idle_session, scan_without_session,
        "idle transaction lookup changed scan allocations: without_session={scan_without_session:?} with_session={scan_with_idle_session:?}"
    );

    let write_without_session =
        measure_allocation_floor(|| measure_allocations_for_rejected_write(&service, None)).await;
    let write_with_idle_session = measure_allocation_floor(|| {
        measure_allocations_for_rejected_write(&service, Some(VALID_IDLE_SESSION_ID))
    })
    .await;
    assert_eq!(
        write_with_idle_session, write_without_session,
        "idle transaction lookup changed write allocations: without_session={write_without_session:?} with_session={write_with_idle_session:?}"
    );
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn autocommit_read_write_latency_regression_stays_within_five_percent() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let service = OperationService::new(Arc::clone(&app_ctx));

    let write_baseline_table =
        create_shared_table(&app_ctx, &unique_namespace("autocommit_write_base"), "items").await;
    let write_candidate_table =
        create_shared_table(&app_ctx, &unique_namespace("autocommit_write_candidate"), "items")
            .await;
    let read_table =
        create_shared_table(&app_ctx, &unique_namespace("autocommit_read"), "items").await;

    seed_shared_table(&service, &read_table, 10_000, READ_SEED_ROWS).await;

    let _ = measure_insert_round(&service, &write_baseline_table, None, 100_000, 4).await;
    let _ = measure_insert_round(
        &service,
        &write_candidate_table,
        Some(VALID_IDLE_SESSION_ID),
        200_000,
        4,
    )
    .await;
    let _ = measure_scan_round(&service, &read_table, None, 4).await;
    let _ = measure_scan_round(&service, &read_table, Some(VALID_IDLE_SESSION_ID), 4).await;

    let mut write_baseline_samples = Vec::with_capacity(WRITE_ROUNDS);
    let mut write_candidate_samples = Vec::with_capacity(WRITE_ROUNDS);
    let mut read_baseline_samples = Vec::with_capacity(READ_ROUNDS);
    let mut read_candidate_samples = Vec::with_capacity(READ_ROUNDS);

    for round in 0..WRITE_ROUNDS {
        let baseline_start_id = 300_000 + (round * WRITE_OPS_PER_ROUND) as i64;
        let candidate_start_id = 400_000 + (round * WRITE_OPS_PER_ROUND) as i64;
        if round % 2 == 0 {
            write_baseline_samples.push(
                measure_insert_round(
                    &service,
                    &write_baseline_table,
                    None,
                    baseline_start_id,
                    WRITE_OPS_PER_ROUND,
                )
                .await,
            );
            write_candidate_samples.push(
                measure_insert_round(
                    &service,
                    &write_candidate_table,
                    Some(VALID_IDLE_SESSION_ID),
                    candidate_start_id,
                    WRITE_OPS_PER_ROUND,
                )
                .await,
            );
        } else {
            write_candidate_samples.push(
                measure_insert_round(
                    &service,
                    &write_candidate_table,
                    Some(VALID_IDLE_SESSION_ID),
                    candidate_start_id,
                    WRITE_OPS_PER_ROUND,
                )
                .await,
            );
            write_baseline_samples.push(
                measure_insert_round(
                    &service,
                    &write_baseline_table,
                    None,
                    baseline_start_id,
                    WRITE_OPS_PER_ROUND,
                )
                .await,
            );
        }
    }

    for round in 0..READ_ROUNDS {
        if round % 2 == 0 {
            read_baseline_samples
                .push(measure_scan_round(&service, &read_table, None, READ_OPS_PER_ROUND).await);
            read_candidate_samples.push(
                measure_scan_round(
                    &service,
                    &read_table,
                    Some(VALID_IDLE_SESSION_ID),
                    READ_OPS_PER_ROUND,
                )
                .await,
            );
        } else {
            read_candidate_samples.push(
                measure_scan_round(
                    &service,
                    &read_table,
                    Some(VALID_IDLE_SESSION_ID),
                    READ_OPS_PER_ROUND,
                )
                .await,
            );
            read_baseline_samples
                .push(measure_scan_round(&service, &read_table, None, READ_OPS_PER_ROUND).await);
        }
    }

    let write_baseline_ns = median_nanos(&write_baseline_samples);
    let write_candidate_ns = median_nanos(&write_candidate_samples);
    let read_baseline_ns = median_nanos(&read_baseline_samples);
    let read_candidate_ns = median_nanos(&read_candidate_samples);

    println!(
        "autocommit perf regression medians: write baseline={}ns candidate={}ns, read baseline={}ns candidate={}ns",
        write_baseline_ns,
        write_candidate_ns,
        read_baseline_ns,
        read_candidate_ns
    );

    assert!(app_ctx.transaction_coordinator().active_metrics().is_empty());
    assert_regression("write", write_baseline_ns, write_candidate_ns);
    assert_regression("read", read_baseline_ns, read_candidate_ns);
}
