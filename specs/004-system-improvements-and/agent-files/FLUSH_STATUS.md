# Flush Tracking Summary

## What Was Added

Comprehensive debug logging throughout the flush pipeline to track:

### ✅ User Table Flush (`user_table_flush.rs`)
- 🚀 Job start with job_id and timestamp
- 🔄 Execution start
- 📸 Snapshot creation
- 🔍 Row scanning (progress every 1000 rows)
- 💾 Per-user flush operations
- 📝 Parquet write operations
- 🗑️ Row deletion from RocksDB
- ✅ Completion with full metrics (rows_flushed, users_count, parquet_files, duration_ms)
- ⚠️ Warnings for empty flushes
- ❌ Errors with full context

### ✅ Shared Table Flush (`shared_table_flush.rs`)
- Same comprehensive logging as user tables
- Tracks rows_flushed, parquet_file, duration_ms

### ✅ Scheduler (`scheduler.rs`)
- 🚀 Job trigger logging
- 📊 Job execution status
- ⚠️ **WARNING: Flush logic not wired to scheduler (TODO at line 692)**
- ✅ Job completion (currently shows 0 rows as not wired)

## Current Status

### ✅ What Works
1. **Flush job execution code is complete and working**
   - Tests pass: `test_user_table_flush_single_user`, `test_user_table_flush_multiple_users`
   - Direct calls to `UserTableFlushJob::execute()` work correctly
   - Rows are flushed to Parquet files
   - Rows are deleted from RocksDB after flush
   - Full logging is in place

2. **Logging infrastructure is complete**
   - INFO level: Major events
   - DEBUG level: Detailed step-by-step execution
   - WARN level: Empty flushes, configuration issues
   - ERROR level: Failures with context

### ❌ What Doesn't Work

1. **Automatic Flush (Scheduler)**
   - The scheduler's `check_and_trigger_flushes` method has a TODO
   - It creates job records but doesn't execute actual flush
   - It just sleeps for 100ms and resets trigger state
   - **Location**: `backend/crates/kalamdb-core/src/scheduler.rs:692-720`

2. **Manual Flush Commands**
   - If manual flush isn't working, it likely has the same issue
   - The command might create a job record but not execute the flush

## The Core Problem

### Scheduler Code (Line 686-720)
```rust
let job_future = Box::pin(async move {
    // TODO: Execute actual flush logic here
    // This requires access to the table provider and storage registry
    // For now, we log what would happen
    log::warn!(
        "⚠️  Flush logic not yet wired to scheduler (job_id={}). 
        Need to wire UserTableFlushJob or SharedTableFlushJob execution here.",
        job_id_clone
    );

    // Simulate flush delay for testing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Reset trigger state after flush
    // ...
});
```

### What Needs to Happen
The scheduler needs to:
1. Access the appropriate table provider (UserTableProvider or SharedTableProvider)
2. Get table metadata (namespace, schema, storage location)
3. Create the flush job instance
4. Call `job.execute()`
5. Handle the result

### Dependencies Required
To fix this, the scheduler needs access to:
- `Arc<UserTableStore>` or `Arc<SharedTableStore>`
- `Arc<StorageRegistry>`
- Table metadata (namespace_id, table_name, schema, storage_location)
- These need to be passed when tables are scheduled

## How to Verify the Fix

### Test Direct Flush Execution
```bash
cd backend
$env:KALAMDB_LOG_LEVEL="kalamdb_core::flush=debug"
cargo test test_user_table_flush_single_user --lib -- --nocapture
```

You won't see debug logs in tests (tests run without logger initialization), but the tests passing proves the flush logic works.

### Test via Server
Once the scheduler is wired:

1. Start server with debug logging:
```bash
cd backend
$env:KALAMDB_LOG_LEVEL="debug"
cargo run --bin kalamdb-server
```

2. Insert data that triggers automatic flush (based on FlushPolicy)

3. Watch for log output:
```
[INFO ] 🚀 Starting flush job: job_id=..., table=..., cf=...
[DEBUG] 🔄 Starting flush execution: table=...
[DEBUG] 📸 RocksDB snapshot created for table=...
[DEBUG] 🔍 Scanning rows for table=...
[DEBUG] 💾 Flushing X rows for user_id=...
[DEBUG] 📝 Writing Parquet file: path=..., rows=...
[INFO ] ✅ Flushed X rows for user_id=... to ...
[INFO ] ✅ Flush execution completed: total_rows_flushed=X, users_count=Y, parquet_files=Z
```

### Test Manual Flush
```bash
# Via API or SQL command
STORAGE FLUSH TABLE namespace.table_name;
```

Should see the same logging sequence.

## Next Steps to Fix

### Option 1: Pass Dependencies to Scheduler
Modify `FlushScheduler::schedule_table()` to accept:
```rust
pub async fn schedule_table(
    &self,
    table_name: TableName,
    cf_name: String,
    policy: FlushPolicy,
    store: Arc<UserTableStore>,  // NEW
    schema: SchemaRef,            // NEW
    storage_location: String,     // NEW
    namespace_id: NamespaceId,    // NEW
) -> Result<(), KalamDbError>
```

Store this metadata in `ScheduledTable` struct.

### Option 2: Create Flush Job Factory
Create a factory trait that the scheduler can call:
```rust
trait FlushJobFactory {
    fn create_flush_job(&self, cf_name: &str) -> Box<dyn FlushJob>;
}
```

### Option 3: Event-Based Architecture
Instead of scheduler executing flushes directly:
1. Scheduler emits "flush needed" event
2. Table provider listens for events
3. Table provider creates and executes flush job

## Visual Indicators Reference

- 🚀 = Start of operation
- 📊 = Data metrics/status
- 📸 = Snapshot creation
- 🔍 = Scanning/searching
- 💾 = Data persistence operation
- 📝 = File write operation
- ✅ = Success/completion
- 🗑️ = Deletion operation
- ⚠️ = Warning (non-fatal)
- ❌ = Error/failure
- 🔄 = Process starting/cycling

## Files Modified

1. `backend/crates/kalamdb-core/src/scheduler.rs` - Added logging, identified TODO
2. `backend/crates/kalamdb-core/src/flush/user_table_flush.rs` - Complete logging
3. `backend/crates/kalamdb-core/src/flush/shared_table_flush.rs` - Complete logging

## Related Documentation

- See `FLUSH_DEBUG_TRACKING.md` for detailed logging examples
- See spec: `specs/004-system-improvements-and/spec.md` for flush requirements
- See tasks: `specs/004-system-improvements-and/tasks.md` for implementation tasks
