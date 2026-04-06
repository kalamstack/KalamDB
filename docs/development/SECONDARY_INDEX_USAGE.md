# Secondary Index Usage in DataFusion TableProviders

## Summary

We've implemented automatic secondary index discovery and usage in system table providers (users, jobs, live_queries). DataFusion's TableProvider interface now:

1. **Signals filter support** via `supports_filters_pushdown()` returning `Inexact`
2. **Uses indexes automatically** via `find_best_index_for_filters()` in scan()
3. **Logs index usage** for debugging and verification

## How It Works

### 1. Index Discovery

The `IndexedEntityStore` provides helper methods:
```rust
// Find index by partition name
find_index_by_partition(partition: &str) -> Option<usize>

// Find index covering a specific column
find_index_covering_column(column: &str) -> Option<usize>

// Automatically select best index for given filters
find_best_index_for_filters(filters: &[Expr]) -> Option<(usize, Vec<u8>)>
```

### 2. Filter Pushdown

Each `IndexDefinition` implements `filter_to_prefix()` to convert DataFusion Expr filters into index scan prefixes:

**Example**: UserUsernameIndex
```rust
fn filter_to_prefix(&self, filter: &Expr) -> Option<Vec<u8>> {
    // Handle: username = 'value'
    if let Some((col, val)) = extract_string_equality(filter) {
        if col == "username" {
            return Some(val.to_lowercase().into_bytes());
        }
    }
    
    // Handle: username LIKE 'prefix%'
    if let Expr::Like(like_expr) = filter {
        if let Some(pattern) = extract_like_prefix(like_expr) {
            return Some(pattern.to_lowercase().into_bytes());
        }
    }
    
    None
}
```

### 3. Scan Implementation

TableProvider `scan()` method:
```rust
async fn scan(&self, filters: &[Expr], ...) -> Result<ExecutionPlan> {
    // Try to use a secondary index
    let rows = if let Some((index_idx, prefix)) = 
        self.store.find_best_index_for_filters(filters) 
    {
        log::info!("Using secondary index {} for filters: {:?}", index_idx, filters);
        self.store.scan_by_index(index_idx, Some(&prefix), limit)?
    } else {
        log::info!("Full table scan (no index match) for filters: {:?}", filters);
        self.store.scan_all(limit, None, None)?
    };
    
    // Convert to RecordBatch and return
    let batch = self.create_batch(rows)?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    table.scan(state, projection, filters, limit).await
}
```

## Verification Methods

### Method 1: Server Logs

Enable INFO level logging and look for messages:
```
[system.users] Using secondary index 0 for filters: [username = 'root']
[system.jobs] Using secondary index 0 for filters: [status = 'running']
[system.users] Full table scan (no index match) for filters: [email LIKE '%@example.com']
```

### Method 2: EXPLAIN VERBOSE

Run queries with EXPLAIN:
```sql
EXPLAIN VERBOSE SELECT * FROM system.users WHERE username = 'root';
EXPLAIN VERBOSE SELECT * FROM system.jobs WHERE status = 'running';
EXPLAIN VERBOSE SELECT * FROM system.users WHERE username LIKE 'root%';
```

DataFusion will show the execution plan with:
- DataSourceExec (table scan)
- FilterExec (if Inexact pushdown)
- ProjectExec (column selection)

### Method 3: Performance Testing

Compare query latency with/without indexes:
```rust
// With 250 users:
// Index scan (username='user_123'): ~5-10ms
// Full scan (username='user_123'): ~50-100ms

// Ratio should be < 5x if index is used
assert!(latency_50_users / latency_250_users < 5.0);
```

## Supported Index Patterns

### Current Implementation

**UserUsernameIndex** (system_users_username_idx):
- ✅ `username = 'value'` (exact match)
- ✅ `username LIKE 'prefix%'` (prefix match)
- ❌ `username LIKE '%suffix'` (suffix not supported)
- ❌ `username LIKE '%middle%'` (substring not supported)

**UserRoleIndex** (system_users_role_idx):
- ✅ `role = 'admin'` (exact match)

**JobStatusCreatedAtIndex** (system_jobs_status_created_at_idx):
- ✅ `status = 'running'` (exact match on status)
- 🔄 `status = 'running' AND created_at > timestamp` (compound key support planned)

**JobIdempotencyKeyIndex** (system_jobs_idempotency_key_idx):
- ✅ `idempotency_key = 'key_value'` (exact match)

`system.live` is an in-memory virtual view backed by the active connection registry.
It does not use RocksDB secondary indexes.

## DataFusion Integration Notes

### Why Inexact Pushdown?

We return `TableProviderFilterPushDown::Inexact` because:
1. **Case-insensitive indexes**: Username index stores lowercase, but SQL may use any case
2. **Partial prefix matching**: LIKE 'prefix%' may match more rows than the actual pattern
3. **Safety**: DataFusion still applies the full filter for correctness

This is the recommended pattern per DataFusion docs:
> "Inexact pushdown means the TableProvider will attempt to use the filter for optimization (e.g., partition pruning, index scans) but DataFusion must still apply the filter afterwards to ensure correctness."

### Filter Lifecycle

1. **Logical Planning**: DataFusion parses SQL and creates `Expr` filters
2. **Pushdown Negotiation**: Calls `supports_filters_pushdown()` to ask provider support
3. **Physical Planning**: Provider's `scan()` receives filters, uses them for index selection
4. **Execution**: If pushdown was Inexact, DataFusion adds FilterExec after scan
5. **Result**: Final output has correct semantics even if index scan returned extra rows

## Testing

See:
- [`tests/misc/test_system_table_index_usage.rs`](../../backend/tests/misc/test_system_table_index_usage.rs) - Functional tests
- [`tests/misc/test_explain_index_usage.rs`](../../backend/tests/misc/test_explain_index_usage.rs) - EXPLAIN tests

Run tests:
```bash
# Test index functionality
cargo test --test test_system_table_index_usage

# Test with log output
cargo test --test test_explain_index_usage -- --nocapture
```

## Future Improvements

1. **Compound Index Support**: Use multiple columns (status + created_at)
2. **Statistics**: Return row count estimates via `TableProvider::statistics()`
3. **Bloom Filters**: For high-cardinality columns with low selectivity
4. **Cost-Based Selection**: Choose between multiple applicable indexes
5. **Range Scans**: Support `>`, `<`, `BETWEEN` operators more efficiently
