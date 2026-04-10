# Finished Notes

Last audit: 2026-04-10

This file contains both the original completed items from the old notes file and the tasks that were moved out of `todo` during the 2026-04-10 repo audit.

## Reclassified During The 2026-04-10 Audit

### Core

3) [DONE - Features/Core] Parameterized queries now convert API JSON params into `ScalarValue` at the HTTP boundary and are exposed in the TypeScript SDK query API. Evidence: [params parser](../../backend/crates/kalamdb-api/src/http/sql/helpers/params.rs), [SDK query API](../../link/sdks/typescript/client/src/client.ts).

13) [DONE - Features/Core] `BEGIN`, `COMMIT`, and `ROLLBACK` are classified and executed through request-scoped transaction state, with multi-statement HTTP transaction coverage in tests. Evidence: [SQL executor transaction handlers](../../backend/crates/kalamdb-core/src/sql/executor/sql_executor.rs), [HTTP transaction test](../../backend/tests/testserver/sql/test_transaction_quickstart_http.rs).

39) [DONE - Features/Core] Shared-table access policies are implemented and enforced through `TableAccess` plus shared-access SQL tests. Evidence: [permissions logic](../../backend/crates/kalamdb-session/src/permissions.rs), [shared access tests](../../backend/tests/misc/sql/test_shared_access.rs).

130) [DONE - Features/Core] `DESCRIBE TABLE` is implemented in the SQL dialect and exposed in the CLI through `\d` and `\describe`. Evidence: [dialect parser](../../backend/crates/kalamdb-dialect/src/ddl/describe_table.rs), [CLI parser](../../cli/src/parser.rs), [CLI docs](../../cli/README.md).

144) [DONE - Features/Core] Manifest state already has an in-memory hot cache via `ManifestService`, with system-table/provider support around the same cache entry model. Evidence: [manifest service](../../backend/crates/kalamdb-core/src/manifest/service.rs), [manifest provider](../../backend/crates/kalamdb-system/src/providers/manifest/manifest_provider.rs).

150) [DONE - Features/Core] Stream TTL eviction is implemented and covered by dedicated TTL tests. Evidence: [TTL subscription test](../../backend/tests/testserver/subscription/test_stream_ttl_eviction_sql.rs), [stream eviction executor](../../backend/crates/kalamdb-jobs/src/executors/stream_eviction.rs).

162) [DONE - Features/Core] Graceful shutdown already sends shutdown events to live connections and waits for WebSocket cleanup to finish. Evidence: [server shutdown flow](../../backend/src/lifecycle.rs), [connection shutdown manager](../../backend/crates/kalamdb-core/src/live/manager/connections_manager.rs).

169) [DONE - Features/Core] DDL execution invalidates the SQL plan cache through the shared helper used by the DDL executor. Evidence: [plan-cache helper](../../backend/crates/kalamdb-core/src/applier/executor/utils/mod.rs), [DDL executor usage](../../backend/crates/kalamdb-core/src/applier/executor/ddl.rs).

### CLI & DX

14) [SUPERSEDED - CLI] A dedicated CLI kill-live-query command is no longer necessary because the current SQL surface already supports `KILL LIVE QUERY`. Evidence: [SQL reference](../reference/sql.md), [kill-live-query parser](../../backend/crates/kalamdb-dialect/src/ddl/kill_live_query.rs).

35) [DONE - General] CLI and server build metadata already include commit and build date information. Evidence: [CLI version output](../../cli/src/args.rs), [CLI session info](../../cli/src/session/info.rs), [server startup log](../../backend/src/main.rs).

37) [DONE - CLI] Non-interactive localhost sessions already attempt root auto-auth when no stored credentials are available. Evidence: [CLI connect flow](../../cli/src/connect.rs).

40) [DONE - CLI] `\info` and `\session` already show current session information. Evidence: [CLI parser](../../cli/src/parser.rs), [session info implementation](../../cli/src/session/info.rs).

41) [SUPERSEDED - CLI/DX] Credentials are already stored per instance and URL through the CLI credential store under `~/.kalam`, so the older note about a separate `kalamdb-credentials` file is outdated. Evidence: [credential commands](../../cli/src/commands/credentials.rs), [connection/bootstrap flow](../../cli/src/connect.rs), [session credential info](../../cli/src/session/info.rs).

42) [DONE - CLI] The CLI auth flow already supports non-root users, and user-focused integration tests exist under `cli/tests/users`. Evidence: [CLI connect flow](../../cli/src/connect.rs), [admin/user tests](../../cli/tests/users/test_admin.rs), [concurrent user tests](../../cli/tests/users/test_concurrent_users.rs).

### Code Quality

46) [DONE - Code Quality] This is a duplicate of the existing centralized `NodeId` work: `NodeId` is typed and carried through `AppContext`. Evidence: [NodeId type](../../backend/crates/kalamdb-commons/src/models/ids/node_id.rs), [AppContext node_id accessor](../../backend/crates/kalamdb-core/src/app_context.rs).

48) [DONE - Code Quality] `TableAccess` is already the access-control surface used by permissions and access-level tests. Evidence: [permissions logic](../../backend/crates/kalamdb-session/src/permissions.rs), [shared access tests](../../backend/tests/misc/sql/test_shared_access.rs).

86) [DONE - Code Quality] This is another duplicate of the typed `NodeId` cleanup: `node_id` is not just a raw `String` in the current model. Evidence: [NodeId type](../../backend/crates/kalamdb-commons/src/models/ids/node_id.rs), [AppContext node_id field](../../backend/crates/kalamdb-core/src/app_context.rs).

122) [DONE - Code Quality] Job executors already use typed `Params: JobParams` and support typed `pre_validate` hooks. Evidence: [executor trait](../../backend/crates/kalamdb-jobs/src/executors/executor_trait.rs), [executor registry](../../backend/crates/kalamdb-jobs/src/executors/registry.rs).

149) [DONE - Code Quality] `if_not_exists` is already represented directly as a boolean in DDL models and parsers. Evidence: [create-table types](../../backend/crates/kalamdb-dialect/src/ddl/create_table/types.rs), [create-table parser](../../backend/crates/kalamdb-dialect/src/ddl/create_table/parser.rs).

## Original Completed Section

50) [DONE - 2024-12-30] Anonymous user restrictions - block write operations
     - Added ANONYMOUS_USER_ID constant to constants.rs
     - Added ExecutionContext::is_anonymous() method
     - Added block_anonymous_write() guard function in guards.rs
     - Integrated into all DDL handlers: CREATE/ALTER/DROP TABLE, CREATE/ALTER/DROP NAMESPACE
     - Integrated into all DML handlers: INSERT, UPDATE, DELETE
     - Anonymous users can only SELECT from public tables

55) [DONE - 2024-12-30] SQL query length limit to prevent DoS attacks
     - Added MAX_SQL_QUERY_LENGTH constant (1MB) to constants.rs
     - Enforced at SQL executor entry point before parsing
     - Returns clear error: "SQL query too long: X bytes (maximum Y bytes)"

151) [DONE - 2024-12-30] Reserved column names and naming conventions
     - Expanded RESERVED_COLUMN_NAMES to include: _seq, _deleted, _id, _row_id, _rowid, _key, _updated, _created, _timestamp, _version, _hash
     - validate_column_name() now checks against reserved names (case-insensitive)
     - Existing validation already blocks: underscore prefix, numbers prefix, spaces, special chars
     - Added comprehensive unit tests for reserved column name validation

108) [DONE - 2024-12-30] Prevent creating namespace with reserved names (system/sys/root/kalamdb/kalam/main/default/sql etc)
     - Added RESERVED_NAMESPACE_NAMES constant to constants.rs
     - Added NamespaceId::is_reserved() method for case-insensitive checking
     - Validation already enforced in CREATE NAMESPACE handler via Namespace::validate_name()

99) [DONE - 2024-12-30] Centralize NodeId usage - already implemented
     - NodeId is loaded from config.server.node_id in lifecycle.rs
     - Stored in AppContext and accessible via app_context.node_id()
     - All production code uses centralized NodeId (test code uses hardcoded values which is acceptable)

12) [PARTIALLY DONE - 2024-12-30] Replace unwrap()/expect() with proper error handling
     - Fixed health_monitor.rs: get_current_pid().unwrap() -> proper error handling
     - Fixed flush_table.rs: table_def.unwrap() pattern after is_none() check
     - Note: 1230 total unwrap() calls in codebase - focused on critical paths (SQL handlers, jobs, API)
     - Remaining unwraps are mostly in tests or non-critical paths

43) [DONE - 2025-06-14] Role check before execute - already implemented
     - check_authorization() already runs BEFORE execute() in handler_registry.rs
     - Anonymous users blocked at authorization layer, not after reading data
     - Role hierarchy checked via check_authorization before any DDL/DML execution

189) [DONE - 2025-06-14] Subscription parsing via DataFusion/sqlparser
     - Rewrote query_parser.rs in kalamdb-core/src/live/ to use sqlparser-rs AST
     - extract_table_name() now uses Parser::parse_sql() and AST traversal
     - extract_where_clause() uses parsed Query AST instead of string manipulation
     - extract_projections() properly parses SelectItem variants
     - Added proper ObjectNamePart::Identifier handling for schema-qualified names

147) [DONE - 2025-06-14] Async flushing with spawn_blocking
     - Wrapped flush_job.execute() with tokio::task::spawn_blocking in FlushExecutor
     - Wrapped backend.compact_partition() with spawn_blocking
     - Prevents blocking async runtime during RocksDB I/O operations (10-100ms+)
     - Both User and Shared table flush jobs now run in blocking thread pool

184) [DONE - 2025-06-14] File handle leak diagnostics
     - Added FileHandleTracker in kalamdb-filestore/src/file_handle_diagnostics.rs
     - Atomic counters: active_handles, total_opened, total_closed, peak_handles
     - record_open() / record_close() functions with context and path logging
     - check_for_leaks() returns mismatch between opens and closes
     - log_stats_summary() outputs diagnostic info
     - Integrated into remote_materializer.rs for file downloads
     - High handle count warnings at 1000+ open handles

139) [INVESTIGATED - 2025-06-14] ScalarValue instead of JsonValue - DEFERRED
     - Internal storage already uses Row with BTreeMap<String, ScalarValue>
     - serde_json::Value only used at API boundary (append.rs input)
     - json_to_row() converts API input to internal ScalarValue-based Row
     - Full replacement requires API breaking changes - defer to future sprint

114) [DONE - Verified 2024-12-30] Atomic index writes with WriteBatch
     - Already implemented in indexed_store.rs using RocksDB WriteBatch
     - Main table and secondary index writes are atomic via db.write(batch)?

183) [DONE - Verified 2024-12-30] WebSocket payload size limits
     - max_ws_message_size config (default 1MB) in SecuritySettings
     - max_ws_messages_per_second rate limiting (default 50/sec)
     - Configurable via server.toml [security] section

187) [DONE - Verified 2024-12-30] Parameterized queries in SDK
     - Rust client: execute_query(sql, params, namespace_id) with $1, $2 placeholders
     - WASM client: queryWithParams() method
     - TypeScript SDK: wraps WASM client properly
     - Backend ParameterLimits validation (max 50 params, 512KB each)

## Original Completed / Not Relevant Section

12) [NOT RELEVANT - CLI shows proper timing] In cli if a query took took_ms = 0 then it's a failure
18) [NOT RELEVANT - DONE in kalamdb-core/src/live/] Move all code which deals with live subscriptions into kalamdb-live module, like these codes in here: backend/crates/kalamdb-core/src/live_query
22) [NOT RELEVANT - Using bincode] For storing inside rocksdb as bytearray we should use protobuf instead of json
45) [NOT RELEVANT - Already done with ColumnFamilyNames] "system_users" is repeated so many times in the code base we should use a column family enum for all of them, and making all of them as Partition::new("system_users") instead of hardcoding the string multiple times, we already have SystemTable enum we can add ColumnFamily as well
47) [NOT RELEVANT - Using Role enum] No need to map developer to service role we need to use only Role's values
49) [NOT RELEVANT - Uses TableId now] execute_create_table need to take namespaceId currently it creates inside default namespace only, no need to have default namespaceid any place
82) [NOT RELEVANT - DONE] make sure rocksdb is only used inside kalamdb-store
84) [NOT RELEVANT - DONE with handlers/] Divide backend/crates/kalamdb-core/src/sql/executor.rs into multiple files for better maintainability
92) [NOT RELEVANT - Using bincode] Check the datatypes converting between rust to arrow datatypes and to rocksdb its named json i dont want to use json for this, i want fast serdes for datatypes, maybe util.rs need to manage both serialize from parquet to arrow arrow to parquet both wys currently its located inside flush folder
98) [NOT RELEVANT - Now uses _seq column] IMPORTANT - If no primary key found for a table then we will add our own system column _id to be primary key with snowflake id, make sure this is implemented everywhere correctly. If the user already specified primary key then we dont do that, the _id we add also should check if the id is indeed unique as well
104) [NOT RELEVANT - Old structure] backend\crates\kalamdb-core\src\tables\system\tables_v2 is not needed anymore we have schemas which stores the tables/columns, all should be located in the new folder: backend\crates\kalamdb-core\src\tables\system\schemas
107) [NOT RELEVANT - schema_registry consolidated] Check if we can here combine this with our main cache: backend\crates\kalamdb-core\src\sql\registry.rs, or if this needed anymore?
109) [NOT RELEVANT - Removed] why do we need backend/crates/kalamdb-auth/src/user_repo.rs anymore? we have kalamdb-core/src/auth/user_repo.rs
111) [NOT RELEVANT - Done in schema_registry/views/] Add virtualTables module to kalamdb-core/src/tables/virtual_tables to include all virtual tables for example information_schema and other virtual tables we may have in the future, virtual tables should be also registered with schema registry
128) [NOT RELEVANT - DONE with AS USER impersonation] IMPORTANT - Insert a row as a system/service user to a different user_id this will be used for users to send messages to others as well or an ai send to different user or publish to different user's stream table: INSERT INTO <namespace>.<table> [AS USER '<user_id>'] VALUES (...);
132) [NOT RELEVANT - Old columns removed, using _seq/_deleted] add_row_id_column should be removed and all the _updated, _id should be removed from everywhere even deprecation shouldnt be added remove the code completely
133) [NOT RELEVANT - Using typed RowIds] _row_id: &str shouldnt be there we now use SharedTableRowId or UserTableRowId in all places instead of string
136) [NOT RELEVANT - Old columns removed] Cleanup old data from: backend/crates/kalamdb-commons/src/string_interner.rs and also remove everything have to do with old system columns: _row_id, _id, _updated and also have one place which is SystemColumnNames which is in commons to have the word _seq/_deleted in, so we can refer to it from one place only
145) [NOT RELEVANT - Manifests implemented] User tables manifest should be in rocksdb and persisted into storage disk after flush
172) [NOT RELEVANT - Using TableId now] instead of returning: (NamespaceId, TableName) return TableId directly
179) [NOT RELEVANT - views/ exists and is the implementation] No need to have backend\crates\kalamdb-core\src\schema_registry\views since we will be impl;ementing a views which are supported by datafusion, we only persist the view create sql to be applied or run on startup of the server
204) [NOT RELEVANT - Using TableId now] we should use TableId instead of passing both:        namespace: &NamespaceId,table: &TableName, like in update_manifest_after_flush
206) [NOT RELEVANT - Using Moka cache] the last_accessed in manifest is not needed anymore since now we rely on Moka cache for knowing the last accessed time

IMPORTANT Section - Completed:
1) [NOT RELEVANT - DONE] Schema information_schema
2) [NOT RELEVANT - DONE] Datatypes for columns
4) [NOT RELEVANT - DONE] Add manifest file for each user table, that will help us locate which parquet files we need to read in each query, and if in fact we need to read parquet files at all, since sometimes the data will be only inside rocksdb and no need for file io
4) [NOT RELEVANT - DONE] Support update/deleted as a separate join table per user by MAX(_updated)
6) [NOT RELEVANT - DONE] AS USER support for DML statements - to be able to insert/update/delete as a specific user_id (Only service/admin roles can do that)

[NOT RELEVANT - Known issue being addressed] Key Findings
Flush Timing Issue: Data inserted immediately before flush may not be in RocksDB column families yet, resulting in 0 rows flushed
Parquet Querying Limitation: After flush, data is removed from RocksDB but queries don't yet retrieve from Parquet files - this is a known gap

---