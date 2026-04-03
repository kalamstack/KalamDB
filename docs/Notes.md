# KalamDB Tasks & Notes

## ✅ COMPLETED TASKS

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

## 🔒 SECURITY (HIGH PRIORITY)

(See COMPLETED TASKS section for #43 and #189 - now done)


## ⚡ PERFORMANCE & OPTIMIZATION (HIGH/MEDIUM PRIORITY)

53) [HIGH] IMPORTANT: Add a story about the need for giving ability to subscribe for: * which means all users tables at once, this is done by the ai agent which listen to all the user messages at once, add also ability to listen per storageId, for this we need to add to the user message key a userId:rowId:storageId

(See COMPLETED TASKS section for #139, #147, #184 - now done/investigated)

126) [MEDIUM] Combine the 2 shared/user tables flushing and querying using a shared hybrid service which is used in both cases to reduce code duplication and maintenance burden
both of them read from a path and from a store but user table filter the store which is the hot storage based on user id as well
137) [MEDIUM] SharedTableFlushJob AND UserTableFlushJob have so much code duplication we need to combine them into one flush job with some parameters to differ between user/shared table flushing
148) [MEDIUM] Create a compiler for paths templates to avoid doing string replacements each time we need to get the path for a user/table
    we can combine all the places and use this compiler even in tests as well

    there is many places where we need to compile paths:
    1) In User Table output batch parquet file
    2) In Shared Table parquet path
    3) Caching the storage paths for user tables/shared tables
    4) Manifest paths for user/shared tables
    5) All error logic will be checked in the compiler, it should be optomized as much as possible
    6) We can get tableid and also namespaceId/tablename so whenever we are calling it we can get whatever method we need instead of always combining or separating it to fit
    7) Make sure manifest.json file should be next to the other parquet files in the same folder
195) [MEDIUM] we should always have a default order by column so we always have the same vlues returned in the same order, this is important for pagination as well
205) [MEDIUM] Add test which check having like 100 parquet batches per shared table and having manifest file has 100 segments and test the performance

10) [LOW] use hashbrown instead of hashmap for better performance where possible

## 🧹 CODE QUALITY & CLEANUP (HIGH/MEDIUM/LOW PRIORITY)

**High Priority:**
12) [HIGH] Check all unwrap() and expect() calls and replace them with proper error handling

**Medium Priority:**
2) [MEDIUM] Replace all instances of String types for namespace/table names with their respective NamespaceId/TableName, we should use TableId instead of passing both:        namespace: &NamespaceId,table: &TableName, like in update_manifest_after_flush
4) [MEDIUM] Make sure all using UserId/NamespaceId/TableName/TableId/StorageId types instead of raw strings across the codebase
17) [MEDIUM] why we have 2 implementations for flushing: user_table_flush.rs and shared_table_flush.rs can we merge them into one? i believe they share a lot of code, we can reduce maintenance burden by having only one implementation, or can have a parent class with common code and have 2 child classes for each type of flush
46) [MEDIUM] nodeId should be unique and used from the config file and use NodeId enum
99) [MEDIUM] We are still using NodeId::from(format!("node-{}", std::process::id())) in multiple places we should use the same nodeId from config or context everywhere
122) [MEDIUM] in impl JobExecutor for FlushExecutor add generic to the model instead of having json parameters we can have T: DeserializeOwned + Send + Sync + 'static and then we can deserialize into the right struct directly instead of having to parse json each time

**Low Priority:**
6) [LOW] Remove un-needed imports across the codebase
8) [LOW] Check where we use AppContext::get() multiple times in the same struct and make it a member of the struct instead, or if the code already have AppContext as a member use it directly
10) [LOW] Use todo!() instead of unimplemented!() where needed
11) [LOW] Remove all commented code across the codebase
48) [LOW] make sure we use TableAccess
86) [LOW] Make node_id: String, into NodeId type
97) [LOW] check if we have duplicates backend/crates/kalamdb-commons/src/constants.rs and backend/crates/kalamdb-commons/src/system_tables.rs both have system table names defined
100) [LOW] JobId need to be shorter its now using timestamp and uuid which is too long, we can use namespace-table-timestamp or even a snowflake id
103) [LOW] Check to see any libraries/dependencies not needed and rmeove them, check each one of the dependencies
113) [LOW] Check if we need to remove ColumnFamilyManager
115) [LOW] we are having so much AppContext:get() calls in schema_registry add it to the struct as a member and use it in all methods
116) [LOW] some places we have something like this: let storage_id = StorageId::from(statement.storage_id.as_str()); its not needed since storage_id is already a StorageId type
and things like this:
        let name = statement.name.as_str();
        let namespace_id = NamespaceId::new(name);
135) [LOW] There is some places were we have self.app_context and we at the same time refetch the app_context again
138) [LOW] Split into more crates - look at the file crates-splitting.md for more info
140) [LOW] can we get rid of using EntityStore:: and directlky use the desired store? it will be more type safe
141) [LOW] Add a doc file for crates and backend server dependency graph in: docs\architecture, base it on the current code design and add to AGENTS.md to always update the archeticture, this is the base spec for the last change: crates-splitting.md
143) [LOW] Add type-safe modles to ChangeNotification
146) [LOW] Instead of these let partition_name = format!(
                "{}{}:{}",
                ColumnFamilyNames::USER_TABLE_PREFIX,
                table_id.namespace_id().as_str(),
                table_id.table_name().as_str()
            );
    read the partion as it is from a const or static function in commons for all of them
149) [LOW] We currently check if not exists we need to use this boolean for that not the contains one,
    pub if_not_exists: bool,
153) [LOW] SharedTableFlushJob and UserTableFlushJob should take as an input the appContext and the TableId and TableType as an input for creating them
158) [LOW] extract_seq_bounds is duplicated we cna combine it
165) [LOW] No need to have tableType in tableOptions since we already have it as column
181) [LOW] cut the took into 3 decimal places only:
{
  "status": "success",
  "results": [
    {
      "row_count": 1,
      "columns": [],
      "message": "Inserted 1 row(s)"
    }
  ],
  "took": 1.2685000000000002
}
192) [LOW] Remove last_seen from user or update it just only once per day to avoid too many writes to rocksdb for no reason
197) [LOW] why do we have things like this? shouldnt we prevent entering if no rows?
[2025-12-13 01:51:58.957] [INFO ] - main - kalamdb_core::jobs::jobs_manager::utils:38 - [CL-a258332a4315] Job completed: Cleaned up table insert_bench_mj3iu8zz_0:single_mj3iu900_0 successfully - 0 rows deleted, 0 bytes freed
199) [LOW] change the cli history to storing the history of queries as regular queries and not base64 but keeping in mind adding quotes to preserve adding the multi-lines queries, and also replacing password on alter user to remove the password
200) [LOW] in manifest we have duplicated values id and path use only the path:
  "segments": [
    {
      "id": "batch-0.parquet",
      "path": "batch-0.parquet",
201) [LOW] optimize the manifest by doing:
{
  "table_id": {
    "namespace_id": "chat",
    "table_name": "messages"
  },
  "user_id": "root",
  "version": 5,
  "created_at": 1765787805,
  "updated_at": 1765790837,
  "segments": [
    {
      "id": "batch-0.parquet", //TODO: Not needed we are using the path now
      "path": "batch-0.parquet",
      "column_stats": {  //TODO: Change to stats
        "id": {
          "min": 258874983317667840,
          "max": 258874997628633089,
          "null_count": 0
        }
      },
      "min_seq": 258874983321862144,
      "max_seq": 258874997628633091,
      "row_count": 24,  //TODO: Change to rows
      "size_bytes": 2701,  //TODO: Change to bytes
      "created_at": 1765787814, 
      "tombstone": false
    },
  ],
  "last_sequence_number": 3 //TODO: Change to last
}

## 🧪 TESTING & CI/CD (HIGH/MEDIUM PRIORITY)

**Testing - High Priority:**
67) [HIGH] test each role the actions he can do and cannot do, to cover the rbac system well, this should be done from the cli
73) [HIGH] Test creating different users and checking each operation they can do from cli integration tests
159) [HIGH] Add tests to cover the indexes and manifest reading - check if it's actually working and the planner works with indexes now and doesnt read the un-needed parquet files
167) [HIGH] create a cli test to cover all InitialDataOptions and InitialDataResult for stream tables/users table as well
  - last rows limit
  - with multiple batches
  - specify batch size and verify it works correctly
  - with seq bounds
  - with time bounds in streams with ttl passed
  - Check the order of the recieved rows is correct in the batch
  - Check if the rows is correct in the changes notification as well
[HIGH] Make sure there is tests which insert/updte data and then check if the actual data we inserted/updated is there and exists in select then flush the data and check again if insert/update works with the flushed data in cold storage, check that insert fails when inserting a row id primary key which already exists and update do works

**Testing - Medium Priority:**
80) [MEDIUM] More integration tests inside cli tool which covers importing sql files with multiple statements
81) [MEDIUM] CLI - add integration tests which covers a real-life use case for chat app with an ai where we have conversations and messages between users and ai agents, real-time streams for ai thinking/thoughts/typing/user online/offline status, and also flushing messages to parquet files and reloading them back
102) [MEDIUM] CLI Tests common - Verify that we have a timeout set while we wait for the subscription changes/notifications
176) [MEDIUM] Add a test to chekc if we can kill a live_query and verify the user's socket got closed and user disconnected
191) [MEDIUM] In cli tests whenever we have flush directly after it check the storage files manifest.json and the parquet files if they are exists there and the size is not 0, use one function in common which helps with this if not already having one like that

**Testing - Low Priority:**
15) [LOW] for better tracking the integration tests should the names print also the folder path as well with the test name
20) [LOW] For flushing tests first create a storage and direct all the storage into a temporary directory so we can remove it after each flush test to not leave with un-needed temporary data

**CI/CD - Medium Priority:**
2) [MEDIUM] Add code coverage to the new repo

## 🚀 FEATURES - CORE FUNCTIONALITY (HIGH/MEDIUM PRIORITY)

**High Priority:**
3) [HIGH] Parametrized Queries needs to work with ScalarValue and be added to the api endpoint
14) [HIGH] Add upsert support
72) [HIGH] Whenever we drop the namespace remove all tables under it
77) [HIGH] Flush table job got stuck for a long time, need to investigate why and also why the tests don't detect this issue and still passes?!
105) [HIGH] when we have Waiting up to 300s for active flush jobs to complete... and the user click CTRL+C again it will force the stopping and mark those jobs as failed with the right error
150) [HIGH] the expiration or ttl in stream tables is not working at all, i subscribe after 30 seconds and still have the same rows returned to me
162) [HIGH] Whenever we shutdown the server we force all subscrioptions to be closed and websockets as well gracefully with an event set to the user
174) [HIGH] Make sure we flush the table before we alter it to avoid any data loss from a previous schema, also make sure we lock the table for writing/reading while it is being altered

**Medium Priority:**
3) [MEDIUM] Combine all the subscription logic for stream/user tables into one code base
5) [MEDIUM] Storage files compaction
12) [MEDIUM] aDD objectstore for storing files in s3/azure/gcs compatible storages
13) [MEDIUM] Add BEGIN TRANSACTION / COMMIT TRANSACTION support for multiple statements in one transaction, This will make the insert batch faster
39) [MEDIUM] Whenever we create a table we can create it with a specific access policy: public, private, protected
41) [MEDIUM] storing credentials in kalamdb-credentials alongside the url of the database
56) [MEDIUM] Add to README.md
    - Vector database
    - Vector Search
    - Has a deep design for how they should be and also update TASKS.MD and the design here
63) [MEDIUM] check for each system table if the results returned cover all the columns defined in the TableSchema
66) [MEDIUM] Make sure actions like: drop/export/import/flush is having jobs rows when they finishes (TODO: Also check what kind of jobs we have)
68) [MEDIUM] A service user can also create other regular users
70) [MEDIUM] Check cleaning up completed jobs, we already have a config of how long should we retain them
74) [MEDIUM] check if we are using system/kalam catalogs correctly in the datafusion sessions
ctx.register_catalog("system", Arc::new(SystemCatalogProvider::new()));
ctx.register_catalog("app", Arc::new(AppCatalogProvider::new(namespace)));
Also check that registering ctaalogs are done in one place and one session, we shall combine everywhere were we register sessions and ctalogs into one place
83) [MEDIUM] Make sure the sql engine works on the schemas first and from there he can get the actual data of the tables
89) [MEDIUM] When deleting a namespace check if all tables are deleted, re-create the namespace again and check if the tables still exists there, and try to cvreate the same table again and check if it works correctly
91) [MEDIUM] If i set the logging to info inside config.toml i still see debug logs: level = "info"
93) [MEDIUM] Add a new dataType which preserve the timezone info when storing timestamp with timezone
112) [MEDIUM] In JobType add another model for each type with the parameters it should have in the Json in this way we can validate it easily by deserializing into the right struct for each job type
117) [MEDIUM] Make the link client send a X-Request-ID header with a unique id per request for better tracing and debugging
118) [MEDIUM] Add to kalamdb-link client the ability to set custom headers for each request
130) [MEDIUM] We need to have describe table <namespace>.<table> to show the table schema in cli and server as well also to display: cold rows count, hot rows count, total rows count, storage id, primary key(s), indexes, etc
144) [MEDIUM] Shared tables should have a manifest stored in memory as well for better performance with also the same as user table to disk 
155) [MEDIUM] Whenever we drop a table cancel all jobs for this table which are still running or queued
156) [MEDIUM] When there is an sql parsing or any error the parser should return a clear error with maybe line or description of what is it
instead of: 1 failed: Invalid operation: No handler registered for statement type 'UNKNOWN'
157) [MEDIUM] Are we closing all ParquetWriter? whenever we use them?
163) [MEDIUM] If there is any stuck live_query when starting the server clear them all, might be the server crashed without graceful shutdown
169) [MEDIUM] clear_plan_cache should be called in any DDL that is happening
175) [MEDIUM] When altering a table to add/remove columns we need to update the manifest file as well
177) [MEDIUM] The loading of tables and registering its providers is scattered, i want to make it one place for on server starts and on create table
178) [MEDIUM] Flushing - Maybe we need to change the flush to look at the active manifests and from them we can know what needs to be flushed instead of scaning the whole hot rows, this way we can make sure all the manifests are flushed as well.
180) [MEDIUM] For jobs add a method for each executor called: preValidate which check if we should create that job or not, so we can check if we need to evict data or there is no need, this will not need a created job to run
190) [MEDIUM] NamespaceId should be maximum of 32 characters only to avoid long names which may cause issues in file systems
17) [MEDIUM] Persist views in the system_views table and load them on database starts
203) [MEDIUM] Can you check if we can use the manifest as an indication of having rows which needs flushing or you think its better to keep it this way which is now? if we flush and we didnt find any manifest does it fails? can you make sure this scenario is well written?

## 🎨 FEATURES - USER EXPERIENCE (MEDIUM/LOW PRIORITY)

**CLI - Medium Priority:**
13) [MEDIUM] In the cli add a command to show all live queries
14) [MEDIUM] In the cli add a command to kill a live query by its live id

**CLI - Low Priority:**
7) [LOW] when reading --file todo-app.sql from the cli ignore the lines with -- since they are comments, and create a test to cover this case
37) [LOW] Make the cli connect to the root user by default
40) [LOW] Add to the cli a command session which will show the current user all info, namespace, and other session related info.
42) [LOW] CLI should also support a regular user as well and not only the root user
75) [LOW] Fix cli highlight select statements
76) [LOW] Fix auto-complete in cli
166) [LOW] Add cli clear command which clears the console previous output
170) [LOW] Make a way to set the namespace once per session and then we can use it in the next queries

**UI - Medium Priority:**
1) [MEDIUM] The browser should fetch all namespaces/tables/columns in one query not multiple queries for better performance
3) [MEDIUM] even when there is an error display the error alone with time took display separatly, we need to be similar to the cli
10) [MEDIUM] whenever there is an error in query display only the error not the json: {"status":"error","results":[],"took":4.9274000000000004,"error":{"code":"SQL_EXECUTION_ERROR","message":"Statement 1 failed: Execution error: Schema error: No field named i44d. Valid fields are chat.messages.id, chat.messages.conversation_id, chat.messages.role_id, chat.messages.content, chat.messages.created_at, chat.messages._seq, chat.messages._deleted.","details":"select * from chat.messages where i44d = 256810499606478848"}}

**UI - Low Priority:**
2) [LOW] Display the type of table stream/shared/user tables with icon per type
9) [LOW] whenever subscribing: Live - Subscribed [2:11:04 PM] INSERT: +1 rows (total: 16) add another button which you can open a dialog to view all the mesages/logging which was recieved from the websocket subscription
11) [LOW] currently when stoping subscription the live checkbox is turned off automatically, it should stay on
12) [LOW] The table cells should be selectable when clicking on them, and on each cell you can right mouse click -> view data if it has more data there
13) [LOW] when subscribing the first column which is the type should indicate its not an actual column which the user can query its only indication what event type is this, also add to it's left side a timestamp column to indicate when this event came, and whenever a new change is added the newly added row should splash

**General - Low Priority:**
1) [LOW] Alter a table and move it's storage from storage_id to different one
2) [LOW] Support changing stream table TTL via ALTER TABLE
33) [LOW] Add option for a specific user to download all his data this is done with an endpoint in rest api which will create a zip file with all his tables data in parquet format and then provide a link to download it
34) [LOW] Add to the roadmap adding join which can join tables: shared<->shared, shared<->user, user<->user, user<->stream
35) [LOW] Add to cli/server a version which will print the commit and build date as well which is auto-increment: add prompt instead of this one: Starting KalamDB Server v0.1.0
36) [LOW] update all packages to the latest available version
38) [LOW] Force always including namespace to the queries alongside the tableName
54) [LOW] Mention in the README.md that instead of using redis/messaging system/database you can use one for all of these, and subscribing directly to where your messages are stored in an easy way
69) [LOW] Server click ctrl+z two times will force kill even if it's still flushing or doing some job
78) [LOW] Support an endpoint for exporting user data as a whole in a zip file
90) [LOW] Create/alter table support table doesnt return the actual rows affected count
95) [LOW] while: [2025-11-01 23:55:16.242] [INFO ] - main - kalamdb_server::lifecycle:413 - Waiting up to 300s for active flush jobs to complete...
display what active jobs we are waiting on
110) [LOW] Instead of having system.<system table> we can use sys.<system table> for less typing and easier to remember
123) [LOW] Query users table doesnt return _deleted columns only the deleted_at date
182) [LOW] Add to the README.md an example for managing notifications in a mobile app
188) [LOW] Check why websocket is not using the http2 protocol even if we set it in the config file

## 🔮 FUTURE FEATURES (LOW PRIORITY)

7) [LOW - FUTURE] Vector Search + HNSW indexing with deletes support
11) [LOW - FUTURE] Investigate using vortex instead of parquet or as an option for the user to choose which format to use for storing flushed data
15) [LOW - FUTURE] Support postgress protocol
16) [LOW - FUTURE] Add file DataType for storing files/blobs next to the storage parquet files
24) [LOW] Check if we can replace rocksdb with this one: https://github.com/foyer-rs/foyer, it already support objectstore so we can also store the non-flushed tables into s3 directly, and not forcing flushing when server goes down, even whenever we use the filesystem we can rely on the same logic inside foyer as well

[LOW - FUTURE FEATURE] Here's the updated 5-line spec with embedding storage inside Parquet and managed HNSW indexing (with delete handling):
	1.	Parquet Storage: All embeddings are stored as regular columns in the Parquet file alongside other table columns to keep data unified and versioned per batch.
	2.	Temp Indexing: On each row insert/update, serialize embeddings into a temporary .hnsw file under /tmp/kalamdb/{namespace}/{table}/{column}-hot_index.hnsw for fast incremental indexing.
	3.	Flush Behavior: During table flush, if {table}/{column}-index.hnsw doesn't exist, create it from all embeddings in the Parquet batches; otherwise, load and append new vectors while marking any deleted rows in the index.
	4.	Search Integration: Register a DataFusion scalar function vector_search(column, query_vector, top_k) that loads the HNSW index, filters out deleted entries, and returns nearest row IDs + distances.
	5.	Job System Hook: Add an async background IndexUpdateJob triggered post-flush to merge temporary indexes, apply deletions, and update last_indexed_batch metadata for each table column.

## ✅ COMPLETED / NOT RELEVANT

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

**IMPORTANT Section - Completed:**
1) [NOT RELEVANT - DONE] Schema information_schema
2) [NOT RELEVANT - DONE] Datatypes for columns
4) [NOT RELEVANT - DONE] Add manifest file for each user table, that will help us locate which parquet files we need to read in each query, and if in fact we need to read parquet files at all, since sometimes the data will be only inside rocksdb and no need for file io
4) [NOT RELEVANT - DONE] Support update/deleted as a separate join table per user by MAX(_updated)
6) [NOT RELEVANT - DONE] AS USER support for DML statements - to be able to insert/update/delete as a specific user_id (Only service/admin roles can do that)

[NOT RELEVANT - Known issue being addressed] Key Findings
Flush Timing Issue: Data inserted immediately before flush may not be in RocksDB column families yet, resulting in 0 rows flushed
Parquet Querying Limitation: After flush, data is removed from RocksDB but queries don't yet retrieve from Parquet files - this is a known gap

---

## 📊 BENCHMARK RESULTS

┌────────────────────────────────────────────────────────────┐
│                    BENCHMARK RESULTS                       │
├────────────────────────────────────────────────────────────┤
│  Test Type              │  Rows   │  Time    │  Rate       │
├────────────────────────────────────────────────────────────┤
│  Single-row inserts     │    200  │    0.13s │    1532.5/s │
│  Batched (100/batch)    │   2000  │    1.84s │    1088.3/s │
│  Parallel (10 threads)  │   1000  │    0.26s │    3878.1/s │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                    BENCHMARK RESULTS                       │
├────────────────────────────────────────────────────────────┤
│  Test Type              │  Rows   │  Time    │  Rate       │
├────────────────────────────────────────────────────────────┤
│  Single-row inserts     │    200  │    0.14s │    1445.0/s │
│  Batched (100/batch)    │   2000  │    1.63s │    1229.0/s │
│  Parallel (10 threads)  │   1000  │    0.20s │    5119.9/s │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                    BENCHMARK RESULTS                       │
├────────────────────────────────────────────────────────────┤
│  Test Type              │  Rows   │  Time    │  Rate       │
├────────────────────────────────────────────────────────────┤
│  Single-row inserts     │    200  │    0.09s │    2260.2/s │
│  Batched (100/batch)    │   2000  │    0.03s │   73093.1/s │
│  Parallel (10 threads)  │    980  │    0.09s │   10943.2/s │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                    BENCHMARK RESULTS                       │
├────────────────────────────────────────────────────────────┤
│  Test Type              │  Rows   │  Time    │  Rate       │
├────────────────────────────────────────────────────────────┤
│  Single-row inserts     │    200  │    0.09s │    2288.5/s │
│  Batched (100/batch)    │   2000  │    0.04s │   51687.4/s │
│  Parallel (10 threads)  │   1000  │    0.09s │   11409.2/s │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                    BENCHMARK RESULTS                       │
├────────────────────────────────────────────────────────────┤
│  Test Type              │  Rows   │  Time    │  Rate       │
├────────────────────────────────────────────────────────────┤
│  Single-row inserts     │    200  │    0.30s │     675.8/s │
│  Batched (100/batch)    │   2000  │    0.04s │   45610.3/s │
│  Parallel (10 threads)  │      1  │    0.47s │       2.1/s │
└────────────────────────────────────────────────────────────┘

14) [MEDIUM] In the cli add a command to kill a live query by its live id
15) [LOW] for better tracking the integration tests should the names print also the folder path as well with the test name
17) [MEDIUM] why we have 2 implementations for flushing: user_table_flush.rs and shared_table_flush.rs can we merge them into one? i believe they share a lot of code, we can reduce maintenance burden by having only one implementation, or can have a parent class with common code and have 2 child classes for each type of flush
18) [NOT RELEVANT - DONE in kalamdb-core/src/live/] Move all code which deals with live subscriptions into kalamdb-live module, like these codes in here: backend/crates/kalamdb-core/src/live_query
20) [LOW] For flushing tests first create a storage and direct all the storage into a temporary directory so we can remove it after each flush test to not leave with un-needed temporary data
22) [NOT RELEVANT - Using bincode] For storing inside rocksdb as bytearray we should use protobuf instead of json
24) [LOW] Check if we can replace rocksdb with this one: https://github.com/foyer-rs/foyer, it already support objectstore so we can also store the non-flushed tables into s3 directly, and not forcing flushing when server goes down, even whenever we use the filesystem we can rely on the same logic inside foyer as well

33) [LOW] Add option for a specific user to download all his data this is done with an endpoint in rest api which will create a zip file with all his tables data in parquet format and then provide a link to download it
34) [LOW] Add to the roadmap adding join which can join tables: shared<->shared, shared<->user, user<->user, user<->stream
35) [LOW] Add to cli/server a version which will print the commit and build date as well which is auto-increment: add prompt instead of this one: Starting KalamDB Server v0.1.0
36) [LOW] update all packages to the latest available version
37) [LOW] Make the cli connect to the root user by default
38) [LOW] Force always including namespace to the queries alongside the tableName
39) [MEDIUM] Whenever we create a table we can create it with a specific access policy: public, private, protected
40) [LOW] Add to the cli a command session which will show the current user all info, namespace, and other session related info.
41) [MEDIUM] storing credentials in kalamdb-credentials alongside the url of the database
42) [LOW] CLI should also support a regular user as well and not only the root user
43) [HIGH] Whenever a user send a query/sql statement first of all we check the role he has if he is creating create/alter tables then we first check the user role before we display an error like: namespace does not exists, maybe its better to include in these CREATE/ALTER sql also which roles can access them so we dont read data from untrusted users its a sensitive topic.
45) [NOT RELEVANT - Already done with ColumnFamilyNames] "system_users" is repeated so many times in the code base we should use a column family enum for all of them, and making all of them as Partition::new("system_users") instead of hardcoding the string multiple times, we already have SystemTable enum we can add ColumnFamily as well
46) [MEDIUM] nodeId should be unique and used from the config file and use NodeId enum
47) [NOT RELEVANT - Using Role enum] No need to map developer to service role we need to use only Role's values
48) [LOW] make sure we use TableAccess
49) [NOT RELEVANT - Uses TableId now] execute_create_table need to take namespaceId currently it creates inside default namespace only, no need to have default namespaceid any place
50) [HIGH] anonymous user shouldnt be allowed to create tables or do anything except select from public tables
53) [HIGH] IMPORTANT: Add a story about the need for giving ability to subscribe for: * which means all users tables at once, this is done by the ai agent which listen to all the user messages at once, add also ability to listen per storageId, for this we need to add to the user message key a userId:rowId:storageId
54) [LOW] Mention in the README.md that instead of using redis/messaging system/database you can use one for all of these, and subscribing directly to where your messages are stored in an easy way
55) [HIGH] Check the queries coming and scan for vulnerability limit the string content length
56) [MEDIUM] Add to README.md
    - Vector database
    - Vector Search
    - Has a deep design for how they should be and also update TASKS.MD and the design here


63) [MEDIUM] check for each system table if the results returned cover all the columns defined in the TableSchema
66) [MEDIUM] Make sure actions like: drop/export/import/flush is having jobs rows when they finishes (TODO: Also check what kind of jobs we have)
67) [HIGH] test each role the actions he can do and cannot do, to cover the rbac system well, this should be done from the cli
68) [MEDIUM] A service user can also create other regular users
69) [LOW] Server click ctrl+z two times will force kill even if it's still flushing or doing some job
70) [MEDIUM] Check cleaning up completed jobs, we already have a config of how long should we retain them
71) [HIGH] When flushing user table flush only the user who is requesting the flush to happen
72) [HIGH] Whenever we drop the namespace remove all tables under it
73) [HIGH] Test creating different users and checking each operation they can do from cli integration tests
74) [MEDIUM] check if we are using system/kalam catalogs correctly in the datafusion sessions
ctx.register_catalog("system", Arc::new(SystemCatalogProvider::new()));
ctx.register_catalog("app", Arc::new(AppCatalogProvider::new(namespace)));
Also check that registering ctaalogs are done in one place and one session, we shall combine everywhere were we register sessions and ctalogs into one place

75) [LOW] Fix cli highlight select statements
76) [LOW] Fix auto-complete in cli
77) [HIGH] Flush table job got stuck for a long time, need to investigate why and also why the tests don't detect this issue and still passes?!
78) [LOW] Support an endpoint for exporting user data as a whole in a zip file
79) [MEDIUM] Need to scan the code to make it more lighweight and less dependencies, by doing that we can lower the file binary size and also memory consumption
80) [MEDIUM] More integration tests inside cli tool which covers importing sql files with multiple statements
81) [MEDIUM] CLI - add integration tests which cover a real-life use case for chat app with an ai where we have conversations and messages between users and ai agents, real-time streams for ai thinking/thoughts/typing/user online/offline status, and also flushing messages to parquet files and reloading them back
82) [NOT RELEVANT - DONE] make sure rocksdb is only used inside kalamdb-store
83) [MEDIUM] Make sure the sql engine works on the schemas first and from there he can get the actual data of the tables
84) [NOT RELEVANT - DONE with handlers/] Divide backend/crates/kalamdb-core/src/sql/executor.rs into multiple files for better maintainability
85) [LOW] Jobs model add NamespaceId type
86) [LOW] Make node_id: String, into NodeId type
87) [MEDIUM] implement a service which answer these kind of things: fn get_memory_usage_mb() and will be used in stats/jobs memory/cpu and other places when needed
89) [MEDIUM] When deleting a namespace check if all tables are deleted, re-create the namespace again and check if the tables still exists there, and try to cvreate the same table again and check if it works correctly
90) [LOW] Create/alter table support table doesnt return the actual rows affected count
91) [MEDIUM] If i set the logging to info inside config.toml i still see debug logs: level = "info"
92) [NOT RELEVANT - Using bincode] Check the datatypes converting between rust to arrow datatypes and to rocksdb its named json i dont want to use json for this, i want fast serdes for datatypes, maybe util.rs need to manage both serialize from parquet to arrow arrow to parquet both wys currently its located inside flush folder
93) [MEDIUM] Add a new dataType which preserve the timezone info when storing timestamp with timezone
95) [LOW] while: [2025-11-01 23:55:16.242] [INFO ] - main - kalamdb_server::lifecycle:413 - Waiting up to 300s for active flush jobs to complete...
display what active jobs we are waiting on
97) [LOW] check if we have duplicates backend/crates/kalamdb-commons/src/constants.rs and backend/crates/kalamdb-commons/src/system_tables.rs both have system table names defined
98) [NOT RELEVANT - Now uses _seq column] IMPORTANT - If no primary key found for a table then we will add our own system column _id to be primary key with snowflake id, make sure this is implemented everywhere correctly
If the user already specified primary key then we dont do that, the _id we add also should check if the id is indeed unique as well
99) [MEDIUM] We are still using NodeId::from(format!("node-{}", std::process::id())) in multiple places we should use the same nodeId from config or context everywhere
100) [LOW] JobId need to be shorter its now using timestamp and uuid which is too long, we can use namespace-table-timestamp or even a snowflake id

102) [MEDIUM] CLI Tests common - Verify that we have a timeout set while we wait for the subscription changes/notifications
103) [LOW] Check to see any libraries/dependencies not needed and rmeove them, check each one of the dependencies
104) [NOT RELEVANT - Old structure] backend\crates\kalamdb-core\src\tables\system\tables_v2 is not needed anymore we have schemas which stores the tables/columns, all should be located in the new folder: backend\crates\kalamdb-core\src\tables\system\schemas
105) [HIGH] when we have Waiting up to 300s for active flush jobs to complete... and the user click CTRL+C again it will force the stopping and mark those jobs as failed with the right error

107) [NOT RELEVANT - schema_registry consolidated] Check if we can here combine this with our main cache: backend\crates\kalamdb-core\src\sql\registry.rs, or if this needed anymore?
108) [HIGH] Prevent creating namespace with names like: sys/system/root/kalamdb/kalam/main/default/sql and name these as SYSTEM_RESERVED_NAMES, also add function to Namespaceid.isSystem() to check if the namespace is a system one
109) [NOT RELEVANT - Removed] why do we need backend/crates/kalamdb-auth/src/user_repo.rs anymore? we have kalamdb-core/src/auth/user_repo.rs
110) [LOW] Instead of having system.<system table> we can use sys.<system table> for less typing and easier to remember
111) [NOT RELEVANT - Done in schema_registry/views/] Add virtualTables module to kalamdb-core/src/tables/virtual_tables to include all virtual tables for example information_schema and other virtual tables we may have in the future, virtual tables should be also registered with schema registry
112) [MEDIUM] In JobType add another model for each type with the parameters it should have in the Json in this way we can validate it easily by deserializing into the right struct for each job type
113) [LOW] Check if we need to remove ColumnFamilyManager
114) [HIGH] Make sure when we are writing to a table with secondary index we do it in a transaction style like this:
Transaction:
  1️⃣ Insert actual value into main table (or CF)
  2️⃣ Insert corresponding key/value into secondary index CF
  3️⃣ Commit atomically
115) [LOW] we are having so much AppContext:get() calls in schema_registry add it to the struct as a member and use it in all methods
116) [LOW] some places we have something like this: let storage_id = StorageId::from(statement.storage_id.as_str()); its not needed since storage_id is already a StorageId type
and things like this:
        let name = statement.name.as_str();
        let namespace_id = NamespaceId::new(name);

122) [MEDIUM] in impl JobExecutor for FlushExecutor add generic to the model instead of having json parameters we can have T: DeserializeOwned + Send + Sync + 'static and then we can deserialize into the right struct directly instead of having to parse json each time

123) [LOW] Query users table doesnt return _deleted columns only the deleted_at date

126) [MEDIUM] Combine the 2 shared/user tables flushing and querying using a shared hybrid service which is used in both cases to reduce code duplication and maintenance burden
both of them read from a path and from a store but user table filter the store which is the hot storage based on user id as well

130) [MEDIUM] We need to have describe table <namespace>.<table> to show the table schema in cli and server as well also to display: cold rows count, hot rows count, total rows count, storage id, primary key(s), indexes, etc

132) [NOT RELEVANT - Old columns removed, using _seq/_deleted] add_row_id_column should be removed and all the _updated, _id should be removed from everywhere even deprecation shouldnt be added remove the code completely
133) [NOT RELEVANT - Using typed RowIds] _row_id: &str shouldnt be there we now use SharedTableRowId or UserTableRowId in all places instead of string
134) [MEDIUM] Instead of passing namespace/table_name and also tableid pass only TableId also places where there is both of NamespaceId and TableName pass TableId instead  
    namespace_id: &NamespaceId, //TODO: Remove we have TableId
    table_name: &TableName, //TODO: Remove we have TableId
    table_id: &TableId,

135) [LOW] There is some places were we have self.app_context and we at the same time refetch the app_context again

136) [NOT RELEVANT - Old columns removed] Cleanup old data from: backend/crates/kalamdb-commons/src/string_interner.rs and also remove everything have to do with old system columns: _row_id, _id, _updated and also have one place which is SystemColumnNames which is in commons to have the word _seq/_deleted in, so we can refer to it from one place only

137) [MEDIUM] SharedTableFlushJob AND UserTableFlushJob have so much code duplication we need to combine them into one flush job with some parameters to differ between user/shared table flushing

138) [LOW] Split into more crates - look at the file crates-splitting.md for more info

139) [HIGH] Instead of using JsonValue for the fields use arrow Array directly for better performance and less serdes overhead: HashMap<String, ScalarValue> should solve this issue completely.
then we wont be needing: json_to_scalar_value
SqlRequest will use the same thing as well
ColumnDefault will use ScalarValue directly as well
FilterPredicate will use ScalarValue directly as well
json_rows_to_arrow_batch will be removed completely or less code since we will be using arrow arrays directly
scalar_value_to_json will be removed completely as well
ServerMessage will use arrow arrays directly as well


140) [LOW] can we get rid of using EntityStore:: and directlky use the desired store? it will be more type safe
141) [LOW] Add a doc file for crates and backend server dependency graph in: docs\architecture, base it on the current code design and add to AGENTS.md to always update the archeticture, this is the base spec for the last change: crates-splitting.md

142) [MEDIUM] make get_storage_path return PathBuf and any function which responsible for paths and storage should be the same using PathBuf instead of String for better path handling across different OSes, resolve_storage_path_for_user,     pub storage_path_template: String, 

143) [LOW] Add type-safe modles to ChangeNotification
144) [MEDIUM] Shared tables should have a manifest stored in memory as well for better performance with also the same as user table to disk 
145) [NOT RELEVANT - Manifests implemented] User tables manifest should be in rocksdb and persisted into storage disk after flush

147) [HIGH] When flushing shouldnt i do that async? so i dont block while waiting for rocksdb flushing or deleting folder to finish

148) [MEDIUM] Create a compiler for paths templates to avoid doing string replacements each time we need to get the path for a user/table
    we can combine all the places and use this compiler even in tests as well

    there is many places where we need to compile paths:
    1) In User Table output batch parquet file
    2) In Shared Table parquet path
    3) Caching the storage paths for user tables/shared tables
    4) Manifest paths for user/shared tables
    5) All error logic will be checked in the compiler, it should be optomized as much as possible
    6) We can get tableid and also namespaceId/tablename so whenever we are calling it we can get whatever method we need instead of always combining or separating it to fit
    7) Make sure manifest.json file should be next to the other parquet files in the same folder


149) [LOW] We currently check if not exists we need to use this boolean for that not the contains one,
    pub if_not_exists: bool,

150) [HIGH] the expiration or ttl in stream tables is not working at all, i subscribe after 30 seconds and still have the same rows returned to me

151) [HIGH] Add field and tablename naming convention and system reserved words like you can't create namespace with name system/sys/root/kalamdb/main/default etc or field names with _row_id/_id/_updated etc or anything which starts with _ or spaces or special characters
Create a validation function to be used in create/alter table for namespace/table/column names
The reserved words should be in commons in one place for the whole codebase to refer to it from one place only
Also make sure we dont add the "id" column as we used to do before, we rely solely on _seq system column for uniqueness and rely on the user's own fields adding

153) [LOW] SharedTableFlushJob and UserTableFlushJob should take as an input the appContext and the TableId and TableType as an input for creating them

155) [MEDIUM] Whenever we drop a table cancel all jobs for this table which are still running or queued

156) [MEDIUM] When there is an sql parsing or any error the parser should return a clear error with maybe line or description of what is it
instead of: 1 failed: Invalid operation: No handler registered for statement type 'UNKNOWN'


157) [MEDIUM] Are we closing all ParquetWriter? whenever we use them?
158) [LOW] extract_seq_bounds is duplicated we cna combine it
159) [HIGH] Add tests to cover the indexes and manifest reading - check if it's actually working and the planner works with indexes now and doesnt read the un-needed parquet files

162) [HIGH] Whenever we shutdown the server we force all subscrioptions to be closed and websockets as well gracefully with an event set to the user

163) [MEDIUM] If there is any stuck live_query when starting the server clear them all, might be the server crashed without graceful shutdown

165) [LOW] No need to have tableType in tableOptions since we already have it as column

166) [LOW] Add cli clear command which clears the console previous output
167) [HIGH] create a cli test to cover all InitialDataOptions and InitialDataResult for stream tables/users table as well
  - last rows limit
  - with multiple batches
  - specify batch size and verify it works correctly
  - with seq bounds
  - with time bounds in streams with ttl passed
  - Check the order of the recieved rows is correct in the batch
  - Check if the rows is correct in the changes notification as well



169) [MEDIUM] clear_plan_cache should be called in any DDL that is happening
170) [LOW] Make a way to set the namespace once per session and then we can use it in the next queries

172) [NOT RELEVANT - Using TableId now] instead of returning: (NamespaceId, TableName) return TableId directly
174) [HIGH] Make sure we flush the table before we alter it to avoid any data loss from a previous schema, also make sure we lock the table for writing/reading while it is being altered
175) [MEDIUM] When altering a table to add/remove columns we need to update the manifest file as well
176) [MEDIUM] Add a test to chekc if we can kill a live_query and verify the user's socket got closed and user disconnected

177) [MEDIUM] The loading of tables and registering its providers is scattered, i want to make it one place for on server starts and on create table

178) [MEDIUM] Flushing - Maybe we need to change the flush to look at the active manifests and from them we can know what needs to be flushed instead of scaning the whole hot rows, this way we can make sure all the manifests are flushed as well.

179) [NOT RELEVANT - views/ exists and is the implementation] No need to have backend\crates\kalamdb-core\src\schema_registry\views since we will be impl;ementing a views which are supported by datafusion, we only persist the view create sql to be applied or run on startup of the server

180) [MEDIUM] For jobs add a method for each executor called: preValidate which check if we should create that job or not, so we can check if we need to evict data or there is no need, this will not need a created job to run


181) [LOW] cut the took into 3 decimal places only:
{
  "status": "success",
  "results": [
    {
      "row_count": 1,
      "columns": [],
      "message": "Inserted 1 row(s)"
    }
  ],
  "took": 1.2685000000000002
}

182) [LOW] Add to the README.md an example for managing notifications in a mobile app
183) [HIGH] in WebSocketSession limit the size of the request coming from the client to avoid dos attacks

184) [HIGH] i see when the system is idle again after a high load Open Files: 421 this is too high we need to investigate why and make sure we close all file handles correctly, add a logging or display logs when we request it to see where its leaking from

186) [MEDIUM] delete_by_connection_id_async should use an index in live_queries table instead of scanning all the rows to find the matching connection_id

187) [HIGH] Add to the link libr and the sdk an ability to pass a query with parameters to avoid sql injection attacks and support caching of the sql in the backend

188) [LOW] Check why websocket is not using the http2 protocol even if we set it in the config file

189) [HIGH] For subscription parsing the query should be done with datafusion even the parsing of the tableName to avoid any sql injection attacks, and re-add the projections as well and support parameters

190) [MEDIUM] NamespaceId should be maximum of 32 characters only to avoid long names which may cause issues in file systems

191) [MEDIUM] In cli tests whenever we have flush directly after it check the storage files manifest.json and the parquet files if they are exists there and the size is not 0, use one function in common which helps with this if not already having one like that

192) [LOW] Remove last_seen from user or update it just only once per day to avoid too many writes to rocksdb for no reason

194) [HIGH] Block update/insert/delete directly on system tables like users/namespaces/tables/live_queries

195) [MEDIUM] we should always have a default order by column so we always have the same vlues returned in the same order, this is important for pagination as well

197) [LOW] why do we have things like this? shouldnt we prevent entering if no rows?
[2025-12-13 01:51:58.957] [INFO ] - main - kalamdb_core::jobs::jobs_manager::utils:38 - [CL-a258332a4315] Job completed: Cleaned up table insert_bench_mj3iu8zz_0:single_mj3iu900_0 successfully - 0 rows deleted, 0 bytes freed

199) [LOW] change the cli history to storing the history of queries as regular queries and not base64 but keeping in mind adding quotes to preserve adding the multi-lines queries, and also replacing password on alter user to remove the password

200) [LOW] in manifest we have duplicated values id and path use only the path:
  "segments": [
    {
      "id": "batch-0.parquet",
      "path": "batch-0.parquet",


201) [LOW] optimize the manifest by doing:
{
  "table_id": {
    "namespace_id": "chat",
    "table_name": "messages"
  },
  "user_id": "root",
  "version": 5,
  "created_at": 1765787805,
  "updated_at": 1765790837,
  "segments": [
    {
      "id": "batch-0.parquet", //TODO: Not needed we are using the path now
      "path": "batch-0.parquet",
      "column_stats": {  //TODO: Change to stats
        "id": {
          "min": 258874983317667840,
          "max": 258874997628633089,
          "null_count": 0
        }
      },
      "min_seq": 258874983321862144,
      "max_seq": 258874997628633091,
      "row_count": 24,  //TODO: Change to rows
      "size_bytes": 2701,  //TODO: Change to bytes
      "created_at": 1765787814, 
      "tombstone": false
    },
  ],
  "last_sequence_number": 3 //TODO: Change to last
}

203) [MEDIUM] Can you check if we can use the manifest as an indication of having rows which needs flushing or you think its better to keep it this way which is now? if we flush and we didnt find any manifest does it fails? can you make sure this scenario is well written?

204) [NOT RELEVANT - Using TableId now] we should use TableId instead of passing both:        namespace: &NamespaceId,table: &TableName, like in update_manifest_after_flush


205) [MEDIUM] Add test which check having like 100 parquet batches per shared table and having manifest file has 100 segments and test the performance

[HIGH] Make sure there is tests which insert/updte data and then check if the actual data we inserted/updated is there and exists in select then flush the data and check again if insert/update works with the flushed data in cold storage, check that insert fails when inserting a row id primary key which already exists and update do works


Here’s the updated 5-line spec with embedding storage inside Parquet and managed HNSW indexing (with delete handling):
	1.	Parquet Storage: All embeddings are stored as regular columns in the Parquet file alongside other table columns to keep data unified and versioned per batch.
	2.	Temp Indexing: On each row insert/update, serialize embeddings into a temporary .hnsw file under /tmp/kalamdb/{namespace}/{table}/{column}-hot_index.hnsw for fast incremental indexing.
	3.	Flush Behavior: During table flush, if {table}/{column}-index.hnsw doesn’t exist, create it from all embeddings in the Parquet batches; otherwise, load and append new vectors while marking any deleted rows in the index.
	4.	Search Integration: Register a DataFusion scalar function vector_search(column, query_vector, top_k) that loads the HNSW index, filters out deleted entries, and returns nearest row IDs + distances.
	5.	Job System Hook: Add an async background IndexUpdateJob triggered post-flush to merge temporary indexes, apply deletions, and update last_indexed_batch metadata for each table column.



IMPORTANT:
1) [NOT RELEVANT - DONE] Schema information_schema
2) [NOT RELEVANT - DONE] Datatypes for columns
3) [HIGH] Parametrized Queries needs to work with ScalarValue and be added to the api endpoint
4) [NOT RELEVANT - DONE] Add manifest file for each user table, that will help us locate which parquet files we need to read in each query, and if in fact we need to read parquet files at all, since sometimes the data will be only inside rocksdb and no need for file io
4) [NOT RELEVANT - DONE] Support update/deleted as a separate join table per user by MAX(_updated)
5) [MEDIUM] Storage files compaction
6) [NOT RELEVANT - DONE] AS USER support for DML statements - to be able to insert/update/delete as a specific user_id (Only service/admin roles can do that)
7) [LOW - FUTURE] Vector Search + HNSW indexing with deletes support
8) Now configs are centralized inside AppContext and accessible everywhere easily, we need to check:
  - All places where we read config from file directly and change them to read from AppContext
  - Remove any duplicate config models which is a dto and use only the configs instead of mirroring it to different structs

9) [MEDIUM] Partial - In impl JobExecutor for FlushExecutor add generic to the model instead of having json parameters we can have T: DeserializeOwned + Send + Sync + 'static and then we can deserialize into the right struct directly instead of having to parse json each time

10) [LOW] use hashbrown instead of hashmap for better performance where possible
11) [LOW - FUTURE] Investigate using vortex instead of parquet or as an option for the user to choose which format to use for storing flushed data
12) [MEDIUM] aDD objectstore for storing files in s3/azure/gcs compatible storages
13) [MEDIUM] Add BEGIN TRANSACTION / COMMIT TRANSACTION support for multiple statements in one transaction, This will make the insert batch faster
14) [HIGH] Add upsert support
15) [LOW - FUTURE] Support postgress protocol
16) [LOW - FUTURE] Add file DataType for storing files/blobs next to the storage parquet files
17) [MEDIUM] Persist views in the system_views table and load them on database starts
18) [HIGH] Remove the usage of scan_all from EntityStore and replace all calls to always include filter or limit and check ability to have a stream instead of returning a vector


Code Cleanup Operations:
2) [MEDIUM] Replace all instances of String types for namespace/table names with their respective NamespaceId/TableName, we should use TableId instead of passing both:        namespace: &NamespaceId,table: &TableName, like in update_manifest_after_flush
3) [MEDIUM] Instead of passing to a method both NamespaceId and TableName, pass only TableId
4) [MEDIUM] Make sure all using UserId/NamespaceId/TableName/TableId/StorageId types instead of raw strings across the codebase
6) [LOW] Remove un-needed imports across the codebase
7) [HIGH] Fix all clippy warnings and errors
8) [LOW] Check where we use AppContext::get() multiple times in the same struct and make it a member of the struct instead, or if the code already have AppContext as a member use it directly
9) [HIGH] Use clippy suggestions to improve code quality
10) [LOW] Use todo!() instead of unimplemented!() where needed
11) [LOW] Remove all commented code across the codebase
12) [HIGH] Check all unwrap() and expect() calls and replace them with proper error handling
13) [LOW] make sure "_seq" and "_deleted" we use the enums statics instead of strings


Tasks To Repo:
1) [HIGH] Add ci/cd pipelines to the new repo
2) [MEDIUM] Add code coverage to the new repo


┌────────────────────────────────────────────────────────────┐
│                    BENCHMARK RESULTS                       │
├────────────────────────────────────────────────────────────┤
│  Test Type              │  Rows   │  Time    │  Rate       │
├────────────────────────────────────────────────────────────┤
│  Single-row inserts     │    200  │    0.09s │    2260.2/s │
│  Batched (100/batch)    │   2000  │    0.03s │   73093.1/s │
│  Parallel (10 threads)  │    980  │    0.09s │   10943.2/s │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                    BENCHMARK RESULTS                       │
├────────────────────────────────────────────────────────────┤
│  Test Type              │  Rows   │  Time    │  Rate       │
├────────────────────────────────────────────────────────────┤
│  Single-row inserts     │    200  │    0.09s │    2288.5/s  │
│  Batched (100/batch)    │   2000  │    0.04s │   51687.4/s  │
│  Parallel (10 threads)  │   1000  │    0.09s │   11409.2/s  │
└────────────────────────────────────────────────────────────┘

UI Changes:
1) [MEDIUM] The browser should fetch all namespaces/tables/columns in one query not multiple queries for better performance
2) [LOW] Display the type of table stream/shared/user tables with icon per type
3) [MEDIUM] even when there is an error display the error alone with time took display separatly, we need to be similar to the cli
9) [LOW] whenever subscribing: Live - Subscribed [2:11:04 PM] INSERT: +1 rows (total: 16) add another button which you can open a dialog to view all the mesages/logging which was recieved from the websocket subscription
10) [MEDIUM] whenever there is an error in query display only the error not the json: {"status":"error","results":[],"took":4.9274000000000004,"error":{"code":"SQL_EXECUTION_ERROR","message":"Statement 1 failed: Execution error: Schema error: No field named i44d. Valid fields are chat.messages.id, chat.messages.conversation_id, chat.messages.role_id, chat.messages.content, chat.messages.created_at, chat.messages._seq, chat.messages._deleted.","details":"select * from chat.messages where i44d = 256810499606478848"}}
11) [LOW] currently when stoping subscription the live checkbox is turned off automatically, it should stay on
12) [LOW] The table cells should be selectable when clicking on them, and on each cell you can right mouse click -> view data if it has more data there
13) [LOW] when subscribing the first column which is the type should indicate its not an actual column which the user can query its only indication what event type is this, also add to it's left side a timestamp column to indicate when this event came, and whenever a new change is added the newly added row should splash



TODOS:
11) backend\crates\kalamdb-raft\src\applier\system_applier.rs need to be type-safe, keep checking to find other str's which can be type-safe using the same types we already have, also struct SystemSnapshot can store the type-safe StorageId/TableId

23) WHEN BUILDING ON WINDOWS OR MAYBE LINUX ADD TO THE BINARY properties details like verison and other things
33) Add test where we flush a table in a cluster and verify the data
34) isnt struct MetaSnapshot - a waste of resources? or memory? since why having them here as a hashmap in memory?
35) In backend tests add testserver which runs a cluster of 3 nodes and run tests against it

8) Make sure we first replicate the system changes and then the data after that when a node joins the cluster
9) instead of using u64 use NodeId, like in target: u64,

10) change job_id: &str to JobId, namespace_id: &str to NamespaceId, table_id: &str to TableId, storage_id: &str to StorageId, table_type: &str to TableType, user_id: &str to UserId everywhere in kalamdb-raft crate, node_id: u64 to NodeId

11) make sure we are not parsing the sql statement multiple time to know where to forward it to, even when forwarding we need to create another grpc endpoint which takes the parsed one already when forwarded so we dont parse it multiple times

17) Moniotor node health using our own health endpoint: https://deepwiki.com/databendlabs/openraft/6.4-metrics-and-monitoring which should return the nodes as well, only if he has a valid tokens or from localhost or same machine

18) Make sure kalamdb-raft crate doesnt use rocksdb at all, only rely on kalamdb-store crate for storage

19) remove all deprecated meta raft groups and also use the GroupId instead of using a strings everywhere also created_by: Option<String>, use UserId or maybe UserName

39) I still see places where user_id and user_name is treated as str instead of UserId or UserName objects
43) For shared tables add indexes which uses the same secondary indexes we already have and also passing them to datafusion as well
44) Change all tests instead of using VARCHAR use TEXT
45) fix the paths its better to have them all rewritten and used using this filehelper code we created: RocksDB initialized at C:\Jamal\git\KalamDB\backend\./data\rocksdb
45) Add to stats base_dir/rocksdb dir/namespaces in memory/tables in memory/cluster info (snapshots/changes/...) everything we can display for the cluster
46) prevent subscribing to system/shared tables with a proper error message, current message:
[2026-01-13 14:55:01.206] [ERROR] - actix-rt|system:0|arbiter:12 - kalamdb_api::handlers::events::subscription:182 - Failed to register subscription sub-80ba1de3481d280d: Not found: Table not found: system:cluster (sql: 'SELECT * FROM system.cluster LIMIT 100')

51) no need to wroite total rows here:
[21:14:33.419] ✓ SUBSCRIBED [sub_1768331673410754000] 0 total rows, batch 1 (ready), 7 columns
[21:14:33.419] BATCH 1 [sub_1768331673410754000] 2 rows (complete)


54) Add option so that we block the ui - used in cluster mode when we have multiple nodes and we dont want the ui to be in all
55) Verify that we have the gRPC secured so noone can access it accidently from the outside and view the data, add a secret key for joining the cluster that the user can define in server.toml and can override it in environment variable

56) make the backend tests/cli tests all of them use the kalamLink client instead of each one using it's own direct code access to the backend or custom http client, this way we will have one aythentication which is user/password and getting token for this user and password

57) make sure we specify compression for each table created - like snappy/lz4/zstd/none and then we use enum for the compression type we support zstd and snappy or none, and then we can pass the same enum to rocksdb column family and to parquet compression as well

58) Time format is not being displayed in the cli make a default display of the time columns in the cli and ui in a human readable format and can be configurable from the cli config which is stored at the client side, make sure we use the kalamdb-link for that display it should be configured ther ein the connection configs

51) Need to add a flush all command which flushes all tables which needs flushing according to the system.manifest table;

52) check the raft log serialization ad deserialization to make sure we are optimized as much as possible since this is the new layer we added between the commiting and the actual sql command we are running on the data, also the snapshoting i can see its using json is this the ideal way for it?


53) Do we still need this in connection socketS:
    /// Increment the changes counter for a live query
    ///
    /// Uses provider's async method which handles spawn_blocking internally.
    pub async fn increment_changes(&self, live_id: &LiveQueryId) -> Result<(), KalamDbError> {
        let timestamp = Self::current_timestamp_ms();

        self.live_queries_provider
            .increment_changes_async(live_id.as_str(), timestamp)
            .await
            .into_kalamdb_error("Failed to increment changes")?;

        Ok(())
    }

55) instead of streams tables being in memory use commitlog to persist them into a folder which we can select them fast and clean them fast as well
/data/streams/<table>/<YYYYMMDD>/<shardid>/<userId>/<windowStartMs>.log this will use the same way we read rocksdb/snapshots and storage paths
the implementation for this storage need to be done in a separate crate called kalamdb-stream-log
It should support:
1) append only writes
2) reading from a specific time range for a specific table/userid
3) deleting old logs based on retention policy date
4) If the stream table has ttl by hours the folders should be by hour as well for faster deletion, if its by day then by day, other by week/month
5) We should block creating stream table with TTL or the ttl is more than a month
6) create a new trait which has these methods:
    - append_rows(TableId, UserId, HashMap<StreamTableRowId, StreamTableRow>)
    - read_with_limit(TableId, UserId, limit) -> HashMap<StreamTableRowId, StreamTableRow>
    - read_in_time_range(TableId, UserId, start_time: u64, end_time: u64, limit) -> HashMap<StreamTableRowId, StreamTableRow>
    - delete_old_logs(before_time: u64) -> Result<()>

    basicly it will be used inside: backend/crates/kalamdb-tables/src/stream_tables/stream_table_store.rs


56) move backend/crates/kalamdb-raft/src/group_id.rs into sharding crate
57) Make sure the shard template in the user table uses the sharding same ones as well
58) check if we have duplicate structs which we use in configs and also define or duplicate else wehere and maybe we can use enum's directly in configs then we can use them everywhere then

59) i want you to: 
make a file with all: pub enum 
and pub struct 
and then try to find duplicated structs and duplicated enum's in the codebase backend
and remove the duplicates all of them
currently i have one duplicate which is:
backend/crates/kalamdb-commons/src/system_tables.rs
i prefer to keep the one in kalamdb-commons

make sure the backend compiles after this


62) the crate kalamdb-core is taking long to compile i guess we can remove some of the code from inside and divide it into multiple crates for better compile times and also better code organization

63) all cluster commands should be only dba or root:
            ["CLUSTER", "STATUS", ..] | ["CLUSTER", "LS", ..] => {
                // Read-only, allowed for all users
                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterList))
            }

64) check this case:
- I have a cliuster which has 3 nodes
- The ;eader perform a flushing to external storage
- The other followers will not flush since they are followers
- The data which is in rocksdb stays there forever since followers never flush
- So we need to make sure followers also flush their data when the leader flushes but without copying the files to external storage since only the leader should do that
- Each Job should have in it if the executor is a leader then for leader only we copy the manifest/parquet files to external storage otherwise we just flush the rocksdb data to free memory and not copy files
- So in this subject the jobs will run in all nodes in the cluster not only in the leader node
- But we should also make a boolean inside JobType which indicate if this JobType is for leader only

Complete job list (what they do + storage copy/deletion + leader-only notes):

Flush (JobType::Flush)
Writes Parquet files + manifest updates (object store + rename), then compacts RocksDB. This is the only job that creates/writes data files today.
Source: flush.rs:80-179 and users.rs:150-219.
Leader-only today via job loop.

Cleanup (JobType::Cleanup)
Deletes table data + removes Parquet files + invalidates manifest cache + removes metadata.
Source: cleanup.rs:66-166.
Leader-only today.

Compact (JobType::Compact)
RocksDB compaction (tombstone cleanup). No external file writes.
Source: compact.rs:64-142.
Leader-only today.

JobCleanup (JobType::JobCleanup)
Deletes old rows from system.jobs (job history retention).
Source: job_cleanup.rs:52-109.
Leader-only today.

ManifestEviction (JobType::ManifestEviction)
Evicts stale manifest cache entries (RAM + RocksDB).
Source: manifest_eviction.rs:68-157.
Leader-only today.

Retention (JobType::Retention)
Placeholder; intended to delete expired soft-deleted rows (no implementation yet).
Source: retention.rs:70-154.
Leader-only today.

StreamEviction (JobType::StreamEviction)
Deletes old stream logs (TTL).
Source: stream_eviction.rs:98-214.
Leader-only today.

UserCleanup (JobType::UserCleanup)
Placeholder; intended to cascade-delete user data/tokens/ACLs.
Source: user_cleanup.rs:54-121.
Leader-only today.

Backup (JobType::Backup)
Placeholder; intended to export data + upload to external storage.
Source: backup.rs:60-117.
Leader-only today.

Restore (JobType::Restore)
Placeholder; intended to download + restore from external storage.
Source: restore.rs:60-115.
Leader-only today.

Short answer: no, followers won’t automatically clean their local RocksDB data under the current design.
Details:
Job history cleanup (JobCleanupExecutor) deletes directly via the jobs provider store, not via a Raft command. So only the leader deletes those rows locally. See job_cleanup.rs:78-99 and the direct store deletes in jobs_provider.rs:447-514.
Result: followers do not delete their local system.jobs history.
Flush/compaction: only the leader runs flush and compaction jobs today. Followers won’t write Parquet, won’t compact, and won’t purge their local RocksDB data. See the leader-only job loop in runner.rs:70-169 and flush executor behavior in flush.rs:80-179.
So if your goal is “followers also flush/compact locally but don’t upload,” that does not happen in the current design.

Leader-only actions:
Flush → external storage copy + manifest update (single source of truth).
Cleanup → delete Parquet trees + metadata removal (avoid double-delete).
Backup / Restore (external storage operations, global state).
Job history cleanup if job records are owned by meta Raft (single authoritative ledger).
Manifest eviction if it mutates the shared manifest store (not just local cache).

Actions that should run on all nodes:
RocksDB compaction (each node owns its local RocksDB files).
Local cache eviction (if cache is node-local only).
Stream eviction if streams are node-local storage.

also we need to cover the failover we once implemented:

**Current behavior (leader-only + failover):**
- Only the Meta-group leader runs the job loop (`run_loop()`). Followers sleep and wait for leadership changes. See runner.rs.
- When a job is about to run, it’s marked “Running” by issuing a Raft `ClaimJob` command (`mark_job_running()`). See runner.rs.
- On success or failure, the leader writes terminal state via Raft (`CompleteJob` / `FailJob`). See runner.rs and runner.rs.
- When a new leader is elected, it runs failover recovery: scans `Running` jobs and requeues or fails them based on job type and whether the owning node is offline or timed out. See leader_failover.rs.
- The “release” during requeue is done via `LeaderOnlyJobGuard::release_job()`, which sends a Raft `ReleaseJob` command. See leader_guard.rs.

**What’s implemented for “move to other nodes”:**
- There is no direct “move” while a leader is healthy. Jobs are leader-only. 
- Jobs are “moved” only during leader failover: `LeaderFailoverHandler` requeues `Running` jobs if the previous owner is offline or timed out. See leader_failover.rs.

**Is it good?**
- **Strengths:** leader-only execution, Raft-backed state transitions, explicit orphan recovery, job-type-based requeue safety.
- **Gaps/risks:** 
  - `mark_job_running()` issues `ClaimJob` but doesn’t check for “already claimed” or failed claim. That can allow duplicate work on edge cases. Compare with `LeaderOnlyJobGuard::claim_job()` logic. See leader_guard.rs.
  - Failover scans *all* jobs to find `Running` (no filtered query). This can be expensive as the table grows. See leader_failover.rs.
  - Requeue uses `ReleaseJob` but doesn’t explicitly set status back to `Queued` here; it relies on Raft command behavior.
  - The job loop is sequential (TODO for concurrency), which can bottleneck recovery. See runner.rs.

**Possible improvements (low risk):**
- Use `LeaderOnlyJobGuard::claim_job()` and only execute if claim succeeds, handling “already claimed”.
- Add a filtered query for `Running` jobs in failover instead of scanning all jobs.
- Add job “lease/heartbeat” updates and requeue after lease expiry (more robust than fixed timeout).
- Implement concurrency control to avoid long recovery windows.

now i want you to create a plan to implement the job system to run on all nodes at once in the cluster, and the jobs which needs a leader only action the actions needed by the leader will be performed only by the leader, if it failed then the new leader will be left off and do it again
i guess we need to add a new column to the jobs table to track each node the status of the job and if its running/failed/completed its like a hashmap<nodeId, JobStatus> do you think its a good design here?



65) Create a real-world penetration test which target the cluster and each time target a different node with a real cases of real users how they chat with each others using the kalamdb

68) node_id should always be read from Appcontext not passed from outside

71) Is it better to use macros for MetaCommand and other commands to make the code runs faster and more clear?

73) add test which check the server health and cpompliance after some of the big tests before/after which check jobs stuck, cluster not in sync and memory usages being skyrocketing

74) Add another columns which are computed ones from manifest table

76) add ability to the kalam-link to automatically forward to the leader

78) no need for Running startup compaction for, we should add a command to do storage compact all

79) backend/crates/kalamdb-core/src/sql/executor/mod.rs move the permission/security check into a folder backend/crates/kalamdb-core/src/sql/permissions/mod.rs to organize the code better, scan all the places where we check permissions and move them into here so we can keep an eye on them all the time

80) Check that the permissions for system tables is also checked inside the provider as well, so that in case anybody tried to add query inside another query he can't bypass the security as well, make sure we always call the same quard for all permissions check make it centralized so that we can use the same code everywhere maybe adding a parent class between our code and impl TableProvider so that we can always check permissions there before going to the actual provider


81) why we have pub struct PlanCache and also: pub struct QueryCache we can use only the plancahe for both


82) instead of adding security and permission check for each system.table we can add permission for system namespace instead

83) maybe we should go with catalog for system and catalog for public/user namespaces instead of having system tables in the same catalog as user tables


85) pub struct ErrorDetail.code: String,
should be an enum so we can compare it when not leader
err_msg.contains("NOT_LEADER")

87) this should be a type-safe instead of json:
                let sub_data = serde_json::json!({
                    "status": "active",
                    "ws_url": channel,
                    "subscription": {
                        "id": subscription_id,
                        "sql": select_query
                    },
                    "message": "WebSocket subscription created. Connect to ws_url to receive updates."
                });
                Ok(QueryResult::subscription(sub_data))

88) remove object_store from kalamdb-code it should onloy be included in kalamdb-filestore if it needs any function we will be adding it in kalamdb-filestore then
also sqlparser should be inside kalamdb-sql only since there we parse sql's
tonic should be only in kalamdb-raft since there only we use the networking
num_cpus should be inside kalamdb-observability only
also check why we need reqwest?

89) in backend\crates\kalamdb-sql\src\classifier\engine\core.rs we have a switch case which we use strings why not using enum's from: backend\crates\kalamdb-sql\src\classifier\types.rs its type-safe more than using case, also check if its the best way to parse them, i think the parsing cna be done better and use the already sqlparser or datafusion things

90) move ShardingRegistry into kalamdb-sharding also split: backend\crates\kalamdb-sharding\src\lib.rs into multiple files for each struct, replace backend\crates\kalamdb-commons\src\models\ids\shard_id.rs with the Shard we already have and name it ShardId instead

93) no need to have shard id in: impl StorageCached we can compute this in the template whenever we have a userid

94) whene deleting a table/user we remove the folder not looping over the files all of them

95) check that looping over a folder with parquet files do it depending on the manifest.json not all the list of parquet files

97) since we now have StorageCache object, add unit tests which check each method in there 

98) pass Appcontext to backend/crates/kalamdb-core/src/schema_registry/registry/core.rs and instead of passing it to each function

99)         //TODO: Since we need only parquet lets only fetch parquet files here
        let list_result = match storage_cached.list_sync(table_type, table_id, user_id) {
            Ok(result) => result,
100) check if we need to make this with other parquet file reading: backend/crates/kalamdb-core/src/pk/existence_checker.rs
check all other functions which read parquet like: backend/crates/kalamdb-core/src/providers/flush
and see if there is any pattern or duplication or things we can move to filestore
and also this one reads them: backend/crates/kalamdb-core/src/manifest/planner.rs
make only one place where it reads parquet extract the common logic and check whats the pattern in all the readers and combine them into one or few functions in filestore crate


101) backend/crates/kalamdb-core/src/manifest/service.rs should contain storageregistry and schemaregistry as members not passing them to each function or computing them

102) We dont need to have flushing of manifest and reading it using get path and them put or read we can directly call a function to do so

107) add a cache to: backend/crates/kalamdb-system/src/providers/manifest/manifest_store.rs to in-memory and cache through fetching for shared tables only manifests and make sure ManifestService uses the manifestprovider with the caching as well

108) evict_stale_entries(&self, ttl_seconds: i64) -> Result<usize, StorageError> {
  is problematic since it scan all manifest's from rocksdb we should make a better way to do so maybe holding another column family with last_accessed timestamps only for faster eviction we now can have a secondary index easily for that

109) is this needed: backend/crates/kalamdb-core/src/sql/executor/helpers/table_creation.rs

110) Check how to make datafusion session or loader look for tables inside the schemaregistry we have directly? this way no need for registring into datafusion

112) i think its better to store this as hashmap:    /// List of data segments
    pub segments: Vec<SegmentMetadata>,



119) change backend/crates/kalamdb-core/src/jobs/jobs_manager to backend/crates/kalamdb-core/src/jobs/manager

120) should we move: backend/crates/kalamdb-core/src/error_extensions.rs to commons crate src/errors, and use the same error extension across the codebase? everywhere? because currently we have many scattered errors and error enums we can organize them better, also take a look at this one: backend/crates/kalamdb-core/src/error.rs
maybe we can have a big list of all error codes with categories and then we can use them everywhere across the codebase, check if this is better for kalamdb or not, since each crate must be responsible for its error codes as well


125) no need for a column node_id in: system.jobs only the leader is needed there, also cpu_used not needed

add test which cover and make sure keys doesnt collide or get in the filtering of other keys when scanning with prefix
these should be unit tests

also add tests which cover the users/shared/stream providers for scanning with prefix as well

129) check if these functions/methods inside this file already exists in other places and remove duplicates: backend/crates/kalamdb-core/src/manifest/flush/base.rs

131) why we need: SystemTableProviderExt? why not directly use TableProvider trait?


133) add a new option in the sql api endpoint which i send with systemColumns = true or false
by default it should be true all the time
in the cli we can add a new config which the user can choose not to return system columns like seq and deleted

134) fix this: ● KalamDB[docker-cluster] root@http://kalamdb-node1:8080 ❯ explain table chat.typing_events;
✗ Server error (400): Statement 1 failed: Execution error: SQL error: ParserError("Expected: an SQL statement, found: table at Line: 1, Column: 9")
and then make sure we have a test for it as well

136) Check the downloading of file permissions:
- User impersonation for file download
- insert/update/delete as user for file operations with user impersonation

137) when i run: SELECT * FROM chat.uploads limit 5 i always get a different order of rows if no order by is specified will use the key ordering instead to have consistent results, then add test cases to check this with queriying number of times

139) For the cli if we click enter dont open the history menu again only execute enter

140) Make sure we have default namespace whenever we setup the system, and make sure its used by default unless the user changed it using user namespace for that session


143) Support multiple statements running and in each statement run a separate command

144) Whenever we create a new storage or alter a storage we do a connection check before creating the storage and only return success or failed if only the test passes and also the creation passes test: create dir/upload file/list file/remove file if any of those failed then we fail the creation of the storage and return an error and revert the change
this should be done for both create and alter as well
make this test as a separate service which you give it a storage object and it will check the connectivity
we will be needing to add another storage command which check the storage health and this will return information of this storage if they are available like: total/used size and other things we can gather of that storage
after that we can add a button in ui for checking storage health


145) Check that whenever the server starts it reads the server.toml into a global object and can be accessable anywhere in the codebase for example there is a code now custom for reading from kalamdb-filestore, it will be better to allocate them one time only

148) remove  storage_base_path: String, from AppContext init since we already pass the config there, so we end up passing the same thing twice

149) Add http/webdav storage which object_Store already supports it, in both the backend and the ui

150) Check if we have any un-safe unwrap methods r code using and fix it

152) backend/crates/kalamdb-core/src/live/manager/connections_manager.rs and livequery_manager.rs have similar code for managing connections and queries we can make the connection_manager a service or the registry inside the livequerymanager and everywhere we directly use only livequerymanager for everything there, then we can name connections manager to connection_registry

153) ConsumerConfig shouldnt support properties only type-safe object to configure it

154) Add test which check all commands which belong to admin/dba only with normal user and make sure they all fail with proper error message, also for service consumer commands as well so that whenever we change anything we always make sure these commands fail for normal users

155) Add a table which i can view all topic messages

156) 
$ cd cli && ../target/debug/kalam -u http://127.0.0.1:8090 --username root --password kalamdb123 --command "STORAGE FLUSH ALL IN test_cli

Storage flush started for 0 table(s) in namespace 'test_cli'. Job IDs: []
Query OK, 0 rows affected


157) Check that whenever we are creating a topic we create it inside a specific namespace_id
158) drop namespace chat - does drop the namespace but the tables still there, it should drop all the tables as well (check tests)
159) whenever i create namespace if its already exists it should give me an error (check tests)
160) at least one delivery if tghere is multiple consumers send to only one of them not all of them, we can add a round robin or random selection for this, check how kafka/nats do it and implement the same way, and also add tests to verify this
162) processMessage should pass a context which contains the userName of who did this action
165) get a new option to pass userid instead of username: backend/crates/kalamdb-api/src/handlers/topics/consume.rs
166) add command in cli \subscribe to multiple queries at once
167) Fix the subscribe issue in the ui itds not reciving any changes at all

169) 
"SQL_EXECUTION_ERROR"
"UPDATE chat.conversations SET updated_at = NOW() WHERE id = 278486627852849152"
"Statement 1 failed: Invalid operation: Unsupported UPDATE expression: NOW()"

171) when using consume from topic if the network disconnected keep looping and reconnect, follow the same configuration of the websocket reconnection logics, we can use this logic in both the subscription and the consumer

172) Whenever we subscribe to a query and there is an update only push the updated columns and not all the other columns data
173) Support admin ui download file as_user=<user> so we can support downloading files from the service as well

174) When we implement the delivery_policy we mustc verify the user is subscribing to this tableid before calling out the insert

175) why fetching the table again here? it should be already passed to there: backend/crates/kalamdb-tables/src/manifest/manifest_helpers.rs

176) insetad of holding: pub struct ManifestCacheEntry and manifest we can combine them together for the entitystore, then we wont be needing any custom schema for them, we can make the main object as the schema for the table

178) change this to flatbuffers: backend/crates/kalamdb-raft/src/state_machine/serde_helpers.rs for faster serdes

179) Re-check if the serdes of the table data is zero-copy

185) should we still be visible? maybe we should always use the schema registry for everything instead of: pub fn system_tables(&self) -> Arc<SystemTablesRegistry>

186) make this: const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(60); confugurable per topic

187) subscription id is too long   "subscription_id": "u_ec6f250a2c094e2492ee61a83d0839d4-bc83de82331b416fb0f54be67759f55b-sub-359870fa18caeb70",
come uop with a better and shorter one without causing issues in the future
Also for a better userid unique one: NanoID


188) Instead of notify all followers remember where the user is connected to through the livequerymanager and forward the notification to that exact node only

189) pub fn parse_basic_auth_header(auth_header: &str) -> AuthResult<(String, String)> should return UserId instead of String for the username
also:     Credentials { username: String, password: String },
async fn authenticate_credentials(
    username: &str,


190) when auto create user use the userid generation like the rest in the code

191) /// TODO: This will hurt the sharding distribution of provider users, i prefer adding the provider code as a prefix to the username instead of suffixing it, but we can revisit this if it becomes an issue.
pub(crate) fn compose_provider_username(issuer: &str, subject: &str) -> UserName

192) make AuthErrorResponse typesafe error code

193) remove checkSetupStatus/serverSetup from the dart and typescript sdk


194) why we need this: compute_update_delta? shouldnt the change query which we run have already the list of columns which was changed there and we can use it directly?

195) Why we still send in the update change in websocket 2 arrays? shouldnt we make them map<string, string> column -> value only?

196) change the sdks to return instead of RowData and is a hashmap in it, to return a RowData: seqid/deleted/columns: HashMap<string, string> this way we can make sure that the sdks read them separatly instead of from the hashmap and map them then to an internal object

197) Make sure we have this option in kalamclient which we specify that the websocket connection is lazy loaded which means only whenever we first subscribe it will connect

198) when subscribing if the query has an orderby or group by then return an error its not supported only a select and where is supported, also the _seq and _deleted cant be included in the subscription at all

199) if we run select * from dba.stats limit 100 - the results each time come in different order they should always come by the order of the primary key of the table


Main Epics:
------------
1) Done - Add observability using otel + jaeger + prometheus + grafana
2) Make cluster more stable and reliable
3) Done - Add file storage FILE DataType
4) Add embedding + vector search support
5) Combine the models of kalamdb-link and kalamdb-commons into kalamdb-shared crate and use it everywhere
6) Service consumer - Subscription to shards
7) Done - Change the code to use FlatBuffers for: Raft/RocksDb storage
8) Done - Add page for Server Initial Setup
9) In Progress - Check S3/WebDAV Storages
10) Move permissions to Shared tables with policies
11) A new option for STREAM table to only insert the row if the user is online option (delivery_policy = 'online_only'  -- or 'always') also For STREAM tables, to avoid unbounded RAM growth, we can implement a disk-backed buffer (e.g., mmap-backed ring buffer). mmap reduces heap usage, but we still need TTL eviction + max-bytes limits + backpressure to prevent OOM. https://lib.rs/crates/mmap-io, drop policy: drop_oldest
13) backend/crates/kalamdb-core/src/providers - need to be removed if they are not needed anymore
14) Done - Subscription - Send the schema before the initial load starts, so the client can prepare for the incoming data and also we can use it to validate the incoming data as well
15) Done - When insert support RETURNING id;



Target Usecase:
----------------
1) Have a service code which subscribes to chat messages/conversations/typing events and process them in real-time
2) Having a chat app messages which recieves and send messages in real-time using subscriptions websocket
3) Authenticate using Firebase auth tokens



Postgres Extension:
---------------------
- Remove all the expirement xcode for the embedded server
- Add mTLS + short-lived JWT - for Postgres to kalamdb-server connection
- In KalamDB unify thr mTLS to use in both cluster and pg extension connection and not repeat the same logic twice organize the code in kalamdb-security crate
- Add transaction begin/commit to kalamdb then wire it to pg
- Make sure in the sqlparser we are using Postgres Dialect instead of geenric, i want kalamdb to be so close to postgres in all ways
- Lock on the create table statement maybe we can create regular or any table and then we can alter it to be a kalam table with options and also migrate the data which is currently there
- since these ar eused in both pg_kalam and cluster rename:  KALAMDB_CLUSTER_RPC_ADDR,KALAMDB_CLUSTER_API_ADDR
- KALAMDB_PG_AUTH_TOKEN is only temporary for now come up with a new auth which we can generate 
- Check ability to use this syntax: CREATE TABLE compression_test
(
...
)  USING kalamdb
   WITH (type = 'user', migrate = true, compress = 5, ...);
- Can we support migrating a current table from postgres to kalamdb using something like: SELECT set_kalam_table('schema.table1', migrate => true, compress => 5, ...); this will convert the current table to a kalamdb table and move the data as well without needing to create a new table and move the data there
- for parquet stick with only one compression for now
- Remove the requirement the auth first we can rely on headers instead which will make the connection faster
