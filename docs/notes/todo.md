# Todo Notes

Last audit: 2026-04-10

Anything still listed here was checked against the current repo and is either still open, only partially addressed, or still relevant enough to keep on the backlog. Items moved out during the audit are in [finished.md](finished.md).

Task numbers are historical and not globally unique. Use the section plus the task number together when referencing an item.

## Performance & Optimization

53) [HIGH] IMPORTANT: Add a story about the need for giving ability to subscribe for: * which means all users tables at once, this is done by the ai agent which listen to all the user messages at once, add also ability to listen per storageId, for this we need to add to the user message key a userId:rowId:storageId

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

## Code Quality & Cleanup

High Priority:
12) [HIGH][PARTIAL] Check all unwrap() and expect() calls and replace them with proper error handling

Medium Priority:
2) [MEDIUM] Replace all instances of String types for namespace/table names with their respective NamespaceId/TableName, we should use TableId instead of passing both:        namespace: &NamespaceId,table: &TableName, like in update_manifest_after_flush

4) [MEDIUM] Make sure all using UserId/NamespaceId/TableName/TableId/StorageId types instead of raw strings across the codebase

17) [MEDIUM] why we have 2 implementations for flushing: user_table_flush.rs and shared_table_flush.rs can we merge them into one? i believe they share a lot of code, we can reduce maintenance burden by having only one implementation, or can have a parent class with common code and have 2 child classes for each type of flush

Low Priority:
6) [LOW] Remove un-needed imports across the codebase

8) [LOW] Check where we use AppContext::get() multiple times in the same struct and make it a member of the struct instead, or if the code already have AppContext as a member use it directly

10) [LOW] Use todo!() instead of unimplemented!() where needed

11) [LOW] Remove all commented code across the codebase

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

153) [LOW] SharedTableFlushJob and UserTableFlushJob should take as an input the appContext and the TableId and TableType as an input for creating them

158) [LOW] extract_seq_bounds is duplicated we cna combine it

165) [LOW] No need to have tableType in tableOptions since we already have it as column

181) [LOW][PARTIAL] cut the took into 3 decimal places only:
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

## Testing & CI/CD

Testing - High Priority:
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

Testing - Medium Priority:
80) [MEDIUM] More integration tests inside cli tool which covers importing sql files with multiple statements

81) [MEDIUM] CLI - add integration tests which covers a real-life use case for chat app with an ai where we have conversations and messages between users and ai agents, real-time streams for ai thinking/thoughts/typing/user online/offline status, and also flushing messages to parquet files and reloading them back

102) [MEDIUM] CLI Tests common - Verify that we have a timeout set while we wait for the subscription changes/notifications

176) [MEDIUM] Add a test to chekc if we can kill a live_query and verify the user's socket got closed and user disconnected

191) [MEDIUM] In cli tests whenever we have flush directly after it check the storage files manifest.json and the parquet files if they are exists there and the size is not 0, use one function in common which helps with this if not already having one like that

Testing - Low Priority:
15) [LOW] for better tracking the integration tests should the names print also the folder path as well with the test name

20) [LOW] For flushing tests first create a storage and direct all the storage into a temporary directory so we can remove it after each flush test to not leave with un-needed temporary data

CI/CD - Medium Priority:
2) [MEDIUM] Add code coverage to the new repo

## Features - Core Functionality

High Priority:
14) [HIGH] Add upsert support

72) [HIGH] Whenever we drop the namespace remove all tables under it

77) [HIGH] Flush table job got stuck for a long time, need to investigate why and also why the tests don't detect this issue and still passes?!

105) [HIGH] when we have Waiting up to 300s for active flush jobs to complete... and the user click CTRL+C again it will force the stopping and mark those jobs as failed with the right error

174) [HIGH] Make sure we flush the table before we alter it to avoid any data loss from a previous schema, also make sure we lock the table for writing/reading while it is being altered

Medium Priority:
3) [MEDIUM] Combine all the subscription logic for stream/user tables into one code base

5) [MEDIUM] Storage files compaction

12) [MEDIUM] aDD objectstore for storing files in s3/azure/gcs compatible storages

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

155) [MEDIUM] Whenever we drop a table cancel all jobs for this table which are still running or queued

156) [MEDIUM] When there is an sql parsing or any error the parser should return a clear error with maybe line or description of what is it
instead of: 1 failed: Invalid operation: No handler registered for statement type 'UNKNOWN'

157) [MEDIUM] Are we closing all ParquetWriter? whenever we use them?

163) [MEDIUM] If there is any stuck live_query when starting the server clear them all, might be the server crashed without graceful shutdown

175) [MEDIUM] When altering a table to add/remove columns we need to update the manifest file as well

177) [MEDIUM] The loading of tables and registering its providers is scattered, i want to make it one place for on server starts and on create table

178) [MEDIUM] Flushing - Maybe we need to change the flush to look at the active manifests and from them we can know what needs to be flushed instead of scaning the whole hot rows, this way we can make sure all the manifests are flushed as well.

180) [MEDIUM] For jobs add a method for each executor called: preValidate which check if we should create that job or not, so we can check if we need to evict data or there is no need, this will not need a created job to run

190) [MEDIUM] NamespaceId should be maximum of 32 characters only to avoid long names which may cause issues in file systems

17) [MEDIUM] Persist views in the system_views table and load them on database starts

203) [MEDIUM] Can you check if we can use the manifest as an indication of having rows which needs flushing or you think its better to keep it this way which is now? if we flush and we didnt find any manifest does it fails? can you make sure this scenario is well written?

## Features - User Experience

CLI - Medium Priority:
13) [MEDIUM] In the cli add a command to show all live queries

CLI - Low Priority:
7) [LOW] when reading --file todo-app.sql from the cli ignore the lines with -- since they are comments, and create a test to cover this case

75) [LOW] Fix cli highlight select statements

76) [LOW] Fix auto-complete in cli

166) [LOW] Add cli clear command which clears the console previous output

170) [LOW] Make a way to set the namespace once per session and then we can use it in the next queries

UI - Medium Priority:
1) [MEDIUM] The browser should fetch all namespaces/tables/columns in one query not multiple queries for better performance

3) [MEDIUM] even when there is an error display the error alone with time took display separatly, we need to be similar to the cli

10) [MEDIUM] whenever there is an error in query display only the error not the json: {"status":"error","results":[],"took":4.9274000000000004,"error":{"code":"SQL_EXECUTION_ERROR","message":"Statement 1 failed: Execution error: Schema error: No field named i44d. Valid fields are chat.messages.id, chat.messages.conversation_id, chat.messages.role_id, chat.messages.content, chat.messages.created_at, chat.messages._seq, chat.messages._deleted.","details":"select * from chat.messages where i44d = 256810499606478848"}}

UI - Low Priority:
2) [LOW] Display the type of table stream/shared/user tables with icon per type

9) [LOW] whenever subscribing: Live - Subscribed [2:11:04 PM] INSERT: +1 rows (total: 16) add another button which you can open a dialog to view all the mesages/logging which was recieved from the websocket subscription

11) [LOW] currently when stoping subscription the live checkbox is turned off automatically, it should stay on

12) [LOW] The table cells should be selectable when clicking on them, and on each cell you can right mouse click -> view data if it has more data there

13) [LOW] when subscribing the first column which is the type should indicate its not an actual column which the user can query its only indication what event type is this, also add to it's left side a timestamp column to indicate when this event came, and whenever a new change is added the newly added row should splash

General - Low Priority:
1) [LOW] Alter a table and move it's storage from storage_id to different one

2) [LOW] Support changing stream table TTL via ALTER TABLE

33) [LOW] Add option for a specific user to download all his data this is done with an endpoint in rest api which will create a zip file with all his tables data in parquet format and then provide a link to download it

34) [LOW] Add to the roadmap adding join which can join tables: shared<->shared, shared<->user, user<->user, user<->stream

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

## Future Features

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