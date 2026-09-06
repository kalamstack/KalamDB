# KalamDB SQL Reference

**Version**: 0.1.3  
**Last Updated**: September 6, 2026

This page documents SQL commands and SQL usage only.

## Statement Separator

```sql
SELECT 1;
SELECT 2;
```

## Namespace Commands

### CREATE NAMESPACE

```sql
CREATE NAMESPACE <namespace_name>;
CREATE NAMESPACE IF NOT EXISTS <namespace_name>;
```

### DROP NAMESPACE

```sql
DROP NAMESPACE <namespace_name>;
DROP NAMESPACE IF EXISTS <namespace_name>;
DROP NAMESPACE <namespace_name> CASCADE;
DROP NAMESPACE IF EXISTS <namespace_name> CASCADE;
```

### ALTER NAMESPACE

```sql
ALTER NAMESPACE <namespace_name>
  SET DESCRIPTION '<description>';
```

### USE / SET NAMESPACE

Changes the default namespace for the current request or multi-statement batch.
In the interactive CLI, a successful `USE` also updates the CLI's local
namespace so later requests automatically send `namespace_id`.

```sql
USE <namespace_name>;
USE NAMESPACE <namespace_name>;
SET NAMESPACE <namespace_name>;
```

### SHOW NAMESPACES

```sql
SHOW NAMESPACES;
```

## Table DDL

KalamDB supports `USER`, `SHARED`, and `STREAM` tables.

### CREATE TABLE (Unified)

```sql
CREATE [USER|SHARED|STREAM] TABLE [IF NOT EXISTS] [<namespace>.]<table_name> (
  <column_name> <data_type> [NOT NULL|NULL] [DEFAULT <expr>] [PRIMARY KEY],
  ...,
  [CONSTRAINT <name> PRIMARY KEY (<column_name>)]
)
[WITH (
  TYPE = '<USER|SHARED|STREAM>',
  STORAGE_ID = '<storage_id>',
  USE_USER_STORAGE = <TRUE|FALSE>,
  FLUSH_POLICY = '<rows:N|interval:N|rows:N,interval:N>',
  TTL_SECONDS = <seconds>,
  EVICTION_STRATEGY = '<time_based|size_based|hybrid>',
  MAX_STREAM_SIZE_BYTES = <bytes>,
  COMPRESSION = '<none|snappy|zstd>'
)];
```

Table options are type-specific:

- `USER`: `STORAGE_ID`, `USE_USER_STORAGE`, `FLUSH_POLICY`, `COMPRESSION`
- `SHARED`: `STORAGE_ID`, `FLUSH_POLICY`, `COMPRESSION`
- `STREAM`: `TTL_SECONDS`, `EVICTION_STRATEGY`, `MAX_STREAM_SIZE_BYTES`

`COMPRESSION` accepts only `none`, `snappy`, and `zstd`, and is valid only for `USER` and `SHARED`
tables. It controls the Parquet codec used when table data is flushed or compacted into
cold-storage segments. `none` writes uncompressed Parquet pages, `snappy` is the default fast codec,
and `zstd` uses Zstandard level 1 for better density with modest CPU cost. This setting is separate
from WebSocket gzip and RocksDB compression. `STREAM` tables use hot stream log storage and do not
accept table Parquet compression.

Examples:

```sql
CREATE TABLE app.messages (
  id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  conversation_id BIGINT NOT NULL,
  sender TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'user',
  content TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
) WITH (
  TYPE = 'USER',
  STORAGE_ID = 'local',
  USE_USER_STORAGE = false,
  FLUSH_POLICY = 'rows:1000,interval:60',
  COMPRESSION = 'snappy'
);

CREATE SHARED TABLE app.config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT NOW()
) WITH (
  COMPRESSION = 'zstd'
);

CREATE STREAM TABLE app.events (
  event_id TEXT PRIMARY KEY,
  payload TEXT,
  created_at TIMESTAMP DEFAULT NOW()
) WITH (
  TTL_SECONDS = 30,
  EVICTION_STRATEGY = 'hybrid',
  MAX_STREAM_SIZE_BYTES = 1048576
);
```

### ALTER TABLE

```sql
ALTER TABLE [<namespace>.]<table_name> ADD COLUMN <name> <type> [NOT NULL|NULL] [DEFAULT <value>];
ALTER TABLE [<namespace>.]<table_name> DROP COLUMN <name>;
ALTER TABLE [<namespace>.]<table_name> MODIFY COLUMN <name> <type> [NOT NULL|NULL];
ALTER TABLE [<namespace>.]<table_name> SET TBLPROPERTIES (<table_option> = <value>, ...);
```

`SET TBLPROPERTIES` supports the same type-specific persisted options as `CREATE TABLE`.
Use `FLUSH_POLICY = NULL` to clear a user/shared flush policy.

Examples:

```sql
ALTER TABLE app.config
  SET TBLPROPERTIES (COMPRESSION = 'zstd');

ALTER TABLE app.messages
  SET TBLPROPERTIES (FLUSH_POLICY = 'rows:5000', USE_USER_STORAGE = true);

ALTER TABLE app.events
  SET TBLPROPERTIES (
    TTL_SECONDS = 3600,
    EVICTION_STRATEGY = 'size_based',
    MAX_STREAM_SIZE_BYTES = 1048576
  );
```

Shared tables always use FORCE row-level security. Creating a shared table without
`CREATE POLICY` is default-deny for User and Service (zero rows on SELECT; writes fail).
System and DBA bypass RLS. `ACCESS_LEVEL` is not a table option; grant access with
`CREATE POLICY`.

### DROP TABLE

```sql
DROP TABLE [IF EXISTS] [<namespace>.]<table_name>;
DROP USER TABLE [IF EXISTS] [<namespace>.]<table_name>;
DROP SHARED TABLE [IF EXISTS] [<namespace>.]<table_name>;
DROP STREAM TABLE [IF EXISTS] [<namespace>.]<table_name>;
```

### CREATE INDEX / DROP INDEX

Scalar secondary indexes are equality prefix scans on USER and SHARED tables.
Vector indexes stay on the no-parentheses `USING COSINE|L2|DOT` form.

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] <index_name>
  ON [<namespace>.]<table_name> (<column> [, <column> ...]);

ALTER TABLE [<namespace>.]<table_name>
  CREATE [UNIQUE] INDEX [IF NOT EXISTS] <index_name> (<column> [, <column> ...]);

ALTER TABLE [<namespace>.]<table_name> DROP INDEX [IF EXISTS] <index_name>;
```

Chat and membership lookups:

```sql
CREATE INDEX idx_messages_conversation ON app.messages (conversation_id);
CREATE INDEX idx_conversation_members_user ON app.conversation_members (user_id);
```

### CREATE / ALTER / DROP POLICY

Row-level security applies to every shared-table scan, write, live event, and file
download for User and Service. System and DBA bypass. Policies are permissive (`OR`);
`AS RESTRICTIVE` is rejected. `CURRENT_USER` is bound after plan-cache lookup, so the
same cached plan can return different rows for Alice and Bob.

`TO` selects which roles the policy applies to:

- `TO user` — end-user sessions only
- `TO service` — service-account sessions only
- `TO user, service` — both authenticated principals
- `TO PUBLIC` (or omit `TO`) — every role subject to RLS (`user` and `service`)

```sql
-- SELECT: end users see only their own documents
CREATE POLICY owner_read ON app.documents
  FOR SELECT TO user
  USING (owner_id = CURRENT_USER);

-- SELECT: membership subquery (same IR as EXISTS)
CREATE POLICY member_read ON app.messages
  FOR SELECT TO user
  USING (
    group_id IN (
      SELECT group_id FROM app.group_members
      WHERE user_id = CURRENT_USER
    )
  );

-- SELECT: service accounts can read every published row
CREATE POLICY service_published_read ON app.documents
  FOR SELECT TO service
  USING (status = 'published');

-- SELECT: both user and service share the same visibility rule
CREATE POLICY tenant_read ON app.events
  FOR SELECT TO user, service
  USING (tenant_id = CURRENT_USER);

-- SELECT: PUBLIC = user and service (same as TO user, service here)
CREATE POLICY public_catalog_read ON app.catalog
  FOR SELECT TO PUBLIC
  USING (is_public = true);

-- DML: separate policies per command, or one FOR ALL
CREATE POLICY owner_insert ON app.documents
  FOR INSERT TO user
  WITH CHECK (owner_id = CURRENT_USER);

CREATE POLICY owner_update ON app.documents
  FOR UPDATE TO user
  USING (owner_id = CURRENT_USER)
  WITH CHECK (owner_id = CURRENT_USER);

CREATE POLICY owner_delete ON app.documents
  FOR DELETE TO user
  USING (owner_id = CURRENT_USER);

CREATE POLICY service_full ON app.documents
  FOR ALL TO service
  USING (true)
  WITH CHECK (true);

ALTER POLICY owner_read ON app.documents
  USING (owner_id = CURRENT_USER);

DROP POLICY owner_read ON app.documents;
```

`EXISTS` and `IN (SELECT … WHERE principal = CURRENT_USER)` compile to the same
membership relation. Covering primary keys should be `(principal, relation_key)`
so PointGuard can probe without a full membership scan. Client `WHERE` clauses,
including `OR true`, cannot bypass RLS: authorized MVCC winners are selected first.

### CREATE VIEW

```sql
CREATE VIEW [<namespace>.]<view_name> AS <select_query>;
CREATE VIEW [<namespace>.]<view_name> (<column1>, <column2>, ...) AS <select_query>;
```

### SHOW TABLES

```sql
SHOW TABLES;
SHOW TABLES IN <namespace>;
SHOW TABLES IN NAMESPACE <namespace>;
```

### DESCRIBE TABLE

```sql
DESCRIBE TABLE [<namespace>.]<table_name>;
DESC TABLE [<namespace>.]<table_name>;
DESCRIBE TABLE [<namespace>.]<table_name> HISTORY;
```

### SHOW STATS FOR TABLE

```sql
SHOW STATS FOR TABLE [<namespace>.]<table_name>;
```

## Types

Named types are PostgreSQL-style composites, enums, and table row types.
They are the same type system used by table columns and procedure signatures.
`CREATE TYPE`, `ALTER TYPE`, and `DROP TYPE` require a DBA or System role.

Creating a table also catalogs an implicit row type with the same
schema-qualified name as the table (`app.users` is both the table and the row
type). `CREATE TYPE ... FROM TABLE` adds an optional second name, usually
singular, bound to that live row type.

### CREATE TYPE

```sql
CREATE TYPE [IF NOT EXISTS] [<schema>.]<name> AS (
  <field> <type> [NOT NULL] [NONEMPTY] [, ...]
);

CREATE TYPE [IF NOT EXISTS] [<schema>.]<name> AS ENUM ('<label>' [, ...]);

CREATE TYPE [IF NOT EXISTS] [<schema>.]<name> FROM TABLE [<schema>.]<table>;
```

Examples:

```sql
CREATE TYPE app.address AS (
  city TEXT NOT NULL,
  country TEXT NOT NULL
);

CREATE TYPE app.message_status AS ENUM ('sent', 'delivered', 'read');

CREATE SHARED TABLE app.users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  home app.address
);

CREATE TYPE app.user FROM TABLE app.users;
```

Rules:

1. Unqualified names use the current default namespace.
2. Fields are nullable unless `NOT NULL` is present.
3. `NONEMPTY` is allowed on `TEXT`, `BYTES`, and arrays, and requires `NOT NULL`.
4. Arrays are one-dimensional (`T[]`).
5. Fields may reference scalars, enums, named composites, table row types, or
   `JSON`/`JSONB`.
6. `FROM TABLE` aliases must live in the same schema as the table. A table may
   have at most one explicit row-type alias.
7. A standalone type cannot reuse a table's implicit row-type name.
8. `AS UNION` and `AS INTERFACE` are reserved and rejected.
9. Catalog rows live in `system.types` and `system.type_fields`.

Use named types in procedure signatures and as nested table columns:

```sql
CREATE PROCEDURE app.get_user(user_id TEXT NOT NULL)
RETURNS ROW TYPE app.users
LANGUAGE JAVASCRIPT
AS $$
  return ctx.db.sql("SELECT * FROM app.users WHERE id = '" + input + "'");
$$;
```

### ALTER TYPE

```sql
ALTER TYPE [<schema>.]<name> ADD ATTRIBUTE <field> <type> [NOT NULL];
ALTER TYPE [<schema>.]<name> DROP ATTRIBUTE <field>;
ALTER TYPE [<schema>.]<name> RENAME ATTRIBUTE <from> TO <to>;
ALTER TYPE [<schema>.]<name> ALTER ATTRIBUTE <field> TYPE <type>;
ALTER TYPE [<schema>.]<name> SET SCHEMA <schema>;
```

Attribute operations apply to composite types. `DROP ATTRIBUTE CASCADE` is not
supported; drop dependents first. `SET SCHEMA` applies to named composites and
enums only, not implicit row types or `FROM TABLE` aliases.

### DROP TYPE

```sql
DROP TYPE [<schema>.]<name>;
DROP TYPE IF EXISTS [<schema>.]<name>;
DROP TYPE [<schema>.]<name> RESTRICT;
```

`DROP TYPE` fails while another type, alias, or procedure still references it.
Implicit table row types cannot be dropped; drop the table instead.
`DROP TYPE CASCADE` is not supported.

## Data Manipulation (DML)

### INSERT

```sql
INSERT INTO [<namespace>.]<table_name> (<column1>, <column2>, ...)
VALUES (<value1>, <value2>, ...);

INSERT INTO [<namespace>.]<table_name> (<column1>, <column2>, ...)
VALUES
  (<value1a>, <value2a>, ...),
  (<value1b>, <value2b>, ...);
```

### UPDATE

```sql
UPDATE [<namespace>.]<table_name>
SET <column1> = <value1>, <column2> = <value2>
WHERE <condition>;
```

### DELETE

```sql
DELETE FROM [<namespace>.]<table_name>
WHERE <condition>;
```

### SELECT

```sql
SELECT <columns>
FROM [<namespace>.]<table_name>
[WHERE <condition>]
[GROUP BY <expr>]
[ORDER BY <expr>]
[LIMIT <n>];
```

## Procedures

Server procedures are transactional business operations invoked with `CALL`.
They are not SQL expression functions: `SNOWFLAKE_ID()`, `NOW()`, and similar
built-ins stay in `SELECT` lists. A procedure runs in a V8 isolate, can read
and write tables, publish topics, call other procedures, and return a typed
value.

`CREATE PROCEDURE`, `DROP PROCEDURE`, `GRANT EXECUTE`, and `REVOKE EXECUTE`
require a DBA or System role. `CALL` is allowed for any authenticated role that
holds `EXECUTE` on that procedure.

### CREATE PROCEDURE

```sql
CREATE [OR REPLACE] PROCEDURE [<schema>.]<name> (
  <arg> <type> [NOT NULL] [NONEMPTY] [, ...]
)
[RETURNS [ROW TYPE] <type>]
LANGUAGE <JAVASCRIPT|JS|TYPESCRIPT|TS>
[SECURITY INVOKER | SECURITY DEFINER]
AS $$
  <javascript_body>
$$;
```

Rules:

1. One procedure per `schema.name`. Overloads are rejected.
2. Unqualified names use the current default namespace (`USE` / session schema).
3. Parameters are `IN` only. They are nullable unless `NOT NULL` is present.
4. `SECURITY INVOKER` is the default. The procedure runs as the caller; RLS and
   `CURRENT_USER` use that principal.
5. `SECURITY DEFINER` runs as the procedure owner for that frame. The original
   actor is preserved for audit. Table privileges and RLS use the owner.
6. `LANGUAGE JAVASCRIPT`, `JS`, `TYPESCRIPT`, and `TS` all compile to the same
   V8 runtime. Dollar-quoted (`$$ ... $$`) or string-literal bodies are accepted.
7. `LANGUAGE SQL` catalogs the routine but `CALL` is not supported.
8. `CREATE OR REPLACE` replaces an existing procedure. Without `OR REPLACE`, a
   duplicate name fails.

The body is wrapped as `(ctx, input) => { ... }` unless it already defines
`function kalamInvoke(name, args)`. With one argument, `input` is that value.
With several arguments, `input` is an array in declaration order.

Host objects injected into `ctx`:

| Host | Purpose |
| --- | --- |
| `ctx.source.kind` | Always `"call"` for SQL, REST, and PGWire invocation. Clients cannot supply this. |
| `ctx.db.sql(sql)` | Run nested SQL on the same request transaction. |
| `ctx.functions.call(name, args)` | Nested procedure call. `name` may be `schema.name` or unqualified. |
| `ctx.topics.publish(topic, payload)` | Stage a typed topic publish. Commit flushes it; rollback drops it. |
| `ctx.http.request.header(name)` | Read a request header. Only set on HTTP-root invocations; SQL `CALL` returns null. |
| `ctx.http.status(code)` | Set the HTTP status. HTTP-root only; nested procedures cannot mutate `ctx.http`. |
| `ctx.http.header(name, value)` | Set a response header. HTTP-root only. |

Examples:

```sql
CREATE OR REPLACE PROCEDURE app.echo(msg TEXT)
LANGUAGE JAVASCRIPT
AS $$
  return input;
$$;

CREATE OR REPLACE PROCEDURE app.inc(x INT)
LANGUAGE JAVASCRIPT
AS $$
  return input + 1;
$$;

CREATE OR REPLACE PROCEDURE app.plus_one(x INT)
LANGUAGE JAVASCRIPT
AS $$
  return ctx.functions.call('app.inc', [input]);
$$;

CREATE OR REPLACE PROCEDURE app.place_order(p_id INT)
LANGUAGE JAVASCRIPT
SECURITY DEFINER
AS $$
  ctx.db.sql("INSERT INTO app.orders (id, status) VALUES (" + input + ", 'ok')");
  ctx.topics.publish('app.events', { id: input, status: 'ok' });
  return { id: input, status: 'ok' };
$$;
```

### DROP PROCEDURE

```sql
DROP PROCEDURE [<schema>.]<name>;
DROP PROCEDURE IF EXISTS [<schema>.]<name>;
```

### GRANT / REVOKE EXECUTE

`EXECUTE` is independent of table grants and row-level security. It only decides
whether a principal may enter the procedure. Nested `ctx.db.sql` still uses the
effective principal's table privileges and RLS.

```sql
GRANT EXECUTE ON PROCEDURE [<schema>.]<name> TO <PUBLIC|user|service|<role>>;
REVOKE EXECUTE ON PROCEDURE [<schema>.]<name> FROM <PUBLIC|user|service|<role>>;
```

Rules:

1. New procedures have no `PUBLIC` execute privilege. Grant access deliberately.
2. The owner, DBA, and System roles may always `CALL` a procedure they own or
   administer.
3. `TO user` allows end-user sessions. `TO service` allows service accounts.
   `TO PUBLIC` allows every authenticated role except anonymous.
4. Anonymous sessions cannot execute procedures.
5. A user can `CALL` a `SECURITY DEFINER` API without holding `INSERT` on the
   underlying table, as long as they have `EXECUTE` and the owner does.

```sql
REVOKE EXECUTE ON PROCEDURE app.echo FROM PUBLIC;
GRANT EXECUTE ON PROCEDURE app.echo TO user;
CALL app.echo('ok');
REVOKE EXECUTE ON PROCEDURE app.echo FROM user;
```

### CALL

```sql
CALL [<schema>.]<name>();
CALL [<schema>.]<name>(<arg> [, ...]);
CALL [<schema>.]<name>($1, $2);
```

SQL `CALL` arguments are positional literals or 1-based placeholders:

- `NULL`, `TRUE`, `FALSE`
- integers and floats
- single-quoted strings
- `$1`, `$2`, ... bound from the prepared-statement parameter list

Named composite arguments belong on the REST body, not in SQL `CALL`.
Unqualified `CALL ping()` uses the current default namespace.

The result is one column named `result`. A root `CALL` starts a request
transaction when none is open. Nested `ctx.functions.call` and `ctx.db.sql`
share that transaction. `BEGIN; CALL ...; ROLLBACK;` drops nested inserts and
staged topic publishes together.

```sql
CALL app.echo('hello');
CALL app.plus_one(41);
CALL app.place_order(7);

BEGIN;
CALL app.place_order(99);
ROLLBACK;
```

PGWire and the SQL HTTP API run the same `CALL` statement through `SqlExecutor`.

### REST invocation

Every executable procedure is also available over HTTP. This is the same
runtime as SQL `CALL`, not a second controller contract.

```http
POST /v1/functions/{schema}/{procedure}
Authorization: Bearer <token>
Content-Type: application/json
```

The body is a JSON object of named parameters, a JSON array of positional
values, or empty/`null` for a procedure with no arguments:

```http
POST /v1/functions/app/echo
Content-Type: application/json

{ "msg": "rest" }
```

Success response:

```json
{ "status": "success", "result": "rest" }
```

Clients must not send `context`, `ctx`, `source`, `actor`, or `tx`. The host
builds those from the authenticated session. HTTP-root procedures may set
`ctx.http.status` and `ctx.http.header`; those apply only to this REST
response.

Catalog rows live in `system.routines`, `system.routine_parameters`, and
`system.routine_grants`.

### Topic triggers

Durable topic delivery is `CREATE TRIGGER … ON TOPIC … EXECUTE PROCEDURE`,
not table AFTER ROW triggers.

```sql
CREATE TRIGGER chat.process_message
  ON TOPIC chat.message_created
  EXECUTE PROCEDURE chat.on_message_created(PAYLOAD)
  WITH (
    principal = 'system',
    start = 'latest',
    retries = 5,
    retry_backoff = '1s',
    concurrency = 1
  );

ALTER TRIGGER chat.process_message DISABLE;
ALTER TRIGGER chat.process_message ENABLE;
DROP TRIGGER IF EXISTS chat.process_message;
```

`start` is `latest` (default) or `earliest` and is captured when the trigger
is created. The dispatcher consumes each partition in order, ACKs after a
successful commit, retries with backoff, then writes `system.trigger_attempts`
status `dlq`. Nested `ctx.functions.call` keeps `ctx.source.kind = "topic"`
and sets `ctx.parent` to the trigger procedure. Disabling or dropping a
trigger keeps committed offsets.

Catalog rows live in `system.triggers` and `system.trigger_attempts`.
Consumer group id is `trigger:{trigger_id}`.

## Execute As

`EXECUTE AS` syntax is wrapper-only. It switches USER-table or
STREAM-table execution to a target user ID only when the authenticated actor
role is allowed to target that ID's cached role class.

```sql
EXECUTE AS '<user_id>' (
  <single_statement>
);
```

Examples:

```sql
EXECUTE AS 'user_123' (
  SELECT * FROM app.messages WHERE conversation_id = 42
);
```

Rules:

1. The wrapper must contain exactly one SQL statement.
2. The target user ID must be single-quoted.
3. System users may target system, dba, service, and user accounts.
4. DBA users may target dba, service, and user accounts.
5. Service users may target service and user accounts.
6. Regular users may only use self-targeted `EXECUTE AS '<user_id>'` as a no-op identity boundary.
7. The wrapper is valid for USER and STREAM tables; shared tables use their table policy directly.
8. Target role checks are hot-path cached: service, DBA, and system user IDs are tracked in memory from `system.users`; soft-deleted privileged IDs stay classified by their persisted role, and target IDs not present in that privileged cache are treated as regular users.
9. Legacy inline `... AS USER 'name'` syntax is not supported.

## User Management

### CREATE USER

```sql
CREATE USER '<username>'
  WITH <PASSWORD '<password>' | OIDC '<oidc_json>'>
  ROLE <user|service|dba|system>
  [EMAIL '<email>']
  [STORAGE_MODE <table|region>]
  [STORAGE_ID '<storage_id>'];
```

`WITH OIDC` creates an external OIDC user. The payload must contain the OIDC issuer and subject. `WITH OAUTH` is still accepted as a compatibility alias for older scripts.

```sql
CREATE USER 'provider-subject'
  WITH OIDC '{"issuer": "https://idp.example.com/realms/kalamdb", "subject": "provider-subject"}'
  ROLE user
  EMAIL 'alice@example.com';
```

For OIDC users, the `CREATE USER` id must match the OIDC `subject`. KalamDB uses that subject directly as the authenticated user id.

### ALTER USER

```sql
ALTER USER '<username>' SET PASSWORD '<new_password>';
ALTER USER '<username>' SET ROLE <user|service|dba|system>;
ALTER USER '<username>' SET EMAIL '<new_email>';
ALTER USER '<username>' SET STORAGE_MODE <table|region>;
ALTER USER '<username>' SET STORAGE_ID '<storage_id>';
ALTER USER '<username>' SET STORAGE_ID NULL;
```

### DROP USER

```sql
DROP USER '<username>';
DROP USER IF EXISTS '<username>';
```

## Storage Commands

### CREATE STORAGE

```sql
CREATE STORAGE <storage_id>
  TYPE '<filesystem|s3|gcs|azure>'
  [NAME '<storage_name>']
  [DESCRIPTION '<description>']
  [PATH '<path>']
  [BUCKET '<bucket_or_s3_url>']
  [REGION '<region>']
  [BASE_DIRECTORY '<path_or_url>']
  [SHARED_TABLES_TEMPLATE '<template>']
  [USER_TABLES_TEMPLATE '<template>']
  [CREDENTIALS '<json_credentials>']
  [CONFIG '<json_config>'];
```

Examples:

```sql
CREATE STORAGE local
  TYPE 'filesystem'
  PATH './data';

CREATE STORAGE s3_prod
  TYPE 's3'
  BUCKET 'my-bucket'
  REGION 'us-west-2'
  CREDENTIALS '{"access_key_id":"...","secret_access_key":"..."}';
```

### ALTER STORAGE

```sql
ALTER STORAGE <storage_id>
  [SET NAME '<new_name>']
  [SET DESCRIPTION '<new_description>']
  [SET SHARED_TABLES_TEMPLATE '<new_template>']
  [SET USER_TABLES_TEMPLATE '<new_template>']
  [SET CONFIG '<json_config>'];
```

### DROP STORAGE

```sql
DROP STORAGE <storage_id>;
DROP STORAGE IF EXISTS <storage_id>;
```

### SHOW STORAGES

```sql
SHOW STORAGES;
```

### STORAGE CHECK

```sql
STORAGE CHECK <storage_id>;
STORAGE CHECK <storage_id> EXTENDED;
```

### STORAGE FLUSH

```sql
STORAGE FLUSH TABLE <namespace>.<table_name>;
STORAGE FLUSH ALL IN <namespace>;
STORAGE FLUSH ALL IN NAMESPACE <namespace>;
STORAGE FLUSH ALL;
```

### STORAGE COMPACT

```sql
STORAGE COMPACT TABLE <namespace>.<table_name>;
STORAGE COMPACT ALL IN <namespace>;
STORAGE COMPACT ALL IN NAMESPACE <namespace>;
STORAGE COMPACT ALL;
```

### SHOW MANIFEST

```sql
SHOW MANIFEST;
```

## Job Commands

### KILL JOB

```sql
KILL JOB '<job_id>';
```

## Live Query Commands

### SUBSCRIBE TO

```sql
SUBSCRIBE TO <namespace>.<table_name>
[WHERE <condition>]
[OPTIONS (last_rows=<n>, batch_size=<n>, from_seq_id=<n>)];
```

### KILL LIVE QUERY

```sql
KILL LIVE QUERY '<subscription_id>';
```

## Topic / Consume Commands

### CREATE TOPIC

```sql
CREATE TOPIC <topic_name>;
CREATE TOPIC <topic_name> PARTITIONS <count>;
```

### DROP TOPIC

```sql
DROP TOPIC <topic_name>;
```

### CLEAR TOPIC

```sql
CLEAR TOPIC <topic_name>;
```

### ALTER TOPIC ADD SOURCE

```sql
ALTER TOPIC <topic_name>
ADD SOURCE <table_name_or_namespace.table_name>
ON <INSERT|UPDATE|DELETE>
[WHERE <filter_expression>]
[WITH (payload = '<key|full|diff>')];
```

`WHERE` is evaluated against the row routed for the selected operation. That lets
you publish only a subset of inserts or updates into a worker topic.

Example: publish task-cancellation work only when a task is already cancelled on
insert, or becomes cancelled on update.

```sql
ALTER TOPIC app.task_cancellations
ADD SOURCE app.tasks
ON INSERT
WHERE cancelled = true
WITH (payload = 'full');

ALTER TOPIC app.task_cancellations
ADD SOURCE app.tasks
ON UPDATE
WHERE cancelled = true
WITH (payload = 'full');
```

### CONSUME FROM

```sql
CONSUME FROM <topic_name>
[GROUP '<group_id>']
[FROM <LATEST|EARLIEST|offset>]
[LIMIT <count>];
```

Examples:

```sql
CONSUME FROM app.new_messages;
CONSUME FROM app.new_messages GROUP 'worker-1' FROM EARLIEST LIMIT 100;
CONSUME FROM app.new_messages GROUP 'worker-1' FROM 250;
```

`CONSUME FROM ... GROUP ...` reserves a delivery range for the group but does
not commit progress. After processing the returned rows, commit progress with
`ACK`. If the caller does not ACK before the configured topic visibility
timeout, the unacked range can be delivered again to the same group.

### ACK

```sql
ACK <topic_name>
GROUP '<group_id>'
[PARTITION <partition_id>]
UPTO OFFSET <offset>;
```

### RESET CONSUMER GROUP

```sql
RESET CONSUMER GROUP '<group_id>'
ON <topic_name>
[PARTITION <partition_id>]
TO <next_offset>;
```

Examples:

```sql
RESET CONSUMER GROUP 'worker-1' ON app.new_messages TO 0;
RESET CONSUMER GROUP 'worker-1' ON app.new_messages PARTITION 0 TO 250;
```

`RESET CONSUMER GROUP` is admin-only and moves one consumer-group partition to
the next offset you specify. It also clears pending in-memory claims for that
group partition so the reset takes effect immediately.

## Cluster Commands

```sql
CLUSTER LIST;
CLUSTER STATUS;
CLUSTER SNAPSHOT;
CLUSTER PURGE --UPTO <index>;
CLUSTER PURGE <index>;
CLUSTER TRIGGER ELECTION;
CLUSTER TRIGGER-ELECTION;
CLUSTER TRANSFER LEADER <node_id>;
CLUSTER TRANSFER-LEADER <node_id>;
CLUSTER STEPDOWN;
CLUSTER STEP-DOWN;
CLUSTER CLEAR;
```

## Backup / Restore Commands

### EXPORT USER DATA

```sql
EXPORT USER DATA;
```

### SHOW EXPORT

```sql
SHOW EXPORT;
```

`SHOW EXPORT` returns a `download_url` URI path such as
`/v1/exports/<user_id>/<export_id>`. Prefix it with your KalamDB server base URL
when downloading the finished ZIP over HTTP.

The Admin UI table editor also supports scoped table data transfer for `user` and
`shared` tables. A user-table export requires a `user_id`; shared-table export omits
the user scope. Table export ZIPs contain committed Parquet segments plus KalamDB
manifest metadata, and table import accepts that ZIP format through the Admin UI when
the target table already exists with matching columns.

### BACKUP DATABASE

```sql
BACKUP DATABASE TO '<backup_path>';
```

`<backup_path>` is a path on the server filesystem. If it ends with `.tar.gz`
or `.tgz`, KalamDB writes a single archive file there. Otherwise it writes the
backup directory layout directly under that path. `BACKUP DATABASE` requires a
DBA or System role.

### RESTORE DATABASE

```sql
RESTORE DATABASE FROM '<backup_path>';
```

`<backup_path>` is a path on the server filesystem and may point to either a
backup directory or a `.tar.gz` / `.tgz` archive created by `BACKUP DATABASE`.
The restore job copies Parquet and stream files in place and stages RocksDB into
a sibling `rocksdb_restore_pending_*` directory. A server restart promotes the
newest complete staged copy onto the live RocksDB path and deletes leftover
staging directories. Incomplete or older unmarked staging dirs are discarded
without replacing the live database. `RESTORE DATABASE` requires a DBA or System
role.

## Built-in Functions (Common)

These are SQL expression functions for `SELECT` lists, defaults, and predicates.
They are not procedures. Application logic that writes tables or publishes
topics uses [`CALL`](#call), not these names.

```sql
SELECT SNOWFLAKE_ID();
SELECT UUID_V7();
SELECT ULID();
SELECT CURRENT_USER();
SELECT NOW();
```
