# Quickstart: PostgreSQL-Style Transactions for KalamDB

## Goal

Validate that explicit transactions behave like PostgreSQL transactions for pg_kalam callers and for KalamDB's own SQL execution path.

## Prerequisites

1. Start the KalamDB server from `/Users/jamal/git/KalamDB/backend`.
2. Ensure the pg RPC service is reachable by the pg_kalam test environment.
3. Create a test namespace and test tables for shared and user-scoped data.

## Scenario 1: pg_kalam commit across multiple statements

Execute from PostgreSQL against foreign tables backed by KalamDB:

```sql
BEGIN;
INSERT INTO app.messages_fdw (id, name) VALUES (1001, 'first');
INSERT INTO app.audit_fdw (id, name) VALUES (2001, 'audit');
COMMIT;
```

Expected result:

- Both rows are visible after commit.
- No partial state is visible to other sessions before commit.

## Scenario 2: pg_kalam rollback and read-your-writes

```sql
BEGIN;
INSERT INTO app.messages_fdw (id, name) VALUES (1002, 'draft');
SELECT id, name FROM app.messages_fdw WHERE id = 1002;
ROLLBACK;
SELECT id, name FROM app.messages_fdw WHERE id = 1002;
```

Expected result:

- The first `SELECT` inside the transaction returns the inserted row.
- The post-rollback `SELECT` returns no rows.

## Scenario 3: concurrent-session isolation

1. Session A:

```sql
BEGIN;
INSERT INTO app.messages_fdw (id, name) VALUES (1003, 'hidden');
```

2. Session B:

```sql
SELECT id, name FROM app.messages_fdw WHERE id = 1003;
```

3. Session A:

```sql
COMMIT;
```

4. Session B repeats the `SELECT`.

Expected result:

- Session B does not see row `1003` before Session A commits.
- Session B sees row `1003` after Session A commits.

## Scenario 4: KalamDB server SQL transaction statements

Send one request to `/v1/api/sql`:

```json
{
  "sql": "BEGIN; INSERT INTO app.messages (id, name) VALUES (3001, 'rest'); INSERT INTO app.messages (id, name) VALUES (3002, 'rest-2'); COMMIT;",
  "namespace_id": "app"
}
```

Expected result:

- Response status is `success`.
- Both rows are visible in a later query.

This validates that `BEGIN` and `COMMIT` are supported by the KalamDB server SQL execution path itself.

## Scenario 5: multiple transaction blocks in one API request

Send one request to `/v1/api/sql` with two sequential transaction blocks:

```json
{
  "sql": "BEGIN; INSERT INTO app.messages (id, name) VALUES (3010, 'kept'); COMMIT; BEGIN; INSERT INTO app.messages (id, name) VALUES (3011, 'dropped'); ROLLBACK;",
  "namespace_id": "app"
}
```

Expected result:

- Row `3010` is visible afterward.
- Row `3011` is not visible afterward.
- No request-scoped transaction remains active after the response.

## Scenario 6: rollback on statement failure

Send one request to `/v1/api/sql` where the second statement fails:

```json
{
  "sql": "BEGIN; INSERT INTO app.messages (id, name) VALUES (3003, 'ok'); INSERT INTO app.messages (id, missing_col) VALUES (3004, 'bad'); COMMIT;",
  "namespace_id": "app"
}
```

Expected result:

- The response is an error.
- Row `3003` is not visible afterward.
- No partial transaction state remains.

## Scenario 7: request-end cleanup for unclosed SQL transactions

Send one request to `/v1/api/sql` that opens a transaction and never closes it:

```json
{
  "sql": "BEGIN; INSERT INTO app.messages (id, name) VALUES (3012, 'orphaned');",
  "namespace_id": "app"
}
```

Expected result:

- The request returns an error.
- Row `3012` is not visible afterward.
- The server has no remaining request-scoped transaction from that call.

## Scenario 8: timeout and disconnect cleanup

1. Begin an explicit transaction.
2. Stage at least one write.
3. Force client disconnect or wait past the configured timeout.
4. Query from a fresh session.

Expected result:

- The staged writes are absent.
- The transaction no longer appears in active transaction metrics.

## Recommended Validation Commands

- Backend targeted tests: `cd /Users/jamal/git/KalamDB/backend && cargo nextest run -p kalamdb-pg -p kalamdb-core`
- PostgreSQL extension tests: `cd /Users/jamal/git/KalamDB/pg && cargo nextest run`
- SQL REST integration checks: exercise `/v1/api/sql` with `BEGIN ... COMMIT`, `BEGIN ... ROLLBACK`, and a failing transaction that proves rollback
- SQL REST integration checks: also verify multiple sequential transaction blocks in one request and request-end rollback of any unclosed block
- Performance checks: compare autocommit reads/writes before and after the feature to verify non-transaction regressions stay within 5%