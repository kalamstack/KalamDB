# Contract: SQL Transaction Batch over `/v1/api/sql`

## Purpose

Define how explicit transaction SQL statements work in KalamDB's server-side SQL execution path, exposed first through the existing SQL REST endpoint, without introducing a new cross-request HTTP transaction session.

## Endpoint

- Method: `POST`
- Path: `/v1/api/sql`
- Request body: existing `QueryRequest { sql, params?, namespace_id? }`
- Response body: existing `SqlResponse`

## Supported Statements Inside One Request

- `BEGIN`
- `START TRANSACTION`
- `COMMIT`
- `ROLLBACK`
- DML statements between those control statements

These statements must be executed by KalamDB's own SQL engine, not treated as pg-only transport markers.

## Request-Scoped Transaction Rules

- A transaction started by `BEGIN` exists only for the lifetime of the current multi-statement SQL request.
- One request may contain zero, one, or more sequential explicit transaction blocks.
- Statements after `BEGIN` in the same request execute in the currently active explicit transaction until `COMMIT` or `ROLLBACK`.
- After a `COMMIT` or `ROLLBACK`, a later `BEGIN` in the same request starts a new request-scoped transaction block.
- If the request completes with one or more open transactions that were not closed explicitly, the server rolls them all back before the request finishes and returns an error response.
- Nested `BEGIN` statements are rejected.

## Contrast With pg gRPC

- `/v1/api/sql`: transaction lifetime is bounded by a single HTTP request.
- pg gRPC: transaction lifetime is bounded by the pg session and may span multiple RPC calls until explicit commit/rollback or session close.

## Visibility Rules

- Reads inside the request-scoped transaction see:
  - committed rows from the snapshot captured at `BEGIN`
  - the request's own staged writes
- Reads outside that explicit transaction do not see staged writes.

## Non-Goals for This Contract

- No persistent HTTP transaction token is added in this phase.
- No savepoints or nested transactions.
- No DDL inside explicit transaction blocks.

## Example Request

```json
{
  "sql": "BEGIN; INSERT INTO app.messages (id, name) VALUES (1, 'a'); UPDATE app.messages SET name = 'b' WHERE id = 1; COMMIT;",
  "namespace_id": "app"
}
```

## Example Success Behavior

- The request returns `status: "success"`.
- Result sets for control statements may be empty, but all enclosed DML succeeds atomically.
- Subsequent requests see the committed data.

## Example Multi-Transaction Request

```json
{
  "sql": "BEGIN; INSERT INTO app.messages (id, name) VALUES (10, 'first'); COMMIT; BEGIN; INSERT INTO app.messages (id, name) VALUES (11, 'second'); ROLLBACK;",
  "namespace_id": "app"
}
```

Expected behavior:

- Row `10` is committed.
- Row `11` is discarded.
- No request-scoped transaction remains alive after the response.

## Example Failure Behavior

- If any enclosed DML statement fails before `COMMIT`, the explicit transaction is rolled back.
- The response returns `status: "error"` with the existing SQL error structure.
- No partial writes remain visible after the failed request.

## Required Automated Coverage

- A test for `BEGIN ... COMMIT` through `/v1/api/sql`
- A test for `BEGIN ... ROLLBACK` through `/v1/api/sql`
- A test for multiple sequential transaction blocks in one `/v1/api/sql` request
- A test proving request termination rolls back all still-open request-scoped transactions
- A test for rollback when a statement inside the explicit transaction fails
- A test that nested `BEGIN` is rejected
- A regression test proving plain autocommit SQL still behaves as before when no transaction statements are present
