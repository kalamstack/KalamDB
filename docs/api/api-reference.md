# KalamDB API Reference

**Base URL**: `http://<host>:8080`  
**Version prefix**: `/v1`

This reference is aligned with the current route + handler implementations.

## 1) Route Map

### Health and status

- `GET /health` (localhost-only)
- `GET /v1/api/healthcheck` (localhost-only)
- `GET /v1/api/cluster/health` (localhost-only)

### SQL and files

- `POST /v1/api/sql`
- `GET /v1/files/{namespace}/{table_name}/{subfolder}/{file_id}`

### WebSocket

- `GET /v1/ws`

### Auth (Admin UI + token management)

- `POST /v1/api/auth/login`
- `POST /v1/api/auth/refresh`
- `POST /v1/api/auth/logout`
- `GET /v1/api/auth/me`
- `POST /v1/api/auth/setup`
- `GET /v1/api/auth/status`

### Topic HTTP API

- `POST /v1/api/topics/consume`
- `POST /v1/api/topics/ack`

## 2) Authentication Rules by Endpoint

### Bearer-token-only endpoints

These endpoints use `AuthSessionExtractor` and require:

```http
Authorization: Bearer <JWT_TOKEN>
```

Basic auth is rejected on these routes.

Direct `user` / `password` credentials are only accepted on `POST /v1/api/auth/login`.

- `POST /v1/api/sql`
- `GET /v1/files/...`
- `POST /v1/api/topics/consume`
- `POST /v1/api/topics/ack`

### Cookie or Bearer endpoints

These endpoints accept bearer token in header, or auth cookie fallback:

- `POST /v1/api/auth/refresh`
- `GET /v1/api/auth/me`

### Public (no auth required)

- `POST /v1/api/auth/login`
- `POST /v1/api/auth/logout` (just clears cookie)
- `POST /v1/api/auth/setup` (localhost-only unless remote setup allowed)
- `GET /v1/api/auth/status` (localhost-only unless remote setup allowed)
- `GET /health` (localhost-only)
- `GET /v1/api/healthcheck` (localhost-only)
- `GET /v1/api/cluster/health` (localhost-only)

### WebSocket auth

`GET /v1/ws` upgrades unauthenticated; authentication is done by an in-band WebSocket message:

```json
{"type":"authenticate","method":"jwt","token":"..."}
```

See [WebSocket Protocol](websocket-protocol.md).

## 3) SQL API

## `POST /v1/api/sql`

Execute SQL using JSON or multipart payload.

### Request headers

- `Authorization: Bearer <JWT_TOKEN>` (required)
- `Content-Type: application/json` or `multipart/form-data`

### JSON body

```json
{
  "sql": "SELECT * FROM default.users WHERE id = $1",
  "params": [123],
  "namespace_id": "default"
}
```

Fields:

- `sql` (required string)
- `params` (optional array)
- `namespace_id` (optional string)

### Multipart body (FILE datatype path)

Expected form parts:

- `sql` (required)
- `params` (optional; JSON array string)
- `namespace_id` (optional)
- `file:<placeholder>` one or more file parts

Placeholder mapping:

- SQL uses `FILE("name")` or `FILE('name')`
- Multipart part must be named `file:name`

Example:

```sql
INSERT INTO app.docs (id, attachment)
VALUES ('d1', FILE("contract"));
```

Multipart must include part name `file:contract`.

### SQL execution behavior

- Multiple statements separated by `;` are supported
- Parameters with multi-statement batches are rejected (`PARAMS_WITH_BATCH`)
- File uploads require exactly one SQL statement
- SQL length is bounded by `MAX_SQL_QUERY_LENGTH`
- In cluster mode:
  - write operations on followers are forwarded to leader
  - file uploads must be sent to leader (`NOT_LEADER` if not)

### Success response shape

```json
{
  "status": "success",
  "results": [
    {
      "schema": [{"name":"id","data_type":"BigInt","index":0}],
      "rows": [[1]],
      "row_count": 1,
      "as_user": "alice"
    }
  ],
  "took": 12.34
}
```

`results[]` fields:

- `schema` (omitted when empty)
- `rows` (omitted when none)
- `row_count` (always present)
- `message` (optional; e.g. DDL/DML summary)
- `as_user` (always present)

### Error response shape

```json
{
  "status": "error",
  "results": [],
  "took": 1.23,
  "error": {
    "code": "INVALID_SQL",
    "message": "...",
    "details": null
  }
}
```

### SQL error codes

Current `error.code` values include:

- `RATE_LIMIT_EXCEEDED`
- `INVALID_PARAMETER`
- `BATCH_PARSE_ERROR`
- `EMPTY_SQL`
- `PARAMS_WITH_BATCH`
- `SQL_EXECUTION_ERROR`
- `FORWARD_FAILED`
- `NOT_LEADER`
- `INVALID_SQL`
- `TABLE_NOT_FOUND`
- `PERMISSION_DENIED`
- `CLUSTER_UNAVAILABLE`
- `LEADER_NOT_AVAILABLE`
- `INTERNAL_ERROR`
- `INVALID_INPUT`
- `FILE_TOO_LARGE`
- `TOO_MANY_FILES`
- `MISSING_FILE`
- `EXTRA_FILE`
- `FILE_NOT_FOUND`
- `INVALID_MIME_TYPE`

## 4) File Download API

## `GET /v1/files/{namespace}/{table_name}/{subfolder}/{file_id}`

Download previously stored file bytes.

### Auth

- Bearer token required

### Query params

- `user_id` (optional)
  - Only meaningful for **user tables**
  - Requires impersonation-capable role when different from caller

### Behavior by table type

- `User` table: downloads from effective user scope
- `Shared` table: allowed only if shared access policy permits; `user_id` query is rejected
- `Stream`/`System` table: rejected (`file storage not supported`)

### Validation

- `subfolder` and `file_id` are path-validated (no traversal patterns)

### Responses

- `200 OK`: binary content with inferred content-type and `Content-Disposition: inline`
- `400/403/404` depending on validation/permission/not-found conditions

## 5) Health Endpoints

## `GET /health` and `GET /v1/api/healthcheck`

Both return the same payload and are localhost-only.

Success:

```json
{
  "status": "healthy",
  "version": "...",
  "api_version": "v1",
  "build_date": "..."
}
```

Remote callers receive `403`.

## `GET /v1/api/cluster/health`

Localhost-only cluster/raft health summary.

Response shape:

```json
{
  "status": "healthy|degraded|unhealthy",
  "version": "...",
  "build_date": "...",
  "is_cluster_mode": true,
  "cluster_id": "...",
  "node_id": 1,
  "is_leader": true,
  "total_groups": 3,
  "groups_leading": 2,
  "current_term": 42,
  "last_applied": 1234,
  "millis_since_quorum_ack": 10,
  "nodes": [
    {
      "node_id": 1,
      "role": "Leader",
      "status": "Active",
      "api_addr": "127.0.0.1:8080",
      "is_self": true,
      "is_leader": true,
      "replication_lag": 0,
      "catchup_progress_pct": null
    }
  ]
}
```

## 6) Auth Endpoints

## `POST /v1/api/auth/login`

Authenticates `user` / `password`, returns an access/refresh token pair, and sets the HttpOnly auth cookie.

Request:

```json
{
  "user": "alice",
  "password": "Secret123!"
}
```

Constraints:

- user max length: `128`
- password max length: `256`

Success response:

```json
{
  "user": {
    "id": "u_...",
    "role": "dba",
    "email": null,
    "created_at": "...",
    "updated_at": "..."
  },
  "admin_ui_access": true,
  "expires_at": "...",
  "access_token": "...",
  "refresh_token": "...",
  "refresh_expires_at": "..."
}
```

Only this endpoint accepts direct user/password credentials. Protected SQL, topic, refresh, `/me`, and WebSocket auth flows use bearer tokens or cookies instead.

## `POST /v1/api/auth/refresh`

Accepts bearer token header or auth cookie, validates token, issues new access + refresh pair, and resets auth cookie.

Direct user/password auth is rejected on this endpoint.

Returns same shape as login.

## `POST /v1/api/auth/logout`

Clears auth cookie.

Response:

```json
{"message":"Logged out successfully"}
```

## `GET /v1/api/auth/me`

Returns current user info plus `admin_ui_access` (same `user` object shape as login, without token fields).

## `POST /v1/api/auth/setup`

Initial server bootstrap endpoint.

Allowed only when:

1. root user has no password yet
2. request is localhost (unless `auth.allow_remote_setup = true`)

Request:

```json
{
  "user": "admin",
  "password": "AdminPass123!",
  "root_password": "RootPass123!",
  "email": "admin@example.com"
}
```

Behavior:

- sets root password
- creates new DBA user
- does **not** log in automatically

## `GET /v1/api/auth/status`

Localhost-only (unless remote setup allowed). Returns setup status:

```json
{
  "needs_setup": true,
  "message": "Server requires initial setup..."
}
```

## 7) Topic HTTP API

Both topic endpoints require bearer auth **and** role in `{service, dba, system}`.

### `POST /v1/api/topics/consume`

Request:

```json
{
  "topic_id": "orders_topic",
  "group_id": "worker_group",
  "start": "Latest",
  "limit": 100,
  "partition_id": 0,
  "timeout_seconds": 10
}
```

`start` accepted forms:

- `"Latest"` (default)
- `"Earliest"`
- `{ "Offset": 123 }` (also accepts lowercase `offset` key)

Response:

```json
{
  "messages": [
    {
      "topic_id": "orders_topic",
      "partition_id": 0,
      "offset": 10,
      "payload": "<base64>",
      "key": "optional-key",
      "timestamp_ms": 1730000000000,
      "user": "alice",
      "op": "Insert"
    }
  ],
  "next_offset": 11,
  "has_more": false
}
```

Notes:

- `payload` is base64-encoded bytes
- `timeout_seconds` is accepted in request but is not currently used by handler logic

### `POST /v1/api/topics/ack`

Request:

```json
{
  "topic_id": "orders_topic",
  "group_id": "worker_group",
  "partition_id": 0,
  "upto_offset": 10
}
```

Response:

```json
{
  "success": true,
  "acknowledged_offset": 10
}
```

Topic error shape:

```json
{
  "error": "...",
  "code": "FORBIDDEN|NOT_FOUND|INTERNAL_ERROR"
}
```

## 8) WebSocket Entry Point

## `GET /v1/ws`

- Performs upgrade if origin/security checks pass
- Connection then requires `authenticate` message using JWT
- Subscription/change/error payloads are documented in [WebSocket Protocol](websocket-protocol.md)

## 9) Notes on Non-routed Handlers

`/healthz` and `/readyz` handlers exist in source but are not currently wired into the route configuration. The active health endpoints are `/health` and `/v1/api/healthcheck`.
