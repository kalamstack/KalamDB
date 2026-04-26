# KalamDB WebSocket Protocol

**Endpoint**: `ws://<host>:8080/v1/ws`  
**Transport**: RFC 6455 WebSocket  
**Message encoding**: JSON text frames, plus optional gzip-compressed binary frames for large server messages

This document is aligned with the current server implementation in `kalamdb-api` + `kalamdb-commons`.

## 1) Connection and Authentication

### 1.1 HTTP Upgrade

- Client opens `GET /v1/ws`
- Connection can be rejected before upgrade when:
  - Server is shutting down (`503 Service Unavailable`)
  - Origin is not allowed (`403 Forbidden`)
  - Origin header is required (strict mode) but missing (`403 Forbidden`)

### 1.2 Origin validation

Origin allow-list is read from `security.cors.allowed_origins`.

Behavior:

- Empty allow-list = no origin restriction
- Allow-list containing `"*"` = any origin
- Otherwise origin must exactly match an allowed value
- If `security.strict_ws_origin_check = true`, missing `Origin` header is rejected

### 1.3 Post-connect authentication (required)

A connection is **unauthenticated** after upgrade. Client must send:

```json
{
  "type": "authenticate",
  "method": "jwt",
  "token": "<JWT_TOKEN>"
}
```

Important:

- Only JWT auth is supported for WebSocket auth messages
- If authentication fails, server sends `auth_error` and closes the connection
- Auth timeout is controlled by `websocket.auth_timeout_secs` (default `3` seconds)

Success message:

```json
{
  "type": "auth_success",
  "user_id": "u_...",
  "role": "Dba"
}
```

Failure message:

```json
{
  "type": "auth_error",
  "message": "Invalid username or password"
}
```

## 2) Frame Behavior and Compression

Server sends JSON in two frame styles:

- **Text frames**: normal JSON
- **Binary frames**: gzip-compressed JSON when payload is large enough and compression is beneficial

Current compression behavior:

- Compression threshold: `512` bytes
- Compression format: gzip
- If compressed payload is not smaller, server keeps plain text frame

Client requirements:

- Must handle both text JSON and binary gzip JSON from server
- Must not send binary frames to server (binary client frames are rejected)

## 3) Client → Server Messages

All client messages use `{"type": ...}` tagged JSON.

### 3.1 Authenticate

```json
{
  "type": "authenticate",
  "method": "jwt",
  "token": "<JWT_TOKEN>"
}
```

### 3.2 Subscribe

```json
{
  "type": "subscribe",
  "subscription": {
    "id": "orders_live",
    "sql": "SELECT id, status, _seq FROM app.orders WHERE status = 'open'",
    "options": {
      "batch_size": 500,
      "last_rows": 100,
      "from_seq_id": 12345
    }
  }
}
```

`subscription` fields:

- `id` (string, required, must not be empty/blank)
- `sql` (string, required)
- `options` (optional object)
  - `batch_size?: number`
  - `last_rows?: number`
  - `from_seq_id?: number`

Option precedence used by server:

1. If `from_seq_id` exists: resume from that sequence
2. Else if `last_rows` exists: fetch last N rows
3. Else: default batch mode

### 3.3 Next batch

```json
{
  "type": "next_batch",
  "subscription_id": "orders_live",
  "last_seq_id": 12345
}
```

`last_seq_id` is optional.

### 3.4 Unsubscribe

```json
{
  "type": "unsubscribe",
  "subscription_id": "orders_live"
}
```

No unsubscribe acknowledgement message is currently emitted.

## 4) Server → Client Messages

### 4.1 Authentication result

- `auth_success`
- `auth_error`

(see examples above)

### 4.2 Subscription acknowledgment

```json
{
  "type": "subscription_ack",
  "subscription_id": "orders_live",
  "total_rows": 0,
  "batch_control": {
    "batch_num": 0,
    "has_more": true,
    "status": "loading",
    "last_seq_id": 12345
  },
  "schema": [
    {"name": "id", "data_type": "BigInt", "index": 0},
    {"name": "status", "data_type": "Text", "index": 1},
    {"name": "_seq", "data_type": "BigInt", "index": 2}
  ]
}
```

Notes:

- `schema` is included in `subscription_ack`
- `total_rows` is currently set to `0` by handler path

### 4.3 Initial data batch

```json
{
  "type": "initial_data_batch",
  "subscription_id": "orders_live",
  "rows": [
    {"id": 1, "status": "open", "_seq": 12346}
  ],
  "batch_control": {
    "batch_num": 0,
    "has_more": true,
    "status": "loading",
    "last_seq_id": 12346
  }
}
```

The client resume cursor is always `last_seq_id`; snapshot and commit
boundaries are backend-owned and are not part of the WebSocket contract.

`batch_control.status` values:

- `loading` (first batch, more pending)
- `loading_batch` (subsequent batch, more pending)
- `ready` (initial load complete, live streaming active)

### 4.4 Change notification

```json
{
  "type": "change",
  "subscription_id": "orders_live",
  "change_type": "insert",
  "rows": [
    {"id": 2, "status": "open", "_seq": 13001}
  ]
}
```

`change_type` is one of:

- `insert`
- `update`
- `delete`

Shape rules:

- `insert`: `rows` present, `old_values` absent
- `update`: both `rows` and `old_values` present
- `delete`: `old_values` present, `rows` absent

### 4.5 WebSocket error notification

```json
{
  "type": "error",
  "subscription_id": "orders_live",
  "code": "INVALID_SQL",
  "message": "..."
}
```

For protocol/rate-limit level errors, `subscription_id` may be synthetic values like `"protocol"`, `"rate_limit"`, or `"subscribe"`.

## 5) Validation and Authorization Rules

### 5.1 Authentication gating

- `subscribe` before auth: returns error `AUTH_REQUIRED`
- `next_batch` or `unsubscribe` before auth: currently ignored (no response)

### 5.2 Subscription SQL constraints

Current server-side constraints include:

- SQL must resolve to `namespace.table` format
- System namespace subscriptions require `dba` or `system` role
- Shared tables are rejected for live subscriptions
- User and stream tables are allowed (subject to role/permissions)

### 5.3 Subscription limits

Two limits can apply:

- Per-user configured limit: `rate_limit.max_subscriptions_per_user` (default `10`)
- Hard per-connection cap: `100` subscriptions

### 5.4 Message limits and rate limiting

- Max incoming message size: `security.max_ws_message_size` (default `1MB`)
- Max message rate per connection: `rate_limit.max_messages_per_sec` (default `50`)
- Client binary message frames are rejected (`UNSUPPORTED_DATA`)

## 6) Error Codes

WebSocket error codes currently used:

- `AUTH_REQUIRED`
- `INVALID_SUBSCRIPTION_ID`
- `SUBSCRIPTION_LIMIT_EXCEEDED`
- `UNAUTHORIZED`
- `NOT_FOUND`
- `INVALID_SQL`
- `UNSUPPORTED`
- `SUBSCRIPTION_FAILED`
- `CONVERSION_ERROR`
- `BATCH_FETCH_FAILED`
- `MESSAGE_TOO_LARGE`
- `RATE_LIMIT_EXCEEDED`
- `UNSUPPORTED_DATA`
- `PROTOCOL`

## 7) Timeouts and Keepalive

Connection manager settings:

- `websocket.auth_timeout_secs` (default `3`)
- `websocket.client_timeout_secs` (default `10`)
- `websocket.heartbeat_interval_secs` (default `5`)

Behavior:

- Server sends ping control frames
- Client should respond with pong (standard WebSocket behavior)
- Heartbeat timeout leads to connection close

## 8) Failure Behavior Summary

- Invalid JSON message payload: connection is closed with error close code
- Auth timeout: `auth_error` then close
- Auth failure: `auth_error` then close
- Server shutdown: connection closed with shutdown reason
- Oversized/rate-limited/binary client message: server sends `type=error` notification
