# WebSocket Server Architecture

## Overview

KalamDB's WebSocket server provides real-time live query subscriptions. Clients connect, authenticate, subscribe to SQL queries, and receive change notifications (INSERT/UPDATE/DELETE) as they happen.

**Stack**: actix-web HTTP upgrade → actix-ws (non-actor) → per-connection Tokio task → `ConnectionsManager` (shared heartbeat checker)

---

## Connection Lifecycle

```
Client                         Server
  │                              │
  ├── GET /v1/ws ───────────────►│  HTTP upgrade
  │                              │  1. Validate Origin header (if configured)
  │                              │  2. Check server not shutting down
  │                              │  3. Check max_connections not exceeded (default 25,000)
  │                              │  4. Generate ConnectionId (UUID)
  │                              │  5. Register in ConnectionsManager
  │                              │  6. Spawn per-connection Tokio task
  │◄──────────────── 101 Switch ─┤
  │                              │
  │                        ┌─────┤  Auth timeout starts (default 10s)
  │── Authenticate{JWT} ──►│     │
  │                        │     │  7. Rate limit check
  │                        │     │  8. Validate JWT token
  │                        │     │  9. Audit log
  │◄── AuthSuccess ────────┘     │  10. Mark authenticated in ConnectionState
  │                              │
  │── Subscribe{id,sql} ──►│     │  11. Rate limit + subscription limit check
  │                        │     │  12. Parse SQL, extract table/filter/projections
  │                        │     │  13. Register in LiveQueryManager
  │                        │     │  14. Fetch initial data snapshot
  │◄── SubscriptionAck ───┘     │  15. Send Ack + InitialDataBatch
  │◄── InitialDataBatch ──┘     │
  │                              │
  │  (If has_more=true)          │
  │── NextBatch ───────────►│    │  16. Fetch next page from snapshot
  │◄── InitialDataBatch ──┘     │
  │                              │
  │   ... live notifications ... │
  │◄── Notification{type,data} ─┤  17. Change detected → filter → deliver
  │                              │
  │── Unsubscribe{id} ────►│    │  18. Remove from LiveQueryManager
  │                              │
  │── Close ───────────────►│    │  19. Cleanup all subscriptions
  │                              │     20. Unregister from ConnectionsManager
```

---

## Core Components

### 1. `websocket_handler` (kalamdb-api)
HTTP endpoint (`GET /v1/ws`) that upgrades to WebSocket. Validates origin, generates connection ID, registers with ConnectionsManager, then spawns a per-connection `handle_websocket` task.

### 2. `handle_websocket` (kalamdb-api)
Per-connection `tokio::select!` loop with **biased** priority ordering:
1. **Events** (highest): `AuthTimeout`, `HeartbeatTimeout`, `Shutdown` from ConnectionsManager
2. **Messages**: Client WebSocket frames (Ping/Pong/Text/Binary/Close)
3. **Notifications** (lowest): Live query change notifications

### 3. `ConnectionsManager` (kalamdb-core)
Shared singleton managing ALL connections:
- **Primary storage**: `DashMap<ConnectionId, SharedConnectionState>` 
- **Subscription indices**: `(UserId, TableId) → DashMap<LiveQueryId, SubscriptionHandle>` for O(1) notification routing
- **Background heartbeat checker**: Single Tokio task, ticks every `heartbeat_interval` (default 5s), iterates ALL connections
- **Channel-based communication**: Each connection gets bounded `event_tx/rx` (cap 64) and `notification_tx/rx` (cap 1000)

### 4. `LiveQueryManager` (kalamdb-core)
Handles subscription registration/unregistration:
- Parses SQL to extract table, filter expressions, projections
- Creates `SubscriptionState` and `SubscriptionHandle`
- Fetches initial data snapshot
- Manages flow control (buffering notifications during initial load)

### 5. `NotificationService` (kalamdb-core)
Receives `ChangeNotification` from table providers when data changes:
- Looks up all `SubscriptionHandle`s for `(user_id, table_id)`
- Evaluates per-subscription filter expressions
- Applies column projections
- Builds `Notification` JSON and sends via `notification_tx.try_send()`
- Non-blocking: drops notification if channel is full

---

## Heartbeat System

```
Client (kalam-client)                       Server (ConnectionsManager)
    │                                           │
    ├── Every keepalive_interval (6s):          ├── Every heartbeat_interval (5s):
    │   └── Send WebSocket Ping frame ──────►   │   ├── For each connection:
    │                                           │   │   ├── If not authenticated && !auth_started
    │                                           │   │   │   && elapsed > auth_timeout → AuthTimeout
    │                                           │   │   └── If millis_since_heartbeat > client_timeout
    │                                           │   │       → HeartbeatTimeout
    │                                           │   └── Force-unregister if event channel full
    │                                           │
    └── Server Pong auto-reply ◄────────────    └── MissedTickBehavior::Skip
```

**Client-driven keepalive**: kalam-client sends periodic WebSocket `Ping` frames (default 6s).
The server never initiates pings — it only checks for stale connections by reading atomic
timestamps. This eliminates thundering-herd effects at scale (>40K connections).

**Default Timeouts**:
- `client_timeout`: 10s (how long since last activity before disconnect)
- `auth_timeout`: 3s (how long for initial authentication)
- `heartbeat_interval`: 5s (how often the checker runs)

**Activity tracking**: `connection_state.read().update_heartbeat()` is called (lock-free atomic store) on every Ping, Pong, and Text message received.

---

## Notification Delivery Path

```
Table Provider (INSERT/UPDATE/DELETE)
    │
    ▼
NotificationService::notify_table_change()
    │
    ├── Spawns async task (tokio::spawn)
    ├── Looks up SubscriptionHandles for (user_id, table_id)
    ├── For each handle:
    │   ├── Evaluate filter expression against row data
    │   ├── Apply column projections
    │   ├── Convert Row → JSON HashMap (cached for full-row subscriptions)
    │   ├── Build Notification struct
    │   ├── If initial load incomplete → buffer in FlowControl
    │   └── Else → try_send(Arc<Notification>) to notification_tx
    │
    ▼
Per-connection select! loop
    │
    ├── notification_rx.recv()
    ├── serde_json::to_string(notification)  ← serialized per-connection
    ├── maybe_compress() if > 512 bytes
    └── session.text(json) or session.binary(compressed)
```

---

## Channel Capacities

| Channel | Capacity | Purpose |
|---------|----------|---------|
| `event_tx/rx` | 16 | Control events (ping, timeout, shutdown) |
| `notification_tx/rx` | 1000 | Live query notifications per connection |

---

## Security Layers

1. **Origin validation**: Configurable allowed origins list
2. **Message size limit**: Default 64KB max WebSocket message
3. **Rate limiting**: Per-connection message rate + per-user subscription count
4. **Auth timeout**: Connection closed if no auth within timeout
5. **Max connections**: Server-wide limit (default 25,000)
6. **DoS protection**: Rejects new connections at capacity

---

## Configuration (server.toml)

```toml
[websocket]
client_timeout_secs = 30      # Heartbeat timeout
auth_timeout_secs = 10        # Auth deadline
heartbeat_interval_secs = 5   # Checker tick interval

[performance]
max_connections = 25000        # Server-wide WS limit

[security]
max_ws_message_size = 65536    # 64KB message limit
allowed_ws_origins = []        # Origin whitelist (empty = allow all)
strict_ws_origin_check = false # Require Origin header
```
