# kalam_link

Official Dart and Flutter SDK for [KalamDB](https://kalamdb.org) — private, realtime storage for AI agents.

> Status: **Alpha** — the API surface is still evolving and may include breaking changes between releases.

KalamDB is a SQL-first realtime database. Every user or tenant gets a private namespace. Subscribe to any SQL query live over WebSocket. Publish and consume events via Topics. Store recent data fast and move cold history to object storage.

→ **[kalamdb.org](https://kalamdb.org)** · [Docs](https://kalamdb.org/docs) · [GitHub](https://github.com/jamals86/KalamDB)

## Features

- **SQL Queries** — execute parameterized SQL with `$1`, `$2` placeholders
- **Live Subscriptions** — subscribe to any SQL query; receive inserts, updates, and deletes in real-time over WebSocket
- **Per-tenant isolation** — each user gets a private namespace; no app-side `WHERE user_id = ?` filters needed
- **Topics & Pub/Sub** — publish events to topics and consume them from any client or agent
- **Authentication** — HTTP Basic Auth, JWT tokens, dynamic `authProvider` callback, or anonymous access
- **Cross-platform (Flutter + FFI)** — iOS, Android, macOS, Windows, Linux (powered by `flutter_rust_bridge`)

## Installation

```yaml
dependencies:
  kalam_link: ^0.1.3-alpha.1
```

```bash
flutter pub add kalam_link
```

## Initialization

**`KalamClient.init()` must be called once at app startup** before any other SDK call. It initializes the underlying Rust runtime (loads the native FFI library).

```dart
void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await KalamClient.init();
  runApp(MyApp());
}
```

> **Important:** Only `init()` should be awaited before `runApp()`. Do **not** call `KalamClient.connect()` before `runApp()` — it performs network I/O (WebSocket handshake) and will freeze your app's UI until the connection is established. See [Flutter Integration](#flutter-integration) below for the recommended pattern.

## Connecting

```dart
import 'package:kalam_link/kalam_link.dart';

final client = await KalamClient.connect(
  url: 'https://db.example.com',
  authProvider: () async {
    final token = await myApp.getOrRefreshJwt();
    return Auth.jwt(token);
  },
);
```

### `connect()` options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `String` | required | Server URL |
| `authProvider` | `AuthProvider?` | `null` | **Recommended.** Async callback invoked before each connection for fresh credentials. Takes precedence over `auth`. |
| `auth` | `Auth` | `Auth.none()` | Static credentials. Use `authProvider` for tokens that can expire. |
| `timeout` | `Duration` | 30 s | HTTP request timeout |
| `maxRetries` | `int` | 3 | Retry count for idempotent (SELECT) queries |
| `disableCompression` | `bool` | `false` | Disable gzip on WebSocket (useful during development) |
| `connectionHandlers` | `ConnectionHandlers?` | `null` | Lifecycle event callbacks — see below |

## Authentication

### `authProvider` — recommended

Use `authProvider` for OAuth flows, refresh tokens, or any credentials fetched from secure storage. The callback is invoked before every connection attempt, so tokens are always fresh.

```dart
final client = await KalamClient.connect(
  url: 'https://db.example.com',
  authProvider: () async {
    // fetch from keychain, refresh if expired, etc.
    final token = await myApp.getOrRefreshJwt();
    return Auth.jwt(token);
  },
);
```

Re-invoke the `authProvider` on demand (e.g. after a 401 or on a schedule):

```dart
await client.refreshAuth();

// Periodic refresh example:
Timer.periodic(Duration(minutes: 55), (_) => client.refreshAuth());
```

### Static auth

For simpler scenarios where tokens do not expire:

```dart
// HTTP Basic Auth
final client = await KalamClient.connect(
  url: 'https://db.example.com',
  auth: Auth.basic('alice', 'secret123'),
);

// JWT token
final client = await KalamClient.connect(
  url: 'https://db.example.com',
  auth: Auth.jwt('eyJhbGci...'),
);

// No authentication (localhost bypass)
final client = await KalamClient.connect(
  url: 'http://localhost:8080',
);
```

### Login and token refresh

Exchange Basic credentials for a JWT and use it for subsequent connections:

```dart
final bootstrap = await KalamClient.connect(url: serverUrl);
final tokens = await bootstrap.login('alice', 'secret123');
await bootstrap.dispose();

final client = await KalamClient.connect(
  url: serverUrl,
  auth: Auth.jwt(tokens.accessToken),
);
```

Refresh an expiring token:

```dart
final fresh = await client.refreshToken(tokens.refreshToken!);
// use fresh.accessToken for the next connection
```

## Executing Queries

```dart
// Simple query
final result = await client.query('SELECT * FROM users LIMIT 10');
for (final row in result.rows) {
  print(row); // List<dynamic>
}

// Rows as maps keyed by column name
final maps = result.results.first.toMaps();

// Parameterized query
final result = await client.query(
  r'SELECT * FROM orders WHERE user_id = $1 AND status = $2',
  params: ['user-uuid-123', 'pending'],
);

// Query scoped to a user namespace
final result = await client.query(
  'SELECT * FROM messages LIMIT 5',
  namespace: 'alice',
);
```

### `QueryResponse` fields

| Field | Type | Description |
|-------|------|-------------|
| `success` | `bool` | Whether the query succeeded |
| `results` | `List<QueryResult>` | Result sets (one per statement) |
| `tookMs` | `double?` | Execution time in milliseconds |
| `error` | `ErrorDetail?` | Error details when `success` is `false` |

### `QueryResult` fields

| Field | Type | Description |
|-------|------|-------------|
| `columns` | `List<SchemaField>` | Column metadata |
| `rows` | `List<List<dynamic>>` | Parsed row values |
| `rowCount` | `int` | Rows affected or returned |
| `message` | `String?` | DDL message (e.g. `"Table created"`) |
| `toMaps()` | `List<Map<String, dynamic>>` | Rows keyed by column name |

## Live Subscriptions

Subscribe to any SQL query. The returned `Stream<ChangeEvent>` emits real-time events:

```dart
final stream = client.subscribe(
  'SELECT * FROM chat.messages WHERE room_id = $1 ORDER BY created_at ASC',
  batchSize: 100,   // rows per initial-data batch
  lastRows: 50,     // rewind: include last N rows before live changes
);

await for (final event in stream) {
  switch (event) {
    case AckEvent(:final subscriptionId, :final totalRows):
      print('Subscribed $subscriptionId. Snapshot rows: $totalRows');
    case InitialDataBatch(:final hasMore):
      print('Snapshot batch, hasMore=$hasMore');
    case InsertEvent():
      print('New row: ${event.typedRow}');
    case UpdateEvent():
      print('Updated: ${event.typedOldRow} → ${event.typedRow}');
    case DeleteEvent():
      print('Deleted: ${event.typedRow}');
    case SubscriptionError(:final message):
      print('Error: $message');
  }
}
```

Cancel the subscription by cancelling the `StreamSubscription`:

```dart
final sub = stream.listen((_) {});
// later:
await sub.cancel();
```

### `subscribe()` options

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | `String` | SQL query to watch |
| `batchSize` | `int?` | Max rows per initial snapshot batch |
| `lastRows` | `int?` | Include the last N rows before live changes begin |
| `subscriptionId` | `String?` | Custom subscription ID |

### `ChangeEvent` variants

| Variant | Fields |
|---------|--------|
| `AckEvent` | `subscriptionId`, `totalRows`, `schema`, `batchNum`, `hasMore`, `status` |
| `InitialDataBatch` | `subscriptionId`, `rowsJson`, `batchNum`, `hasMore`, `status` |
| `InsertEvent` | `subscriptionId`, `rowsJson`, `row` |
| `UpdateEvent` | `subscriptionId`, `rowsJson`, `oldRowsJson`, `row`, `oldRow` |
| `DeleteEvent` | `subscriptionId`, `oldRowsJson`, `row` |
| `SubscriptionError` | `subscriptionId`, `code`, `message` |

## Connection Lifecycle Handlers

Hook into connection events for logging, UI state, or diagnostics:

```dart
final client = await KalamClient.connect(
  url: 'https://db.example.com',
  authProvider: () async => Auth.jwt(await getToken()),
  connectionHandlers: ConnectionHandlers(
    onConnect: () => print('connected'),
    onDisconnect: (reason) => print('disconnected: ${reason.message}'),
    onError: (error) => print('error: ${error.message}'),
    onReceive: (message) => print('[recv] $message'),
    onSend: (message) => print('[send] $message'),
  ),
);
```

## Server Health & Setup

```dart
// Check server health (version, status)
final health = await client.healthCheck();
print('${health.status} — v${health.version}');

// Check if first-time setup is required
final setup = await client.checkSetupStatus();
if (setup.needsSetup) {
  await client.serverSetup(ServerSetupRequest(
    username: 'admin',
    password: 'AdminPass123!',
    rootPassword: 'RootPass456!',
  ));
}
```

## Disposing

Always dispose the client when done to release resources:

```dart
await client.dispose();
```

## Flutter Integration

The SDK performs two async operations during startup:

1. **`KalamClient.init()`** — loads the native Rust library (~1–2 s on slower Android devices)
2. **`KalamClient.connect()`** — opens a WebSocket to the server (network-dependent, can take seconds)

If you `await` both before `runApp()`, your app will show a blank screen until the network handshake completes. The correct pattern is:

### Recommended: lazy service with deferred connection

Only `init()` goes before `runApp()`. The connection is established lazily when the user is authenticated and your sync service starts — never during app boot.

```dart
// main.dart
void main() async {
  runZonedGuarded(() async {
    WidgetsFlutterBinding.ensureInitialized();

    // Load native lib — fast, OK to await before rendering
    try {
      await KalamClient.init();
    } catch (_) {}

    runApp(const ProviderScope(child: MyApp()));
  }, (error, stack) {
    // zone error handling...
  });
}
```

Create a service that lazily creates the client **only when needed** (e.g. after login):

```dart
class SyncService {
  Future<KalamClient>? _clientFuture;
  StreamSubscription<ChangeEvent>? _messagesSubscription;

  bool _shouldBeConnected = false;
  bool _isDisposed = false;

  /// Lazily creates and returns the shared [KalamClient].
  Future<KalamClient> _getClient() => _clientFuture ??= _initClient();

  Future<KalamClient> _initClient() async {
    // Guard: init() is idempotent, safe to call again
    try { await KalamClient.init(); } catch (_) {}

    return KalamClient.connect(
      url: AppConfig.kalamDbUrl,
      authProvider: _resolveAuth,
      connectionHandlers: ConnectionHandlers(
        onConnect: () => print('connected'),
        onDisconnect: (reason) => print('disconnected: ${reason.message}'),
        onError: (error) => print('error: ${error.message}'),
      ),
      keepaliveInterval: const Duration(seconds: 5),
      maxRetries: 1,
    );
  }

  Future<Auth> _resolveAuth() async {
    // Fetch a fresh token from your auth system
    final token = await myAuthService.getOrRefreshToken();
    if (token == null) return const NoAuth();
    return Auth.jwt(token);
  }

  /// Call after user login to start syncing.
  Future<void> start() async {
    if (_isDisposed || _shouldBeConnected) return;
    _shouldBeConnected = true;

    final client = await _getClient();
    _messagesSubscription ??= client
        .subscribe('SELECT * FROM messages', fromSeqId: lastSeenSeqId)
        .listen(
          _handleEvent,
          onError: (e, st) => print('subscription error: $e'),
          cancelOnError: false,
        );
  }

  /// Call on logout or app teardown.
  Future<void> stop() async {
    _shouldBeConnected = false;
    await _messagesSubscription?.cancel();
    _messagesSubscription = null;
  }

  Future<void> dispose() async {
    _isDisposed = true;
    await stop();
    if (_clientFuture != null) {
      final client = await _clientFuture!;
      await client.dispose();
    }
  }

  void _handleEvent(ChangeEvent event) {
    switch (event) {
      case AckEvent(:final totalRows):
        print('Subscribed — $totalRows initial rows');
      case InitialDataBatch(:final rows, :final hasMore):
        print('Batch: ${rows.length} rows, more=$hasMore');
      case InsertEvent(:final row):
        print('New: $row');
      case UpdateEvent(:final row, :final oldRow):
        print('Updated: $oldRow → $row');
      case DeleteEvent(:final row):
        print('Deleted: $row');
      case SubscriptionError(:final code, :final message):
        print('Error [$code]: $message');
    }
  }
}
```

### With InheritedWidget / Provider

For state management patterns, expose the client via a provider:

```dart
void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await KalamClient.init();
  runApp(const MyApp());
}

// Using `provider` package or similar:
class KalamService extends ChangeNotifier {
  KalamClient? _client;
  bool get isReady => _client != null;
  KalamClient get client => _client!;

  Future<void> connect(String url, AuthProvider authProvider) async {
    _client = await KalamClient.connect(
      url: url,
      authProvider: authProvider,
    );
    notifyListeners();
  }

  @override
  void dispose() {
    _client?.dispose();
    super.dispose();
  }
}
```

### Anti-pattern: blocking `main()`

**Do not** do this — it freezes the UI on the splash/blank screen:

```dart
// BAD — app is frozen until WebSocket connects
void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await KalamClient.init();
  final client = await KalamClient.connect(url: '...');  // blocks rendering!
  runApp(MyApp(client: client));
}
```

## Full API Reference

| Method | Description |
|--------|-------------|
| `KalamClient.init()` | **Required.** Initialize the Rust runtime once at app startup |
| `KalamClient.connect(url, {authProvider, auth, timeout, maxRetries, disableCompression, connectionHandlers})` | Create a connected client |
| `query(sql, {params, namespace})` | Execute parameterized SQL |
| `subscribe(sql, {batchSize, lastRows, subscriptionId})` | Subscribe to live changes — returns `Stream<ChangeEvent>` |
| `login(username, password)` | Exchange Basic credentials for JWT tokens |
| `refreshToken(refreshToken)` | Refresh an expiring access token |
| `refreshAuth()` | Re-invoke `authProvider` and update credentials in-place |
| `healthCheck()` | Check server health and version |
| `checkSetupStatus()` | Check if first-time setup is needed |
| `serverSetup(request)` | Perform initial server setup |
| `dispose()` | Release resources |

## License

Apache-2.0

## Links

- Website: [kalamdb.org](https://kalamdb.org)
- Docs: [kalamdb.org/docs](https://kalamdb.org/docs)
- SDK reference: [kalamdb.org/docs/sdk](https://kalamdb.org/docs/sdk)
- Authentication & token setup: [kalamdb.org/docs/getting-started/authentication](https://kalamdb.org/docs/getting-started/authentication)
- Docker deployment: [kalamdb.org/docs/getting-started/docker](https://kalamdb.org/docs/getting-started/docker)
- Live Query / WebSocket architecture: [kalamdb.org/docs/architecture/live-query](https://kalamdb.org/docs/architecture/live-query)
- WebSocket protocol reference: [kalamdb.org/docs/api/websocket-protocol](https://kalamdb.org/docs/api/websocket-protocol)
- GitHub: [github.com/jamals86/KalamDB](https://github.com/jamals86/KalamDB)

---

> Native performance on iOS and Android is powered by the excellent [flutter_rust_bridge](https://cjycode.com/flutter_rust_bridge/) project — thank you to its maintainers and contributors.

