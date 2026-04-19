import 'dart:async';
import 'dart:convert';

import 'auth.dart';
import 'cell_value.dart';
import 'logger.dart';
import 'models.dart';
import 'seq_id.dart';
import 'generated/api.dart' as bridge;
import 'generated/models.dart' as gen;
import 'generated/frb_generated.dart';
import 'subscription_stream.dart';

/// Default [AuthProvider] used when no auth is specified — returns [NoAuth].
Future<Auth> _noAuth() async => const NoAuth();

/// KalamDB client for Dart and Flutter.
///
/// Provides query execution, live subscriptions, and authentication.
///
/// ```dart
/// final client = await KalamClient.connect(
///   url: 'https://db.example.com',
///   authProvider: () async => Auth.basic('alice', 'secret123'),
/// );
///
/// final result = await client.query('SELECT * FROM users LIMIT 10');
/// print(result.rows);
///
/// await client.dispose();
/// ```
class KalamClient {
  final bridge.DartKalamClient _handle;
  final ConnectionHandlers? _connectionHandlers;
  final AuthProvider _authProvider;
  Auth _auth;
  var _isDisposed = false;
  Future<void>? _connectionEventPump;
  Future<void>? _refreshAuthInFlight;
  Future<void>? _basicAuthExchangeInFlight;
  bool _skipNextAutoRefreshAuth = false;
  int _subscriptionIdCounter = 0;

  KalamClient._(this._handle, this._connectionHandlers,
      {required Auth initialAuth, required AuthProvider authProvider})
      : _authProvider = authProvider,
        _auth = initialAuth;

  /// Initialize the Rust runtime. Call once at app startup before any
  /// [KalamClient.connect] calls.
  ///
  /// This loads the native FFI library and is safe to `await` before
  /// `runApp()`. However, do **not** also `await` [connect] before
  /// `runApp()` — that performs network I/O and will freeze your UI.
  ///
  /// ```dart
  /// void main() async {
  ///   WidgetsFlutterBinding.ensureInitialized();
  ///   await KalamClient.init();   // OK — fast FFI load
  ///   runApp(MyApp());            // render immediately
  ///   // call connect() inside your widget/provider after the first frame
  /// }
  /// ```
  static Future<void> init() => RustLib.init();

  /// Connect to a KalamDB server.
  ///
  /// This method establishes a WebSocket connection to the server, which
  /// involves network I/O. **Do not `await` this before `runApp()`** in a
  /// Flutter app — it will block rendering. Instead, call it from
  /// `initState()` or a provider and show a loading indicator until ready.
  ///
  /// ```dart
  /// // In a StatefulWidget:
  /// @override
  /// void initState() {
  ///   super.initState();
  ///   _initClient();
  /// }
  ///
  /// Future<void> _initClient() async {
  ///   final client = await KalamClient.connect(url: '...');
  ///   if (mounted) setState(() => _client = client);
  /// }
  /// ```
  ///
  /// * [url] — server URL, e.g. `"https://db.example.com"`.
  /// * [authProvider] — async callback invoked to obtain credentials before
  ///   each connection attempt. Return [Auth.jwt] for JWT-based auth,
  ///   [Auth.basic] for user/password (the SDK automatically exchanges
  ///   these for a JWT before the WebSocket connection), or [Auth.none] for
  ///   anonymous access. Ideal for refresh-token flows.
  /// * [disableCompression] — set to `true` to disable gzip compression for
  ///   WebSocket messages (useful during development for easier inspection).
  ///   Defaults to `false`.
  /// * [wsLazyConnect] — controls when the WebSocket connection is
  ///   established.  When `true` (the default), the connection is deferred
  ///   until the first `subscribe()` call, avoiding unnecessary connections
  ///   when the client is only used for HTTP queries.  When `false`, the
  ///   connection is established eagerly during `connect()`.  The SDK
  ///   manages the connection lifecycle automatically — there is no need
  ///   to call a separate `connect()` method. When static [Auth.basic]
  ///   credentials are used, the SDK also exchanges them for a JWT
  ///   automatically before the first query or WebSocket connection.
  /// * [timeout] — HTTP request timeout. Defaults to 30 seconds.
  /// * [maxRetries] — retry count for idempotent (SELECT) queries. Defaults to 3.
  /// * [keepaliveInterval] — WebSocket keep-alive ping interval.
  ///   Defaults to 10 seconds. Set to [Duration.zero] to disable.
  /// * [logLevel] — Minimum log level for SDK diagnostic messages.
  ///   Defaults to [Level.warning]. Set to [Level.debug] to see
  ///   connection lifecycle, keepalive pings, auth refreshes, etc.
  /// * [logListener] — Optional callback invoked for every log entry
  ///   at or above [logLevel]. When `null`, logs are printed via
  ///   `print()` (visible in `flutter run`, `adb logcat`, etc.).
  /// * [authProviderMaxAttempts] — max attempts for resolving credentials
  ///   from [authProvider] on transient failures.
  /// * [authProviderInitialBackoff] — backoff before the first retry.
  /// * [authProviderMaxBackoff] — maximum retry backoff.
  static Future<KalamClient> connect({
    required String url,
    AuthProvider authProvider = _noAuth,
    bool disableCompression = false,
    bool wsLazyConnect = true,
    Duration timeout = const Duration(seconds: 30),
    int maxRetries = 3,
    ConnectionHandlers? connectionHandlers,
    Duration? keepaliveInterval,
    Level? logLevel,
    LogListener? logListener,
    int authProviderMaxAttempts = 3,
    Duration authProviderInitialBackoff = const Duration(milliseconds: 250),
    Duration authProviderMaxBackoff = const Duration(seconds: 2),
  }) async {
    // Apply logging configuration if provided.
    if (logLevel != null) {
      KalamLogger.level = logLevel;
    }
    if (logListener != null) {
      KalamLogger.listener = logListener;
    }

    KalamLogger.info('client', 'Connecting to $url');

    // Resolve initial auth from the provider.
    final effectiveAuth = await resolveAuthWithRetry(
      authProvider,
      maxAttempts: authProviderMaxAttempts,
      initialBackoff: authProviderInitialBackoff,
      maxBackoff: authProviderMaxBackoff,
    );

    KalamLogger.debug(
      'client',
      'Auth resolved: ${effectiveAuth.runtimeType}',
    );

    final handle = await bridge.dartCreateClient(
      baseUrl: url,
      auth: _toBridgeAuth(effectiveAuth),
      timeoutMs: timeout.inMilliseconds,
      maxRetries: maxRetries,
      enableConnectionEvents: connectionHandlers?.hasAny ?? false,
      disableCompression: disableCompression,
      keepaliveIntervalMs: keepaliveInterval?.inMilliseconds,
      wsLazyConnect: wsLazyConnect,
    );
    final client = KalamClient._(
      handle,
      connectionHandlers,
      initialAuth: effectiveAuth,
      authProvider: authProvider,
    );
    client._armInitialSubscriptionAuthReuse();
    client._startConnectionEventPumpIfNeeded();

    // When ws_lazy_connect is enabled (default), defer the WebSocket
    // connection until the first subscribe() call.  The Rust client's
    // subscribe_with_config will call connect() automatically.
    if (!wsLazyConnect) {
      // Establish a shared WebSocket connection so all subscriptions are
      // multiplexed over a single connection.  The Rust SharedConnection
      // handles auto-reconnection and re-subscription internally.
      await client._ensureJwtForBasicAuth();
      KalamLogger.debug('client', 'Calling Rust dartConnect...');
      await bridge.dartConnect(client: client._handle);
      KalamLogger.info('client', 'Connected to $url successfully');
    } else {
      KalamLogger.info(
        'client',
        'wsLazyConnect enabled — WebSocket will connect on first subscribe',
      );
    }

    return client;
  }

  /// Refresh authentication credentials using the configured [authProvider].
  ///
  /// Call this before re-subscribing after a token expiry, or on a schedule
  /// to keep credentials fresh. Has no effect when no [authProvider] is set.
  ///
  /// ```dart
  /// // Refresh tokens every 55 minutes
  /// Timer.periodic(Duration(minutes: 55), (_) => client.refreshAuth());
  /// ```
  Future<void> refreshAuth({
    int maxAttempts = 3,
    Duration initialBackoff = const Duration(milliseconds: 250),
    Duration maxBackoff = const Duration(seconds: 2),
  }) async {
    if (_isDisposed) return;

    if (_refreshAuthInFlight != null) {
      return _refreshAuthInFlight;
    }

    final refreshTask = () async {
      KalamLogger.debug('auth', 'Refreshing auth credentials...');
      final freshAuth = await resolveAuthWithRetry(
        _authProvider,
        maxAttempts: maxAttempts,
        initialBackoff: initialBackoff,
        maxBackoff: maxBackoff,
      );
      if (_isDisposed) return;
      KalamLogger.info(
          'auth', 'Auth credentials refreshed: ${freshAuth.runtimeType}');
      _auth = freshAuth;
      if (freshAuth is BasicAuth) {
        // Exchange basic credentials for JWT.
        await login(freshAuth.user, freshAuth.password);
      } else {
        await bridge.dartUpdateAuth(
            client: _handle, auth: _toBridgeAuth(freshAuth));
      }
    }();

    _refreshAuthInFlight =
        refreshTask.whenComplete(() => _refreshAuthInFlight = null);
    return _refreshAuthInFlight;
  }

  void _armInitialSubscriptionAuthReuse() {
    _skipNextAutoRefreshAuth = true;
  }

  Future<void> _refreshAuthBeforeSubscriptionConnect() async {
    if (_skipNextAutoRefreshAuth) {
      _skipNextAutoRefreshAuth = false;
      KalamLogger.debug(
        'auth',
        'Skipping immediate auth refresh; reusing credentials resolved during connect()',
      );
      return;
    }

    await refreshAuth();
  }

  Future<void> _prepareSubscriptionConnect() async {
    await _refreshAuthBeforeSubscriptionConnect();
    await _ensureJwtForBasicAuth();
  }

  Future<void> _prepareManualReconnect() async {
    await refreshAuth();
    await _ensureJwtForBasicAuth();
  }

  static bool _isLikelyReconnectAuthError(Object error) {
    final message = error.toString().toLowerCase();
    const authMarkers = <String>[
      '401',
      '403',
      'auth',
      'authentication',
      'authorization',
      'credential',
      'expired',
      'forbidden',
      'jwt',
      'token',
      'unauthorized',
      'unauthenticated',
    ];
    return authMarkers.any(message.contains);
  }

  gen.DartSubscriptionConfig? _buildSubscriptionConfig({
    required String sql,
    required String subscriptionId,
    int? batchSize,
    int? lastRows,
    SeqId? from,
  }) {
    if (batchSize == null &&
        lastRows == null &&
        subscriptionId.isEmpty &&
        from == null) {
      return null;
    }

    return gen.DartSubscriptionConfig(
      sql: sql,
      batchSize: batchSize,
      lastRows: lastRows,
      id: subscriptionId,
      from: from?.toInt(),
    );
  }

  static gen.DartLiveRowsConfig? _buildLiveRowsConfig({
    int? limit,
    List<String>? keyColumns,
  }) {
    if (limit == null && (keyColumns == null || keyColumns.isEmpty)) {
      return null;
    }

    return gen.DartLiveRowsConfig(
      limit: limit,
      keyColumns: keyColumns,
    );
  }

  // ---------------------------------------------------------------------------
  // Queries
  // ---------------------------------------------------------------------------

  /// Execute a SQL query.
  ///
  /// ```dart
  /// final res = await client.query(
  ///   r'SELECT * FROM orders WHERE user_id = $1 AND status = $2',
  ///   params: ['user-uuid-123', 'pending'],
  /// );
  /// ```
  Future<QueryResponse> query(
    String sql, {
    List<dynamic>? params,
    String? namespace,
  }) async {
    await _ensureJwtForBasicAuth();
    final paramsJson = params != null ? jsonEncode(params) : null;
    final resp = await bridge.dartExecuteQuery(
      client: _handle,
      sql: sql,
      paramsJson: paramsJson,
      namespace: namespace,
    );
    final result = _fromBridgeQueryResponse(resp);
    // On TOKEN_EXPIRED, refresh via authProvider and retry exactly once.
    if (result.error?.code == 'TOKEN_EXPIRED') {
      KalamLogger.warn(
          'auth', 'TOKEN_EXPIRED — refreshing credentials via authProvider');
      await refreshAuth();
      final retryResp = await bridge.dartExecuteQuery(
        client: _handle,
        sql: sql,
        paramsJson: paramsJson,
        namespace: namespace,
      );
      return _fromBridgeQueryResponse(retryResp);
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // Authentication
  // ---------------------------------------------------------------------------

  /// Optional helper to log in with user and password.
  ///
  /// Most apps do not need to call this manually — when the client was
  /// created with static [Auth.basic] credentials, the SDK automatically
  /// performs this exchange before the first query or WebSocket connection.
  ///
  /// Returns tokens and user info. The client's in-memory auth state is
  /// updated to [Auth.jwt] automatically.
  Future<LoginResponse> login(String user, String password) async {
    final resp = await bridge.dartLogin(
      client: _handle,
      user: user,
      password: password,
    );
    final loginResponse = _fromBridgeLoginResponse(resp);
    final jwt = Auth.jwt(loginResponse.accessToken);
    _auth = jwt;
    await bridge.dartUpdateAuth(client: _handle, auth: _toBridgeAuth(jwt));
    return loginResponse;
  }

  /// Refresh an expiring access token.
  Future<LoginResponse> refreshToken(String refreshToken) async {
    final resp = await bridge.dartRefreshToken(
      client: _handle,
      refreshToken: refreshToken,
    );
    final loginResponse = _fromBridgeLoginResponse(resp);
    final jwt = Auth.jwt(loginResponse.accessToken);
    _auth = jwt;
    await bridge.dartUpdateAuth(client: _handle, auth: _toBridgeAuth(jwt));
    return loginResponse;
  }

  // ---------------------------------------------------------------------------
  // Subscriptions
  // ---------------------------------------------------------------------------

  /// Subscribe to live changes on a SQL query.
  ///
  /// Returns a `Stream<ChangeEvent>` that emits:
  /// - [AckEvent] — subscription confirmed with schema info
  /// - [InitialDataBatch] — initial snapshot rows
  /// - [InsertEvent] / [UpdateEvent] / [DeleteEvent] — live changes
  /// - [SubscriptionError] — server-side error
  ///
  /// Options:
  /// - [batchSize] — hint for server-side batch sizing during the initial load.
  /// - [lastRows] — number of newest rows to rewind before live changes begin.
  /// - [from] — resume from a specific sequence ID (only changes
  ///   after this seq_id are sent). Typically used after reconnection.
  /// - [subscriptionId] — custom subscription ID (auto-generated if omitted).
  ///
  /// The underlying Rust shared connection automatically reconnects and
  /// resumes the subscription when the WebSocket drops. The Dart stream
  /// stays attached to the same Rust subscription handle until it is
  /// cancelled or the subscription ends permanently.
  ///
  /// Cancel the subscription by cancelling the `StreamSubscription`.
  ///
  /// ```dart
  /// final stream = client.subscribe('SELECT * FROM messages');
  /// await for (final event in stream) {
  ///   switch (event) {
  ///     case InsertEvent():
  ///       print('New row: ${event.row}');
  ///     case DeleteEvent():
  ///       print('Deleted: ${event.row}');
  ///     case _:
  ///       break;
  ///   }
  /// }
  /// ```
  Stream<ChangeEvent> subscribe(
    String sql, {
    int? batchSize,
    int? lastRows,
    SeqId? from,
    String? subscriptionId,
  }) {
    final logicalSubscriptionId =
        _ensureSubscriptionId(subscriptionId, prefix: 'sub');
    return runBridgeSubscriptionStream<bridge.DartSubscription,
        gen.DartChangeEvent, ChangeEvent>(
      description: 'subscription for: $sql',
      prepare: _prepareSubscriptionConnect,
      open: () => bridge.dartSubscribe(
        client: _handle,
        sql: sql,
        config: _buildSubscriptionConfig(
          sql: sql,
          subscriptionId: logicalSubscriptionId,
          batchSize: batchSize,
          lastRows: lastRows,
          from: from,
        ),
      ),
      next: (sub) => bridge.dartSubscriptionNext(subscription: sub),
      close: (sub) => bridge.dartSubscriptionClose(subscription: sub),
      cancel: () => bridge.dartCancelSubscription(
        client: _handle,
        subscriptionId: logicalSubscriptionId,
      ),
      decode: _fromBridgeChangeEvent,
      isDisposed: () => _isDisposed,
    );
  }

  /// Subscribe to a SQL query and receive the current materialized row set.
  ///
  /// The row materialization happens inside the shared Rust client layer, so Dart
  /// applications can use a higher-level live query API without reimplementing
  /// insert/update/delete reconciliation.
  ///
  /// The SQL must use the strict live-query shape: `SELECT ... FROM ... WHERE ...`.
  /// Do not include `ORDER BY` or `LIMIT` here.
  ///
  /// The controls are intentionally separate:
  /// - [batchSize] chunks the initial snapshot delivered by the server.
  /// - [lastRows] chooses how much history to rewind before live changes begin.
  /// - [limit] caps the materialized live row set that the SDK keeps after
  ///   startup, including future inserts and updates.
  ///
  /// Keep using Dart-side sorting and grouping after rows arrive. Use [limit]
  /// only when the materialized live state itself should stay bounded.
  Stream<List<T>> liveQueryRowsWithSql<T>(
    String sql, {
    int? batchSize,
    int? lastRows,
    SeqId? from,
    String? subscriptionId,
    int? limit,
    List<String>? keyColumns,
    T Function(Map<String, KalamCellValue> row)? mapRow,
  }) {
    final logicalSubscriptionId =
        _ensureSubscriptionId(subscriptionId, prefix: 'live');
    return runBridgeSubscriptionStream<bridge.DartLiveRowsSubscription,
        gen.DartLiveRowsEvent, List<T>>(
      description: 'live rows subscription for: $sql',
      prepare: _prepareSubscriptionConnect,
      open: () => bridge.dartLiveQueryRowsSubscribe(
        client: _handle,
        sql: sql,
        config: _buildSubscriptionConfig(
          sql: sql,
          subscriptionId: logicalSubscriptionId,
          batchSize: batchSize,
          lastRows: lastRows,
          from: from,
        ),
        liveConfig: _buildLiveRowsConfig(
          limit: limit,
          keyColumns: keyColumns,
        ),
      ),
      next: (sub) => bridge.dartLiveQueryRowsNext(subscription: sub),
      close: (sub) => bridge.dartLiveQueryRowsClose(subscription: sub),
      cancel: () => bridge.dartCancelSubscription(
        client: _handle,
        subscriptionId: logicalSubscriptionId,
      ),
      decode: (event) => _fromBridgeLiveRowsEvent(event, mapRow: mapRow),
      isDisposed: () => _isDisposed,
    );
  }

  /// Subscribe to a table and receive the current materialized row set.
  Stream<List<T>> liveTableRows<T>(
    String tableName, {
    int? batchSize,
    int? lastRows,
    SeqId? from,
    String? subscriptionId,
    int? limit,
    List<String>? keyColumns,
    T Function(Map<String, KalamCellValue> row)? mapRow,
  }) {
    return liveQueryRowsWithSql<T>(
      'SELECT * FROM $tableName',
      batchSize: batchSize,
      lastRows: lastRows,
      from: from,
      subscriptionId: subscriptionId,
      limit: limit,
      keyColumns: keyColumns,
      mapRow: mapRow,
    );
  }

  /// List all active subscriptions on the shared connection.
  ///
  /// Returns a snapshot of each subscription's metadata including the
  /// subscription ID, SQL query, last received sequence ID, and timestamps.
  ///
  /// ```dart
  /// final subs = await client.getSubscriptions();
  /// for (final sub in subs) {
  ///   print('${sub.id}: ${sub.query} (closed=${sub.closed})');
  /// }
  /// ```
  Future<List<SubscriptionInfo>> getSubscriptions() async {
    final dartInfos = await bridge.dartListSubscriptions(client: _handle);
    return dartInfos
        .map((info) => SubscriptionInfo(
              id: info.id,
              query: info.query,
              lastSeqId:
                  info.lastSeqId == null ? null : SeqId.parse(info.lastSeqId!),
              lastEventTimeMs: info.lastEventTimeMs,
              createdAtMs: info.createdAtMs,
              closed: info.closed,
            ))
        .toList();
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /// Release resources. The client should not be used after this call.
  Future<void> dispose() async {
    KalamLogger.info('client', 'Disposing KalamClient...');
    _isDisposed = true;
    // Disconnect the shared WebSocket (unsubscribes all subscriptions).
    await bridge.dartDisconnect(client: _handle);
    // Signal the Rust event pump to unblock so the Dart pull-loop exits
    // instead of hanging forever when the event queue is empty.
    bridge.dartSignalDispose(client: _handle);
    await _connectionEventPump;
    KalamLogger.debug('client', 'KalamClient disposed');
  }

  /// Whether the shared WebSocket connection is currently open.
  Future<bool> get isConnected async => bridge.dartIsConnected(client: _handle);

  /// Disconnect the shared WebSocket connection.
  ///
  /// Active subscriptions will stop receiving events until
  /// [reconnectWebSocket] is called.
  Future<void> disconnectWebSocket() => bridge.dartDisconnect(client: _handle);

  /// Re-establish the shared WebSocket connection after a manual
  /// [disconnectWebSocket] or if the automatic reconnection was exhausted.
  Future<void> reconnectWebSocket() async {
    await _ensureJwtForBasicAuth();

    try {
      await bridge.dartConnect(client: _handle);
    } catch (error) {
      if (!_isLikelyReconnectAuthError(error)) {
        rethrow;
      }

      KalamLogger.warn(
        'auth',
        'Reconnect failed with an auth-related error; refreshing credentials and retrying once',
      );
      await _prepareManualReconnect();
      await bridge.dartConnect(client: _handle);
    }
  }

  String _ensureSubscriptionId(String? subscriptionId,
      {required String prefix}) {
    if (subscriptionId != null && subscriptionId.isNotEmpty) {
      return subscriptionId;
    }
    _subscriptionIdCounter += 1;
    return '$prefix-${DateTime.now().microsecondsSinceEpoch}-$_subscriptionIdCounter';
  }

  // ---------------------------------------------------------------------------
  // Internal converters
  // ---------------------------------------------------------------------------

  static gen.DartAuthProvider _toBridgeAuth(Auth auth) {
    return switch (auth) {
      BasicAuth(:final user, :final password) =>
        gen.DartAuthProvider.basicAuth(user: user, password: password),
      JwtAuth(:final token) => gen.DartAuthProvider.jwtToken(token: token),
      NoAuth() => const gen.DartAuthProvider.none(),
    };
  }

  static QueryResponse _fromBridgeQueryResponse(gen.DartQueryResponse resp) {
    return QueryResponse(
      success: resp.success,
      results: resp.results
          .map((r) => QueryResult(
                columns: r.columns
                    .map((c) => SchemaField(
                          name: c.name,
                          dataType: c.dataType,
                          index: c.index,
                          flags: c.flags,
                        ))
                    .toList(),
                namedRowsJson: r.namedRowsJson,
                rowCount: r.rowCount,
                message: r.message,
              ))
          .toList(),
      tookMs: resp.tookMs,
      error: resp.error != null
          ? ErrorDetail(
              code: resp.error!.code,
              message: resp.error!.message,
              details: resp.error!.details,
            )
          : null,
    );
  }

  static LoginResponse _fromBridgeLoginResponse(gen.DartLoginResponse resp) {
    return LoginResponse(
      accessToken: resp.accessToken,
      refreshToken: resp.refreshToken,
      expiresAt: resp.expiresAt,
      refreshExpiresAt: resp.refreshExpiresAt,
      adminUiAccess: resp.adminUiAccess,
      user: LoginUserInfo(
        id: resp.user.id,
        role: _fromBridgeRole(resp.user.role),
        email: resp.user.email,
        createdAt: resp.user.createdAt,
        updatedAt: resp.user.updatedAt,
      ),
    );
  }

  static Role _fromBridgeRole(gen.DartRole role) {
    return switch (role) {
      gen.DartRole.anonymous => Role.anonymous,
      gen.DartRole.user => Role.user,
      gen.DartRole.service => Role.service,
      gen.DartRole.dba => Role.dba,
      gen.DartRole.system => Role.system,
    };
  }

  static ChangeEvent _fromBridgeChangeEvent(gen.DartChangeEvent event) {
    return switch (event) {
      gen.DartChangeEvent_Ack(
        :final subscriptionId,
        :final totalRows,
        :final schema,
        :final batchNum,
        :final hasMore,
        :final status
      ) =>
        AckEvent(
          subscriptionId: subscriptionId,
          totalRows: totalRows,
          schema: schema
              .map((s) => SchemaField(
                    name: s.name,
                    dataType: s.dataType,
                    index: s.index,
                    flags: s.flags,
                  ))
              .toList(),
          batchNum: batchNum,
          hasMore: hasMore,
          status: status,
        ),
      gen.DartChangeEvent_InitialDataBatch(
        :final subscriptionId,
        :final rowsJson,
        :final batchNum,
        :final hasMore,
        :final status
      ) =>
        InitialDataBatch(
          subscriptionId: subscriptionId,
          rowsJson: rowsJson,
          batchNum: batchNum,
          hasMore: hasMore,
          status: status,
        ),
      gen.DartChangeEvent_Insert(:final subscriptionId, :final rowsJson) =>
        InsertEvent(subscriptionId: subscriptionId, rowsJson: rowsJson),
      gen.DartChangeEvent_Update(
        :final subscriptionId,
        :final rowsJson,
        :final oldRowsJson
      ) =>
        UpdateEvent(
            subscriptionId: subscriptionId,
            rowsJson: rowsJson,
            oldRowsJson: oldRowsJson),
      gen.DartChangeEvent_Delete(:final subscriptionId, :final oldRowsJson) =>
        DeleteEvent(subscriptionId: subscriptionId, oldRowsJson: oldRowsJson),
      gen.DartChangeEvent_Error(
        :final subscriptionId,
        :final code,
        :final message
      ) =>
        SubscriptionError(
            subscriptionId: subscriptionId, code: code, message: message),
    };
  }

  static List<T> _fromBridgeLiveRowsEvent<T>(
    gen.DartLiveRowsEvent event, {
    T Function(Map<String, KalamCellValue> row)? mapRow,
  }) {
    return switch (event) {
      gen.DartLiveRowsEvent_Rows(:final rowsJson) => _mapLiveRows(
          _decodeTypedRows(rowsJson),
          mapRow,
        ),
      gen.DartLiveRowsEvent_Error(
        :final subscriptionId,
        :final code,
        :final message,
      ) =>
        throw SubscriptionError(
          subscriptionId: subscriptionId,
          code: code,
          message: message,
        ),
    };
  }

  static List<Map<String, KalamCellValue>> _decodeTypedRows(
    List<String> rowsJson,
  ) {
    return rowsJson
        .map((json) => Map<String, dynamic>.from(jsonDecode(json) as Map))
        .map((row) =>
            row.map((key, value) => MapEntry(key, KalamCellValue.from(value))))
        .toList(growable: false);
  }

  static List<T> _mapLiveRows<T>(
    List<Map<String, KalamCellValue>> rows,
    T Function(Map<String, KalamCellValue> row)? mapRow,
  ) {
    final mapper = mapRow;
    if (mapper == null) {
      return rows.cast<T>();
    }
    return rows.map(mapper).toList(growable: false);
  }

  /// Resolve credentials from [provider] with bounded retries for transient
  /// errors such as network timeouts. Useful for flaky mobile networks where
  /// one failed auth lookup should not abort the whole connection attempt.
  static Future<Auth> resolveAuthWithRetry(
    AuthProvider provider, {
    int maxAttempts = 3,
    Duration initialBackoff = const Duration(milliseconds: 250),
    Duration maxBackoff = const Duration(seconds: 2),
    Future<void> Function(Duration delay)? sleep,
  }) async {
    if (maxAttempts < 1) {
      throw RangeError.range(maxAttempts, 1, null, 'maxAttempts');
    }
    if (initialBackoff.isNegative) {
      throw ArgumentError.value(
        initialBackoff,
        'initialBackoff',
        'must be >= 0',
      );
    }
    if (maxBackoff.isNegative) {
      throw ArgumentError.value(maxBackoff, 'maxBackoff', 'must be >= 0');
    }

    final wait = sleep ?? Future<void>.delayed;
    var delay = initialBackoff;

    for (var attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await provider();
      } catch (error, stackTrace) {
        final hasMore = attempt < maxAttempts;
        final retryable = isLikelyTransientAuthProviderError(error);
        if (!hasMore || !retryable) {
          Error.throwWithStackTrace(error, stackTrace);
        }
        if (delay > Duration.zero) {
          await wait(delay);
        }
        final doubledMs = delay.inMilliseconds * 2;
        final cappedMs = doubledMs > maxBackoff.inMilliseconds
            ? maxBackoff.inMilliseconds
            : doubledMs;
        delay = Duration(milliseconds: cappedMs);
      }
    }

    throw StateError('Unreachable: auth provider retry loop exited');
  }

  /// Best-effort classifier for transient auth-provider failures.
  ///
  /// We avoid platform-specific exception types so this works on mobile, web,
  /// and desktop targets.
  static bool isLikelyTransientAuthProviderError(Object error) {
    if (error is TimeoutException) return true;
    final message = error.toString().toLowerCase();
    const transientMarkers = <String>[
      'timeout',
      'timed out',
      'network',
      'socket',
      'connection',
      'unreachable',
      'temporar',
      '429',
      '502',
      '503',
      '504',
    ];
    return transientMarkers.any(message.contains);
  }

  Future<void> _ensureJwtForBasicAuth() async {
    if (_isDisposed) return;

    final auth = _auth;
    if (auth is! BasicAuth) return;

    if (_basicAuthExchangeInFlight != null) {
      return _basicAuthExchangeInFlight;
    }

    KalamLogger.debug(
      'auth',
      'Auto-login: exchanging basic credentials for JWT...',
    );

    final exchangeTask = () async {
      await login(auth.user, auth.password);
      KalamLogger.info('auth', 'Auto-login successful, switched to JWT auth');
    }();

    _basicAuthExchangeInFlight =
        exchangeTask.whenComplete(() => _basicAuthExchangeInFlight = null);
    return _basicAuthExchangeInFlight;
  }

  void _startConnectionEventPumpIfNeeded() {
    final handlers = _connectionHandlers;
    if (handlers == null || !handlers.hasAny || _connectionEventPump != null) {
      return;
    }

    KalamLogger.debug('events', 'Starting connection event pump');

    _connectionEventPump = () async {
      try {
        while (!_isDisposed) {
          final event = await bridge.dartNextConnectionEvent(client: _handle);
          if (_isDisposed || event == null) {
            break;
          }
          final converted = _fromBridgeConnectionEvent(event);
          KalamLogger.verbose(
            'events',
            _describeConnectionEvent(converted),
          );
          handlers.dispatch(converted);
        }
      } catch (error) {
        KalamLogger.error('events', 'Connection event pump error: $error');
        if (!_isDisposed) {
          handlers.onError?.call(
            ConnectionErrorInfo(
              message: error.toString(),
              recoverable: false,
            ),
          );
        }
      }
    }();
  }

  static ConnectionEvent _fromBridgeConnectionEvent(
    gen.DartConnectionEvent event,
  ) {
    return switch (event) {
      gen.DartConnectionEvent_Connect() => const ConnectEvent(),
      gen.DartConnectionEvent_Disconnect(:final reason) => DisconnectEvent(
          reason: DisconnectReason(
            message: reason.message,
            code: reason.code,
          ),
        ),
      gen.DartConnectionEvent_Error(:final error) => ConnectionErrorEvent(
          error: ConnectionErrorInfo(
            message: error.message,
            recoverable: error.recoverable,
          ),
        ),
      gen.DartConnectionEvent_Receive(:final message) =>
        ReceiveEvent(message: message),
      gen.DartConnectionEvent_Send(:final message) =>
        SendEvent(message: message),
    };
  }

  static String _describeConnectionEvent(ConnectionEvent event) {
    return switch (event) {
      ConnectEvent() => 'Connection opened and authenticated',
      DisconnectEvent(:final reason) => reason.code == null
          ? 'Connection closed: ${reason.message}'
          : 'Connection closed: ${reason.message} (code=${reason.code})',
      ConnectionErrorEvent(:final error) =>
        'Connection error (recoverable=${error.recoverable}): ${error.message}',
      ReceiveEvent(:final message) =>
        'Receive: ${_summarizeWireMessage(message)}',
      SendEvent(:final message) => 'Send: ${_summarizeWireMessage(message)}',
    };
  }

  static String _summarizeWireMessage(String message) {
    final trimmed = message.trim();
    if (trimmed.isEmpty) {
      return 'empty payload';
    }

    if (trimmed == '[ping]') {
      return 'keepalive ping';
    }

    try {
      final decoded = jsonDecode(trimmed);
      if (decoded is Map<String, dynamic>) {
        final type = decoded['type'];
        final subId = decoded['subscription_id'] ?? decoded['subscriptionId'];
        final status = decoded['status'];
        final code = decoded['code'];

        final parts = <String>[];
        if (type != null) parts.add('type=$type');
        if (subId != null) parts.add('sub=$subId');
        if (status != null) parts.add('status=$status');
        if (code != null) parts.add('code=$code');

        if (parts.isNotEmpty) {
          return '${parts.join(', ')} | raw=${_truncateForLog(trimmed, 180)}';
        }
      }
    } catch (_) {}

    return _truncateForLog(trimmed, 180);
  }

  static String _truncateForLog(String value, int maxChars) {
    if (value.length <= maxChars) {
      return value;
    }
    return '${value.substring(0, maxChars)}…';
  }
}
