import 'dart:async';
import 'dart:convert';

import 'auth.dart';
import 'logger.dart';
import 'models.dart';
import 'generated/api.dart' as bridge;
import 'generated/models.dart' as gen;
import 'generated/frb_generated.dart';

/// KalamDB client for Dart and Flutter.
///
/// Provides query execution, live subscriptions, authentication,
/// and server health endpoints.
///
/// ```dart
/// final client = await KalamClient.connect(
///   url: 'https://db.example.com',
///   auth: Auth.basic('alice', 'secret123'),
/// );
///
/// final result = await client.query('SELECT * FROM users LIMIT 10');
/// print(result.rows);
///
/// await client.dispose();
/// ```
class KalamClient {
  static final BigInt _minI64 = BigInt.parse('-9223372036854775808');
  static final BigInt _maxI64 = BigInt.parse('9223372036854775807');

  final bridge.DartKalamClient _handle;
  final ConnectionHandlers? _connectionHandlers;
  final AuthProvider? _authProvider;
  var _isDisposed = false;
  Future<void>? _connectionEventPump;
  Future<void>? _refreshAuthInFlight;

  KalamClient._(this._handle, this._connectionHandlers,
      {AuthProvider? authProvider})
      : _authProvider = authProvider;

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

  static int _toI64(BigInt value, {required String fieldName}) {
    if (value < _minI64 || value > _maxI64) {
      throw ArgumentError.value(
        value,
        fieldName,
        'must fit signed 64-bit range',
      );
    }
    return value.toInt();
  }

  static BigInt _toBigInt(Object value) {
    if (value is BigInt) return value;
    if (value is int) return BigInt.from(value);
    return BigInt.parse(value.toString());
  }

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
  /// * [auth] — **Deprecated.** Use [authProvider] instead. Static credentials
  ///   cannot refresh automatically — tokens expire mid-session without
  ///   transparent reconnect support.  Defaults to [Auth.none].
  ///   Ignored when [authProvider] is set.
  /// * [authProvider] — async callback invoked to obtain fresh credentials
  ///   before each connection attempt. Use this for refresh-token flows.
  ///   Takes precedence over [auth].
  /// * [disableCompression] — set to `true` to disable gzip compression for
  ///   WebSocket messages (useful during development for easier inspection).
  ///   Defaults to `false`.
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
    Auth auth = const NoAuth(),
    AuthProvider? authProvider,
    bool disableCompression = false,
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

    // Resolve initial auth from provider if set.
    final effectiveAuth = authProvider != null
        ? await resolveAuthWithRetry(
            authProvider,
            maxAttempts: authProviderMaxAttempts,
            initialBackoff: authProviderInitialBackoff,
            maxBackoff: authProviderMaxBackoff,
          )
        : auth;

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
    );
    final client =
        KalamClient._(handle, connectionHandlers, authProvider: authProvider);
    client._startConnectionEventPumpIfNeeded();

    // Establish a shared WebSocket connection so all subscriptions are
    // multiplexed over a single connection.  The Rust SharedConnection
    // handles auto-reconnection and re-subscription internally.
    KalamLogger.debug('client', 'Calling Rust dartConnect...');
    await bridge.dartConnect(client: client._handle);
    KalamLogger.info('client', 'Connected to $url successfully');

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
    final provider = _authProvider;
    if (provider == null || _isDisposed) return;

    if (_refreshAuthInFlight != null) {
      return _refreshAuthInFlight;
    }

    final refreshTask = () async {
      KalamLogger.debug('auth', 'Refreshing auth credentials...');
      final freshAuth = await resolveAuthWithRetry(
        provider,
        maxAttempts: maxAttempts,
        initialBackoff: initialBackoff,
        maxBackoff: maxBackoff,
      );
      if (_isDisposed) return;
      KalamLogger.info(
          'auth', 'Auth credentials refreshed: ${freshAuth.runtimeType}');
      await bridge.dartUpdateAuth(
          client: _handle, auth: _toBridgeAuth(freshAuth));
    }();

    _refreshAuthInFlight =
        refreshTask.whenComplete(() => _refreshAuthInFlight = null);
    return _refreshAuthInFlight;
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
    final paramsJson = params != null ? jsonEncode(params) : null;
    final resp = await bridge.dartExecuteQuery(
      client: _handle,
      sql: sql,
      paramsJson: paramsJson,
      namespace: namespace,
    );
    return _fromBridgeQueryResponse(resp);
  }

  // ---------------------------------------------------------------------------
  // Authentication
  // ---------------------------------------------------------------------------

  /// Log in with username and password.
  ///
  /// Returns tokens and user info. Use the access token with [Auth.jwt]
  /// for subsequent authenticated requests.
  Future<LoginResponse> login(String username, String password) async {
    final resp = await bridge.dartLogin(
      client: _handle,
      username: username,
      password: password,
    );
    return _fromBridgeLoginResponse(resp);
  }

  /// Refresh an expiring access token.
  Future<LoginResponse> refreshToken(String refreshToken) async {
    final resp = await bridge.dartRefreshToken(
      client: _handle,
      refreshToken: refreshToken,
    );
    return _fromBridgeLoginResponse(resp);
  }

  // ---------------------------------------------------------------------------
  // Health
  // ---------------------------------------------------------------------------

  /// Check server health (version, status, etc.).
  Future<HealthCheckResponse> healthCheck() async {
    final resp = await bridge.dartHealthCheck(client: _handle);
    return HealthCheckResponse(
      status: resp.status,
      version: resp.version,
      apiVersion: resp.apiVersion,
      buildDate: resp.buildDate,
    );
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
  /// - [batchSize] — hint for server-side batch sizing during initial load.
  /// - [lastRows] — number of newest rows to fetch for initial data.
  /// - [fromSeqId] — resume from a specific sequence ID (only changes
  ///   after this seq_id are sent). Typically used after reconnection.
  /// - [subscriptionId] — custom subscription ID (auto-generated if omitted).
  ///
  /// The subscription **automatically reconnects** with exponential backoff
  /// when the underlying WebSocket disconnects (heartbeat timeout, network
  /// change, server restart, etc.).  The stream stays open and continues
  /// emitting events from the new connection.
  ///
  /// Before each reconnect attempt the [authProvider] (if set) is called to
  /// obtain fresh credentials.
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
    BigInt? fromSeqId,
    String? subscriptionId,
  }) {
    late StreamController<ChangeEvent> controller;
    var closed = false;

    // The subscription ID of the currently active subscription.
    // Used by onCancel to cancel via the shared connection without
    // needing the DartSubscription Mutex (which would deadlock if
    // dartSubscriptionNext is blocked on it).
    String? activeSubId;

    /// Create and run one subscription lifetime (connect → pull → close).
    /// Returns normally when the subscription ends (server close, ping
    /// failure, etc.).  The caller decides whether to reconnect.
    Future<void> runSubscription() async {
      bridge.DartSubscription? sub;
      try {
        // Refresh auth before each connect when a provider is configured.
        await refreshAuth();

        sub = await bridge.dartSubscribe(
          client: _handle,
          sql: sql,
          config: (batchSize != null ||
                  lastRows != null ||
                  subscriptionId != null ||
                  fromSeqId != null)
              ? gen.DartSubscriptionConfig(
                  sql: sql,
                  batchSize: batchSize,
                  lastRows: lastRows,
                  id: subscriptionId,
                  fromSeqId: fromSeqId == null
                      ? null
                      : _toI64(fromSeqId, fieldName: 'fromSeqId'),
                )
              : null,
        );
        activeSubId = bridge.dartSubscriptionId(subscription: sub);

        while (!closed && !_isDisposed) {
          final event = await bridge.dartSubscriptionNext(subscription: sub);
          if (event == null || closed) break;
          controller.add(_fromBridgeChangeEvent(event));
        }
      } catch (error, stackTrace) {
        if (!closed && !_isDisposed) {
          KalamLogger.error('subscription', 'Subscription error: $error');
          controller.addError(error, stackTrace);
        }
      } finally {
        activeSubId = null;
        if (sub != null) {
          try {
            await bridge.dartSubscriptionClose(subscription: sub);
          } catch (_) {}
        }
      }
    }

    /// Auto-reconnect loop with exponential backoff.
    Future<void> reconnectLoop() async {
      const initialDelay = Duration(seconds: 1);
      const maxDelay = Duration(seconds: 30);
      var delay = initialDelay;

      while (!closed && !_isDisposed) {
        try {
          delay = initialDelay; // reset on successful connect
          KalamLogger.debug('subscription', 'Running subscription for: $sql');
          await runSubscription();
        } catch (e, st) {
          KalamLogger.warn('subscription', 'Subscription error: $e');
          if (!closed && !_isDisposed) {
            controller.addError(e, st);
          }
        }

        // Subscription ended — reconnect after backoff unless cancelled.
        if (closed || _isDisposed) break;

        KalamLogger.info(
          'subscription',
          'Subscription ended, reconnecting in ${delay.inMilliseconds}ms',
        );
        await Future<void>.delayed(delay);
        // Exponential backoff capped at maxDelay.
        delay = Duration(
          milliseconds:
              (delay.inMilliseconds * 2).clamp(0, maxDelay.inMilliseconds),
        );
      }

      if (!closed) {
        await controller.close();
      }
    }

    controller = StreamController<ChangeEvent>(
      onListen: () => reconnectLoop(),
      onCancel: () async {
        closed = true;
        // Cancel the subscription via the shared connection (bypasses the
        // DartSubscription Mutex that dartSubscriptionNext may be holding).
        // This drops the event channel sender, unblocking dartSubscriptionNext
        // which returns None, allowing runSubscription to reach its finally
        // block and call dartSubscriptionClose for full cleanup.
        final subId = activeSubId;
        if (subId != null) {
          try {
            await bridge.dartCancelSubscription(
              client: _handle,
              subscriptionId: subId,
            );
          } catch (_) {}
        }
      },
    );

    return controller.stream;
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
                  info.lastSeqId == null ? null : _toBigInt(info.lastSeqId!),
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
  Future<void> reconnectWebSocket() => bridge.dartConnect(client: _handle);

  // ---------------------------------------------------------------------------
  // Internal converters
  // ---------------------------------------------------------------------------

  static gen.DartAuthProvider _toBridgeAuth(Auth auth) {
    return switch (auth) {
      BasicAuth(:final username, :final password) =>
        gen.DartAuthProvider.basicAuth(username: username, password: password),
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
      user: LoginUserInfo(
        id: resp.user.id,
        username: resp.user.username,
        role: resp.user.role,
        email: resp.user.email,
        createdAt: resp.user.createdAt,
        updatedAt: resp.user.updatedAt,
      ),
    );
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
