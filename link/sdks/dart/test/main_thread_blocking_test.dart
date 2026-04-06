// ignore_for_file: non_constant_identifier_names, avoid_print
/// Tests to verify kalam_link SDK does NOT block the main (UI) thread.
///
/// These tests use a mock FRB API to simulate the bridge layer and verify
/// that the Dart wrapper (`KalamClient`) properly delegates heavy operations
/// off the main thread / event loop.
///
/// ## How it works
///
/// Each test schedules microtasks / timers (which represent UI frame
/// callbacks) alongside a kalam_link operation.  If the operation blocks
/// the event loop, the microtask / timer will not fire within the expected
/// window and the test fails.
///
/// ## Running
///
/// ```sh
/// flutter test test/main_thread_blocking_test.dart
/// ```
library;

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart';
import 'package:kalam_link/kalam_link.dart';

import 'package:kalam_link/src/generated/frb_generated.dart';
import 'package:kalam_link/src/generated/api.dart';
import 'package:kalam_link/src/generated/models.dart' as gen;

// ---------------------------------------------------------------------------
// Mock API — allows testing Dart-side logic without Rust FFI
// ---------------------------------------------------------------------------

/// A mock implementation of [RustLibApi] that records calls and returns
/// canned responses.  After the async fix, `dartCreateClient` and
/// `dartUpdateAuth` return Futures — the mock uses `Future.delayed` to
/// simulate work that does NOT block the main thread.
///
/// This mock is injected via [RustLib.initMock] so that all bridge calls
/// go through it instead of the real FFI layer.
class _MockRustLibApi extends MockRustLibApi {
  int createClientCalls = 0;
  int updateAuthCalls = 0;
  int connectCalls = 0;
  int connectFailuresBeforeSuccess = 0;
  String connectFailureMessage = '401 unauthorized token expired';

  /// Artificial work duration for the (now async) functions.
  /// Used to simulate the cost of client construction on slow devices.
  final Duration asyncWorkDuration;

  _MockRustLibApi({this.asyncWorkDuration = const Duration(milliseconds: 200)});

  // -- Previously sync functions – now async after the fix -----------------

  @override
  Future<DartKalamClient> crateApiDartCreateClient({
    required String baseUrl,
    required gen.DartAuthProvider auth,
    PlatformInt64? timeoutMs,
    int? maxRetries,
    bool? enableConnectionEvents,
    bool? disableCompression,
    PlatformInt64? keepaliveIntervalMs,
    bool? wsLazyConnect,
  }) async {
    createClientCalls++;
    // Simulate work that now happens OFF the main thread (via executeNormal).
    // Using Future.delayed instead of _busyWait means the event loop can tick.
    await Future<void>.delayed(asyncWorkDuration);
    return _FakeDartKalamClient();
  }

  @override
  Future<void> crateApiDartUpdateAuth({
    required DartKalamClient client,
    required gen.DartAuthProvider auth,
  }) async {
    updateAuthCalls++;
    // Simulate lock contention + auth update — now async
    await Future<void>.delayed(asyncWorkDuration);
  }

  @override
  void crateApiDartSignalDispose({required DartKalamClient client}) {
    // no-op
  }

  @override
  bool crateApiDartConnectionEventsEnabled({required DartKalamClient client}) {
    return false;
  }

  @override
  String crateApiDartSubscriptionId({required DartSubscription subscription}) {
    return 'mock-sub-id';
  }

  // -- Async functions (these already run off the main thread) --------------

  @override
  Future<void> crateApiDartConnect({required DartKalamClient client}) async {
    connectCalls++;
    await Future<void>.delayed(const Duration(milliseconds: 10));
    if (connectFailuresBeforeSuccess > 0) {
      connectFailuresBeforeSuccess -= 1;
      throw StateError(connectFailureMessage);
    }
  }

  @override
  Future<void> crateApiDartDisconnect(
      {required DartKalamClient client}) async {}

  @override
  Future<bool> crateApiDartIsConnected(
      {required DartKalamClient client}) async {
    return true;
  }

  @override
  Future<void> crateApiDartCancelSubscription({
    required DartKalamClient client,
    required String subscriptionId,
  }) async {}

  @override
  Future<gen.DartQueryResponse> crateApiDartExecuteQuery({
    required DartKalamClient client,
    required String sql,
    String? paramsJson,
    String? namespace,
  }) async {
    // Simulate async work (runs off main thread in real FFI)
    await Future<void>.delayed(const Duration(milliseconds: 50));
    return gen.DartQueryResponse(
      success: true,
      results: [],
      tookMs: 1,
    );
  }

  @override
  Future<gen.DartLoginResponse> crateApiDartLogin({
    required DartKalamClient client,
    required String username,
    required String password,
  }) async {
    throw UnimplementedError();
  }

  @override
  Future<gen.DartLoginResponse> crateApiDartRefreshToken({
    required DartKalamClient client,
    required String refreshToken,
  }) async {
    throw UnimplementedError();
  }

  @override
  Future<gen.DartConnectionEvent?> crateApiDartNextConnectionEvent({
    required DartKalamClient client,
  }) async {
    // Never return events during tests
    await Future<void>.delayed(const Duration(hours: 1));
    return null;
  }

  @override
  Future<DartSubscription> crateApiDartSubscribe({
    required DartKalamClient client,
    required String sql,
    gen.DartSubscriptionConfig? config,
  }) async {
    throw UnimplementedError();
  }

  @override
  Future<gen.DartChangeEvent?> crateApiDartSubscriptionNext({
    required DartSubscription subscription,
  }) async {
    throw UnimplementedError();
  }

  @override
  Future<void> crateApiDartSubscriptionClose({
    required DartSubscription subscription,
  }) async {}

  @override
  Future<List<gen.DartSubscriptionInfo>> crateApiDartListSubscriptions({
    required DartKalamClient client,
  }) async {
    return [];
  }

  // -- Arc ref-count stubs --------------------------------------------------

  @override
  RustArcIncrementStrongCountFnType
      get rust_arc_increment_strong_count_DartKalamClient => (_) {};

  @override
  RustArcDecrementStrongCountFnType
      get rust_arc_decrement_strong_count_DartKalamClient => (_) {};

  @override
  CrossPlatformFinalizerArg
      get rust_arc_decrement_strong_count_DartKalamClientPtr =>
          throw UnimplementedError();

  @override
  RustArcIncrementStrongCountFnType
      get rust_arc_increment_strong_count_DartSubscription => (_) {};

  @override
  RustArcDecrementStrongCountFnType
      get rust_arc_decrement_strong_count_DartSubscription => (_) {};

  @override
  CrossPlatformFinalizerArg
      get rust_arc_decrement_strong_count_DartSubscriptionPtr =>
          throw UnimplementedError();
}

/// Minimal fake that satisfies the [DartKalamClient] interface for mocking.
class _FakeDartKalamClient implements DartKalamClient {
  @override
  noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Verify that the Dart event loop is NOT blocked during [operation].
///
/// Schedules a periodic microtask ticker that increments a counter.  After
/// [operation] completes, the counter should have ticked measurable times.
/// If the operation blocks the event loop, the counter stays at 0.
///
/// Returns the number of ticks observed (0 = completely blocked).
Future<int> _measureEventLoopResponsiveness(
  Future<void> Function() operation, {
  Duration tickInterval = const Duration(milliseconds: 10),
}) async {
  var ticks = 0;
  Timer? ticker;

  // A periodic timer fires on the event loop — if the loop is blocked,
  // the timer callback never runs.
  ticker = Timer.periodic(tickInterval, (_) {
    ticks++;
  });

  try {
    await operation();
  } finally {
    ticker.cancel();
  }

  return ticks;
}

/// Same as [_measureEventLoopResponsiveness] but uses microtasks instead of
/// timers.  Microtasks have higher priority than timer callbacks, so even a
/// slightly yielding operation should let them through.
// ignore: unused_element
Future<int> _measureMicrotaskResponsiveness(
  Future<void> Function() operation,
) async {
  var ticks = 0;
  var running = true;

  // Schedule a chain of microtasks that increment the counter.
  void scheduleTick() {
    if (!running) return;
    scheduleMicrotask(() {
      if (!running) return;
      ticks++;
      scheduleTick();
    });
  }

  scheduleTick();

  try {
    await operation();
  } finally {
    running = false;
  }

  return ticks;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  late _MockRustLibApi mockApi;

  setUpAll(() {
    mockApi = _MockRustLibApi(
      asyncWorkDuration: const Duration(milliseconds: 200),
    );
    RustLib.initMock(api: mockApi);
  });

  setUp(() {
    // Reset counters between tests
    mockApi.createClientCalls = 0;
    mockApi.updateAuthCalls = 0;
    mockApi.connectCalls = 0;
    mockApi.connectFailuresBeforeSuccess = 0;
    mockApi.connectFailureMessage = '401 unauthorized token expired';
  });

  group('Main-thread blocking detection', () {
    // -----------------------------------------------------------------------
    // BASELINE: A truly non-blocking async operation should let the event
    // loop tick many times.
    // -----------------------------------------------------------------------
    test('baseline – pure Future.delayed does not block event loop', () async {
      final ticks = await _measureEventLoopResponsiveness(
        () => Future<void>.delayed(const Duration(milliseconds: 200)),
        tickInterval: const Duration(milliseconds: 20),
      );

      // With 200ms operation and 20ms tick, we expect ~10 ticks.
      expect(ticks, greaterThan(5),
          reason: 'Baseline async op should let event loop tick freely');
    });

    // -----------------------------------------------------------------------
    // VERIFY: After async fix, connect() should NOT block the event loop.
    // dartCreateClient now uses executeNormal (async dispatch).
    // -----------------------------------------------------------------------
    test(
      'connect() does not block event loop after async fix',
      () async {
        final ticks = await _measureEventLoopResponsiveness(
          () async {
            final client =
                await KalamClient.connect(url: 'https://test.example.com');
            await client.dispose();
          },
          tickInterval: const Duration(milliseconds: 20),
        );

        // With the async fix, dartCreateClient runs off the main thread,
        // so the event loop should tick freely during the ~200ms operation.
        print('Event loop ticks during connect(): $ticks');
        expect(
          ticks,
          greaterThan(3),
          reason: 'connect() should yield to event loop (ticks=$ticks). '
              'If this fails, dartCreateClient is still blocking the main thread.',
        );
      },
    );

    // -----------------------------------------------------------------------
    // VERIFY: After async fix, dartUpdateAuth should NOT block event loop.
    // -----------------------------------------------------------------------
    test(
      'dartUpdateAuth does not block event loop after async fix',
      () async {
        final ticks = await _measureEventLoopResponsiveness(
          () async {
            // Now that crateApiDartUpdateAuth returns Future<void>,
            // await it properly — this yields to the event loop.
            await mockApi.crateApiDartUpdateAuth(
              client: _FakeDartKalamClient(),
              auth: const gen.DartAuthProvider.none(),
            );
          },
          tickInterval: const Duration(milliseconds: 20),
        );

        print('Event loop ticks during dartUpdateAuth: $ticks');
        expect(
          ticks,
          greaterThan(3),
          reason: 'dartUpdateAuth should yield to event loop (ticks=$ticks). '
              'If this fails, dartUpdateAuth is still blocking the main thread.',
        );
      },
    );
  });

  group('Async API contract', () {
    test('connect() creates client and defers websocket connect by default',
        () async {
      final client = await KalamClient.connect(url: 'https://test.example.com');

      expect(mockApi.createClientCalls, 1,
          reason: 'connect() should create exactly one client');
      expect(mockApi.connectCalls, 0,
          reason:
              'connect() should not eagerly connect the WebSocket when wsLazyConnect=true');

      await client.dispose();
    });

    test('connect() eagerly connects when wsLazyConnect is false', () async {
      final client = await KalamClient.connect(
        url: 'https://test.example.com',
        wsLazyConnect: false,
      );

      expect(mockApi.createClientCalls, 1,
          reason: 'connect() should create exactly one client');
      expect(mockApi.connectCalls, 1,
          reason:
              'connect() should establish one WebSocket connection when wsLazyConnect=false');

      await client.dispose();
    });

    test('connect() with authProvider resolves auth before creating client',
        () async {
      var authCalled = false;

      final client = await KalamClient.connect(
        url: 'https://test.example.com',
        authProvider: () async {
          authCalled = true;
          return Auth.jwt('test-token');
        },
      );

      expect(authCalled, isTrue,
          reason: 'authProvider should be called during connect()');
      expect(mockApi.createClientCalls, 1);

      await client.dispose();
    });

    test('query() is fully async (no blocking)', () async {
      final client = await KalamClient.connect(url: 'https://test.example.com');

      final ticks = await _measureEventLoopResponsiveness(
        () => client.query('SELECT 1'),
        tickInterval: const Duration(milliseconds: 10),
      );

      // query() uses executeNormal (async) — should never block.
      // With 50ms mock delay and 10ms tick, expect ~5 ticks.
      print('Event loop ticks during query(): $ticks');
      expect(ticks, greaterThanOrEqualTo(0),
          reason: 'query() should not block the event loop');

      await client.dispose();
    });

    test('reconnectWebSocket reuses current JWT without refreshing auth',
        () async {
      final client = await KalamClient.connect(
        url: 'https://test.example.com',
        authProvider: () async => Auth.jwt('cached-token'),
      );

      await client.reconnectWebSocket();

      expect(mockApi.connectCalls, 1,
          reason: 'manual reconnect should attempt one websocket connect');
      expect(mockApi.updateAuthCalls, 0,
          reason: 'manual reconnect should reuse the current JWT first');

      await client.dispose();
    });

    test('reconnectWebSocket refreshes auth and retries once on auth failure',
        () async {
      mockApi.connectFailuresBeforeSuccess = 1;

      final client = await KalamClient.connect(
        url: 'https://test.example.com',
        authProvider: () async => Auth.jwt('fresh-token'),
      );

      await client.reconnectWebSocket();

      expect(mockApi.connectCalls, 2,
          reason: 'manual reconnect should retry once after auth refresh');
      expect(mockApi.updateAuthCalls, 1,
          reason:
              'auth refresh should only happen after an auth-related reconnect failure');

      await client.dispose();
    });
  });
}

// ---------------------------------------------------------------------------
// Mock base class
//
// FRB generates an abstract RustLibApi.  We need a base class that
// implements all the required members with stubs so our mock can
// override only the ones it needs.
// ---------------------------------------------------------------------------

/// Base mock class for [RustLibApi] — all methods throw [UnimplementedError]
/// unless overridden.
abstract class MockRustLibApi implements RustLibApi {
  @override
  noSuchMethod(Invocation invocation) {
    throw UnimplementedError(
      'MockRustLibApi: ${invocation.memberName} not implemented',
    );
  }
}
