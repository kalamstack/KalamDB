/// Client lifecycle e2e tests — connect, dispose, disableCompression,
/// connection events.
///
/// Mirrors: tests/e2e/lifecycle/lifecycle.test.mjs (TypeScript)
library;

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

void main() {
  group('Client Lifecycle', skip: skipIfNoIntegration, () {
    // ─────────────────────────────────────────────────────────────────
    // Connect / dispose
    // ─────────────────────────────────────────────────────────────────
    test(
      'connect creates a functional client, dispose cleans up',
      () async {
        final client = await connectJwtClient();

        // Client should be able to query.
        final res = await client.query('SELECT 1 AS n');
        expect(res.success, isTrue);

        // Dispose should not throw.
        await client.dispose();
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // disableCompression
    // ─────────────────────────────────────────────────────────────────
    test(
      'disableCompression: true still connects and queries',
      () async {
        await ensureSdkReady();

        // First get a JWT.
        final bootstrap = await KalamClient.connect(
          url: serverUrl,
          timeout: const Duration(seconds: 10),
        );
        LoginResponse login;
        try {
          login = await bootstrap.login(adminUser, adminPass);
        } finally {
          await bootstrap.dispose();
        }

        // Connect with compression disabled.
        final client = await KalamClient.connect(
          url: serverUrl,
          authProvider: () async => Auth.jwt(login.accessToken),
          disableCompression: true,
          timeout: const Duration(seconds: 10),
        );
        try {
          final resp = await client.query("SELECT 'no-compress' AS val");
          expect(resp.success, isTrue);
          expect(resp.results, isNotEmpty);
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Connection with timeout
    // ─────────────────────────────────────────────────────────────────
    test(
      'custom timeout is accepted',
      () async {
        await ensureSdkReady();

        final client = await KalamClient.connect(
          url: serverUrl,
          timeout: const Duration(seconds: 5),
        );
        try {
          final login = await client.login(adminUser, adminPass);
          expect(login.accessToken, isNotEmpty);
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Connection with maxRetries
    // ─────────────────────────────────────────────────────────────────
    test(
      'custom maxRetries is accepted',
      () async {
        final client = await connectJwtClient();
        // If maxRetries=2 was set by connectJwtClient, queries should still work.
        final res = await client.query('SELECT 1 AS n');
        expect(res.success, isTrue);
        await client.dispose();
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // ConnectionHandlers — class instantiation and dispatch
    // ─────────────────────────────────────────────────────────────────
    test(
      'ConnectionHandlers class works correctly',
      () async {
        // Test that ConnectionHandlers can be instantiated and dispatched.
        final events = <String>[];

        final handlers = ConnectionHandlers(
          onConnect: () => events.add('connect'),
          onDisconnect: (_) => events.add('disconnect'),
          onError: (_) => events.add('error'),
          onReceive: (_) => events.add('receive'),
          onSend: (_) => events.add('send'),
        );

        expect(handlers.hasAny, isTrue);

        // Dispatch events manually.
        handlers.dispatch(const ConnectEvent());
        handlers.dispatch(DisconnectEvent(
          reason: DisconnectReason(message: 'test', code: 1000),
        ));

        expect(events, contains('connect'));
        expect(events, contains('disconnect'));
      },
      timeout: const Timeout(Duration(seconds: 5)),
    );

    test(
      'ConnectionHandlers with no callbacks has hasAny=false',
      () {
        const handlers = ConnectionHandlers();
        expect(handlers.hasAny, isFalse);
      },
      timeout: const Timeout(Duration(seconds: 5)),
    );

    test(
      'ConnectionHandlers.onEvent receives all event types',
      () {
        final receivedTypes = <Type>[];
        final handlers = ConnectionHandlers(
          onEvent: (event) => receivedTypes.add(event.runtimeType),
        );

        handlers.dispatch(const ConnectEvent());
        handlers.dispatch(DisconnectEvent(
          reason: DisconnectReason(message: 'bye', code: 1000),
        ));
        handlers.dispatch(ConnectionErrorEvent(
          error: ConnectionErrorInfo(message: 'err', recoverable: false),
        ));
        handlers.dispatch(ReceiveEvent(message: 'rx'));
        handlers.dispatch(SendEvent(message: 'tx'));

        expect(receivedTypes, hasLength(5));
      },
      timeout: const Timeout(Duration(seconds: 5)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Multiple clients in parallel
    // ─────────────────────────────────────────────────────────────────
    test(
      'multiple concurrent clients work independently',
      () async {
        final client1 = await connectJwtClient();
        final client2 = await connectJwtClient();

        try {
          final [res1, res2] = await Future.wait([
            client1.query('SELECT 1 AS n'),
            client2.query('SELECT 2 AS n'),
          ]);

          expect(res1.success, isTrue);
          expect(res2.success, isTrue);
        } finally {
          await client1.dispose();
          await client2.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );
  });
}
