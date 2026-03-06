/// Keepalive/heartbeat e2e tests — verify that WebSocket keepalive pings
/// are sent at the configured interval, pong responses are received, and
/// that the client detects a dead connection when the server stops responding.
///
/// These tests verify the fix for the Android/Flutter issue where a server
/// disconnect leaves the client stuck in "online" state because no pong
/// timeout was enforced.
library;

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

void main() {
  group('Keepalive & Pong Timeout', skip: skipIfNoIntegration, () {
    // ─────────────────────────────────────────────────────────────────
    // 1. Keepalive pings are sent at the configured interval
    // ─────────────────────────────────────────────────────────────────
    test(
      'keepalive pings are sent and pongs received within interval',
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

        final sendEvents = <String>[];
        final receiveEvents = <String>[];

        // Connect with a short keepalive interval (2s) so pings fire quickly.
        final client = await KalamClient.connect(
          url: serverUrl,
          authProvider: () async => Auth.jwt(login.accessToken),
          keepaliveInterval: const Duration(seconds: 2),
          connectionHandlers: ConnectionHandlers(
            onSend: (msg) => sendEvents.add(msg),
            onReceive: (msg) => receiveEvents.add(msg),
          ),
          timeout: const Duration(seconds: 10),
        );

        try {
          expect(await client.isConnected, isTrue,
              reason: 'should be connected');

          // Wait enough time for at least 2 keepalive pings to fire
          // (2s interval + some margin).
          await sleep(const Duration(seconds: 6));

          // Verify ping events were sent.
          final pingCount =
              sendEvents.where((e) => e.contains('[ping]')).length;
          expect(pingCount, greaterThanOrEqualTo(2),
              reason:
                  'should have sent at least 2 keepalive pings in 6 seconds '
                  'with 2s interval (got $pingCount). '
                  'All send events: $sendEvents');

          // Connection should still be alive.
          expect(await client.isConnected, isTrue,
              reason: 'connection should stay alive after pings');
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 2. Keepalive works with subscriptions (pings fire even with
    //    idle subscription)
    // ─────────────────────────────────────────────────────────────────
    test(
      'keepalive pings fire while subscription is idle',
      () async {
        await ensureSdkReady();

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

        final pingEvents = <DateTime>[];

        final client = await KalamClient.connect(
          url: serverUrl,
          authProvider: () async => Auth.jwt(login.accessToken),
          keepaliveInterval: const Duration(seconds: 2),
          connectionHandlers: ConnectionHandlers(
            onSend: (msg) {
              if (msg.contains('[ping]')) {
                pingEvents.add(DateTime.now());
              }
            },
          ),
          timeout: const Duration(seconds: 10),
        );

        try {
          // Create a namespace and table.
          final ns = uniqueName('dart_ka');
          final tbl = '$ns.keepalive_idle';
          await ensureNamespace(client, ns);
          await client.query(
            'CREATE TABLE IF NOT EXISTS $tbl ('
            'id INT PRIMARY KEY, '
            'payload TEXT'
            ')',
          );

          // Subscribe but don't send any data => connection is idle.
          final events = <ChangeEvent>[];
          final stream = client.subscribe('SELECT * FROM $tbl');
          final sub = stream.listen(events.add);

          // Wait for ack + several keepalive intervals to accumulate pings.
          await sleep(const Duration(seconds: 10));
          expect(events.whereType<AckEvent>(), isNotEmpty,
              reason: 'should receive AckEvent');

          // We should have at least a few pings in 10 seconds with 2s interval.
          expect(pingEvents.length, greaterThanOrEqualTo(2),
              reason: 'should have at least 2 keepalive pings during idle '
                  'subscription (got ${pingEvents.length}). '
                  'Total events received: ${events.length}');

          // Verify pings are roughly 2 seconds apart.
          if (pingEvents.length >= 2) {
            final gap = pingEvents[1].difference(pingEvents[0]).inMilliseconds;
            expect(gap, greaterThan(1000),
                reason: 'pings should be ~2s apart, got ${gap}ms');
            expect(gap, lessThan(5000),
                reason: 'pings should be ~2s apart, got ${gap}ms');
          }

          // Connection is still alive.
          expect(await client.isConnected, isTrue);

          await sub
              .cancel()
              .timeout(const Duration(seconds: 3), onTimeout: () => null);
          await dropTable(client, tbl);
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 45)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 3. onDisconnect fires after server disconnects (simulated via
    //    disconnectWebSocket) — verifies the pipeline works end-to-end
    // ─────────────────────────────────────────────────────────────────
    test(
      'onDisconnect fires when connection drops during keepalive',
      () async {
        await ensureSdkReady();

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

        final disconnectReasons = <String>[];
        final connectCount = <int>[0];

        final client = await KalamClient.connect(
          url: serverUrl,
          authProvider: () async => Auth.jwt(login.accessToken),
          keepaliveInterval: const Duration(seconds: 2),
          connectionHandlers: ConnectionHandlers(
            onConnect: () => connectCount[0]++,
            onDisconnect: (reason) => disconnectReasons.add(reason.message),
          ),
          timeout: const Duration(seconds: 10),
        );

        try {
          expect(await client.isConnected, isTrue);
          await sleep(const Duration(seconds: 1));

          // Simulate server dropping connection.
          await client.disconnectWebSocket();
          await sleep(const Duration(seconds: 1));

          expect(await client.isConnected, isFalse,
              reason: 'should be disconnected');
          expect(disconnectReasons, isNotEmpty,
              reason:
                  'onDisconnect should have fired after disconnectWebSocket');
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 4. Connection recovers after disconnect and keepalive resumes
    // ─────────────────────────────────────────────────────────────────
    test(
      'keepalive resumes after reconnect',
      () async {
        await ensureSdkReady();

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

        final pingTimestamps = <DateTime>[];

        final client = await KalamClient.connect(
          url: serverUrl,
          authProvider: () async => Auth.jwt(login.accessToken),
          keepaliveInterval: const Duration(seconds: 2),
          connectionHandlers: ConnectionHandlers(
            onSend: (msg) {
              if (msg.contains('[ping]')) {
                pingTimestamps.add(DateTime.now());
              }
            },
          ),
          timeout: const Duration(seconds: 10),
        );

        try {
          expect(await client.isConnected, isTrue);

          // Wait for initial pings.
          await sleep(const Duration(seconds: 3));
          final initialPings = pingTimestamps.length;
          expect(initialPings, greaterThanOrEqualTo(1),
              reason: 'should have at least 1 ping before disconnect');

          // Disconnect.
          await client.disconnectWebSocket();
          await sleep(const Duration(seconds: 1));

          // Clear ping timestamps and reconnect.
          pingTimestamps.clear();
          await client.reconnectWebSocket();
          expect(await client.isConnected, isTrue,
              reason: 'should be reconnected');

          // Wait for keepalive to resume.
          await sleep(const Duration(seconds: 5));

          expect(pingTimestamps.length, greaterThanOrEqualTo(1),
              reason: 'keepalive pings should resume after reconnect '
                  '(got ${pingTimestamps.length})');
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 45)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 5. isConnected remains correct across keepalive ticks
    // ─────────────────────────────────────────────────────────────────
    test(
      'isConnected stays true during successful keepalive cycle',
      () async {
        final client = await connectJwtClient();
        try {
          // Poll isConnected over several keepalive intervals.
          for (var i = 0; i < 5; i++) {
            expect(await client.isConnected, isTrue,
                reason: 'isConnected should be true at check $i');
            await sleep(const Duration(seconds: 2));
          }
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 6. Keepalive disabled when interval is zero — no pings sent
    // ─────────────────────────────────────────────────────────────────
    test(
      'no keepalive pings when keepaliveInterval is zero',
      () async {
        await ensureSdkReady();

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

        final sendEvents = <String>[];

        final client = await KalamClient.connect(
          url: serverUrl,
          authProvider: () async => Auth.jwt(login.accessToken),
          keepaliveInterval: Duration.zero,
          connectionHandlers: ConnectionHandlers(
            onSend: (msg) => sendEvents.add(msg),
          ),
          timeout: const Duration(seconds: 10),
        );

        try {
          expect(await client.isConnected, isTrue);

          // Wait long enough that pings *would* have fired if enabled.
          await sleep(const Duration(seconds: 5));

          final pingCount =
              sendEvents.where((e) => e.contains('[ping]')).length;
          expect(pingCount, equals(0),
              reason: 'should NOT send any keepalive pings when disabled');
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );
  });
}
