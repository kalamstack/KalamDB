/// Reconnect e2e tests — disconnect, reconnect, resume subscriptions,
/// connection event handlers.
///
/// Imitates a real-world scenario where a client subscribes to a table,
/// receives live events, loses the connection, reconnects, and continues
/// receiving events.
///
/// Mirrors: tests/e2e/reconnect/reconnect.test.mjs (TypeScript)
library;

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

/// Cancel a stream subscription with a safety timeout.
Future<void> _safeCancel(StreamSubscription<dynamic> sub) async {
  try {
    await sub.cancel().timeout(const Duration(seconds: 3));
  } on TimeoutException {
    // The cancel is still in flight but we can proceed.
  }
}

void main() {
  group('Reconnect & Resume', skip: skipIfNoIntegration, () {
    late KalamClient client;
    late String ns;
    late String tbl;

    setUpAll(() async {
      client = await connectJwtClient();
      ns = uniqueName('dart_recon');
      tbl = '$ns.events';
      await ensureNamespace(client, ns);
      await client.query(
        'CREATE TABLE IF NOT EXISTS $tbl ('
        'id INT PRIMARY KEY, '
        'payload TEXT'
        ')',
      );
    });

    tearDownAll(() async {
      await dropTable(client, tbl);
      await client.dispose();
    });

    // ─────────────────────────────────────────────────────────────────
    // 1. isConnected, disconnectWebSocket, reconnectWebSocket toggle
    // ─────────────────────────────────────────────────────────────────
    test(
      'connect then disconnect then reconnect toggles isConnected',
      () async {
        final c = await connectJwtClient();
        try {
          expect(await c.isConnected, isTrue,
              reason: 'should be connected after connect');

          await c.disconnectWebSocket();
          expect(await c.isConnected, isFalse,
              reason: 'should be disconnected after disconnectWebSocket');

          await c.reconnectWebSocket();
          expect(await c.isConnected, isTrue,
              reason: 'should be reconnected after reconnectWebSocket');
        } finally {
          await c.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 2. ConnectionHandlers fire on connect/disconnect cycle
    // ─────────────────────────────────────────────────────────────────
    test(
      'ConnectionHandlers fire during reconnect cycle',
      () async {
        await ensureSdkReady();

        final events = <String>[];

        // Get a JWT token first.
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

        final c = await KalamClient.connect(
          url: serverUrl,
          auth: Auth.jwt(login.accessToken),
          connectionHandlers: ConnectionHandlers(
            onConnect: () => events.add('connect'),
            onDisconnect: (_) => events.add('disconnect'),
          ),
          timeout: const Duration(seconds: 10),
        );

        try {
          // Allow the connect event to propagate.
          await sleep(const Duration(seconds: 1));
          expect(events, contains('connect'),
              reason: 'onConnect should fire on initial connect');

          // Disconnect.
          await c.disconnectWebSocket();
          await sleep(const Duration(seconds: 1));
          expect(events, contains('disconnect'),
              reason: 'onDisconnect should fire on disconnect');

          // Reconnect — onConnect should fire again.
          final connectCountBefore = events.where((e) => e == 'connect').length;
          await c.reconnectWebSocket();
          await sleep(const Duration(seconds: 1));
          final connectCountAfter = events.where((e) => e == 'connect').length;

          expect(connectCountAfter, greaterThan(connectCountBefore),
              reason: 'onConnect should fire again after reconnect');
        } finally {
          await c.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 60)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 3. ConnectionHandlers.onError is wirable
    // ─────────────────────────────────────────────────────────────────
    test(
      'ConnectionHandlers with onError can be set without throwing',
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

        final c = await KalamClient.connect(
          url: serverUrl,
          auth: Auth.jwt(login.accessToken),
          connectionHandlers: ConnectionHandlers(
            onError: (_) {},
          ),
          timeout: const Duration(seconds: 10),
        );

        try {
          expect(await c.isConnected, isTrue);
        } finally {
          await c.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 4. Queries work after reconnect
    // ─────────────────────────────────────────────────────────────────
    test(
      'queries work correctly after reconnect cycle',
      () async {
        final c = await connectJwtClient();
        try {
          final res1 = await c.query('SELECT 1 AS n');
          expect(res1.success, isTrue, reason: 'query before disconnect');

          await c.disconnectWebSocket();
          await c.reconnectWebSocket();

          final res2 = await c.query('SELECT 2 AS n');
          expect(res2.success, isTrue, reason: 'query after reconnect');
        } finally {
          await c.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 5. Real-world: subscribe → insert → disconnect → reconnect →
    //    re-subscribe → insert → events resume
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscription resumes after disconnect/reconnect (real-world)',
      () async {
        // Collect events before disconnect.
        final preEvents = <ChangeEvent>[];
        final stream = client.subscribe('SELECT id, payload FROM $tbl');
        final sub = stream.listen(preEvents.add);

        // Wait for ack.
        await sleep(const Duration(seconds: 3));
        final ack = preEvents.whereType<AckEvent>();
        expect(ack, isNotEmpty, reason: 'should receive AckEvent');

        // Insert a row BEFORE disconnect.
        final writer = await connectJwtClient();
        try {
          await writer.query(
            "INSERT INTO $tbl (id, payload) VALUES (1, 'before disconnect')",
          );

          // Wait for the change event.
          await sleep(const Duration(seconds: 3));
          final priorInserts = preEvents.whereType<InsertEvent>();
          expect(priorInserts, isNotEmpty,
              reason: 'should receive InsertEvent before disconnect');

          // Cancel first subscription (it'll reconnect-loop otherwise).
          await _safeCancel(sub);

          // --- Disconnect ---
          await client.disconnectWebSocket();
          expect(await client.isConnected, isFalse,
              reason: 'should be disconnected');

          // Insert while disconnected — we should see this in initial data batch later.
          await writer.query(
            "INSERT INTO $tbl (id, payload) VALUES (2, 'during disconnect')",
          );

          // --- Reconnect ---
          await client.reconnectWebSocket();
          expect(await client.isConnected, isTrue,
              reason: 'should be reconnected');

          // Re-subscribe after reconnect.
          final postEvents = <ChangeEvent>[];
          final postStream = client.subscribe('SELECT id, payload FROM $tbl');
          final postSub = postStream.listen(postEvents.add);

          // Wait for ack + initial data.
          await sleep(const Duration(seconds: 3));
          final postAck = postEvents.whereType<AckEvent>();
          expect(postAck, isNotEmpty,
              reason: 'should receive AckEvent after reconnect');

          // Insert a row AFTER reconnect.
          await writer.query(
            "INSERT INTO $tbl (id, payload) VALUES (3, 'after reconnect')",
          );

          // Wait for the change event.
          await sleep(const Duration(seconds: 5));
          final postInserts = postEvents.whereType<InsertEvent>();
          expect(postInserts, isNotEmpty,
              reason: 'should receive InsertEvent after reconnect');

          // Verify row 3 appeared.
          final row3Found = postInserts.any(
            (e) => e.rows.any((r) => r['id']?.asInt() == 3),
          );
          expect(row3Found, isTrue,
              reason: 'row 3 should appear in InsertEvents');

          await _safeCancel(postSub);
        } finally {
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 120)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 5b. Explicit checkpoint resume: reconnect + re-subscribe from lastSeqId
    // ─────────────────────────────────────────────────────────────────
    test(
      'reconnect then resubscribe from lastSeqId continues without replay',
      () async {
        bool hasRowId(List<ChangeEvent> events, int id) {
          for (final event in events) {
            switch (event) {
              case InsertEvent(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case UpdateEvent(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case InitialDataBatch(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case AckEvent() || DeleteEvent() || SubscriptionError():
                break;
            }
          }
          return false;
        }

        Future<void> waitFor(
          bool Function() predicate, {
          Duration timeout = const Duration(seconds: 20),
          Duration poll = const Duration(milliseconds: 200),
        }) async {
          final started = DateTime.now();
          while (!predicate()) {
            if (DateTime.now().difference(started) > timeout) {
              throw TimeoutException('Timed out waiting for condition');
            }
            await sleep(poll);
          }
        }

        final seed = DateTime.now().millisecondsSinceEpoch % 1000000;
        final preId = (seed * 10) + 1;
        final gapId = (seed * 10) + 2;
        final postId = (seed * 10) + 3;
        final checkpointSubId = 'recon-checkpoint-${uniqueName('sub')}';

        final writer = await connectJwtClient();
        StreamSubscription<ChangeEvent>? sub1;
        StreamSubscription<ChangeEvent>? sub2;

        try {
          await writer.query(
            "INSERT INTO $tbl (id, payload) VALUES ($preId, 'pre-checkpoint')",
          );

          final eventsBefore = <ChangeEvent>[];
          final streamBefore = client.subscribe(
            'SELECT id, payload FROM $tbl WHERE id >= $preId',
            lastRows: 1,
            subscriptionId: checkpointSubId,
          );
          sub1 = streamBefore.listen(eventsBefore.add);

          await waitFor(() => eventsBefore.whereType<AckEvent>().isNotEmpty);
          await waitFor(() => hasRowId(eventsBefore, preId));

          BigInt? checkpoint;
          final started = DateTime.now();
          while (checkpoint == null) {
            final subs = await client.getSubscriptions();
            for (final s in subs) {
              if (s.id == checkpointSubId && s.lastSeqId != null) {
                checkpoint = s.lastSeqId;
                break;
              }
            }
            if (checkpoint != null) break;
            if (DateTime.now().difference(started) > const Duration(seconds: 20)) {
              throw TimeoutException('Timed out waiting for reconnect checkpoint');
            }
            await sleep(const Duration(milliseconds: 200));
          }

          await _safeCancel(sub1);
          sub1 = null;

          await client.disconnectWebSocket();
          expect(await client.isConnected, isFalse);

          await writer.query(
            "INSERT INTO $tbl (id, payload) VALUES ($gapId, 'during-disconnect')",
          );

          await client.reconnectWebSocket();
          expect(await client.isConnected, isTrue);

          final eventsAfter = <ChangeEvent>[];
          final streamAfter = client.subscribe(
            'SELECT id, payload FROM $tbl WHERE id >= $preId',
            fromSeqId: checkpoint,
            lastRows: 0,
          );
          sub2 = streamAfter.listen(eventsAfter.add);

          await waitFor(() => eventsAfter.whereType<AckEvent>().isNotEmpty);

          await writer.query(
            "INSERT INTO $tbl (id, payload) VALUES ($postId, 'post-reconnect')",
          );

          await waitFor(() => hasRowId(eventsAfter, gapId) && hasRowId(eventsAfter, postId));

          expect(hasRowId(eventsAfter, preId), isFalse,
              reason: 'pre-checkpoint row must not replay after reconnect resume');
          expect(hasRowId(eventsAfter, gapId), isTrue,
              reason: 'row written during disconnect must be resumed from checkpoint');
          expect(hasRowId(eventsAfter, postId), isTrue,
              reason: 'row written after reconnect must be received live');
        } finally {
          if (sub1 != null) {
            await _safeCancel(sub1);
          }
          if (sub2 != null) {
            await _safeCancel(sub2);
          }
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 150)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 6. Multiple subscriptions work after reconnect
    // ─────────────────────────────────────────────────────────────────
    test(
      'multiple subscriptions work after reconnect',
      () async {
        // Create a second table.
        final tbl2 = '$ns.events2';
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tbl2 ('
          'id INT PRIMARY KEY, '
          'label TEXT'
          ')',
        );

        // Subscribe to both tables.
        final events1 = <ChangeEvent>[];
        final events2 = <ChangeEvent>[];
        final sub1 = client.subscribe('SELECT * FROM $tbl').listen(events1.add);
        final sub2 =
            client.subscribe('SELECT * FROM $tbl2').listen(events2.add);

        await sleep(const Duration(seconds: 3));
        expect(events1.whereType<AckEvent>(), isNotEmpty, reason: 'sub1 ack');
        expect(events2.whereType<AckEvent>(), isNotEmpty, reason: 'sub2 ack');

        // Cancel subs before disconnect.
        await _safeCancel(sub1);
        await _safeCancel(sub2);

        // Disconnect.
        await client.disconnectWebSocket();
        await sleep(const Duration(milliseconds: 500));

        // Reconnect.
        await client.reconnectWebSocket();

        // Re-subscribe.
        final postEvents1 = <ChangeEvent>[];
        final postEvents2 = <ChangeEvent>[];
        final postSub1 =
            client.subscribe('SELECT * FROM $tbl').listen(postEvents1.add);
        final postSub2 =
            client.subscribe('SELECT * FROM $tbl2').listen(postEvents2.add);

        await sleep(const Duration(seconds: 3));

        // Insert into both tables.
        final writer = await connectJwtClient();
        try {
          await writer.query(
            "INSERT INTO $tbl (id, payload) VALUES (10, 'multi-recon-1')",
          );
          await writer.query(
            "INSERT INTO $tbl2 (id, label) VALUES (10, 'multi-recon-2')",
          );

          await sleep(const Duration(seconds: 5));

          final changes1 = postEvents1.whereType<InsertEvent>();
          final changes2 = postEvents2.whereType<InsertEvent>();
          expect(changes1, isNotEmpty,
              reason: 'table 1 should receive InsertEvent after reconnect');
          expect(changes2, isNotEmpty,
              reason: 'table 2 should receive InsertEvent after reconnect');
        } finally {
          await _safeCancel(postSub2);
          await _safeCancel(postSub1);
          await dropTable(client, tbl2);
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 120)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 7. Three active subscriptions: reconnect catch-up without old replay
    // ─────────────────────────────────────────────────────────────────
    test(
      'three active subscriptions resume without replaying old rows',
      () async {
        final tblA = '$ns.resume_a';
        final tblB = '$ns.resume_b';
        final tblC = '$ns.resume_c';

        await client.query(
          'CREATE TABLE IF NOT EXISTS $tblA ('
          'id INT PRIMARY KEY, '
          'payload TEXT'
          ')',
        );
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tblB ('
          'id INT PRIMARY KEY, '
          'payload TEXT'
          ')',
        );
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tblC ('
          'id INT PRIMARY KEY, '
          'payload TEXT'
          ')',
        );

        final writer = await connectJwtClient();
        const preA = 1001;
        const preB = 2001;
        const preC = 3001;
        const gapA = 1002;
        const gapB = 2002;
        const gapC = 3002;
        const postA = 1003;
        const postB = 2003;
        const postC = 3003;

        bool hasRowId(List<ChangeEvent> events, int id) {
          for (final event in events) {
            switch (event) {
              case InsertEvent(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case UpdateEvent(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case InitialDataBatch(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case AckEvent() || DeleteEvent() || SubscriptionError():
                break;
            }
          }
          return false;
        }

        Future<void> waitFor(
          bool Function() predicate, {
          Duration timeout = const Duration(seconds: 20),
          Duration poll = const Duration(milliseconds: 200),
        }) async {
          final started = DateTime.now();
          while (!predicate()) {
            if (DateTime.now().difference(started) > timeout) {
              throw TimeoutException('Timed out waiting for condition');
            }
            await sleep(poll);
          }
        }

        final preEventsA = <ChangeEvent>[];
        final preEventsB = <ChangeEvent>[];
        final preEventsC = <ChangeEvent>[];

        final subA = client
            .subscribe('SELECT id, payload FROM $tblA', lastRows: 0)
            .listen(preEventsA.add);
        final subB = client
            .subscribe('SELECT id, payload FROM $tblB', lastRows: 0)
            .listen(preEventsB.add);
        final subC = client
            .subscribe('SELECT id, payload FROM $tblC', lastRows: 0)
            .listen(preEventsC.add);

        await waitFor(() {
          return preEventsA.whereType<AckEvent>().isNotEmpty &&
              preEventsB.whereType<AckEvent>().isNotEmpty &&
              preEventsC.whereType<AckEvent>().isNotEmpty;
        });

        await writer
            .query("INSERT INTO $tblA (id, payload) VALUES ($preA, 'pre-a')");
        await writer
            .query("INSERT INTO $tblB (id, payload) VALUES ($preB, 'pre-b')");
        await writer
            .query("INSERT INTO $tblC (id, payload) VALUES ($preC, 'pre-c')");

        await waitFor(() {
          return hasRowId(preEventsA, preA) &&
              hasRowId(preEventsB, preB) &&
              hasRowId(preEventsC, preC);
        });

        await _safeCancel(subA);
        await _safeCancel(subB);
        await _safeCancel(subC);

        await client.disconnectWebSocket();
        expect(await client.isConnected, isFalse);

        await writer
            .query("INSERT INTO $tblA (id, payload) VALUES ($gapA, 'gap-a')");
        await writer
            .query("INSERT INTO $tblB (id, payload) VALUES ($gapB, 'gap-b')");
        await writer
            .query("INSERT INTO $tblC (id, payload) VALUES ($gapC, 'gap-c')");

        await client.reconnectWebSocket();
        expect(await client.isConnected, isTrue);

        final postEventsA = <ChangeEvent>[];
        final postEventsB = <ChangeEvent>[];
        final postEventsC = <ChangeEvent>[];

        final postSubA = client
            .subscribe('SELECT id, payload FROM $tblA WHERE id >= $gapA')
            .listen(postEventsA.add);
        final postSubB = client
            .subscribe('SELECT id, payload FROM $tblB WHERE id >= $gapB')
            .listen(postEventsB.add);
        final postSubC = client
            .subscribe('SELECT id, payload FROM $tblC WHERE id >= $gapC')
            .listen(postEventsC.add);

        await waitFor(() {
          return postEventsA.whereType<AckEvent>().isNotEmpty &&
              postEventsB.whereType<AckEvent>().isNotEmpty &&
              postEventsC.whereType<AckEvent>().isNotEmpty;
        });

        await writer
            .query("INSERT INTO $tblA (id, payload) VALUES ($postA, 'post-a')");
        await writer
            .query("INSERT INTO $tblB (id, payload) VALUES ($postB, 'post-b')");
        await writer
            .query("INSERT INTO $tblC (id, payload) VALUES ($postC, 'post-c')");

        await waitFor(() {
          return hasRowId(postEventsA, gapA) &&
              hasRowId(postEventsA, postA) &&
              hasRowId(postEventsB, gapB) &&
              hasRowId(postEventsB, postB) &&
              hasRowId(postEventsC, gapC) &&
              hasRowId(postEventsC, postC);
        });

        expect(hasRowId(postEventsA, preA), isFalse,
            reason: 'A should not replay pre-disconnect row');
        expect(hasRowId(postEventsB, preB), isFalse,
            reason: 'B should not replay pre-disconnect row');
        expect(hasRowId(postEventsC, preC), isFalse,
            reason: 'C should not replay pre-disconnect row');

        await _safeCancel(postSubA);
        await _safeCancel(postSubB);
        await _safeCancel(postSubC);
        await dropTable(client, tblC);
        await dropTable(client, tblB);
        await dropTable(client, tblA);
        await writer.dispose();
      },
      timeout: const Timeout(Duration(seconds: 150)),
    );

    test(
      'chaos: repeated reconnect cycles with 3 subscriptions stay consistent',
      () async {
        final tblA = '$ns.chaos_a';
        final tblB = '$ns.chaos_b';
        final tblC = '$ns.chaos_c';
        const cycles = 3;

        await client.query(
          'CREATE TABLE IF NOT EXISTS $tblA ('
          'id INT PRIMARY KEY, '
          'payload TEXT'
          ')',
        );
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tblB ('
          'id INT PRIMARY KEY, '
          'payload TEXT'
          ')',
        );
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tblC ('
          'id INT PRIMARY KEY, '
          'payload TEXT'
          ')',
        );

        final writer = await connectJwtClient();
        const preA = 21001;
        const preB = 22001;
        const preC = 23001;

        bool hasRowId(List<ChangeEvent> events, int id) {
          for (final event in events) {
            switch (event) {
              case InsertEvent(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case UpdateEvent(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case InitialDataBatch(:final rows):
                if (rows.any((r) => r['id']?.asInt() == id)) return true;
              case AckEvent() || DeleteEvent() || SubscriptionError():
                break;
            }
          }
          return false;
        }

        Future<void> waitFor(
          bool Function() predicate, {
          Duration timeout = const Duration(seconds: 20),
          Duration poll = const Duration(milliseconds: 200),
        }) async {
          final started = DateTime.now();
          while (!predicate()) {
            if (DateTime.now().difference(started) > timeout) {
              throw TimeoutException('Timed out waiting for condition');
            }
            await sleep(poll);
          }
        }

        final preEventsA = <ChangeEvent>[];
        final preEventsB = <ChangeEvent>[];
        final preEventsC = <ChangeEvent>[];

        final preSubA = client
            .subscribe('SELECT id, payload FROM $tblA', lastRows: 0)
            .listen(preEventsA.add);
        final preSubB = client
            .subscribe('SELECT id, payload FROM $tblB', lastRows: 0)
            .listen(preEventsB.add);
        final preSubC = client
            .subscribe('SELECT id, payload FROM $tblC', lastRows: 0)
            .listen(preEventsC.add);

        await waitFor(() {
          return preEventsA.whereType<AckEvent>().isNotEmpty &&
              preEventsB.whereType<AckEvent>().isNotEmpty &&
              preEventsC.whereType<AckEvent>().isNotEmpty;
        });

        await writer.query(
            "INSERT INTO $tblA (id, payload) VALUES ($preA, 'chaos-pre-a')");
        await writer.query(
            "INSERT INTO $tblB (id, payload) VALUES ($preB, 'chaos-pre-b')");
        await writer.query(
            "INSERT INTO $tblC (id, payload) VALUES ($preC, 'chaos-pre-c')");

        await waitFor(() {
          return hasRowId(preEventsA, preA) &&
              hasRowId(preEventsB, preB) &&
              hasRowId(preEventsC, preC);
        });

        await _safeCancel(preSubA);
        await _safeCancel(preSubB);
        await _safeCancel(preSubC);

        for (var cycle = 1; cycle <= cycles; cycle++) {
          final gapA = 21000 + (cycle * 10) + 2;
          final gapB = 22000 + (cycle * 10) + 2;
          final gapC = 23000 + (cycle * 10) + 2;
          final postA = 21000 + (cycle * 10) + 3;
          final postB = 22000 + (cycle * 10) + 3;
          final postC = 23000 + (cycle * 10) + 3;

          await client.disconnectWebSocket();
          expect(await client.isConnected, isFalse,
              reason: 'cycle $cycle should be disconnected');

          await writer.query(
              "INSERT INTO $tblA (id, payload) VALUES ($gapA, 'chaos-gap-a-$cycle')");
          await writer.query(
              "INSERT INTO $tblB (id, payload) VALUES ($gapB, 'chaos-gap-b-$cycle')");
          await writer.query(
              "INSERT INTO $tblC (id, payload) VALUES ($gapC, 'chaos-gap-c-$cycle')");

          await client.reconnectWebSocket();
          expect(await client.isConnected, isTrue,
              reason: 'cycle $cycle should be reconnected');

          final eventsA = <ChangeEvent>[];
          final eventsB = <ChangeEvent>[];
          final eventsC = <ChangeEvent>[];

          final subA = client
              .subscribe('SELECT id, payload FROM $tblA WHERE id >= $gapA')
              .listen(eventsA.add);
          final subB = client
              .subscribe('SELECT id, payload FROM $tblB WHERE id >= $gapB')
              .listen(eventsB.add);
          final subC = client
              .subscribe('SELECT id, payload FROM $tblC WHERE id >= $gapC')
              .listen(eventsC.add);

          await waitFor(() {
            return eventsA.whereType<AckEvent>().isNotEmpty &&
                eventsB.whereType<AckEvent>().isNotEmpty &&
                eventsC.whereType<AckEvent>().isNotEmpty;
          });

          await writer.query(
              "INSERT INTO $tblA (id, payload) VALUES ($postA, 'chaos-post-a-$cycle')");
          await writer.query(
              "INSERT INTO $tblB (id, payload) VALUES ($postB, 'chaos-post-b-$cycle')");
          await writer.query(
              "INSERT INTO $tblC (id, payload) VALUES ($postC, 'chaos-post-c-$cycle')");

          await waitFor(() {
            return hasRowId(eventsA, gapA) &&
                hasRowId(eventsA, postA) &&
                hasRowId(eventsB, gapB) &&
                hasRowId(eventsB, postB) &&
                hasRowId(eventsC, gapC) &&
                hasRowId(eventsC, postC);
          });

          expect(hasRowId(eventsA, preA), isFalse,
              reason: 'cycle $cycle: A replayed pre row');
          expect(hasRowId(eventsB, preB), isFalse,
              reason: 'cycle $cycle: B replayed pre row');
          expect(hasRowId(eventsC, preC), isFalse,
              reason: 'cycle $cycle: C replayed pre row');

          await _safeCancel(subA);
          await _safeCancel(subB);
          await _safeCancel(subC);
        }

        await dropTable(client, tblC);
        await dropTable(client, tblB);
        await dropTable(client, tblA);
        await writer.dispose();
      },
      timeout: const Timeout(Duration(seconds: 180)),
    );
  });
}
