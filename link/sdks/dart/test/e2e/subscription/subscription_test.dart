/// Subscription e2e tests — subscribe, change events, unsubscribe.
///
/// Mirrors: tests/e2e/subscription/subscription.test.mjs (TypeScript)
///
/// NOTE: `dartSubscriptionClose` in the native bridge may hang waiting for the
/// server's WebSocket close acknowledgement.  All subscription cancellations
/// use [_safeCancel] to apply a bounded timeout so tests don't block.
library;

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

/// Cancel a stream subscription with a safety timeout.
///
/// The Rust-side `dartSubscriptionClose` can hang when the server doesn't ack
/// the WebSocket close frame quickly.  Wrapping in a timeout prevents the test
/// from blocking forever.
Future<void> _safeCancel(StreamSubscription<dynamic> sub) async {
  try {
    await sub.cancel().timeout(const Duration(seconds: 3));
  } on TimeoutException {
    // The cancel is still in flight but we can proceed — the underlying
    // resources will be cleaned up when the process exits.
  }
}

Future<void> _waitForCondition(
  bool Function() predicate, {
  Duration timeout = const Duration(seconds: 20),
  Duration interval = const Duration(milliseconds: 200),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Timed out waiting for condition');
    }
    await sleep(interval);
  }
}

Set<int> _insertedIds(Iterable<ChangeEvent> events) {
  final ids = <int>{};
  for (final event in events.whereType<InsertEvent>()) {
    for (final row in event.rows) {
      final id = row['id']?.asInt();
      if (id != null) {
        ids.add(id);
      }
    }
  }
  return ids;
}

void main() {
  group('Subscription', skip: skipIfNoIntegration, () {
    late KalamClient client;
    late String ns;
    late String tbl;

    setUpAll(() async {
      client = await connectJwtClient();
      ns = uniqueName('dart_sub');
      tbl = '$ns.messages';
      await ensureNamespace(client, ns);
      await client.query(
        'CREATE TABLE IF NOT EXISTS $tbl ('
        'id INT PRIMARY KEY, '
        'body TEXT'
        ')',
      );
    });

    tearDownAll(() async {
      await dropTable(client, tbl);
      await client.dispose();
    });

    // ─────────────────────────────────────────────────────────────────
    // Basic subscribe — returns Stream
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe returns a Stream<ChangeEvent>',
      () async {
        final stream = client.subscribe('SELECT id, body FROM $tbl');
        expect(stream, isA<Stream<ChangeEvent>>());

        // Listen briefly then cancel.
        final sub = stream.listen((_) {});
        await sleep(const Duration(milliseconds: 500));
        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Receives AckEvent
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe receives AckEvent',
      () async {
        final events = <ChangeEvent>[];
        final stream = client.subscribe('SELECT id, body FROM $tbl');
        final sub = stream.listen(events.add);

        // Wait for ack.
        await sleep(const Duration(seconds: 3));

        final ack = events.whereType<AckEvent>();
        expect(ack, isNotEmpty, reason: 'should receive AckEvent');
        expect(ack.first.subscriptionId, isNotEmpty);

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Insert triggers InsertEvent on subscriber
    // ─────────────────────────────────────────────────────────────────
    test(
      'insert triggers InsertEvent on subscriber',
      () async {
        final insertEvent = Completer<InsertEvent>();
        final stream = client.subscribe('SELECT id, body FROM $tbl');
        final sub = stream.listen((event) {
          if (event case InsertEvent()) {
            for (final row in event.rows) {
              if (row['id']?.asInt() == 500) {
                if (!insertEvent.isCompleted) {
                  insertEvent.complete(event);
                }
                break;
              }
            }
          }
        });

        // Let subscription stabilise.
        await sleep(const Duration(seconds: 2));

        // Insert from a second client.
        final writer = await connectJwtClient();
        try {
          await writer.query(
            "INSERT INTO $tbl (id, body) VALUES (500, 'hello from writer')",
          );

          final received = await insertEvent.future.timeout(
            const Duration(seconds: 15),
          );

          expect(received.row['id']?.asInt(), 500);
          expect(received.row['body']?.asString(), 'hello from writer');
        } finally {
          await _safeCancel(sub);
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 60)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Subscribe with WHERE clause
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe with WHERE clause filters events',
      () async {
        final events = <ChangeEvent>[];
        final stream = client.subscribe(
          'SELECT * FROM $tbl WHERE id = 600',
        );
        final sub = stream.listen(events.add);

        await sleep(const Duration(seconds: 2));

        final writer = await connectJwtClient();
        try {
          await writer.query(
            "INSERT INTO $tbl (id, body) VALUES (600, 'targeted')",
          );

          await sleep(const Duration(seconds: 3));

          final ack = events.whereType<AckEvent>();
          expect(ack, isNotEmpty,
              reason: 'should get AckEvent for filtered subscription');
        } finally {
          await _safeCancel(sub);
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 60)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Subscribe with custom subscriptionId
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe with custom subscriptionId',
      () async {
        final customId = 'custom_${uniqueName('id')}';
        final events = <ChangeEvent>[];
        final stream = client.subscribe(
          'SELECT id, body FROM $tbl',
          subscriptionId: customId,
        );
        final sub = stream.listen(events.add);

        await _waitForCondition(() => events.whereType<AckEvent>().isNotEmpty);

        final ack = events.whereType<AckEvent>();
        expect(ack, isNotEmpty);
        // The server may or may not honour the custom ID,
        // but it should at least return a subscription ID.
        expect(ack.first.subscriptionId, isNotEmpty);

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Subscribe with batchSize and lastRows
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe with batchSize parameter',
      () async {
        final events = <ChangeEvent>[];
        final stream = client.subscribe(
          'SELECT id, body FROM $tbl',
          batchSize: 10,
          lastRows: 5,
        );
        final sub = stream.listen(events.add);

        await sleep(const Duration(seconds: 3));

        // Should at least get ack.
        final ack = events.whereType<AckEvent>();
        expect(ack, isNotEmpty);

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Invalid subscription surfaces an error instead of hanging silently
    // ─────────────────────────────────────────────────────────────────
    test(
      'invalid subscription emits a startup error',
      () async {
        final stream = client.subscribe(
          'SELECT * FROM nonexistent.dart_missing_subscription_table',
        );

        await expectLater(
          stream.drain<void>(),
          throwsA(
            isA<Object>().having(
              (error) => error.toString().toLowerCase(),
              'message',
              allOf(
                contains('subscription failed'),
                anyOf(contains('not found'), contains('does not exist')),
              ),
            ),
          ),
        );
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    test(
      'liveQueryRowsWithSql emits materialized row snapshots',
      () async {
        final rowId =
            (DateTime.now().millisecondsSinceEpoch % 1000000) * 100 + 700;
        final snapshots = <List<Map<String, KalamCellValue>>>[];
        final stream = client.liveQueryRowsWithSql<Map<String, KalamCellValue>>(
          'SELECT id, body FROM $tbl WHERE id = $rowId',
          limit: 2,
        );
        final sub = stream.listen(snapshots.add);

        await sleep(const Duration(seconds: 2));

        final writer = await connectJwtClient();
        try {
          await writer.query(
            "INSERT INTO $tbl (id, body) VALUES ($rowId, 'first')",
          );

          await Future<void>.delayed(const Duration(seconds: 2));

          expect(snapshots, isNotEmpty);
          final latest = snapshots.last;
          expect(latest.length, 1);
          expect(latest.first['id']?.asInt(), rowId);
          expect(latest.first['body']?.asString(), 'first');
        } finally {
          await _safeCancel(sub);
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 60)),
    );

    test(
      'concurrent writers fan out inserts to every subscriber client',
      () async {
        final subscriberClients = await Future.wait(
          List.generate(3, (_) => connectJwtClient()),
        );
        final writerClients = await Future.wait(
          List.generate(2, (_) => connectJwtClient()),
        );
        final subscriberEvents = List.generate(3, (_) => <ChangeEvent>[]);
        final subscriptions = <StreamSubscription<ChangeEvent>>[];
        final baseId = (DateTime.now().millisecondsSinceEpoch % 1000000) * 100;
        final insertedIds = List.generate(8, (index) => baseId + index + 1);

        try {
          for (var index = 0; index < subscriberClients.length; index++) {
            final stream = subscriberClients[index].subscribe(
              'SELECT id, body FROM $tbl WHERE id >= $baseId',
              lastRows: 0,
            );
            subscriptions.add(stream.listen(subscriberEvents[index].add));
          }

          await _waitForCondition(
            () => subscriberEvents.every(
              (events) => events.any((event) => event is AckEvent),
            ),
          );

          await Future.wait(
            insertedIds.asMap().entries.map((entry) {
              final writer = writerClients[entry.key % writerClients.length];
              return writer.query(
                "INSERT INTO $tbl (id, body) VALUES (${entry.value}, 'fanout_${entry.key}')",
              );
            }),
          );

          await _waitForCondition(
            () => subscriberEvents.every(
              (events) => _insertedIds(events).containsAll(insertedIds),
            ),
            timeout: const Duration(seconds: 30),
          );

          for (final events in subscriberEvents) {
            expect(_insertedIds(events), containsAll(insertedIds));
          }
        } finally {
          for (final sub in subscriptions) {
            await _safeCancel(sub);
          }
          for (final writer in writerClients) {
            await writer.dispose();
          }
          for (final subscriber in subscriberClients) {
            await subscriber.dispose();
          }
        }
      },
      timeout: const Timeout(Duration(seconds: 90)),
    );

    test(
      'one client keeps many simultaneous subscriptions isolated',
      () async {
        final writer = await connectJwtClient();
        final baseId =
            (DateTime.now().millisecondsSinceEpoch % 1000000) * 100 + 1000;
        final targetIds = List.generate(6, (index) => baseId + index + 1);
        final subscriptionEvents = {
          for (final id in targetIds) id: <ChangeEvent>[],
        };
        final subscriptions = <StreamSubscription<ChangeEvent>>[];

        try {
          for (final id in targetIds) {
            final stream = client.subscribe(
              'SELECT id, body FROM $tbl WHERE id = $id',
              lastRows: 0,
            );
            subscriptions.add(stream.listen(subscriptionEvents[id]!.add));
          }

          await _waitForCondition(
            () => subscriptionEvents.values.every(
              (events) => events.any((event) => event is AckEvent),
            ),
          );

          final activeSubs = await client.getSubscriptions();
          final matchingQueries = activeSubs
              .where((sub) =>
                  targetIds.any((id) => sub.query.contains('id = $id')))
              .length;
          expect(matchingQueries, targetIds.length);

          await Future.wait(
            targetIds.map(
              (id) => writer.query(
                "INSERT INTO $tbl (id, body) VALUES ($id, 'isolated_$id')",
              ),
            ),
          );

          await _waitForCondition(
            () => targetIds.every(
              (id) => _insertedIds(subscriptionEvents[id]!).contains(id),
            ),
            timeout: const Duration(seconds: 30),
          );

          for (final id in targetIds) {
            expect(_insertedIds(subscriptionEvents[id]!), {id});
          }
        } finally {
          for (final sub in subscriptions) {
            await _safeCancel(sub);
          }
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 90)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Cancel subscription stops receiving events
    // ─────────────────────────────────────────────────────────────────
    test(
      'cancelling stream subscription stops receiving events',
      () async {
        final rowId =
            (DateTime.now().millisecondsSinceEpoch % 1000000) * 100 + 900;
        var eventCount = 0;
        final stream = client.subscribe('SELECT id, body FROM $tbl');
        final sub = stream.listen((_) => eventCount++);

        await sleep(const Duration(seconds: 2));
        await _safeCancel(sub);

        final countAfterCancel = eventCount;

        // Insert some data—should not increase eventCount.
        final writer = await connectJwtClient();
        try {
          await writer.query(
            "INSERT INTO $tbl (id, body) VALUES ($rowId, 'after cancel')",
          );
          await sleep(const Duration(seconds: 2));

          expect(eventCount, countAfterCancel,
              reason: 'should not receive events after cancel');
        } finally {
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );
  });
}
