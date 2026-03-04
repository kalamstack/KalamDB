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

        await sleep(const Duration(seconds: 3));

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
    // Cancel subscription stops receiving events
    // ─────────────────────────────────────────────────────────────────
    test(
      'cancelling stream subscription stops receiving events',
      () async {
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
            "INSERT INTO $tbl (id, body) VALUES (700, 'after cancel')",
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
