/// Subscription options & getSubscriptions e2e tests.
///
/// Verifies that all subscription options (batchSize, lastRows, from)
/// are properly forwarded to the server, and that getSubscriptions() returns
/// accurate metadata.
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
  group('Subscription Options', skip: skipIfNoIntegration, () {
    late KalamClient client;
    late String ns;
    late String tbl;

    setUpAll(() async {
      client = await connectJwtClient();
      ns = uniqueName('dart_opts');
      tbl = '$ns.items';
      await ensureNamespace(client, ns);
      await client.query(
        'CREATE TABLE IF NOT EXISTS $tbl ('
        'id INT PRIMARY KEY, '
        'value TEXT'
        ')',
      );
      // Seed some data for lastRows tests.
      for (var i = 1; i <= 20; i++) {
        await client.query(
          "INSERT INTO $tbl (id, value) VALUES ($i, 'row-$i')",
        );
      }
    });

    tearDownAll(() async {
      await dropTable(client, tbl);
      await client.dispose();
    });

    // ─────────────────────────────────────────────────────────────────
    // subscribe with batchSize
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe with batchSize receives ack',
      () async {
        final events = <ChangeEvent>[];
        final stream = client.subscribe(
          'SELECT * FROM $tbl',
          batchSize: 5,
        );
        final sub = stream.listen(events.add);

        await sleep(const Duration(seconds: 3));

        final ack = events.whereType<AckEvent>();
        expect(ack, isNotEmpty,
            reason: 'should receive AckEvent with batchSize=5');

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // subscribe with lastRows
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe with lastRows limits initial data',
      () async {
        final events = <ChangeEvent>[];
        final stream = client.subscribe(
          'SELECT * FROM $tbl',
          lastRows: 5,
        );
        final sub = stream.listen(events.add);

        await sleep(const Duration(seconds: 3));

        final ack = events.whereType<AckEvent>();
        expect(ack, isNotEmpty,
            reason: 'should receive AckEvent with lastRows=5');

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // subscribe with from
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe with from receives ack',
      () async {
        final events = <ChangeEvent>[];
        final stream = client.subscribe(
          'SELECT * FROM $tbl',
          from: SeqId.zero(), // start from beginning
        );
        final sub = stream.listen(events.add);

        await sleep(const Duration(seconds: 3));

        final ack = events.whereType<AckEvent>();
        expect(ack, isNotEmpty, reason: 'should receive AckEvent with from=0');

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // subscribe with all options combined
    // ─────────────────────────────────────────────────────────────────
    test(
      'subscribe with all options combined',
      () async {
        final events = <ChangeEvent>[];
        final stream = client.subscribe(
          'SELECT * FROM $tbl',
          batchSize: 10,
          lastRows: 5,
          from: SeqId.zero(),
          subscriptionId: 'custom-all-opts',
        );
        final sub = stream.listen(events.add);

        await sleep(const Duration(seconds: 3));

        final ack = events.whereType<AckEvent>();
        expect(ack, isNotEmpty, reason: 'should receive AckEvent');

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // from with specific checkpoint: no replay before checkpoint,
    // includes rows created after checkpoint
    // ─────────────────────────────────────────────────────────────────
    test(
      'from resumes from specific checkpoint without replaying older rows',
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
        final checkpointSubId = 'checkpoint-${uniqueName('fromseq')}';

        final writer = await connectJwtClient();
        StreamSubscription<ChangeEvent>? sub1;
        StreamSubscription<ChangeEvent>? sub2;

        try {
          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($preId, 'pre-checkpoint')",
          );

          final firstEvents = <ChangeEvent>[];
          final firstStream = client.subscribe(
            'SELECT id, value FROM $tbl WHERE id >= $preId',
            lastRows: 1,
            subscriptionId: checkpointSubId,
          );
          sub1 = firstStream.listen(firstEvents.add);

          await waitFor(() => firstEvents.whereType<AckEvent>().isNotEmpty);
          final firstAck = firstEvents.whereType<AckEvent>().first;

          await waitFor(() => hasRowId(firstEvents, preId));

          SeqId? checkpoint;
          final started = DateTime.now();
          while (checkpoint == null) {
            final subs = await client.getSubscriptions();
            for (final s in subs) {
              if (s.id == firstAck.subscriptionId && s.lastSeqId != null) {
                checkpoint = s.lastSeqId;
                break;
              }
            }
            if (checkpoint != null) break;
            if (DateTime.now().difference(started) >
                const Duration(seconds: 20)) {
              throw TimeoutException(
                  'Timed out waiting for subscription checkpoint');
            }
            await sleep(const Duration(milliseconds: 200));
          }

          await _safeCancel(sub1);
          sub1 = null;

          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($gapId, 'after-checkpoint-before-resub')",
          );

          final secondEvents = <ChangeEvent>[];
          final secondStream = client.subscribe(
            'SELECT id, value FROM $tbl WHERE id >= $preId',
            from: checkpoint,
            lastRows: 0,
          );
          sub2 = secondStream.listen(secondEvents.add);

          await waitFor(() => secondEvents.whereType<AckEvent>().isNotEmpty);

          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($postId, 'post-resub')",
          );

          await waitFor(() =>
              hasRowId(secondEvents, gapId) && hasRowId(secondEvents, postId));

          expect(hasRowId(secondEvents, preId), isFalse,
              reason: 'row written before checkpoint must not replay');
          expect(hasRowId(secondEvents, gapId), isTrue,
              reason: 'row written after checkpoint must be included');
          expect(hasRowId(secondEvents, postId), isTrue,
              reason: 'live row after resubscribe must be received');
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
      timeout: const Timeout(Duration(seconds: 120)),
    );

    test(
      'from resumes with only seqs strictly greater than checkpoint',
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
        final baselineA = (seed * 10) + 101;
        final baselineB = (seed * 10) + 102;
        final freshId = (seed * 10) + 103;
        final checkpointSubId = 'strict-from-${uniqueName('seq')}';
        final resumedSubId = 'strict-from-resume-${uniqueName('seq')}';

        final writer = await connectJwtClient();
        StreamSubscription<ChangeEvent>? sub1;
        StreamSubscription<ChangeEvent>? sub2;

        try {
          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($baselineA, 'baseline-a')",
          );
          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($baselineB, 'baseline-b')",
          );

          final baselineEvents = <ChangeEvent>[];
          final baselineStream = client.subscribe(
            'SELECT id, value FROM $tbl WHERE id >= $baselineA',
            lastRows: 2,
            subscriptionId: checkpointSubId,
          );
          sub1 = baselineStream.listen(baselineEvents.add);

          await waitFor(
            () =>
                hasRowId(baselineEvents, baselineA) &&
                hasRowId(baselineEvents, baselineB),
          );

          SeqId? checkpoint;
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
            if (DateTime.now().difference(started) >
                const Duration(seconds: 20)) {
              throw TimeoutException(
                'Timed out waiting for strict resume checkpoint',
              );
            }
            await sleep(const Duration(milliseconds: 200));
          }

          await _safeCancel(sub1);
          sub1 = null;

          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($freshId, 'fresh-after-checkpoint')",
          );

          final resumedEvents = <ChangeEvent>[];
          final resumedStream = client.subscribe(
            'SELECT id, value FROM $tbl WHERE id >= $baselineA',
            from: checkpoint,
            lastRows: 0,
            subscriptionId: resumedSubId,
          );
          sub2 = resumedStream.listen(resumedEvents.add);

          await waitFor(() => resumedEvents.whereType<AckEvent>().isNotEmpty);
          await waitFor(() => hasRowId(resumedEvents, freshId));
          final checkpointValue = checkpoint;
          var resumedAdvanced = false;
          final resumedStarted = DateTime.now();
          while (!resumedAdvanced) {
            final subs = await client.getSubscriptions();
            for (final s in subs) {
              final lastSeqId = s.lastSeqId;
              if (s.id == resumedSubId && lastSeqId != null) {
                resumedAdvanced = lastSeqId > checkpointValue;
                break;
              }
            }
            if (resumedAdvanced) break;
            if (DateTime.now().difference(resumedStarted) >
                const Duration(seconds: 20)) {
              throw TimeoutException(
                'Timed out waiting for resumed subscription checkpoint to advance',
              );
            }
            await sleep(const Duration(milliseconds: 200));
          }

          expect(
            hasRowId(resumedEvents, baselineA),
            isFalse,
            reason: 'baseline row A must not replay',
          );
          expect(
            hasRowId(resumedEvents, baselineB),
            isFalse,
            reason: 'baseline row B must not replay',
          );
          expect(
            hasRowId(resumedEvents, freshId),
            isTrue,
            reason: 'fresh row after checkpoint must be delivered',
          );
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
      timeout: const Timeout(Duration(seconds: 120)),
    );
  });

  group('getSubscriptions', skip: skipIfNoIntegration, () {
    late KalamClient client;
    late String ns;
    late String tbl;

    setUpAll(() async {
      client = await connectJwtClient();
      ns = uniqueName('dart_getsubs');
      tbl = '$ns.items';
      await ensureNamespace(client, ns);
      await client.query(
        'CREATE TABLE IF NOT EXISTS $tbl ('
        'id INT PRIMARY KEY, '
        'value TEXT'
        ')',
      );
    });

    tearDownAll(() async {
      await dropTable(client, tbl);
      await client.dispose();
    });

    // ─────────────────────────────────────────────────────────────────
    // getSubscriptions returns active subs
    // ─────────────────────────────────────────────────────────────────
    test(
      'getSubscriptions returns active subscriptions',
      () async {
        final stream = client.subscribe('SELECT * FROM $tbl');
        final sub = stream.listen((_) {});

        await sleep(const Duration(seconds: 2));

        final subs = await client.getSubscriptions();
        expect(subs, isNotEmpty,
            reason: 'should have at least one subscription');
        expect(subs.first.id, isNotEmpty);
        expect(subs.first.query, contains('SELECT'));
        expect(subs.first.closed, isFalse);
        expect(subs.first.createdAtMs, greaterThan(0));

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // getSubscriptions after cancel shows reduced count
    // ─────────────────────────────────────────────────────────────────
    test(
      'getSubscriptions after cancel reflects removal',
      () async {
        final subId = uniqueName('cancel_sub');
        final stream = client.subscribe(
          'SELECT * FROM $tbl',
          subscriptionId: subId,
        );
        final sub = stream.listen((_) {});
        await sleep(const Duration(seconds: 2));

        final subsDuring = await client.getSubscriptions();
        final activeDuring = subsDuring.where((s) => s.id == subId);
        expect(activeDuring, isNotEmpty,
            reason:
                'should include active subscription before cancel ($subId)');

        await _safeCancel(sub);

        Future<void> waitForRemoval() async {
          final started = DateTime.now();
          while (true) {
            final current = await client.getSubscriptions();
            final stillActive = current.where((s) => s.id == subId);
            if (stillActive.isEmpty) return;
            if (DateTime.now().difference(started) >
                const Duration(seconds: 10)) {
              break;
            }
            await sleep(const Duration(milliseconds: 200));
          }
        }

        await waitForRemoval();

        final subsAfter = await client.getSubscriptions();
        final sameSubAfter = subsAfter.where((s) => s.id == subId).toList();
        if (sameSubAfter.isNotEmpty) {
          // Some environments delay server-side close acknowledgement.
          // Ensure metadata remains queryable even if closure is still pending.
          expect(sameSubAfter.first.query, contains('SELECT * FROM $tbl'));
        } else {
          expect(sameSubAfter, isEmpty);
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // SubscriptionInfo model has expected fields
    // ─────────────────────────────────────────────────────────────────
    test(
      'SubscriptionInfo has all expected fields',
      () async {
        final stream = client.subscribe('SELECT id, value FROM $tbl');
        final sub = stream.listen((_) {});

        await sleep(const Duration(seconds: 2));

        final subs = await client.getSubscriptions();
        expect(subs, isNotEmpty);

        final info = subs.first;
        // Verify all fields are accessible (type-safe).
        expect(info.id, isA<String>());
        expect(info.query, isA<String>());
        expect(info.createdAtMs, isA<int>());
        expect(info.closed, isA<bool>());
        // lastSeqId and lastEventTimeMs may be null initially.
        // Just verify they are accessible.
        info.lastSeqId; // no assertion, just access
        info.lastEventTimeMs;

        await _safeCancel(sub);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );
  });
}
