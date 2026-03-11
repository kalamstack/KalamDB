/// Subscription cleanup e2e tests — verify that old subscriptions are properly
/// cleaned up on the server when:
/// 1. A subscription with the same ID is re-created (replace)
/// 2. A subscription is cancelled and re-opened
/// 3. Multiple subscribe/cancel cycles don't leak server resources
///
/// These tests verify the fix for the SUBSCRIPTION_LIMIT_EXCEEDED error
/// that occurred when old subscriptions weren't unsubscribed on the server
/// during re-subscribe with the same ID.
library;

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

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

void main() {
  group('Subscription Cleanup', skip: skipIfNoIntegration, () {
    // ─────────────────────────────────────────────────────────────────
    // 1. Re-subscribing with the same ID doesn't leak old subscriptions
    // ─────────────────────────────────────────────────────────────────
    test(
      'resubscribe with same ID cleans up old server subscription',
      () async {
        final client = await connectJwtClient();
        try {
          final ns = uniqueName('dart_sub');
          final tbl = '$ns.cleanup_test';
          await ensureNamespace(client, ns);
          await client.query(
            'CREATE TABLE IF NOT EXISTS $tbl ('
            'id INT PRIMARY KEY, '
            'value TEXT'
            ')',
          );

          const fixedSubId = 'cleanup-test-fixed-id';

          // Subscribe, receive ACK, then cancel. Repeat several times.
          for (var i = 0; i < 5; i++) {
            final events = <ChangeEvent>[];
            final stream = client.subscribe(
              'SELECT * FROM $tbl',
              subscriptionId: fixedSubId,
            );
            final sub = stream.listen(events.add);

            await _waitForCondition(() => events.whereType<AckEvent>().isNotEmpty);
            expect(events.whereType<AckEvent>(), isNotEmpty,
                reason: 'Iteration $i: should receive AckEvent');

            // Cancel the subscription.
            await sub
                .cancel()
                .timeout(const Duration(seconds: 3), onTimeout: () => null);
            // Brief pause so unsubscribe message reaches server.
            await sleep(const Duration(milliseconds: 500));
          }

          // After 5 cycles, we should still be able to subscribe.
          // If subscriptions leaked, the server limit would be hit.
          final events = <ChangeEvent>[];
          final stream = client.subscribe(
            'SELECT * FROM $tbl',
            subscriptionId: fixedSubId,
          );
          final sub = stream.listen(events.add);

          await _waitForCondition(() => events.whereType<AckEvent>().isNotEmpty);
          expect(events.whereType<AckEvent>(), isNotEmpty,
              reason:
                  'Final subscribe should succeed (not SUBSCRIPTION_LIMIT_EXCEEDED)');

          // Verify no subscription errors.
          final errors = events.whereType<SubscriptionError>().toList();
          expect(
            errors.where((e) => e.code == 'SUBSCRIPTION_LIMIT_EXCEEDED'),
            isEmpty,
            reason: 'Should NOT get SUBSCRIPTION_LIMIT_EXCEEDED',
          );

          await sub
              .cancel()
              .timeout(const Duration(seconds: 3), onTimeout: () => null);
          await dropTable(client, tbl);
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 60)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 2. Multiple concurrent subscriptions with different IDs work
    // ─────────────────────────────────────────────────────────────────
    test(
      'multiple subscriptions with different IDs all receive ACKs',
      () async {
        final client = await connectJwtClient();
        try {
          final ns = uniqueName('dart_multi');
          final tbl = '$ns.multi_sub';
          await ensureNamespace(client, ns);
          await client.query(
            'CREATE TABLE IF NOT EXISTS $tbl ('
            'id INT PRIMARY KEY, '
            'data TEXT'
            ')',
          );

          final subs = <StreamSubscription<ChangeEvent>>[];
          final allEvents = <String, List<ChangeEvent>>{};

          // Create 3 simultaneous subscriptions with different IDs.
          for (var i = 0; i < 3; i++) {
            final subId = 'multi-test-$i';
            allEvents[subId] = [];
            final stream = client.subscribe(
              'SELECT * FROM $tbl',
              subscriptionId: subId,
            );
            subs.add(stream.listen((e) => allEvents[subId]!.add(e)));
          }

          await sleep(const Duration(seconds: 3));

          // All 3 should have received ACKs.
          for (var i = 0; i < 3; i++) {
            final subId = 'multi-test-$i';
            expect(allEvents[subId]!.whereType<AckEvent>(), isNotEmpty,
                reason: 'Subscription $subId should receive AckEvent');
          }

          // Verify subscription count on the shared connection.
          final subInfos = await client.getSubscriptions();
          final activeIds =
              subInfos.where((s) => !s.closed).map((s) => s.id).toSet();
          expect(activeIds,
              containsAll(['multi-test-0', 'multi-test-1', 'multi-test-2']),
              reason: 'All 3 subscriptions should be active');

          // Cancel all.
          for (final sub in subs) {
            await sub
                .cancel()
                .timeout(const Duration(seconds: 3), onTimeout: () => null);
          }
          await dropTable(client, tbl);
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 3. Rapid subscribe/cancel cycles don't exhaust subscription limit
    // ─────────────────────────────────────────────────────────────────
    test(
      'rapid subscribe/cancel cycles do not leak subscriptions',
      () async {
        final client = await connectJwtClient();
        try {
          final ns = uniqueName('dart_rapid');
          final tbl = '$ns.rapid_sub';
          await ensureNamespace(client, ns);
          await client.query(
            'CREATE TABLE IF NOT EXISTS $tbl ('
            'id INT PRIMARY KEY, '
            'msg TEXT'
            ')',
          );

          // Rapidly create and cancel subscriptions (auto-generated IDs).
          for (var i = 0; i < 15; i++) {
            final events = <ChangeEvent>[];
            final stream = client.subscribe('SELECT * FROM $tbl');
            final sub = stream.listen(events.add);

            // Wait briefly for ACK.
            await sleep(const Duration(seconds: 1));

            await sub
                .cancel()
                .timeout(const Duration(seconds: 2), onTimeout: () => null);
            // Small delay so unsubscribe reaches server.
            await sleep(const Duration(milliseconds: 200));
          }

          // Should still work after 15 cycles.
          final events = <ChangeEvent>[];
          final stream = client.subscribe('SELECT * FROM $tbl');
          final sub = stream.listen(events.add);

          await sleep(const Duration(seconds: 2));
          expect(events.whereType<AckEvent>(), isNotEmpty,
              reason:
                  'Subscribe after 15 rapid cycles should work (no limit exceeded)');

          final errors = events.whereType<SubscriptionError>().toList();
          expect(
            errors.where((e) => e.code == 'SUBSCRIPTION_LIMIT_EXCEEDED'),
            isEmpty,
            reason: 'Should NOT hit subscription limit after rapid cycles',
          );

          await sub
              .cancel()
              .timeout(const Duration(seconds: 3), onTimeout: () => null);
          await dropTable(client, tbl);
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 90)),
    );

    // ─────────────────────────────────────────────────────────────────
    // 4. getSubscriptions reflects correct count after cleanup
    // ─────────────────────────────────────────────────────────────────
    test(
      'getSubscriptions shows correct count after subscribe/cancel',
      () async {
        final client = await connectJwtClient();
        try {
          final ns = uniqueName('dart_count');
          final tbl = '$ns.count_sub';
          await ensureNamespace(client, ns);
          await client.query(
            'CREATE TABLE IF NOT EXISTS $tbl ('
            'id INT PRIMARY KEY'
            ')',
          );

          // Start with no subscriptions.
          final initialSubs = await client.getSubscriptions();
          final initialCount = initialSubs.where((s) => !s.closed).length;

          // Create 2 subscriptions.
          final sub1Events = <ChangeEvent>[];
          final sub2Events = <ChangeEvent>[];
          final stream1 = client.subscribe('SELECT * FROM $tbl',
              subscriptionId: 'count-sub-1');
          final stream2 = client.subscribe('SELECT * FROM $tbl',
              subscriptionId: 'count-sub-2');
          final s1 = stream1.listen(sub1Events.add);
          final s2 = stream2.listen(sub2Events.add);

          await sleep(const Duration(seconds: 2));

          final midSubs = await client.getSubscriptions();
          final midCount = midSubs.where((s) => !s.closed).length;
          expect(midCount, equals(initialCount + 2),
              reason: 'Should have 2 more active subscriptions');

          // Cancel one.
          await s1
              .cancel()
              .timeout(const Duration(seconds: 3), onTimeout: () => null);
          await sleep(const Duration(seconds: 2));

          final afterCancelSubs = await client.getSubscriptions();
          final afterCancelCount =
              afterCancelSubs.where((s) => !s.closed).length;
          expect(afterCancelCount, equals(initialCount + 1),
              reason: 'Should have 1 subscription after cancelling one '
                  '(got $afterCancelCount, expected ${initialCount + 1}). '
                  'Active: ${afterCancelSubs.where((s) => !s.closed).map((s) => s.id).toList()}');

          // Cancel the other.
          await s2
              .cancel()
              .timeout(const Duration(seconds: 3), onTimeout: () => null);
          await sleep(const Duration(seconds: 2));

          final finalSubs = await client.getSubscriptions();
          final finalCount = finalSubs.where((s) => !s.closed).length;
          expect(finalCount, equals(initialCount),
              reason: 'Should be back to initial count after cancelling all');

          await dropTable(client, tbl);
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );
  });
}
