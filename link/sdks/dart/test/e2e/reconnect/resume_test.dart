/// Resume e2e tests — disconnect, insert during gap, reconnect from
/// checkpoint, verify seq-based resume without replaying old rows.
///
/// These tests verify the same resume semantics as the Rust proxy tests,
/// using the SDK's built-in disconnect/reconnect API.
///
/// Mirrors: tests/e2e/reconnect/resume.test.mjs (TypeScript)
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
  group('Resume from checkpoint after disconnect', skip: skipIfNoIntegration,
      () {
    late KalamClient client;
    late String ns;

    setUpAll(() async {
      client = await connectJwtClient();
      ns = uniqueName('dart_resume');
      await ensureNamespace(client, ns);
    });

    tearDownAll(() async {
      await client.dispose();
    });

    // Live-update resume: insert before, disconnect, insert gap, reconnect, insert live
    test(
      'subscription resumes from checkpoint after disconnect — no replay',
      () async {
        final tbl = '$ns.resume_live';
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tbl (id INT PRIMARY KEY, value TEXT)',
        );

        final writer = await connectJwtClient();
        const preIds = [91001, 91002];
        const gapIds = [91003, 91004, 91005];
        const liveIds = [91006, 91007, 91008];
        final sql = 'SELECT id, value FROM $tbl';

        StreamSubscription<ChangeEvent>? sub1;
        StreamSubscription<ChangeEvent>? sub2;

        try {
          // Subscribe and wait for ack
          final preEvents = <ChangeEvent>[];
          final stream1 = client.subscribe(sql, lastRows: 0);
          sub1 = stream1.listen(preEvents.add);

          await waitForCondition(
            () => preEvents.whereType<AckEvent>().isNotEmpty,
          );

          for (final id in preIds) {
            await writer.query(
              "INSERT INTO $tbl (id, value) VALUES ($id, 'before-$id')",
            );
          }
          await waitForCondition(
            () => preIds.every((id) => changeEventsContainRowId(preEvents, id)),
          );

          // Capture checkpoint
          SeqId? checkpoint;
          await waitForAsyncCondition(() async {
            final subs = await client.getSubscriptions();
            for (final s in subs) {
              if (s.lastSeqId != null) {
                checkpoint = s.lastSeqId;
                return true;
              }
            }
            return false;
          });
          expect(checkpoint, isNotNull,
              reason: 'checkpoint should exist before disconnect');

          await _safeCancel(sub1);
          sub1 = null;

          // Disconnect
          await client.disconnectWebSocket();
          expect(await client.isConnected, isFalse);

          for (final id in gapIds) {
            await writer.query(
              "INSERT INTO $tbl (id, value) VALUES ($id, 'gap-$id')",
            );
          }

          // Reconnect
          await client.reconnectWebSocket();
          expect(await client.isConnected, isTrue);

          // Re-subscribe from checkpoint
          final resumedEvents = <ChangeEvent>[];
          final stream2 = client.subscribe(sql, lastRows: 0, from: checkpoint);
          sub2 = stream2.listen(resumedEvents.add);

          await waitForCondition(
            () => resumedEvents.whereType<AckEvent>().isNotEmpty,
          );

          for (final id in liveIds) {
            await writer.query(
              "INSERT INTO $tbl (id, value) VALUES ($id, 'live-$id')",
            );
          }

          await waitForCondition(
            () =>
                gapIds.every(
                    (id) => changeEventsContainRowId(resumedEvents, id)) &&
                liveIds.every(
                  (id) => changeEventsContainRowId(resumedEvents, id),
                ),
          );

          // Verify no replay
          for (final id in preIds) {
            expect(
              changeEventsContainRowId(resumedEvents, id),
              isFalse,
              reason: 'pre-disconnect row $id must NOT be replayed',
            );
          }
          expectNoDuplicateSeqs(
            resumedEvents,
            reason: 'resumed subscription must not emit duplicate events',
          );
          expectSeqsStrictlyAfterCheckpoint(
            resumedEvents,
            checkpoint!,
            reason:
                'resumed subscription must only emit events strictly after the checkpoint',
          );

          await _safeCancel(sub2);
          sub2 = null;
        } finally {
          if (sub1 != null) await _safeCancel(sub1);
          if (sub2 != null) await _safeCancel(sub2);
          await dropTable(client, tbl);
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 120)),
    );

    // Three-subscription resume
    test(
      'three subscriptions resume from their checkpoints after disconnect',
      () async {
        final tblA = '$ns.resume3_a';
        final tblB = '$ns.resume3_b';
        final tblC = '$ns.resume3_c';

        await client.query(
            'CREATE TABLE IF NOT EXISTS $tblA (id INT PRIMARY KEY, value TEXT)');
        await client.query(
            'CREATE TABLE IF NOT EXISTS $tblB (id INT PRIMARY KEY, value TEXT)');
        await client.query(
            'CREATE TABLE IF NOT EXISTS $tblC (id INT PRIMARY KEY, value TEXT)');

        final writer = await connectJwtClient();
        const preA = 81001, preB = 82001, preC = 83001;
        const gapA = 81002, gapB = 82002, gapC = 83002;
        const liveA = 81003, liveB = 82003, liveC = 83003;
        final sqlA = 'SELECT id, value FROM $tblA';
        final sqlB = 'SELECT id, value FROM $tblB';
        final sqlC = 'SELECT id, value FROM $tblC';

        final subs = <StreamSubscription<ChangeEvent>>[];

        try {
          final evA = <ChangeEvent>[];
          final evB = <ChangeEvent>[];
          final evC = <ChangeEvent>[];

          subs.add(client.subscribe(sqlA, lastRows: 0).listen(evA.add));
          subs.add(client.subscribe(sqlB, lastRows: 0).listen(evB.add));
          subs.add(client.subscribe(sqlC, lastRows: 0).listen(evC.add));

          await waitForCondition(() =>
              evA.whereType<AckEvent>().isNotEmpty &&
              evB.whereType<AckEvent>().isNotEmpty &&
              evC.whereType<AckEvent>().isNotEmpty);

          // Insert pre rows
          await writer
              .query("INSERT INTO $tblA (id, value) VALUES ($preA, 'pre-a')");
          await writer
              .query("INSERT INTO $tblB (id, value) VALUES ($preB, 'pre-b')");
          await writer
              .query("INSERT INTO $tblC (id, value) VALUES ($preC, 'pre-c')");

          await waitForCondition(() =>
              changeEventsContainRowId(evA, preA) &&
              changeEventsContainRowId(evB, preB) &&
              changeEventsContainRowId(evC, preC));

          // Capture checkpoints
          SeqId? cpA, cpB, cpC;
          await waitForAsyncCondition(() async {
            final allSubs = await client.getSubscriptions();
            for (final s in allSubs) {
              if (s.lastSeqId != null) {
                if (s.query.contains(tblA)) cpA = s.lastSeqId;
                if (s.query.contains(tblB)) cpB = s.lastSeqId;
                if (s.query.contains(tblC)) cpC = s.lastSeqId;
              }
            }
            return cpA != null && cpB != null && cpC != null;
          });

          // Cancel pre subs
          for (final s in subs) {
            await _safeCancel(s);
          }
          subs.clear();

          // Disconnect
          await client.disconnectWebSocket();

          // Insert gap rows
          await writer
              .query("INSERT INTO $tblA (id, value) VALUES ($gapA, 'gap-a')");
          await writer
              .query("INSERT INTO $tblB (id, value) VALUES ($gapB, 'gap-b')");
          await writer
              .query("INSERT INTO $tblC (id, value) VALUES ($gapC, 'gap-c')");

          // Reconnect
          await client.reconnectWebSocket();

          // Re-subscribe from checkpoints
          final rEvA = <ChangeEvent>[];
          final rEvB = <ChangeEvent>[];
          final rEvC = <ChangeEvent>[];

          subs.add(
              client.subscribe(sqlA, lastRows: 0, from: cpA).listen(rEvA.add));
          subs.add(
              client.subscribe(sqlB, lastRows: 0, from: cpB).listen(rEvB.add));
          subs.add(
              client.subscribe(sqlC, lastRows: 0, from: cpC).listen(rEvC.add));

          await waitForCondition(() =>
              rEvA.whereType<AckEvent>().isNotEmpty &&
              rEvB.whereType<AckEvent>().isNotEmpty &&
              rEvC.whereType<AckEvent>().isNotEmpty);

          // Insert live rows
          await writer
              .query("INSERT INTO $tblA (id, value) VALUES ($liveA, 'live-a')");
          await writer
              .query("INSERT INTO $tblB (id, value) VALUES ($liveB, 'live-b')");
          await writer
              .query("INSERT INTO $tblC (id, value) VALUES ($liveC, 'live-c')");

          // Wait for gap + live on all three
          await waitForCondition(() =>
              changeEventsContainRowId(rEvA, gapA) &&
              changeEventsContainRowId(rEvA, liveA) &&
              changeEventsContainRowId(rEvB, gapB) &&
              changeEventsContainRowId(rEvB, liveB) &&
              changeEventsContainRowId(rEvC, gapC) &&
              changeEventsContainRowId(rEvC, liveC));

          // No replay
          expect(changeEventsContainRowId(rEvA, preA), isFalse,
              reason: 'A must NOT replay pre row');
          expect(changeEventsContainRowId(rEvB, preB), isFalse,
              reason: 'B must NOT replay pre row');
          expect(changeEventsContainRowId(rEvC, preC), isFalse,
              reason: 'C must NOT replay pre row');
          expectNoDuplicateSeqs(
            rEvA,
            reason: 'subscription A must not replay duplicate seq ids',
          );
          expectNoDuplicateSeqs(
            rEvB,
            reason: 'subscription B must not replay duplicate seq ids',
          );
          expectNoDuplicateSeqs(
            rEvC,
            reason: 'subscription C must not replay duplicate seq ids',
          );
          expectSeqsStrictlyAfterCheckpoint(
            rEvA,
            cpA!,
            reason: 'subscription A must only emit seq ids after checkpoint',
          );
          expectSeqsStrictlyAfterCheckpoint(
            rEvB,
            cpB!,
            reason: 'subscription B must only emit seq ids after checkpoint',
          );
          expectSeqsStrictlyAfterCheckpoint(
            rEvC,
            cpC!,
            reason: 'subscription C must only emit seq ids after checkpoint',
          );
        } finally {
          for (final s in subs) {
            await _safeCancel(s);
          }
          await dropTable(client, tblA);
          await dropTable(client, tblB);
          await dropTable(client, tblC);
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 120)),
    );

    // Double disconnect recovery
    test(
      'double disconnect recovers and resumes from checkpoint',
      () async {
        final tbl = '$ns.resume_double';
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tbl (id INT PRIMARY KEY, value TEXT)',
        );

        final writer = await connectJwtClient();
        const preId = 71001;
        const gapId = 71002;
        const liveId = 71003;
        final sql = 'SELECT id, value FROM $tbl';

        StreamSubscription<ChangeEvent>? sub1;
        StreamSubscription<ChangeEvent>? sub2;

        try {
          final preEvents = <ChangeEvent>[];
          final stream1 = client.subscribe(sql, lastRows: 0);
          sub1 = stream1.listen(preEvents.add);

          await waitForCondition(
            () => preEvents.whereType<AckEvent>().isNotEmpty,
          );

          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($preId, 'pre')",
          );
          await waitForCondition(
              () => changeEventsContainRowId(preEvents, preId));

          // Capture checkpoint
          SeqId? checkpoint;
          await waitForAsyncCondition(() async {
            final subs = await client.getSubscriptions();
            for (final s in subs) {
              if (s.lastSeqId != null) {
                checkpoint = s.lastSeqId;
                return true;
              }
            }
            return false;
          });

          await _safeCancel(sub1);
          sub1 = null;

          // First disconnect
          await client.disconnectWebSocket();

          // Brief reconnect then immediate second disconnect
          await client.reconnectWebSocket();
          await sleep(const Duration(milliseconds: 300));
          await client.disconnectWebSocket();

          // Insert gap row
          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($gapId, 'gap')",
          );

          // Final recovery
          await client.reconnectWebSocket();

          final resumedEvents = <ChangeEvent>[];
          final stream2 = client.subscribe(sql, lastRows: 0, from: checkpoint);
          sub2 = stream2.listen(resumedEvents.add);

          await waitForCondition(
            () => resumedEvents.whereType<AckEvent>().isNotEmpty,
          );

          await writer.query(
            "INSERT INTO $tbl (id, value) VALUES ($liveId, 'live')",
          );

          await waitForCondition(() =>
              changeEventsContainRowId(resumedEvents, gapId) &&
              changeEventsContainRowId(resumedEvents, liveId));

          expect(changeEventsContainRowId(resumedEvents, preId), isFalse,
              reason: 'pre row must NOT be replayed');
          expectNoDuplicateSeqs(
            resumedEvents,
            reason:
                'double-disconnect resume must not replay duplicate seq ids',
          );
          expectSeqsStrictlyAfterCheckpoint(
            resumedEvents,
            checkpoint!,
            reason:
                'double-disconnect resume must only emit seq ids after checkpoint',
          );

          await _safeCancel(sub2);
          sub2 = null;
        } finally {
          if (sub1 != null) await _safeCancel(sub1);
          if (sub2 != null) await _safeCancel(sub2);
          await dropTable(client, tbl);
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 120)),
    );
  });
}
