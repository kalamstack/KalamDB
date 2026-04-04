/// App lifecycle reconnect e2e test.
///
/// Mirrors the Flutter app's WebSocket service pattern:
/// - one long-lived client with lazy WebSocket connect
/// - three fixed subscription IDs (`messages`, `notifications`, `user_updates`)
/// - pause: cancel subscriptions, then disconnect the socket
/// - resume: reconnect, then re-open the same IDs from saved checkpoints
///
/// The key assertion here is server-side cleanup: `system.live`
/// must return to zero between cycles, otherwise repeated app pause/resume
/// would accumulate retained live queries on the server.
library;

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

Future<KalamClient> _connectAppStyleClient() async {
  await ensureSdkReady();

  final bootstrap = await KalamClient.connect(
    url: serverUrl,
    authProvider: () async => Auth.none(),
    timeout: const Duration(seconds: 10),
  );

  LoginResponse login;
  try {
    login = await bootstrap.login(adminUser, adminPass);
  } finally {
    await bootstrap.dispose();
  }

  return KalamClient.connect(
    url: serverUrl,
    authProvider: () async => Auth.jwt(login.accessToken),
    timeout: const Duration(seconds: 10),
    keepaliveInterval: const Duration(seconds: 3),
    maxRetries: 1,
  );
}

Future<Map<String, SeqId>> _waitForCheckpoints(
  KalamClient client,
  Iterable<String> subscriptionIds,
) async {
  final ids = subscriptionIds.toSet();
  var checkpoints = <String, SeqId>{};

  await waitForAsyncCondition(() async {
    checkpoints = {
      for (final sub in await client.getSubscriptions())
        if (!sub.closed && sub.lastSeqId != null && ids.contains(sub.id))
          sub.id: sub.lastSeqId!,
    };
    return checkpoints.length == ids.length;
  }, timeout: const Duration(seconds: 20));

  return checkpoints;
}

Future<
    ({
      Map<String, List<ChangeEvent>> events,
      List<StreamSubscription<ChangeEvent>> subscriptions,
    })> _openSubscriptions({
  required KalamClient client,
  required Map<String, String> tables,
  required Map<String, String> subscriptionIds,
  required Map<String, SeqId> checkpointById,
}) async {
  final events = {
    for (final channel in tables.keys) channel: <ChangeEvent>[],
  };
  final subscriptions = <StreamSubscription<ChangeEvent>>[];

  for (final entry in tables.entries) {
    final channel = entry.key;
    final subscriptionId = subscriptionIds[channel]!;
    final checkpoint = checkpointById[subscriptionId] ?? const SeqId.zero();

    subscriptions.add(
      client
          .subscribe(
            'SELECT * FROM ${entry.value}',
            subscriptionId: subscriptionId,
            from: checkpoint,
          )
          .listen(events[channel]!.add),
    );
  }

  await waitForCondition(
    () =>
        events.values.every((value) => value.whereType<AckEvent>().isNotEmpty),
    timeout: const Duration(seconds: 20),
  );

  return (events: events, subscriptions: subscriptions);
}

Future<void> _insertRows(
  KalamClient client,
  Map<String, String> tables,
  Map<String, int> rowIds,
  String label,
) async {
  for (final entry in tables.entries) {
    final channel = entry.key;
    final rowId = rowIds[channel]!;
    final payload = '$label-$channel';
    await client.query(
      "INSERT INTO ${entry.value} (id, payload) VALUES ($rowId, '$payload')",
    );
  }
}

bool _allChannelsContainRows(
  Map<String, List<ChangeEvent>> events,
  Map<String, int> rowIds,
) {
  for (final entry in rowIds.entries) {
    if (!changeEventsContainRowId(events[entry.key]!, entry.value)) {
      return false;
    }
  }
  return true;
}

void main() {
  group('App lifecycle reconnect', skip: skipIfNoIntegration, () {
    test(
      'pause/resume cleanup keeps server live queries bounded',
      () async {
        final client = await _connectAppStyleClient();
        final writer = await connectJwtClient();
        final ns = uniqueName('dart_app_lifecycle');
        final userId = uniqueName('user');
        final tables = <String, String>{
          'messages': '$ns.messages',
          'notifications': '$ns.notifications',
          'user_updates': '$ns.user_updates',
        };
        final subscriptionIds = <String, String>{
          for (final channel in tables.keys) channel: 'masky-$channel-$userId',
        };
        final historicalIds = {
          for (final channel in tables.keys) channel: <int>[],
        };
        var activeSubscriptions = <StreamSubscription<ChangeEvent>>[];

        try {
          await ensureNamespace(writer, ns);
          for (final table in tables.values) {
            await writer.query(
              'CREATE TABLE IF NOT EXISTS $table ('
              'id INT PRIMARY KEY, '
              'payload TEXT'
              ')',
            );
          }

          await client.reconnectWebSocket();
          expect(await client.isConnected, isTrue,
              reason: 'client should connect before opening subscriptions');

          var checkpoints = <String, SeqId>{
            for (final subId in subscriptionIds.values)
              subId: const SeqId.zero(),
          };

          final initialOpen = await _openSubscriptions(
            client: client,
            tables: tables,
            subscriptionIds: subscriptionIds,
            checkpointById: checkpoints,
          );
          activeSubscriptions = initialOpen.subscriptions;

          await waitForLiveQueryCount(
            writer,
            subscriptionIds.values,
            expectedCount: 3,
          );

          final initialIds = <String, int>{
            'messages': 101,
            'notifications': 201,
            'user_updates': 301,
          };

          await _insertRows(writer, tables, initialIds, 'initial');
          await waitForCondition(
            () => _allChannelsContainRows(initialOpen.events, initialIds),
            timeout: const Duration(seconds: 20),
          );

          for (final entry in initialIds.entries) {
            historicalIds[entry.key]!.add(entry.value);
          }
          checkpoints =
              await _waitForCheckpoints(client, subscriptionIds.values);

          for (var cycle = 1; cycle <= 3; cycle++) {
            for (final subscription in activeSubscriptions) {
              await safeAwait(
                subscription.cancel(),
                timeout: const Duration(seconds: 3),
              );
            }
            activeSubscriptions = <StreamSubscription<ChangeEvent>>[];

            await waitForLiveQueryCount(
              writer,
              subscriptionIds.values,
              expectedCount: 0,
            );

            await client.disconnectWebSocket();
            expect(await client.isConnected, isFalse,
                reason: 'cycle $cycle should fully disconnect the socket');

            await waitForLiveQueryCount(
              writer,
              subscriptionIds.values,
              expectedCount: 0,
            );

            final gapIds = <String, int>{
              'messages': 1000 + (cycle * 10) + 1,
              'notifications': 2000 + (cycle * 10) + 1,
              'user_updates': 3000 + (cycle * 10) + 1,
            };
            final liveIds = <String, int>{
              'messages': 1000 + (cycle * 10) + 2,
              'notifications': 2000 + (cycle * 10) + 2,
              'user_updates': 3000 + (cycle * 10) + 2,
            };

            await _insertRows(writer, tables, gapIds, 'gap-$cycle');

            await client.reconnectWebSocket();
            expect(await client.isConnected, isTrue,
                reason: 'cycle $cycle should reconnect before resubscribe');

            final reopened = await _openSubscriptions(
              client: client,
              tables: tables,
              subscriptionIds: subscriptionIds,
              checkpointById: checkpoints,
            );
            activeSubscriptions = reopened.subscriptions;

            await waitForLiveQueryCount(
              writer,
              subscriptionIds.values,
              expectedCount: 3,
            );

            await _insertRows(writer, tables, liveIds, 'live-$cycle');
            await waitForCondition(
              () =>
                  _allChannelsContainRows(reopened.events, gapIds) &&
                  _allChannelsContainRows(reopened.events, liveIds),
              timeout: const Duration(seconds: 20),
            );

            for (final channel in tables.keys) {
              for (final oldId in historicalIds[channel]!) {
                expect(
                  changeEventsContainRowId(reopened.events[channel]!, oldId),
                  isFalse,
                  reason:
                      'cycle $cycle for $channel should not replay row $oldId',
                );
              }
            }

            for (final entry in gapIds.entries) {
              historicalIds[entry.key]!.add(entry.value);
            }
            for (final entry in liveIds.entries) {
              historicalIds[entry.key]!.add(entry.value);
            }

            checkpoints =
                await _waitForCheckpoints(client, subscriptionIds.values);
          }

          for (final subscription in activeSubscriptions) {
            await safeAwait(
              subscription.cancel(),
              timeout: const Duration(seconds: 3),
            );
          }
          activeSubscriptions = <StreamSubscription<ChangeEvent>>[];

          await waitForLiveQueryCount(
            writer,
            subscriptionIds.values,
            expectedCount: 0,
          );

          await client.disconnectWebSocket();
          await waitForLiveQueryCount(
            writer,
            subscriptionIds.values,
            expectedCount: 0,
          );
        } finally {
          for (final subscription in activeSubscriptions) {
            await safeAwait(
              subscription.cancel(),
              timeout: const Duration(seconds: 3),
            );
          }

          await safeAwait(client.disconnectWebSocket());
          await safeAwait(
            waitForLiveQueryCount(
              writer,
              subscriptionIds.values,
              expectedCount: 0,
              timeout: const Duration(seconds: 15),
            ),
            timeout: const Duration(seconds: 20),
          );

          for (final table in tables.values) {
            await dropTable(writer, table);
          }

          await client.dispose();
          await writer.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 240)),
    );
  });
}
