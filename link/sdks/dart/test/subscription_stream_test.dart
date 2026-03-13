import 'dart:async';
import 'dart:collection';

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/src/subscription_stream.dart';

void main() {
  group('runBridgeSubscriptionStream', () {
    test('delivers decoded events and closes the handle when exhausted',
        () async {
      final source = Queue<String>.from(['alpha', 'beta']);
      final received = <String>[];
      var prepareCalls = 0;
      var openCalls = 0;
      var closeCalls = 0;
      final stream = runBridgeSubscriptionStream<Object, String, String>(
        description: 'unit-stream',
        prepare: () async {
          prepareCalls += 1;
        },
        open: () async {
          openCalls += 1;
          return Object();
        },
        next: (_) async => source.isEmpty ? null : source.removeFirst(),
        close: (_) async {
          closeCalls += 1;
        },
        cancel: () async {},
        decode: (value) => value.toUpperCase(),
        isDisposed: () => false,
      );

      await for (final event in stream) {
        received.add(event);
      }

      expect(received, ['ALPHA', 'BETA']);
      expect(prepareCalls, 1);
      expect(openCalls, 1);
      expect(closeCalls, 1);
    });

    test('cancel waits for the loop to unblock and close the handle', () async {
      final nextStarted = Completer<void>();
      final releaseNext = Completer<String?>();
      var cancelCalls = 0;
      var closeCalls = 0;

      final stream = runBridgeSubscriptionStream<Object, String, String>(
        description: 'cancel-stream',
        prepare: () async {},
        open: () async => Object(),
        next: (_) {
          nextStarted.complete();
          return releaseNext.future;
        },
        close: (_) async {
          closeCalls += 1;
        },
        cancel: () async {
          cancelCalls += 1;
          if (!releaseNext.isCompleted) {
            releaseNext.complete(null);
          }
        },
        decode: (value) => value,
        isDisposed: () => false,
      );

      final subscription = stream.listen((_) {});
      await nextStarted.future.timeout(const Duration(seconds: 2));

      await subscription.cancel().timeout(const Duration(seconds: 2));

      expect(cancelCalls, 1);
      expect(closeCalls, 1);
    });
  });
}
