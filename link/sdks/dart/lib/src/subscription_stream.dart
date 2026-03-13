import 'dart:async';

import 'logger.dart';

typedef BridgeSubscriptionPrepare = Future<void> Function();
typedef BridgeSubscriptionOpen<THandle> = Future<THandle> Function();
typedef BridgeSubscriptionNext<THandle, TBridgeEvent> = Future<TBridgeEvent?>
    Function(THandle handle);
typedef BridgeSubscriptionClose<THandle> = Future<void> Function(
  THandle handle,
);
typedef BridgeSubscriptionCancel = Future<void> Function();
typedef BridgeSubscriptionDecode<TBridgeEvent, TEvent> = TEvent Function(
  TBridgeEvent event,
);

/// Runs a Rust-backed pull subscription behind a Dart [Stream].
///
/// Rust owns reconnect and replay filtering; this helper only manages the
/// Dart-side stream lifecycle so all subscription APIs behave consistently.
Stream<TEvent> runBridgeSubscriptionStream<THandle, TBridgeEvent, TEvent>({
  required String description,
  required BridgeSubscriptionPrepare prepare,
  required BridgeSubscriptionOpen<THandle> open,
  required BridgeSubscriptionNext<THandle, TBridgeEvent> next,
  required BridgeSubscriptionClose<THandle> close,
  required BridgeSubscriptionCancel cancel,
  required BridgeSubscriptionDecode<TBridgeEvent, TEvent> decode,
  required bool Function() isDisposed,
  Duration cancelWaitTimeout = const Duration(seconds: 2),
}) {
  late StreamController<TEvent> controller;
  var closed = false;
  var closingFromPump = false;
  Future<void>? pump;

  Future<void> runLoop() async {
    THandle? handle;
    try {
      await prepare();
      handle = await open();
      final openedHandle = handle as THandle;

      while (!closed && !isDisposed()) {
        final event = await next(openedHandle);
        if (event == null || closed) {
          break;
        }
        controller.add(decode(event));
      }
    } catch (error, stackTrace) {
      if (!closed && !isDisposed()) {
        KalamLogger.error('subscription', '$description error: $error');
        controller.addError(error, stackTrace);
      }
    } finally {
      if (handle != null) {
        try {
          await close(handle);
        } catch (_) {}
      }
    }
  }

  controller = StreamController<TEvent>(
    onListen: () {
      pump = () async {
        KalamLogger.debug('subscription', 'Running $description');
        await runLoop();
        if (!closed && !isDisposed()) {
          closingFromPump = true;
          try {
            await controller.close();
          } finally {
            closingFromPump = false;
          }
        }
      }();
      unawaited(pump);
    },
    onCancel: () async {
      closed = true;
      if (closingFromPump) {
        return;
      }
      try {
        await cancel();
      } catch (_) {}

      final currentPump = pump;
      if (currentPump == null) {
        return;
      }

      try {
        await currentPump.timeout(cancelWaitTimeout);
      } on TimeoutException {
        KalamLogger.warn(
          'subscription',
          'Timed out waiting for $description to close after cancellation',
        );
      } catch (_) {}
    },
  );

  return controller.stream;
}
