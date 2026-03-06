import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

const _integrationFlag = 'KALAM_INTEGRATION_TEST';
const _buildBridgeFlag = 'KALAM_BUILD_DART_BRIDGE';

Future<void>? _nativeBridgeReady;
Future<void>? _sdkInitialized;

bool get _integrationEnabled {
  final value = Platform.environment[_integrationFlag]?.toLowerCase();
  return value == '1' || value == 'true' || value == 'yes';
}

String _requireEnv(String key, {required String defaultValue}) {
  return _requireEnvAny([key], defaultValue: defaultValue);
}

String _requireEnvAny(List<String> keys, {required String defaultValue}) {
  for (final key in keys) {
    final value = Platform.environment[key];
    if (value != null && value.trim().isNotEmpty) {
      return value;
    }
  }
  return defaultValue;
}

String _uniqueTableName(String prefix) {
  final ts = DateTime.now().millisecondsSinceEpoch;
  return '${prefix}_$ts';
}

Future<KalamClient> _connectJwtClient() async {
  _nativeBridgeReady ??= _ensureNativeBridgeReady();
  await _nativeBridgeReady;
  _sdkInitialized ??= KalamClient.init();
  await _sdkInitialized;

  final serverUrl = _requireEnvAny(['KALAMDB_URL', 'KALAM_URL'],
      defaultValue: 'http://localhost:8080');
  final adminUser =
      _requireEnvAny(['KALAMDB_USER', 'KALAM_USER'], defaultValue: 'admin');
  final adminPass = _requireEnvAny(['KALAMDB_PASSWORD', 'KALAM_PASS'],
      defaultValue: 'kalamdb123');

  final anonClient = await KalamClient.connect(
    url: serverUrl,
    timeout: const Duration(seconds: 10),
  );

  try {
    final login = await anonClient.login(adminUser, adminPass);
    return KalamClient.connect(
      url: serverUrl,
      authProvider: () async => Auth.jwt(login.accessToken),
      timeout: const Duration(seconds: 10),
      maxRetries: 2,
    );
  } finally {
    await anonClient.dispose();
  }
}

Future<void> _ensureNativeBridgeReady() async {
  final shouldBuild = _integrationEnabled &&
      ((_requireEnv(_buildBridgeFlag, defaultValue: '1').toLowerCase() ==
              '1') ||
          (_requireEnv(_buildBridgeFlag, defaultValue: '1').toLowerCase() ==
              'true'));

  if (!shouldBuild) {
    return;
  }

  final sdkDir = Directory.current.path;
  final bridgeDir = Directory('$sdkDir/../../kalam-link-dart').absolute.path;
  final workspaceDir = Directory('$bridgeDir/../..').absolute.path;
  final libPath = _bridgeLibraryPath(bridgeDir);
  final workspaceLibPath = _bridgeLibraryPath(workspaceDir);

  if (_copyIfWorkspaceLibraryExists(workspaceLibPath, libPath)) {
    return;
  }

  if (File(libPath).existsSync()) {
    return;
  }

  final result = await Process.run(
    'cargo',
    ['build', '--release'],
    workingDirectory: bridgeDir,
  );

  if (result.exitCode != 0) {
    throw StateError(
      'Failed to build Dart bridge library for integration tests.\n'
      'Command: cargo build --release (in $bridgeDir)\n'
      'stdout:\n${result.stdout}\n'
      'stderr:\n${result.stderr}',
    );
  }

  _copyIfWorkspaceLibraryExists(workspaceLibPath, libPath);

  if (!File(libPath).existsSync()) {
    throw StateError(
      'Bridge build completed but library not found.\n'
      'Expected: $libPath\n'
      'Also checked workspace target: $workspaceLibPath',
    );
  }
}

Future<void> _safeAwait(
  Future<void> future, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  try {
    await future.timeout(timeout);
  } catch (_) {}
}

bool _copyIfWorkspaceLibraryExists(
    String workspaceLibPath, String expectedLibPath) {
  final workspaceFile = File(workspaceLibPath);
  if (!workspaceFile.existsSync()) {
    return false;
  }

  final expectedFile = File(expectedLibPath);
  if (!expectedFile.existsSync()) {
    expectedFile.parent.createSync(recursive: true);
    workspaceFile.copySync(expectedLibPath);
  }
  return true;
}

String _bridgeLibraryPath(String bridgeDir) {
  if (Platform.isMacOS) {
    return '$bridgeDir/target/release/libkalam_link_dart.dylib';
  }
  if (Platform.isLinux) {
    return '$bridgeDir/target/release/libkalam_link_dart.so';
  }
  if (Platform.isWindows) {
    return '$bridgeDir/target/release/kalam_link_dart.dll';
  }
  throw UnsupportedError(
    'Unsupported platform for integration test native bridge: ${Platform.operatingSystem}',
  );
}

void main() {
  final skipReason = _integrationEnabled
      ? false
      : 'Set $_integrationFlag=1 to run live-server integration tests.';

  group('LiveServerIntegration', () {
    test(
      'query + insert roundtrip against running KalamDB server',
      () async {
        final client = await _connectJwtClient();
        final namespace = _uniqueTableName('sdk_dart_it_ns');
        final table = _uniqueTableName('sdk_dart_it_query');
        final fullTable = '$namespace.$table';

        try {
          final createNamespace = await client.query(
            'CREATE NAMESPACE IF NOT EXISTS $namespace',
          );
          expect(createNamespace.success, isTrue);

          final create = await client.query(
            'CREATE TABLE IF NOT EXISTS $fullTable (id INT PRIMARY KEY, title TEXT, done BOOLEAN)',
          );
          expect(create.success, isTrue);

          final insert = await client.query(
            r'INSERT INTO '
            '$fullTable'
            r' (id, title, done) VALUES ($1, $2, $3)',
            params: [101, 'integration-row', false],
          );
          expect(insert.success, isTrue);

          final select = await client.query(
            'SELECT id, title, done FROM $fullTable WHERE id = 101',
          );
          expect(select.success, isTrue);

          final rows = select.rows;
          expect(rows, isNotEmpty);
          expect(rows.first['id']?.asInt(), 101);
          expect(rows.first['title']?.asString(), 'integration-row');
        } finally {
          await client.query('DROP TABLE IF EXISTS $fullTable');
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 60)),
      skip: skipReason,
    );

    test(
      'subscription receives insert from concurrent writer client',
      () async {
        final subscriberClient = await _connectJwtClient();
        final writerClient = await _connectJwtClient();
        final namespace = _uniqueTableName('sdk_dart_it_ns');
        final table = _uniqueTableName('sdk_dart_it_sub');
        final fullTable = '$namespace.$table';

        StreamSubscription<ChangeEvent>? sub;
        final insertEvent = Completer<InsertEvent>();
        final seenEventTypes = <String>[];

        try {
          final createNamespace = await subscriberClient.query(
            'CREATE NAMESPACE IF NOT EXISTS $namespace',
          );
          expect(createNamespace.success, isTrue);

          final create = await subscriberClient.query(
            'CREATE TABLE IF NOT EXISTS $fullTable (id INT PRIMARY KEY, title TEXT)',
          );
          expect(create.success, isTrue);

          final stream = subscriberClient.subscribe(
            'SELECT id, title FROM $fullTable',
            batchSize: 100,
          );

          sub = stream.listen(
            (event) {
              seenEventTypes.add(event.runtimeType.toString());

              if (event case InsertEvent()) {
                for (final row in event.rows) {
                  if (row['id']?.asInt() == 202) {
                    if (!insertEvent.isCompleted) {
                      insertEvent.complete(event);
                    }
                    break;
                  }
                }
                return;
              }

              if (event case SubscriptionError()) {
                if (!insertEvent.isCompleted) {
                  insertEvent.completeError(
                    StateError(
                      'SubscriptionError(${event.code}): ${event.message}. '
                      'Events seen: ${seenEventTypes.join(', ')}',
                    ),
                  );
                }
              }
            },
            onError: (Object error, StackTrace stackTrace) {
              if (!insertEvent.isCompleted) {
                insertEvent.completeError(error, stackTrace);
              }
            },
          );

          await Future<void>.delayed(const Duration(milliseconds: 300));

          final insert = await writerClient.query(
            r'INSERT INTO '
            '$fullTable'
            r' (id, title) VALUES ($1, $2)',
            params: [202, 'from-writer'],
          );
          expect(
            insert.success,
            isTrue,
            reason:
                'Writer insert failed: ${insert.error?.message ?? 'unknown'}',
          );

          late InsertEvent received;
          try {
            received = await insertEvent.future.timeout(
              const Duration(seconds: 30),
            );
          } on TimeoutException {
            throw TimeoutException(
              'Timed out waiting for InsertEvent(id=202). '
              'Events seen: ${seenEventTypes.join(', ')}',
              const Duration(seconds: 30),
            );
          }

          expect(received.row['id']?.asInt(), 202);
          expect(received.row['title']?.asString(), 'from-writer');
        } finally {
          if (sub != null) {
            await _safeAwait(sub.cancel());
          }
          await _safeAwait(
            subscriberClient
                .query('DROP TABLE IF EXISTS $fullTable')
                .then((_) {}),
          );
          await _safeAwait(
            writerClient.query('DROP TABLE IF EXISTS $fullTable').then((_) {}),
          );
          await _safeAwait(writerClient.dispose());
          await _safeAwait(subscriberClient.dispose());
        }
      },
      timeout: const Timeout(Duration(seconds: 90)),
      skip: skipReason,
    );
  });
}
