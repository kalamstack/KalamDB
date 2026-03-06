/// Shared test helpers for KalamDB Dart SDK e2e tests.
///
/// Requires a running KalamDB server.
/// Configure via env vars:
///   KALAMDB_URL or KALAM_URL               (default: http://localhost:8080)
///   KALAMDB_USER or KALAM_USER             (default: admin)
///   KALAMDB_PASSWORD or KALAM_PASS         (default: kalamdb123)
library;

import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

// ── Configuration ──────────────────────────────────────────────────────

final serverUrl = _env(['KALAMDB_URL', 'KALAM_URL'], 'http://localhost:8080');
final adminUser = _env(['KALAMDB_USER', 'KALAM_USER'], 'admin');
final adminPass = _env(['KALAMDB_PASSWORD', 'KALAM_PASS'], 'kalamdb123');

String _env(List<String> keys, String defaultValue) {
  for (final key in keys) {
    final value = Platform.environment[key];
    if (value != null && value.trim().isNotEmpty) {
      return value;
    }
  }
  return defaultValue;
}

// ── Integration gate ──────────────────────────────────────────────────

const integrationFlag = 'KALAM_INTEGRATION_TEST';

bool get integrationEnabled {
  final value = Platform.environment[integrationFlag]?.toLowerCase();
  return value == '1' || value == 'true' || value == 'yes';
}

dynamic get skipIfNoIntegration => integrationEnabled
    ? false
    : 'Set $integrationFlag=1 to run live-server integration tests.';

// ── SDK initialisation (run once) ─────────────────────────────────────

Future<void>? _nativeBridgeReady;
Future<void>? _sdkInitialized;

/// Ensure the native Rust bridge library is available and the Rust runtime
/// is initialised. Safe to call multiple times — initialises at most once.
Future<void> ensureSdkReady() async {
  _nativeBridgeReady ??= _ensureNativeBridgeReady();
  await _nativeBridgeReady;
  _sdkInitialized ??= KalamClient.init();
  await _sdkInitialized;
}

// ── Client factories ──────────────────────────────────────────────────

/// Creates a JWT-authenticated client by logging in with admin credentials.
Future<KalamClient> connectJwtClient() async {
  await ensureSdkReady();

  // First, log in anonymously to obtain a JWT.
  final anonClient = await KalamClient.connect(
    url: serverUrl,
    authProvider: () async => Auth.none(),
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

/// Creates a client using authProvider callback (recommended API).
Future<KalamClient> connectWithAuthProvider() async {
  await ensureSdkReady();

  String? cachedToken;

  return KalamClient.connect(
    url: serverUrl,
    authProvider: () async {
      if (cachedToken == null) {
        // Bootstrap: basic-auth login to get a JWT.
        final bootstrap = await KalamClient.connect(
          url: serverUrl,
          authProvider: () async => Auth.basic(adminUser, adminPass),
          timeout: const Duration(seconds: 10),
        );
        try {
          final loginResp = await bootstrap.login(adminUser, adminPass);
          cachedToken = loginResp.accessToken;
        } finally {
          await bootstrap.dispose();
        }
      }
      return Auth.jwt(cachedToken!);
    },
    timeout: const Duration(seconds: 10),
  );
}

// ── Utilities ─────────────────────────────────────────────────────────

/// Generate a unique name for test isolation (namespace or table).
String uniqueName(String prefix) {
  final ts = DateTime.now().millisecondsSinceEpoch;
  return '${prefix}_$ts';
}

/// Ensure a namespace exists.
Future<void> ensureNamespace(KalamClient client, String name) async {
  await client.query('CREATE NAMESPACE IF NOT EXISTS $name');
}

/// Drop a table, ignoring errors.
Future<void> dropTable(KalamClient client, String fullTable) async {
  try {
    await client.query('DROP TABLE IF EXISTS $fullTable');
  } catch (_) {
    // ignore
  }
}

/// Safely await a future with a timeout, ignoring errors.
Future<void> safeAwait(
  Future<void> future, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  try {
    await future.timeout(timeout);
  } catch (_) {}
}

/// Sleep for the given duration.
Future<void> sleep(Duration duration) => Future<void>.delayed(duration);

// ── Native bridge build ───────────────────────────────────────────────

Future<void> _ensureNativeBridgeReady() async {
  final buildBridgeFlag = 'KALAM_BUILD_DART_BRIDGE';
  final shouldBuild = integrationEnabled &&
      (_env([buildBridgeFlag], '1').toLowerCase() == '1' ||
          _env([buildBridgeFlag], '1').toLowerCase() == 'true');

  if (!shouldBuild) return;

  final sdkDir = Directory.current.path;
  final bridgeDir = Directory('$sdkDir/../../kalam-link-dart').absolute.path;
  final workspaceDir = Directory('$bridgeDir/../..').absolute.path;
  final libPath = _bridgeLibraryPath(bridgeDir);
  final workspaceLibPath = _bridgeLibraryPath(workspaceDir);

  if (_copyIfExists(workspaceLibPath, libPath)) return;
  if (File(libPath).existsSync()) return;

  final result = await Process.run(
    'cargo',
    ['build', '--release'],
    workingDirectory: bridgeDir,
  );

  if (result.exitCode != 0) {
    throw StateError(
      'Failed to build Dart bridge library.\n'
      'Command: cargo build --release (in $bridgeDir)\n'
      'stderr:\n${result.stderr}',
    );
  }

  _copyIfExists(workspaceLibPath, libPath);

  if (!File(libPath).existsSync()) {
    throw StateError(
      'Bridge build completed but library not found at $libPath',
    );
  }
}

bool _copyIfExists(String src, String dst) {
  final file = File(src);
  if (!file.existsSync()) return false;
  final dstFile = File(dst);
  if (!dstFile.existsSync()) {
    dstFile.parent.createSync(recursive: true);
    file.copySync(dst);
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
    'Unsupported platform: ${Platform.operatingSystem}',
  );
}
