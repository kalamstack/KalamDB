/// Example showing how to use the KalamDB Dart SDK.
///
/// This demonstrates:
///   1. Initializing the Rust runtime
///   2. Connecting with authentication
///   3. Running SQL queries
///   4. Subscribing to live changes
///   5. Logging in and refreshing tokens
// ignore_for_file: avoid_print
library;

import 'dart:async';
import 'dart:io';

import 'package:kalam_link/kalam_link.dart';

Future<void> main() async {
  final serverUrl =
      Platform.environment['KALAM_URL'] ?? 'http://localhost:8080';
  final adminUser = Platform.environment['KALAM_USER'] ?? 'admin';
  final adminPass = Platform.environment['KALAM_PASS'] ?? 'kalamdb123';
  const namespace = 'sdk_examples';
  const fullTable = '$namespace.tasks';
  // -------------------------------------------------------------------------
  // 1. Initialize the Rust runtime (once, at app startup)
  // -------------------------------------------------------------------------
  await KalamClient.init();

  // -------------------------------------------------------------------------
  // 2. Connect (no auth — just to call login)
  // -------------------------------------------------------------------------
  final anonClient = await KalamClient.connect(
    url: serverUrl,
    timeout: const Duration(seconds: 10),
  );

  // -------------------------------------------------------------------------
  // 3. Login to get a JWT Bearer token
  // -------------------------------------------------------------------------
  final login = await anonClient.login(adminUser, adminPass);
  print('Logged in as ${login.user.username} (${login.user.role})');

  // -------------------------------------------------------------------------
  // 4. Connect authenticated client with JWT
  // -------------------------------------------------------------------------
  final client = await KalamClient.connect(
    url: serverUrl,
    authProvider: () async => Auth.jwt(login.accessToken),
    timeout: const Duration(seconds: 10),
  );

  // -------------------------------------------------------------------------
  // 5. Run queries (JWT client)
  // -------------------------------------------------------------------------
  await client.query('CREATE NAMESPACE IF NOT EXISTS $namespace');

  // Recreate the demo table so the example is safe to re-run.
  await client.query('DROP TABLE IF EXISTS $fullTable');
  await client.query(
      'CREATE TABLE $fullTable (id INT PRIMARY KEY, title TEXT, done BOOLEAN)');

  // Insert rows
  await client.query(
    'INSERT INTO $fullTable (id, title, done) VALUES (\$1, \$2, \$3)',
    params: [1, 'Buy groceries', false],
  );
  await client.query(
    'INSERT INTO $fullTable (id, title, done) VALUES (\$1, \$2, \$3)',
    params: [2, 'Write tests', true],
  );

  // Select all rows
  final result = await client.query('SELECT * FROM $fullTable ORDER BY id');
  if (result.success) {
    print('Columns: ${result.columns.map((c) => c.name).join(', ')}');
    for (final row in result.rows) {
      print('  task ${row['id']}: ${row['title']} (done=${row['done']})');
    }
  } else {
    print('Query failed: ${result.error}');
  }

  // -------------------------------------------------------------------------
  // 6. Refresh token demo
  // -------------------------------------------------------------------------
  if (login.refreshToken != null) {
    final refreshed = await anonClient.refreshToken(login.refreshToken!);
    print('Token refreshed — new expiry: ${refreshed.expiresAt}');
  }

  // -------------------------------------------------------------------------
  // 7. Live subscriptions (JWT client)
  // -------------------------------------------------------------------------
  final stream = client.subscribe(
    'SELECT * FROM $fullTable',
  );

  // Listen for 5 seconds, then cancel.
  final subscription = stream.listen((event) {
    switch (event) {
      case AckEvent(:final totalRows, :final schema):
        print('Subscription ack — $totalRows rows, '
            '${schema.length} columns');
      case InitialDataBatch(:final rows, :final hasMore):
        print('Initial batch: ${rows.length} rows (more=$hasMore)');
      case InsertEvent(:final row):
        print('Inserted: $row');
      case UpdateEvent(:final row, :final oldRow):
        print('Updated: $oldRow → $row');
      case DeleteEvent(:final row):
        print('Deleted: $row');
      case SubscriptionError(:final code, :final message):
        print('Subscription error [$code]: $message');
    }
  });

  // Let events flow for a few seconds, then clean up.
  await Future<void>.delayed(const Duration(seconds: 5));
  await subscription.cancel();

  // -------------------------------------------------------------------------
  // Cleanup
  // -------------------------------------------------------------------------
  await client.dispose();
  await anonClient.dispose();
  print('Done.');
}
