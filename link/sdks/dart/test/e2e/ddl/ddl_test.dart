/// DDL e2e tests — CREATE/DROP NAMESPACE, CREATE/DROP TABLE, schema inspection.
///
/// Mirrors: tests/e2e/ddl/ddl.test.mjs (TypeScript)
library;

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

void main() {
  group('DDL', skip: skipIfNoIntegration, () {
    late KalamClient client;
    late String ns;

    setUpAll(() async {
      client = await connectJwtClient();
      ns = uniqueName('dart_ddl');
      await ensureNamespace(client, ns);
    });

    tearDownAll(() async {
      await client.dispose();
    });

    // ─────────────────────────────────────────────────────────────────
    // CREATE TABLE
    // ─────────────────────────────────────────────────────────────────
    test(
      'CREATE TABLE creates a new table',
      () async {
        final tbl = '$ns.${uniqueName('tbl')}';
        final res = await client.query(
          'CREATE TABLE IF NOT EXISTS $tbl ('
          'id INT PRIMARY KEY, '
          'name TEXT'
          ')',
        );
        expect(res.success, isTrue);
        await dropTable(client, tbl);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // DROP TABLE
    // ─────────────────────────────────────────────────────────────────
    test(
      'DROP TABLE IF EXISTS succeeds for existing table',
      () async {
        final tbl = '$ns.${uniqueName('drop_me')}';
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tbl (id INT PRIMARY KEY)',
        );
        final res = await client.query('DROP TABLE IF EXISTS $tbl');
        expect(res.success, isTrue);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    test(
      'DROP TABLE IF EXISTS succeeds for nonexistent table',
      () async {
        final res = await client.query(
          'DROP TABLE IF EXISTS $ns.nonexistent_xyz_999',
        );
        expect(res.success, isTrue);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Table with various column types
    // ─────────────────────────────────────────────────────────────────
    test(
      'table with multiple column types',
      () async {
        final tbl = '$ns.${uniqueName('typed')}';
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tbl ('
          'id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(), '
          'name TEXT NOT NULL, '
          'active BOOLEAN DEFAULT true, '
          'score DOUBLE, '
          'count INT'
          ')',
        );

        // Insert and verify types survive roundtrip.
        await client.query(
          "INSERT INTO $tbl (name, active, score, count) "
          "VALUES ('alice', true, 98.5, 42)",
        );

        final sel = await client.query(
          "SELECT name, active, score, count FROM $tbl WHERE name = 'alice'",
        );
        final row = sel.rows.first;
        expect(row['name']?.asString(), 'alice');
        expect(row['active']?.asBool(), true);
        expect(row['score']?.asDouble(), 98.5);
        expect(row['count']?.asInt(), 42);

        await dropTable(client, tbl);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // CREATE NAMESPACE
    // ─────────────────────────────────────────────────────────────────
    test(
      'CREATE NAMESPACE IF NOT EXISTS is idempotent',
      () async {
        final name = uniqueName('ns_idem');
        await client.query('CREATE NAMESPACE IF NOT EXISTS $name');
        final res = await client.query('CREATE NAMESPACE IF NOT EXISTS $name');
        expect(res.success, isTrue);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Schema inspection
    // ─────────────────────────────────────────────────────────────────
    test(
      'query result columns contain schema metadata',
      () async {
        final tbl = '$ns.${uniqueName('schema_chk')}';
        await client.query(
          'CREATE TABLE IF NOT EXISTS $tbl ('
          'id INT PRIMARY KEY, '
          'name TEXT NOT NULL'
          ')',
        );
        await client.query(
          "INSERT INTO $tbl (id, name) VALUES (1, 'test')",
        );

        final sel = await client.query('SELECT id, name FROM $tbl');
        expect(sel.columns, isNotEmpty);
        expect(sel.columns.first.name, 'id');
        expect(sel.columns.first.dataType, isNotEmpty);

        final nameCol = sel.columns.firstWhere((c) => c.name == 'name');
        expect(nameCol.dataType, isNotEmpty);

        await dropTable(client, tbl);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );
  });
}
