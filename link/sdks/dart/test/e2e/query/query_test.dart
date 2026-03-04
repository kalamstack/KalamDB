/// Query e2e tests — SELECT, INSERT, UPDATE, DELETE, params, errors.
///
/// Mirrors: tests/e2e/query/query.test.mjs (TypeScript)
library;

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

void main() {
  group('Query', skip: skipIfNoIntegration, () {
    late KalamClient client;
    late String ns;
    late String tbl;

    setUpAll(() async {
      client = await connectJwtClient();
      ns = uniqueName('dart_query');
      tbl = '$ns.items';
      await ensureNamespace(client, ns);
      await client.query(
        'CREATE TABLE IF NOT EXISTS $tbl ('
        'id INT PRIMARY KEY, '
        'title TEXT NOT NULL, '
        'done BOOLEAN, '
        'score DOUBLE'
        ')',
      );
    });

    tearDownAll(() async {
      await dropTable(client, tbl);
      await client.dispose();
    });

    // ─────────────────────────────────────────────────────────────────
    // Simple SELECT
    // ─────────────────────────────────────────────────────────────────
    test(
      'SELECT literal returns result',
      () async {
        final res = await client.query("SELECT 1 AS n, 'hello' AS s");
        expect(res.success, isTrue);
        expect(res.results, isNotEmpty);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // INSERT + SELECT roundtrip
    // ─────────────────────────────────────────────────────────────────
    test(
      'INSERT then SELECT returns inserted row',
      () async {
        final ins = await client.query(
          r'INSERT INTO '
          '$tbl'
          r" (id, title, done, score) VALUES (1, 'first', true, 9.5)",
        );
        expect(ins.success, isTrue);

        final sel = await client.query(
          'SELECT * FROM $tbl WHERE id = 1',
        );
        expect(sel.success, isTrue);

        final rows = sel.rows;
        expect(rows, isNotEmpty);
        expect(rows.first['id']?.asInt(), 1);
        expect(rows.first['title']?.asString(), 'first');
        expect(rows.first['done']?.asBool(), true);
        expect(rows.first['score']?.asDouble(), 9.5);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Parameterised query
    // ─────────────────────────────────────────────────────────────────
    test(
      r'parameterised INSERT + SELECT with $1 $2 $3',
      () async {
        await client.query(
          r'INSERT INTO '
          '$tbl'
          r' (id, title, done, score) VALUES ($1, $2, $3, $4)',
          params: [2, 'parameterised', false, 3.14],
        );

        final sel = await client.query(
          r'SELECT title, done FROM '
          '$tbl'
          r' WHERE id = $1',
          params: [2],
        );
        final rows = sel.rows;
        expect(rows.first['title']?.asString(), 'parameterised');
        expect(rows.first['done']?.asBool(), false);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // UPDATE
    // ─────────────────────────────────────────────────────────────────
    test(
      'UPDATE modifies existing row',
      () async {
        await client.query(
          "UPDATE $tbl SET title = 'updated' WHERE id = 1",
        );

        final sel = await client.query(
          'SELECT title FROM $tbl WHERE id = 1',
        );
        final rows = sel.rows;
        expect(rows.first['title']?.asString(), 'updated');
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // DELETE
    // ─────────────────────────────────────────────────────────────────
    test(
      'DELETE removes row',
      () async {
        await client.query(
          "INSERT INTO $tbl (id, title, done) VALUES (999, 'to-delete', false)",
        );
        await client.query('DELETE FROM $tbl WHERE id = 999');

        final sel = await client.query(
          'SELECT * FROM $tbl WHERE id = 999',
        );
        expect(sel.rows, isEmpty);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // CREATE NAMESPACE
    // ─────────────────────────────────────────────────────────────────
    test(
      'CREATE NAMESPACE IF NOT EXISTS succeeds',
      () async {
        final res = await client.query(
          'CREATE NAMESPACE IF NOT EXISTS ${uniqueName('ns_test')}',
        );
        expect(res.success, isTrue);
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Query error for nonexistent table
    // ─────────────────────────────────────────────────────────────────
    test(
      'SELECT from nonexistent table throws error',
      () async {
        expect(
          () => client.query(
            'SELECT * FROM nonexistent_ns_xyz.nonexistent_table',
          ),
          throwsA(anything),
        );
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // QueryResponse convenience methods
    // ─────────────────────────────────────────────────────────────────
    test(
      'rows returns list of KalamCellValue maps',
      () async {
        final res = await client.query('SELECT * FROM $tbl ORDER BY id');
        final rows = res.rows;
        expect(rows, isA<List<Map<String, KalamCellValue>>>());
        expect(rows.length, greaterThanOrEqualTo(1));
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    test(
      'rows returns KalamCellValue entries',
      () async {
        final res = await client.query('SELECT id, title FROM $tbl LIMIT 1');
        final rows = res.rows;
        expect(rows, isA<List<Map<String, KalamCellValue>>>());
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Multiple inserts
    // ─────────────────────────────────────────────────────────────────
    test(
      'multiple inserts in single call',
      () async {
        await client.query(
          "INSERT INTO $tbl (id, title, done) VALUES (10, 'a', true)",
        );
        await client.query(
          "INSERT INTO $tbl (id, title, done) VALUES (11, 'b', false)",
        );

        final sel = await client.query(
          'SELECT id FROM $tbl WHERE id IN (10, 11) ORDER BY id',
        );
        expect(sel.rows.length, greaterThanOrEqualTo(2));
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );
  });
}
