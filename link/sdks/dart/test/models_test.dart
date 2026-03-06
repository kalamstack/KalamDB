import 'dart:convert';
import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

void main() {
  // -----------------------------------------------------------------------
  // Auth
  // -----------------------------------------------------------------------
  group('Auth', () {
    test('basic auth stores credentials', () {
      final auth = Auth.basic('alice', 'secret');
      expect(auth, isA<BasicAuth>());
      final basic = auth as BasicAuth;
      expect(basic.username, 'alice');
      expect(basic.password, 'secret');
    });

    test('jwt auth stores token', () {
      final auth = Auth.jwt('eyJ...');
      expect(auth, isA<JwtAuth>());
      expect((auth as JwtAuth).token, 'eyJ...');
    });

    test('none auth', () {
      final auth = Auth.none();
      expect(auth, isA<NoAuth>());
    });
  });

  // -----------------------------------------------------------------------
  // SchemaField
  // -----------------------------------------------------------------------
  group('SchemaField', () {
    test('flag helpers with pk,nn,uq', () {
      const f =
          SchemaField(name: 'id', dataType: 'Int', index: 0, flags: 'pk,nn,uq');
      expect(f.isPrimaryKey, isTrue);
      expect(f.isNonNull, isTrue);
      expect(f.isUnique, isTrue);
    });

    test('flag helpers with pk only', () {
      const f = SchemaField(name: 'id', dataType: 'Int', index: 0, flags: 'pk');
      expect(f.isPrimaryKey, isTrue);
      expect(f.isNonNull, isFalse);
      expect(f.isUnique, isFalse);
    });

    test('flag helpers with null flags', () {
      const f = SchemaField(name: 'name', dataType: 'Text', index: 1);
      expect(f.isPrimaryKey, isFalse);
      expect(f.isNonNull, isFalse);
      expect(f.isUnique, isFalse);
    });
  });

  // -----------------------------------------------------------------------
  // QueryResult
  // -----------------------------------------------------------------------
  group('QueryResult', () {
    test('rows parses named JSON objects into KalamCellValue maps', () {
      final qr = QueryResult(
        columns: [
          const SchemaField(name: 'id', dataType: 'Int', index: 0),
          const SchemaField(name: 'name', dataType: 'Text', index: 1),
        ],
        namedRowsJson: [
          jsonEncode({'id': 1, 'name': 'Alice'}),
          jsonEncode({'id': 2, 'name': 'Bob'}),
        ],
        rowCount: 2,
      );
      expect(qr.rows.length, 2);
      expect(qr.rows[0]['id']?.asInt(), 1);
      expect(qr.rows[0]['name']?.asString(), 'Alice');
      expect(qr.rows[1]['id']?.asInt(), 2);
      expect(qr.rows[1]['name']?.asString(), 'Bob');
    });

    test('empty rows', () {
      final qr = QueryResult(
        columns: [],
        namedRowsJson: [],
        rowCount: 0,
        message: 'Table created',
      );
      expect(qr.rows, isEmpty);
      expect(qr.message, 'Table created');
    });

    test('handles null cell values', () {
      final qr = QueryResult(
        columns: [
          const SchemaField(name: 'val', dataType: 'Text', index: 0),
        ],
        namedRowsJson: [
          jsonEncode({'val': null}),
        ],
        rowCount: 1,
      );
      expect(qr.rows[0]['val']?.isNull, true);
    });
  });

  // -----------------------------------------------------------------------
  // QueryResponse convenience accessors
  // -----------------------------------------------------------------------
  group('QueryResponse', () {
    test('convenience getters delegate to first result', () {
      final resp = QueryResponse(
        success: true,
        results: [
          QueryResult(
            columns: [
              const SchemaField(name: 'x', dataType: 'Int', index: 0),
            ],
            namedRowsJson: [
              jsonEncode({'x': 7})
            ],
            rowCount: 1,
          ),
        ],
        tookMs: 3.14,
      );
      expect(resp.success, isTrue);
      expect(resp.columns.length, 1);
      expect(resp.rows.length, 1);
      expect(resp.rows[0]['x']?.asInt(), 7);
      expect(resp.tookMs, closeTo(3.14, 0.001));
    });

    test('convenience getters return empty for no results', () {
      const resp = QueryResponse(success: false, results: []);
      expect(resp.rows, isEmpty);
      expect(resp.columns, isEmpty);
    });
  });

  // -----------------------------------------------------------------------
  // ErrorDetail
  // -----------------------------------------------------------------------
  group('ErrorDetail', () {
    test('toString includes code and message', () {
      const e = ErrorDetail(code: 'not_found', message: 'Table missing');
      expect(e.toString(), 'KalamDbError(not_found): Table missing');
    });

    test('details is optional', () {
      const e = ErrorDetail(code: 'err', message: 'msg', details: 'extra');
      expect(e.details, 'extra');
    });
  });

  // -----------------------------------------------------------------------
  // HealthCheckResponse
  // -----------------------------------------------------------------------
  group('HealthCheckResponse', () {
    test('stores all fields', () {
      const h = HealthCheckResponse(
        status: 'healthy',
        version: '0.5.0',
        apiVersion: 'v1',
        buildDate: '2026-02-25',
      );
      expect(h.status, 'healthy');
      expect(h.version, '0.5.0');
      expect(h.apiVersion, 'v1');
      expect(h.buildDate, '2026-02-25');
    });

    test('buildDate is optional', () {
      const h = HealthCheckResponse(
        status: 'ok',
        version: '1.0.0',
        apiVersion: 'v2',
      );
      expect(h.buildDate, isNull);
    });
  });

  // -----------------------------------------------------------------------
  // Login models
  // -----------------------------------------------------------------------
  group('LoginResponse', () {
    test('stores tokens and user info', () {
      const resp = LoginResponse(
        accessToken: 'tok',
        refreshToken: 'ref',
        expiresAt: '2026-12-31',
        refreshExpiresAt: '2027-01-31',
        user: LoginUserInfo(
          id: 'u1',
          username: 'alice',
          role: 'dba',
          email: 'alice@example.com',
          createdAt: '2026-01-01',
          updatedAt: '2026-02-01',
        ),
      );
      expect(resp.accessToken, 'tok');
      expect(resp.refreshToken, 'ref');
      expect(resp.user.username, 'alice');
      expect(resp.user.email, 'alice@example.com');
      expect(resp.user.role, 'dba');
    });
  });

  // -----------------------------------------------------------------------
  // ChangeEvent subclasses
  // -----------------------------------------------------------------------
  group('ChangeEvent', () {
    test('AckEvent stores schema and batch info', () {
      const ack = AckEvent(
        subscriptionId: 'sub-1',
        totalRows: 100,
        schema: [
          SchemaField(name: 'id', dataType: 'Int', index: 0, flags: 'pk')
        ],
        batchNum: 0,
        hasMore: true,
        status: 'loading',
      );
      expect(ack.subscriptionId, 'sub-1');
      expect(ack.totalRows, 100);
      expect(ack.schema.length, 1);
      expect(ack.schema[0].isPrimaryKey, isTrue);
      expect(ack.hasMore, isTrue);
      expect(ack.status, 'loading');
    });

    test('InitialDataBatch lazily parses rows', () {
      final batch = InitialDataBatch(
        subscriptionId: 'sub-2',
        rowsJson: [
          jsonEncode({'id': 1, 'name': 'Alice'}),
          jsonEncode({'id': 2, 'name': 'Bob'}),
        ],
        batchNum: 1,
        hasMore: false,
        status: 'ready',
      );
      expect(batch.rows.length, 2);
      expect(batch.rows[0]['name']?.asString(), 'Alice');
      expect(batch.rows[1]['id']?.asInt(), 2);
    });

    test('InsertEvent row convenience getter', () {
      final insert = InsertEvent(
        subscriptionId: 'sub-3',
        rowsJson: [
          jsonEncode({'id': 42, 'title': 'Hello'})
        ],
      );
      expect(insert.row['id']?.asInt(), 42);
      expect(insert.row['title']?.asString(), 'Hello');
    });

    test('InsertEvent row returns KalamCellValue', () {
      final insert = InsertEvent(
        subscriptionId: 'sub-3',
        rowsJson: [
          jsonEncode({'id': 42, 'title': 'Hello', 'active': true})
        ],
      );
      expect(insert.row['id']?.asInt(), 42);
      expect(insert.row['title']?.asString(), 'Hello');
      expect(insert.row['active']?.asBool(), true);
    });

    test('InsertEvent rows returns all rows typed', () {
      final insert = InsertEvent(
        subscriptionId: 'sub-3',
        rowsJson: [
          jsonEncode({'id': 1, 'name': 'Alice'}),
          jsonEncode({'id': 2, 'name': 'Bob'}),
        ],
      );
      expect(insert.rows.length, 2);
      expect(insert.rows[0]['name']?.asString(), 'Alice');
      expect(insert.rows[1]['name']?.asString(), 'Bob');
    });

    test('UpdateEvent has new and old rows', () {
      final update = UpdateEvent(
        subscriptionId: 'sub-4',
        rowsJson: [
          jsonEncode({'id': 1, 'name': 'Bob2'})
        ],
        oldRowsJson: [
          jsonEncode({'id': 1, 'name': 'Bob'})
        ],
      );
      expect(update.row['name']?.asString(), 'Bob2');
      expect(update.oldRow?['name']?.asString(), 'Bob');
    });

    test('UpdateEvent row and oldRow return KalamCellValue', () {
      final update = UpdateEvent(
        subscriptionId: 'sub-4',
        rowsJson: [
          jsonEncode({'id': 1, 'name': 'Bob2', 'score': 95.5})
        ],
        oldRowsJson: [
          jsonEncode({'id': 1, 'name': 'Bob', 'score': 80.0})
        ],
      );
      expect(update.row['name']?.asString(), 'Bob2');
      expect(update.row['score']?.asDouble(), 95.5);
      expect(update.oldRow?['name']?.asString(), 'Bob');
      expect(update.oldRow?['score']?.asDouble(), 80.0);
    });

    test('DeleteEvent exposes deleted row', () {
      final del = DeleteEvent(
        subscriptionId: 'sub-5',
        oldRowsJson: [
          jsonEncode({'id': 99})
        ],
      );
      expect(del.row['id']?.asInt(), 99);
      expect(del.oldRows.length, 1);
    });

    test('DeleteEvent row and oldRows return KalamCellValue', () {
      final del = DeleteEvent(
        subscriptionId: 'sub-5',
        oldRowsJson: [
          jsonEncode({'id': 99, 'name': 'Charlie'})
        ],
      );
      expect(del.row['id']?.asInt(), 99);
      expect(del.row['name']?.asString(), 'Charlie');
      expect(del.oldRows.length, 1);
    });

    test('InitialDataBatch rows returns KalamCellValue maps', () {
      final batch = InitialDataBatch(
        subscriptionId: 'sub-7',
        rowsJson: [
          jsonEncode({'id': 1, 'name': 'Alice', 'active': true}),
          jsonEncode({'id': 2, 'name': 'Bob', 'active': false}),
        ],
        batchNum: 0,
        hasMore: false,
        status: 'ready',
      );
      expect(batch.rows.length, 2);
      expect(batch.rows[0]['name']?.asString(), 'Alice');
      expect(batch.rows[0]['active']?.asBool(), true);
      expect(batch.rows[1]['id']?.asInt(), 2);
      expect(batch.rows[1]['active']?.asBool(), false);
    });

    test('SubscriptionError toString', () {
      const err = SubscriptionError(
        subscriptionId: 'sub-6',
        code: 'auth_fail',
        message: 'Token expired',
      );
      expect(err.toString(), 'SubscriptionError(auth_fail): Token expired');
    });

    test('sealed class exhaustive switch', () {
      ChangeEvent event = const AckEvent(
        subscriptionId: 's',
        totalRows: 0,
        schema: [],
        batchNum: 0,
        hasMore: false,
        status: 'ready',
      );
      // Exhaustive switch — compile-time guarantee all variants handled.
      final label = switch (event) {
        AckEvent() => 'ack',
        InitialDataBatch() => 'batch',
        InsertEvent() => 'insert',
        UpdateEvent() => 'update',
        DeleteEvent() => 'delete',
        SubscriptionError() => 'error',
      };
      expect(label, 'ack');
    });
  });

  // -----------------------------------------------------------------------
  // Connection lifecycle events / handlers
  // -----------------------------------------------------------------------
  group('ConnectionHandlers', () {
    test('hasAny is false when no callbacks are set', () {
      const handlers = ConnectionHandlers();
      expect(handlers.hasAny, isFalse);
    });

    test('hasAny is true when one callback is set', () {
      final handlers = ConnectionHandlers(onConnect: () {});
      expect(handlers.hasAny, isTrue);
    });

    test('dispatch routes each event to matching handler and onEvent', () {
      var connectCount = 0;
      DisconnectReason? disconnectReason;
      ConnectionErrorInfo? errorInfo;
      String? receivedMessage;
      String? sentMessage;
      final seenEvents = <ConnectionEvent>[];

      final handlers = ConnectionHandlers(
        onConnect: () => connectCount++,
        onDisconnect: (reason) => disconnectReason = reason,
        onError: (error) => errorInfo = error,
        onReceive: (message) => receivedMessage = message,
        onSend: (message) => sentMessage = message,
        onEvent: seenEvents.add,
      );

      handlers.dispatch(const ConnectEvent());
      handlers.dispatch(
        const DisconnectEvent(
          reason: DisconnectReason(message: 'closed', code: 1000),
        ),
      );
      handlers.dispatch(
        const ConnectionErrorEvent(
          error: ConnectionErrorInfo(message: 'timeout', recoverable: true),
        ),
      );
      handlers.dispatch(const ReceiveEvent(message: '{"type":"ack"}'));
      handlers.dispatch(const SendEvent(message: '{"type":"ping"}'));

      expect(connectCount, 1);
      expect(disconnectReason?.message, 'closed');
      expect(disconnectReason?.code, 1000);
      expect(errorInfo?.message, 'timeout');
      expect(errorInfo?.recoverable, isTrue);
      expect(receivedMessage, '{"type":"ack"}');
      expect(sentMessage, '{"type":"ping"}');
      expect(seenEvents.length, 5);
      expect(seenEvents[0], isA<ConnectEvent>());
      expect(seenEvents[1], isA<DisconnectEvent>());
      expect(seenEvents[2], isA<ConnectionErrorEvent>());
      expect(seenEvents[3], isA<ReceiveEvent>());
      expect(seenEvents[4], isA<SendEvent>());
    });

    test('sealed ConnectionEvent supports exhaustive switch', () {
      const ConnectionEvent event = SendEvent(message: 'hello');

      final label = switch (event) {
        ConnectEvent() => 'connect',
        DisconnectEvent() => 'disconnect',
        ConnectionErrorEvent() => 'error',
        ReceiveEvent() => 'receive',
        SendEvent() => 'send',
      };

      expect(label, 'send');
    });
  });

  // -----------------------------------------------------------------------
  // SubscriptionInfo
  // -----------------------------------------------------------------------

  // -----------------------------------------------------------------------
  // KalamCellValue
  // -----------------------------------------------------------------------
  group('KalamCellValue', () {
    test('from wraps values and isNull works', () {
      expect(KalamCellValue.from(null).isNull, true);
      expect(KalamCellValue.from('hello').isNull, false);
    });

    test('asString returns string for various types', () {
      expect(KalamCellValue.from('hello').asString(), 'hello');
      expect(KalamCellValue.from(42).asString(), '42');
      expect(KalamCellValue.from(true).asString(), 'true');
      expect(KalamCellValue.from(false).asString(), 'false');
      expect(KalamCellValue.from(null).asString(), isNull);
    });

    test('asString handles Utf8 envelope', () {
      expect(KalamCellValue.from({'Utf8': 'wrapped'}).asString(), 'wrapped');
      expect(KalamCellValue.from({'String': 'wrapped'}).asString(), 'wrapped');
    });

    test('asInt returns integer or null', () {
      expect(KalamCellValue.from(42).asInt(), 42);
      expect(KalamCellValue.from(3.9).asInt(), 3);
      expect(KalamCellValue.from('99').asInt(), 99);
      expect(KalamCellValue.from(true).asInt(), 1);
      expect(KalamCellValue.from(false).asInt(), 0);
      expect(KalamCellValue.from('abc').asInt(), isNull);
      expect(KalamCellValue.from(null).asInt(), isNull);
    });

    test('asDouble returns double or null', () {
      expect(KalamCellValue.from(3.14).asDouble(), 3.14);
      expect(KalamCellValue.from(42).asDouble(), 42.0);
      expect(KalamCellValue.from('3.14').asDouble(), 3.14);
      expect(KalamCellValue.from(true).asDouble(), 1.0);
      expect(KalamCellValue.from('abc').asDouble(), isNull);
    });

    test('asBool returns boolean or null', () {
      expect(KalamCellValue.from(true).asBool(), true);
      expect(KalamCellValue.from(false).asBool(), false);
      expect(KalamCellValue.from(1).asBool(), true);
      expect(KalamCellValue.from(0).asBool(), false);
      expect(KalamCellValue.from('true').asBool(), true);
      expect(KalamCellValue.from('false').asBool(), false);
      expect(KalamCellValue.from('1').asBool(), true);
      expect(KalamCellValue.from('0').asBool(), false);
      expect(KalamCellValue.from('maybe').asBool(), isNull);
    });

    test('asDate parses timestamps and ISO strings', () {
      final d1 = KalamCellValue.from(1704067200000).asDate();
      expect(d1, isNotNull);
      expect(d1!.millisecondsSinceEpoch, 1704067200000);

      final d2 = KalamCellValue.from('2024-01-01T00:00:00Z').asDate();
      expect(d2, isNotNull);

      expect(KalamCellValue.from('not-a-date').asDate(), isNull);
      expect(KalamCellValue.from(null).asDate(), isNull);
    });

    test('asObject and asArray', () {
      expect(KalamCellValue.from({'key': 'val'}).asObject(), {'key': 'val'});
      expect(KalamCellValue.from([1, 2, 3]).asArray(), [1, 2, 3]);
      expect(KalamCellValue.from('str').asObject(), isNull);
      expect(KalamCellValue.from('str').asArray(), isNull);
    });

    test('toString formats correctly', () {
      expect(KalamCellValue.from(null).toString(), 'NULL');
      expect(KalamCellValue.from('hello').toString(), 'hello');
      expect(KalamCellValue.from(42).toString(), '42');
    });

    test('equality works', () {
      expect(KalamCellValue.from(42) == KalamCellValue.from(42), isTrue);
      expect(KalamCellValue.from('a') == KalamCellValue.from('b'), isFalse);
    });
  });

  // -----------------------------------------------------------------------
  // QueryResult rows (KalamCellValue maps)
  // -----------------------------------------------------------------------
  group('QueryResult rows accessors', () {
    test('rows wraps cells as KalamCellValue', () {
      final qr = QueryResult(
        columns: [
          const SchemaField(name: 'id', dataType: 'Int', index: 0),
          const SchemaField(name: 'name', dataType: 'Text', index: 1),
        ],
        namedRowsJson: [
          jsonEncode({'id': 42, 'name': 'Bob'})
        ],
        rowCount: 1,
      );
      expect(qr.rows.length, 1);
      expect(qr.rows[0]['id']?.asInt(), 42);
      expect(qr.rows[0]['name']?.asString(), 'Bob');
    });

    test('rows handles null cells', () {
      final qr = QueryResult(
        columns: [
          const SchemaField(name: 'val', dataType: 'Text', index: 0),
        ],
        namedRowsJson: [
          jsonEncode({'val': null})
        ],
        rowCount: 1,
      );
      expect(qr.rows[0]['val']?.isNull, true);
    });

    test('multiple rows parsed correctly', () {
      final qr = QueryResult(
        columns: [
          const SchemaField(name: 'x', dataType: 'Int', index: 0),
          const SchemaField(name: 'y', dataType: 'Text', index: 1),
        ],
        namedRowsJson: [
          jsonEncode({'x': 1, 'y': 'a'}),
          jsonEncode({'x': 2, 'y': 'b'}),
        ],
        rowCount: 2,
      );
      expect(qr.rows.length, 2);
      expect(qr.rows[0]['x']?.asInt(), 1);
      expect(qr.rows[1]['y']?.asString(), 'b');
    });
  });

  group('SubscriptionInfo', () {
    test('all fields populated', () {
      final info = SubscriptionInfo(
        id: 'sub-1',
        query: 'SELECT * FROM users',
        lastSeqId: SeqId(42),
        lastEventTimeMs: 1700000000000,
        createdAtMs: 1700000000000,
        closed: false,
      );
      expect(info.id, 'sub-1');
      expect(info.query, 'SELECT * FROM users');
      expect(info.lastSeqId, SeqId(42));
      expect(info.lastEventTimeMs, 1700000000000);
      expect(info.createdAtMs, 1700000000000);
      expect(info.closed, isFalse);
    });

    test('optional fields default to null', () {
      const info = SubscriptionInfo(
        id: 'sub-2',
        query: 'SELECT 1',
        createdAtMs: 1700000000000,
        closed: true,
      );
      expect(info.lastSeqId, isNull);
      expect(info.lastEventTimeMs, isNull);
      expect(info.closed, isTrue);
    });

    test('toString includes key fields', () {
      const info = SubscriptionInfo(
        id: 'sub-3',
        query: 'SELECT * FROM t',
        createdAtMs: 1700000000000,
        closed: false,
      );
      final str = info.toString();
      expect(str, contains('sub-3'));
      expect(str, contains('SELECT * FROM t'));
      expect(str, contains('closed: false'));
    });
  });

  // -----------------------------------------------------------------------
  // SeqId
  // -----------------------------------------------------------------------
  group('SeqId', () {
    test('creates from int', () {
      final seq = SeqId(123456789);
      expect(seq.value, 123456789);
      expect(seq.toInt(), 123456789);
    });

    test('parse from string', () {
      final seq = SeqId.parse('123456789');
      expect(seq.value, 123456789);
    });

    test('parse from BigInt', () {
      final seq = SeqId.parse(BigInt.from(42));
      expect(seq.value, 42);
    });

    test('tryParse returns null for invalid', () {
      expect(SeqId.tryParse(null), isNull);
      expect(SeqId.tryParse('abc'), isNull);
    });

    test('zero constructor', () {
      const seq = SeqId.zero();
      expect(seq.value, 0);
    });

    test('Snowflake field extraction', () {
      final timestampOffset = 5000;
      final workerId = 7;
      final sequence = 21;
      final id = (timestampOffset << 22) | (workerId << 12) | sequence;
      final seq = SeqId(id);

      expect(seq.timestampMillis, SeqId.epoch + timestampOffset);
      expect(seq.workerId, workerId);
      expect(seq.sequence, sequence);
    });

    test('toDateTime returns UTC DateTime', () {
      final seq = SeqId(1000 << 22);
      final dt = seq.toDateTime();
      expect(dt.isUtc, isTrue);
      expect(dt.millisecondsSinceEpoch, SeqId.epoch + 1000);
    });

    test('equality', () {
      expect(SeqId(42), equals(SeqId(42)));
      expect(SeqId(42) == SeqId(43), isFalse);
    });

    test('compareTo', () {
      expect(SeqId(10).compareTo(SeqId(20)), lessThan(0));
      expect(SeqId(20).compareTo(SeqId(10)), greaterThan(0));
      expect(SeqId(10).compareTo(SeqId(10)), 0);
    });

    test('comparison operators', () {
      expect(SeqId(10) < SeqId(20), isTrue);
      expect(SeqId(20) > SeqId(10), isTrue);
      expect(SeqId(10) <= SeqId(10), isTrue);
      expect(SeqId(10) >= SeqId(10), isTrue);
    });

    test('toString returns decimal', () {
      expect(SeqId(42).toString(), '42');
    });

    test('toBigInt', () {
      expect(SeqId(42).toBigInt(), BigInt.from(42));
    });
  });

  // -----------------------------------------------------------------------
  // KalamCellValue.asSeqId()
  // -----------------------------------------------------------------------
  group('KalamCellValue.asSeqId()', () {
    test('converts int to SeqId', () {
      final seq = KalamCellValue.from(42).asSeqId();
      expect(seq, isNotNull);
      expect(seq!.value, 42);
    });

    test('converts string to SeqId', () {
      final seq = KalamCellValue.from('99').asSeqId();
      expect(seq, isNotNull);
      expect(seq!.value, 99);
    });

    test('returns null for non-numeric string', () {
      expect(KalamCellValue.from('abc').asSeqId(), isNull);
    });

    test('returns null for null', () {
      expect(KalamCellValue.from(null).asSeqId(), isNull);
    });
  });

  // -----------------------------------------------------------------------
  // KalamClient.connect — wsLazyConnect parameter
  // -----------------------------------------------------------------------
  group('wsLazyConnect parameter', () {
    // These are compile-time / API-shape tests only. They verify that the
    // named parameter exists, has the correct default, and is accepted by
    // the `connect` factory.  Actual connection behaviour requires a running
    // server and is covered by e2e tests.

    test('wsLazyConnect defaults to true in call signature', () async {
      await expectLater(
        KalamClient.connect(
          url: 'http://127.0.0.1:1',
          authProvider: () async => Auth.none(),
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('flutter_rust_bridge has not been initialized'),
          ),
        ),
      );
    });

    test('wsLazyConnect false is accepted', () async {
      await expectLater(
        KalamClient.connect(
          url: 'http://127.0.0.1:1',
          authProvider: () async => Auth.none(),
          wsLazyConnect: false,
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('flutter_rust_bridge has not been initialized'),
          ),
        ),
      );
    });
  });
}
