import 'dart:convert';

import 'package:kalam_link/src/cell_value.dart';
import 'package:kalam_link/src/seq_id.dart';

/// Column metadata from a query result.
class SchemaField {
  final String name;
  final String dataType;
  final int index;

  /// Comma-separated flag short names, e.g. `"pk,nn,uq"`.
  /// `null` when no flags are present.
  final String? flags;

  const SchemaField({
    required this.name,
    required this.dataType,
    required this.index,
    this.flags,
  });

  /// Whether this field has the primary key flag.
  bool get isPrimaryKey => flags?.contains('pk') ?? false;

  /// Whether this field has the non-null flag.
  bool get isNonNull => flags?.contains('nn') ?? false;

  /// Whether this field has the unique flag.
  bool get isUnique => flags?.contains('uq') ?? false;
}

/// A single result set from a query execution.
class QueryResult {
  /// Column definitions.
  final List<SchemaField> columns;

  /// Named-row JSON objects computed by the Rust kalam-link layer.
  /// Each element is a JSON object string like `{"col":"value", ...}`.
  final List<String> _namedRowsJson;

  /// Number of rows affected or returned.
  final int rowCount;

  /// Optional message (e.g. for DDL statements).
  final String? message;

  QueryResult({
    required this.columns,
    required List<String> namedRowsJson,
    required this.rowCount,
    this.message,
  }) : _namedRowsJson = namedRowsJson;

  /// Rows as maps keyed by column name, with values wrapped as [KalamCellValue].
  ///
  /// This is the primary row access method. Each cell value is a
  /// [KalamCellValue] with type-safe accessors like `.asString()`,
  /// `.asInt()`, `.asFile()`, etc.
  ///
  /// ```dart
  /// final rows = result.rows;
  /// for (final row in rows) {
  ///   final name = row['name']?.asString();
  ///   final url  = row['avatar']?.asFileUrl(
  ///     'http://localhost:18080', 'default', 'users',
  ///   );
  /// }
  /// ```
  late final List<Map<String, KalamCellValue>> rows =
      _namedRowsJson.map((json) {
    final map = Map<String, dynamic>.from(jsonDecode(json) as Map);
    return map.map((k, v) => MapEntry(k, KalamCellValue.from(v)));
  }).toList(growable: false);
}

/// Response from a query execution.
class QueryResponse {
  /// Whether the query succeeded.
  final bool success;

  /// Result sets (one per statement).
  final List<QueryResult> results;

  /// Execution time in milliseconds.
  final double? tookMs;

  /// Error details if the query failed.
  final ErrorDetail? error;

  const QueryResponse({
    required this.success,
    required this.results,
    this.tookMs,
    this.error,
  });

  /// Convenience: rows from the first result set, or empty.
  List<Map<String, KalamCellValue>> get rows =>
      results.isNotEmpty ? results.first.rows : const [];

  /// Convenience: column metadata from the first result set, or empty.
  List<SchemaField> get columns =>
      results.isNotEmpty ? results.first.columns : const [];
}

/// Error details from the server.
class ErrorDetail {
  final String code;
  final String message;
  final String? details;

  const ErrorDetail({
    required this.code,
    required this.message,
    this.details,
  });

  @override
  String toString() => 'KalamDbError($code): $message';
}

/// Health check response from the server.
class HealthCheckResponse {
  final String status;
  final String version;
  final String apiVersion;
  final String? buildDate;

  const HealthCheckResponse({
    required this.status,
    required this.version,
    required this.apiVersion,
    this.buildDate,
  });
}

/// Login response containing tokens and user info.
class LoginResponse {
  final String accessToken;
  final String? refreshToken;
  final String expiresAt;
  final String? refreshExpiresAt;
  final LoginUserInfo user;

  const LoginResponse({
    required this.accessToken,
    this.refreshToken,
    required this.expiresAt,
    this.refreshExpiresAt,
    required this.user,
  });
}

/// User information returned after login.
class LoginUserInfo {
  final String id;
  final String username;
  final String role;
  final String? email;
  final String createdAt;
  final String updatedAt;

  const LoginUserInfo({
    required this.id,
    required this.username,
    required this.role,
    this.email,
    required this.createdAt,
    required this.updatedAt,
  });
}

// ---------------------------------------------------------------------------
// Connection lifecycle events / handlers
// ---------------------------------------------------------------------------

/// Reason why a WebSocket connection closed.
class DisconnectReason {
  final String message;
  final int? code;

  const DisconnectReason({required this.message, this.code});
}

/// Error details from a connection or protocol failure.
class ConnectionErrorInfo {
  final String message;
  final bool recoverable;

  const ConnectionErrorInfo({required this.message, required this.recoverable});
}

/// A connection lifecycle event emitted by the KalamDB client.
sealed class ConnectionEvent {
  const ConnectionEvent._();
}

/// Fired when WebSocket connection is established and authenticated.
final class ConnectEvent extends ConnectionEvent {
  const ConnectEvent() : super._();
}

/// Fired when WebSocket connection closes.
final class DisconnectEvent extends ConnectionEvent {
  final DisconnectReason reason;

  const DisconnectEvent({required this.reason}) : super._();
}

/// Fired when a connection/protocol error occurs.
final class ConnectionErrorEvent extends ConnectionEvent {
  final ConnectionErrorInfo error;

  const ConnectionErrorEvent({required this.error}) : super._();
}

/// Debug hook for raw inbound messages.
final class ReceiveEvent extends ConnectionEvent {
  final String message;

  const ReceiveEvent({required this.message}) : super._();
}

/// Debug hook for raw outbound messages.
final class SendEvent extends ConnectionEvent {
  final String message;

  const SendEvent({required this.message}) : super._();
}

/// Optional connection lifecycle callbacks.
class ConnectionHandlers {
  final void Function()? onConnect;
  final void Function(DisconnectReason reason)? onDisconnect;
  final void Function(ConnectionErrorInfo error)? onError;
  final void Function(String message)? onReceive;
  final void Function(String message)? onSend;
  final void Function(ConnectionEvent event)? onEvent;

  const ConnectionHandlers({
    this.onConnect,
    this.onDisconnect,
    this.onError,
    this.onReceive,
    this.onSend,
    this.onEvent,
  });

  bool get hasAny =>
      onConnect != null ||
      onDisconnect != null ||
      onError != null ||
      onReceive != null ||
      onSend != null ||
      onEvent != null;

  /// Dispatch one event to all matching handlers.
  void dispatch(ConnectionEvent event) {
    onEvent?.call(event);
    switch (event) {
      case ConnectEvent():
        onConnect?.call();
      case DisconnectEvent(:final reason):
        onDisconnect?.call(reason);
      case ConnectionErrorEvent(:final error):
        onError?.call(error);
      case ReceiveEvent(:final message):
        onReceive?.call(message);
      case SendEvent(:final message):
        onSend?.call(message);
    }
  }
}

// ---------------------------------------------------------------------------
// Subscription / Change events
// ---------------------------------------------------------------------------

/// Parse raw JSON row strings into named maps.
///
/// Shared by all [ChangeEvent] subclasses that carry row data, avoiding
/// duplicated parsing logic across [InitialDataBatch], [InsertEvent],
/// [UpdateEvent], and [DeleteEvent].
List<Map<String, dynamic>> _parseRowJsonList(List<String> rowsJson) {
  return rowsJson
      .map((json) => Map<String, dynamic>.from(jsonDecode(json) as Map))
      .toList(growable: false);
}

/// Wrap parsed row maps with [KalamCellValue] for type-safe cell access.
///
/// Shared conversion used by the `rows` / `row` getters on
/// [ChangeEvent] subclasses.
List<Map<String, KalamCellValue>> _wrapRowMaps(
    List<Map<String, dynamic>> rows) {
  return rows
      .map((row) => row.map((k, v) => MapEntry(k, KalamCellValue.from(v))))
      .toList(growable: false);
}

/// A change event from a live subscription.
sealed class ChangeEvent {
  const ChangeEvent._();
}

/// Subscription acknowledged — contains schema metadata.
final class AckEvent extends ChangeEvent {
  final String subscriptionId;
  final int totalRows;
  final List<SchemaField> schema;
  final int batchNum;
  final bool hasMore;
  final String status;

  const AckEvent({
    required this.subscriptionId,
    required this.totalRows,
    required this.schema,
    required this.batchNum,
    required this.hasMore,
    required this.status,
  }) : super._();
}

/// Batch of initial snapshot rows.
final class InitialDataBatch extends ChangeEvent {
  final String subscriptionId;

  /// Raw JSON-encoded row objects.
  final List<String> _rowsJson;
  final int batchNum;
  final bool hasMore;
  final String status;

  InitialDataBatch({
    required this.subscriptionId,
    required List<String> rowsJson,
    required this.batchNum,
    required this.hasMore,
    required this.status,
  })  : _rowsJson = rowsJson,
        super._();

  /// Rows with every cell wrapped as [KalamCellValue] for type-safe access.
  ///
  /// Use the same `row['colname']?.asString()` pattern as [QueryResult.toTypedMaps].
  ///
  /// ```dart
  /// for (final row in batch.rows) {
  ///   print(row['name']?.asString());
  ///   print(row['score']?.asInt());
  /// }
  /// ```
  late final List<Map<String, KalamCellValue>> rows =
      _wrapRowMaps(_parseRowJsonList(_rowsJson));
}

/// One or more rows were inserted.
final class InsertEvent extends ChangeEvent {
  final String subscriptionId;
  final List<String> _rowsJson;

  InsertEvent({
    required this.subscriptionId,
    required List<String> rowsJson,
  })  : _rowsJson = rowsJson,
        super._();

  /// Rows with every cell wrapped as [KalamCellValue] for type-safe access.
  late final List<Map<String, KalamCellValue>> rows =
      _wrapRowMaps(_parseRowJsonList(_rowsJson));

  /// Convenience: first inserted row with typed cell values.
  Map<String, KalamCellValue> get row => rows.first;
}

/// One or more rows were updated.
final class UpdateEvent extends ChangeEvent {
  final String subscriptionId;
  final List<String> _rowsJson;
  final List<String> _oldRowsJson;

  UpdateEvent({
    required this.subscriptionId,
    required List<String> rowsJson,
    required List<String> oldRowsJson,
  })  : _rowsJson = rowsJson,
        _oldRowsJson = oldRowsJson,
        super._();

  /// Rows with every cell wrapped as [KalamCellValue] for type-safe access.
  late final List<Map<String, KalamCellValue>> rows =
      _wrapRowMaps(_parseRowJsonList(_rowsJson));

  /// Previous row values with typed cell values.
  late final List<Map<String, KalamCellValue>> oldRows =
      _wrapRowMaps(_parseRowJsonList(_oldRowsJson));

  /// Convenience: first updated row with typed cell values.
  Map<String, KalamCellValue> get row => rows.first;

  /// Convenience: first old row with typed cell values, or `null`.
  Map<String, KalamCellValue>? get oldRow =>
      oldRows.isNotEmpty ? oldRows.first : null;
}

/// One or more rows were deleted.
final class DeleteEvent extends ChangeEvent {
  final String subscriptionId;
  final List<String> _oldRowsJson;

  DeleteEvent({
    required this.subscriptionId,
    required List<String> oldRowsJson,
  })  : _oldRowsJson = oldRowsJson,
        super._();

  /// Deleted rows with typed cell values.
  late final List<Map<String, KalamCellValue>> oldRows =
      _wrapRowMaps(_parseRowJsonList(_oldRowsJson));

  /// Convenience: first deleted row with typed cell values.
  Map<String, KalamCellValue> get row => oldRows.first;
}

/// Server-side error on this subscription.
final class SubscriptionError extends ChangeEvent {
  final String subscriptionId;
  final String code;
  final String message;

  const SubscriptionError({
    required this.subscriptionId,
    required this.code,
    required this.message,
  }) : super._();

  @override
  String toString() => 'SubscriptionError($code): $message';
}

// ---------------------------------------------------------------------------
// Subscription info (listing)
// ---------------------------------------------------------------------------

/// Read-only snapshot of an active subscription's metadata.
///
/// Returned by [KalamClient.getSubscriptions].
class SubscriptionInfo {
  /// Subscription ID assigned when subscribing.
  final String id;

  /// The SQL query this subscription is tracking.
  final String query;

  /// Last received sequence ID (for resume on reconnect), if any.
  final SeqId? lastSeqId;

  /// Timestamp (millis since epoch) of the last received event.
  final int? lastEventTimeMs;

  /// Timestamp (millis since epoch) when the subscription was created.
  final int createdAtMs;

  /// Whether the subscription has been closed.
  final bool closed;

  const SubscriptionInfo({
    required this.id,
    required this.query,
    this.lastSeqId,
    this.lastEventTimeMs,
    required this.createdAtMs,
    required this.closed,
  });

  @override
  String toString() =>
      'SubscriptionInfo(id: $id, query: $query, closed: $closed)';
}
