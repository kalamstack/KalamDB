import 'package:kalam_link/src/file_ref.dart';

/// Type-safe wrapper for a single cell value in a KalamDB query result row.
///
/// Mirrors `KalamCellValue` in Rust (`kalam-link/src/models/cell_value.rs`).
///
/// Every cell in a [QueryResult] row (and subscription / consume events) is
/// wrapped in a [KalamCellValue] so you can safely extract typed data:
///
/// ```dart
/// final name   = row['name']?.asString();     // String?
/// final age    = row['age']?.asInt();          // int?
/// final score  = row['score']?.asDouble();     // double?
/// final active = row['active']?.asBool();      // bool?
/// final ref    = row['avatar']?.asFile();      // KalamFileRef?
/// final url    = row['avatar']?.asFileUrl(
///   'http://localhost:18080', 'default', 'users',
/// );
/// ```
///
/// Use [KalamCellValue.from] to wrap a raw dynamic value returned by JSON
/// deserialization.
class KalamCellValue {
  /// The underlying raw value (may be `null`, `String`, `num`, `bool`,
  /// `Map<String, dynamic>`, or `List<dynamic>`).
  final dynamic _raw;

  /// Create a [KalamCellValue] wrapping [raw].
  ///
  /// Prefer the [KalamCellValue.from] factory for convenience.
  const KalamCellValue(this._raw);

  // ------------------------------------------------------------------ //
  //  Factory                                                            //
  // ------------------------------------------------------------------ //

  /// Wrap a raw dynamic value as a [KalamCellValue].
  ///
  /// Pass `null` for SQL NULL cells.
  static KalamCellValue from(dynamic raw) => KalamCellValue(raw);

  // ------------------------------------------------------------------ //
  //  Raw access                                                         //
  // ------------------------------------------------------------------ //

  /// Return the underlying raw JSON value.
  ///
  /// Use this when you need to pass the value to code that expects plain
  /// dynamic / JSON data (e.g. [KalamFileRef.tryParse]).
  dynamic toJson() => _raw;

  /// Return the underlying raw JSON value.
  ///
  /// Alias of [toJson] for teams that prefer `asX()` style accessors.
  dynamic asJson() => _raw;

  // ------------------------------------------------------------------ //
  //  Type guards                                                        //
  // ------------------------------------------------------------------ //

  /// `true` if this cell is SQL NULL.
  bool get isNull => _raw == null;

  /// `true` if the underlying value is a [String].
  bool get isString => _raw is String;

  /// `true` if the underlying value is a [num] (int or double).
  bool get isNumber => _raw is num;

  /// `true` if the underlying value is a [bool].
  bool get isBool => _raw is bool;

  /// `true` if the underlying value is a `Map<dynamic, dynamic>`.
  bool get isObject => _raw is Map;

  /// `true` if the underlying value is a [List].
  bool get isArray => _raw is List;

  // ------------------------------------------------------------------ //
  //  Typed accessors                                                    //
  // ------------------------------------------------------------------ //

  /// Return the value as a [String], or `null` for SQL NULL.
  ///
  /// Handles common Rust-side string envelopes like `{"Utf8": "..."}`.
  /// Numbers and booleans are converted to their string representation.
  String? asString() {
    if (isNull) return null;

    // Envelope: { "Utf8": "..." } or { "String": "..." }
    if (_raw is Map) {
      final map = _raw;
      if (map['Utf8'] is String) return map['Utf8'] as String;
      if (map['String'] is String) return map['String'] as String;
      return _raw.toString();
    }

    if (_raw is String) return _raw;
    if (_raw is num) return _raw.toString();
    if (_raw is bool) return _raw ? 'true' : 'false';
    return _raw.toString();
  }

  /// Return the value as an [int], or `null` for SQL NULL / non-integer.
  ///
  /// Truncates floating-point values. String-encoded integers are parsed.
  int? asInt() {
    if (isNull) return null;
    if (_raw is int) return _raw;
    if (_raw is double) return (_raw).truncate();
    if (_raw is bool) return _raw ? 1 : 0;
    if (_raw is String) {
      return int.tryParse(_raw) ?? double.tryParse(_raw)?.truncate();
    }
    return null;
  }

  /// Return the value as a [double], or `null` for SQL NULL / non-numeric.
  ///
  /// String-encoded floats are parsed.
  double? asDouble() {
    if (isNull) return null;
    if (_raw is double) return _raw;
    if (_raw is int) return (_raw).toDouble();
    if (_raw is bool) return _raw ? 1.0 : 0.0;
    if (_raw is String) return double.tryParse(_raw);
    return null;
  }

  /// Return the value as a [bool], or `null` for SQL NULL / non-boolean.
  ///
  /// Handles string-encoded booleans: `"true"`, `"false"`, `"1"`, `"0"`.
  bool? asBool() {
    if (isNull) return null;
    if (_raw is bool) return _raw;
    if (_raw is num) return _raw != 0;
    if (_raw is String) {
      final lc = (_raw).toLowerCase().trim();
      if (lc == 'true' || lc == '1') return true;
      if (lc == 'false' || lc == '0') return false;
    }
    return null;
  }

  /// Return the value as a [DateTime], or `null` for SQL NULL / unparseable.
  ///
  /// Handles:
  /// - Unix milliseconds as [int]
  /// - Unix microseconds as [int] (Postgres TIMESTAMPTZ wire format)
  /// - ISO 8601 strings (`"2024-01-01T00:00:00Z"`)
  /// - Numeric timestamp strings (`"1704067200000"` ms or `"1704067200000000"` µs)
  DateTime? asDate() {
    if (isNull) return null;
    if (_raw is int) {
      return _fromNumericTimestamp(_raw);
    }
    if (_raw is double) {
      return _fromNumericTimestamp((_raw).toInt());
    }
    if (_raw is String) {
      final raw = _raw;
      // Numeric timestamp string
      final n = int.tryParse(raw) ?? double.tryParse(raw)?.toInt();
      if (n != null) {
        return _fromNumericTimestamp(n);
      }
      return DateTime.tryParse(raw);
    }
    return null;
  }

  /// Converts a numeric timestamp to [DateTime].
  ///
  /// KalamDB server sends Postgres TIMESTAMPTZ values as microseconds since
  /// the Unix epoch (16-digit numbers for current dates). Plain milliseconds
  /// (13-digit numbers) are also accepted for backwards compatibility.
  ///
  /// Threshold: values > 9_999_999_999_999 are microseconds; anything smaller
  /// is treated as milliseconds. Year 2001 in ms ≈ 1_000_000_000_000 (13
  /// digits); year 2001 in µs ≈ 1_000_000_000_000_000 (16 digits), so there
  /// is no ambiguity for any date between 1970 and year 2286.
  static DateTime _fromNumericTimestamp(int n) {
    const msThreshold = 9999999999999;
    if (n.abs() > msThreshold) {
      return DateTime.fromMicrosecondsSinceEpoch(n, isUtc: true);
    }
    return DateTime.fromMillisecondsSinceEpoch(n, isUtc: true);
  }

  /// Return the value as a `Map<String, dynamic>`, or `null` if not an object.
  Map<String, dynamic>? asObject() {
    if (_raw is Map<String, dynamic>) return _raw;
    if (_raw is Map) {
      try {
        return Map<String, dynamic>.from(_raw);
      } catch (_) {
        return null;
      }
    }
    return null;
  }

  /// Return the value as a `List<dynamic>`, or `null` if not an array.
  List<dynamic>? asArray() {
    if (_raw is List) return _raw;
    return null;
  }

  // ------------------------------------------------------------------ //
  //  FILE column support                                                //
  // ------------------------------------------------------------------ //

  /// Parse a FILE column value and return a [KalamFileRef], or `null`.
  ///
  /// The FILE column stores a serialised JSON object matching `FileRefData`.
  /// This accessor deserialises it into a class instance with helper methods.
  ///
  /// ```dart
  /// final fileRef = row['avatar']?.asFile();
  /// if (fileRef != null) {
  ///   final url = fileRef.getDownloadUrl('http://localhost:18080', 'default', 'users');
  ///   print('${fileRef.name} (${fileRef.formatSize()})');
  /// }
  /// ```
  KalamFileRef? asFile() {
    if (isNull) return null;
    return KalamFileRef.tryParse(_raw);
  }

  /// Convenience: parse a FILE column and return the download URL directly.
  ///
  /// Returns `null` if the cell is null, not a valid file reference, or no
  /// URL can be built.
  ///
  /// - [baseUrl]   Server base URL (e.g. `"http://localhost:18080"`)
  /// - [namespace] Namespace of the table (e.g. `"default"`)
  /// - [table]     Table name (e.g. `"users"`)
  ///
  /// ```dart
  /// final url = row['avatar']?.asFileUrl(
  ///   'http://localhost:18080', 'default', 'users',
  /// );
  /// image.src = url ?? '/placeholder.png';
  /// ```
  String? asFileUrl(String baseUrl, String namespace, String table) {
    return asFile()?.getDownloadUrl(baseUrl, namespace, table);
  }

  // ------------------------------------------------------------------ //
  //  Serialisation / display                                            //
  // ------------------------------------------------------------------ //

  /// Human-readable string for display purposes.
  ///
  /// - SQL NULL → `"NULL"`
  /// - [String]  → value as-is
  /// - everything else → `_raw.toString()`
  @override
  String toString() {
    if (isNull) return 'NULL';
    if (_raw is String) return _raw;
    return _raw.toString();
  }

  @override
  bool operator ==(Object other) {
    if (other is KalamCellValue) return _raw == other._raw;
    return false;
  }

  @override
  int get hashCode => _raw.hashCode;
}
