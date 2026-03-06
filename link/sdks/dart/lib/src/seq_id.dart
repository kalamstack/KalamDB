/// Lightweight Snowflake-based sequence identifier for MVCC versioning.
///
/// Mirrors the Rust `SeqId` type from `kalam-link/src/seq_id.rs`.
///
/// Layout: `timestamp (42 bits) | worker (10 bits) | sequence (12 bits)`
///
/// Internally stores a 64-bit [int] (matching the Rust `i64` returned by the
/// FFI layer). Use [toBigInt] when you need a [BigInt] representation.
///
/// ```dart
/// final seq = SeqId(123456789);
/// print(seq.timestampMillis);  // millis since Unix epoch
/// print(seq.workerId);         // 0-1023
/// print(seq.sequence);         // 0-4095
/// print(seq.toDateTime());     // DateTime object
/// print(seq.toString());       // "123456789"
/// ```
class SeqId implements Comparable<SeqId> {
  /// Custom epoch: 2024-01-01 00:00:00 UTC (matches backend).
  static const int epoch = 1704067200000;

  /// The raw 64-bit integer value (matches Rust `i64` / FFI `PlatformInt64`).
  final int value;

  // --------------------------------------------------------------------
  //  Constructors
  // --------------------------------------------------------------------

  /// Create a [SeqId] from a raw [int] value.
  const SeqId(this.value);

  /// Create a [SeqId] with value zero (useful for "start from beginning").
  const SeqId.zero() : value = 0;

  /// Parse a [SeqId] from a decimal string, [int], [BigInt], or [double].
  ///
  /// Throws [FormatException] when the value cannot be converted.
  factory SeqId.parse(Object raw) {
    if (raw is int) return SeqId(raw);
    if (raw is BigInt) return SeqId(raw.toInt());
    if (raw is double) return SeqId(raw.truncate());
    if (raw is String) {
      final parsed = int.tryParse(raw);
      if (parsed != null) return SeqId(parsed);
      // Fallback: try BigInt for very large decimal strings then truncate.
      try {
        return SeqId(BigInt.parse(raw).toInt());
      } catch (_) {
        throw FormatException('SeqId.parse: failed to parse "$raw"');
      }
    }
    throw FormatException('SeqId.parse: unsupported type ${raw.runtimeType}');
  }

  /// Try to parse a [SeqId], returning `null` on failure.
  static SeqId? tryParse(Object? raw) {
    if (raw == null) return null;
    try {
      return SeqId.parse(raw);
    } catch (_) {
      return null;
    }
  }

  // --------------------------------------------------------------------
  //  Raw access
  // --------------------------------------------------------------------

  /// Return the raw value as a Dart [int].
  int toInt() => value;

  /// Return the raw value as a [BigInt].
  BigInt toBigInt() => BigInt.from(value);

  // --------------------------------------------------------------------
  //  Snowflake field extraction
  // --------------------------------------------------------------------

  /// Extract timestamp in milliseconds since Unix epoch.
  int get timestampMillis => (value >> 22) + epoch;

  /// Extract timestamp in whole seconds since Unix epoch.
  int get timestampSeconds => timestampMillis ~/ 1000;

  /// Convert to a [DateTime] (UTC).
  DateTime toDateTime() =>
      DateTime.fromMillisecondsSinceEpoch(timestampMillis, isUtc: true);

  /// Compute age in seconds relative to the current time.
  int ageSeconds([DateTime? now]) {
    final nowMs = (now ?? DateTime.now()).millisecondsSinceEpoch;
    final diff = nowMs - timestampMillis;
    return diff > 0 ? diff ~/ 1000 : 0;
  }

  /// Extract worker ID (0-1023).
  int get workerId => (value >> 12) & 0x3FF;

  /// Extract intra-millisecond sequence number (0-4095).
  int get sequence => value & 0xFFF;

  // --------------------------------------------------------------------
  //  Comparison
  // --------------------------------------------------------------------

  @override
  int compareTo(SeqId other) => value.compareTo(other.value);

  @override
  bool operator ==(Object other) => other is SeqId && value == other.value;

  @override
  int get hashCode => value.hashCode;

  /// Less than.
  bool operator <(SeqId other) => value < other.value;

  /// Less than or equal.
  bool operator <=(SeqId other) => value <= other.value;

  /// Greater than.
  bool operator >(SeqId other) => value > other.value;

  /// Greater than or equal.
  bool operator >=(SeqId other) => value >= other.value;

  // --------------------------------------------------------------------
  //  Serialisation / display
  // --------------------------------------------------------------------

  /// Decimal string representation (for JSON / display).
  @override
  String toString() => value.toString();
}
