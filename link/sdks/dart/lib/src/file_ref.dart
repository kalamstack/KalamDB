import 'dart:convert';

/// Data shape for a file reference stored in a KalamDB FILE column.
///
/// Matches the Rust `FileRef` struct and the TypeScript `FileRefData` interface.
class KalamFileRefData {
  /// Unique file identifier (Snowflake ID).
  final String id;

  /// Subfolder name (e.g. "f0001", "f0002").
  final String sub;

  /// Original filename (preserved for display / download).
  final String name;

  /// File size in bytes.
  final int size;

  /// MIME type (e.g. "image/png", "application/pdf").
  final String mime;

  /// SHA-256 hash of file content (hex-encoded).
  final String sha256;

  /// Optional shard ID for shared tables.
  final int? shard;

  const KalamFileRefData({
    required this.id,
    required this.sub,
    required this.name,
    required this.size,
    required this.mime,
    required this.sha256,
    this.shard,
  });
}

/// Type-safe file reference for KalamDB FILE columns.
///
/// Provides parsing from JSON, download URL generation, and metadata access.
///
/// ```dart
/// final ref = KalamFileRef.tryParse(row['attachment']);
/// if (ref != null) {
///   final url = ref.getDownloadUrl('http://localhost:18080', 'default', 'messages');
///   print('${ref.name} (${ref.formatSize()})');
/// }
/// ```
class KalamFileRef implements KalamFileRefData {
  @override
  final String id;
  @override
  final String sub;
  @override
  final String name;
  @override
  final int size;
  @override
  final String mime;
  @override
  final String sha256;
  @override
  final int? shard;

  const KalamFileRef({
    required this.id,
    required this.sub,
    required this.name,
    required this.size,
    required this.mime,
    required this.sha256,
    this.shard,
  });

  /// Create from a [KalamFileRefData] or a raw map.
  factory KalamFileRef.fromData(KalamFileRefData data) {
    return KalamFileRef(
      id: data.id,
      sub: data.sub,
      name: data.name,
      size: data.size,
      mime: data.mime,
      sha256: data.sha256,
      shard: data.shard,
    );
  }

  /// Parse from a JSON string (as stored in FILE columns).
  ///
  /// Returns `null` if [json] is null, empty, or malformed.
  static KalamFileRef? fromJson(String? json) {
    if (json == null || json.isEmpty) return null;
    try {
      final data = jsonDecode(json);
      return _fromMap(data as Map<String, dynamic>);
    } catch (_) {
      return null;
    }
  }

  /// Parse from an unknown value — handles JSON string, `Map`, or `null`.
  ///
  /// This is the recommended entry point when reading FILE column values
  /// from query results.
  static KalamFileRef? tryParse(dynamic value) {
    if (value == null) return null;
    if (value is String) return fromJson(value);
    if (value is Map<String, dynamic>) return _fromMap(value);
    if (value is Map) {
      return _fromMap(Map<String, dynamic>.from(value));
    }
    return null;
  }

  static KalamFileRef _fromMap(Map<String, dynamic> map) {
    return KalamFileRef(
      id: map['id'] as String,
      sub: map['sub'] as String,
      name: map['name'] as String,
      size: (map['size'] as num).toInt(),
      mime: map['mime'] as String,
      sha256: map['sha256'] as String,
      shard: map['shard'] != null ? (map['shard'] as num).toInt() : null,
    );
  }

  /// Generate a download URL for this file.
  ///
  /// * [baseUrl] — KalamDB server URL (e.g. `http://localhost:18080`).
  /// * [namespace] — table namespace (usually `"default"`).
  /// * [table] — table name (e.g. `"messages"`).
  ///
  /// ```dart
  /// final url = ref.getDownloadUrl('http://localhost:18080', 'default', 'messages');
  /// // → http://localhost:18080/api/v1/files/default/messages/f0001/12345
  /// ```
  String getDownloadUrl(String baseUrl, String namespace, String table) {
    final cleanUrl = baseUrl.endsWith('/') ? baseUrl.substring(0, baseUrl.length - 1) : baseUrl;
    return '$cleanUrl/api/v1/files/$namespace/$table/$sub/$id';
  }

  /// Whether this is an image file (based on MIME type).
  bool get isImage => mime.startsWith('image/');

  /// Whether this is a video file (based on MIME type).
  bool get isVideo => mime.startsWith('video/');

  /// Whether this is an audio file (based on MIME type).
  bool get isAudio => mime.startsWith('audio/');

  /// Format [size] in human-readable form (e.g. "1.5 MB").
  String formatSize() {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    double s = size.toDouble();
    int unitIndex = 0;
    while (s >= 1024 && unitIndex < units.length - 1) {
      s /= 1024;
      unitIndex++;
    }
    return '${unitIndex == 0 ? s.toInt() : s.toStringAsFixed(1)} ${units[unitIndex]}';
  }

  /// Serialize back to a JSON string.
  String toJson() {
    final map = <String, dynamic>{
      'id': id,
      'sub': sub,
      'name': name,
      'size': size,
      'mime': mime,
      'sha256': sha256,
    };
    if (shard != null) map['shard'] = shard;
    return jsonEncode(map);
  }

  /// Convert to a plain map.
  Map<String, dynamic> toMap() {
    final map = <String, dynamic>{
      'id': id,
      'sub': sub,
      'name': name,
      'size': size,
      'mime': mime,
      'sha256': sha256,
    };
    if (shard != null) map['shard'] = shard;
    return map;
  }

  @override
  String toString() => 'KalamFileRef($name, $mime, ${formatSize()})';
}
