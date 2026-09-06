# KalamDB Data Types Reference

**Audience:** backend developers, SDK developers, and advanced users

This document is the architecture-level reference for data types in KalamDB:
- what SQL types are accepted,
- what internal `KalamDataType` is used,
- what Arrow/Parquet physical type is written,
- and what is stored in the hot path (RocksDB row payloads).

## Source of truth in code

The canonical implementations are:
- `backend/crates/kalamdb-commons/src/models/datatypes/kalam_data_type.rs`
- `backend/crates/kalamdb-commons/src/conversions/arrow_conversion.rs`
- `backend/crates/kalamdb-dialect/src/compatibility.rs`
- `backend/crates/kalamdb-commons/src/conversions/schema_metadata.rs`
- `backend/crates/kalamdb-system/src/providers/manifest/models/file_ref.rs`

If this document and code diverge, **the code wins**.

---

## 1) Canonical KalamDB types

KalamDB currently supports these `KalamDataType` variants:

- `Boolean`
- `SmallInt`
- `Int`
- `BigInt`
- `Float`
- `Double`
- `Decimal { precision, scale }`
- `Text`
- `Bytes`
- `Json`
- `Timestamp`
- `Date`
- `DateTime`
- `Time`
- `Uuid`
- `Embedding(usize)`
- `File`

### Type tags (wire-format identifiers)

`KalamDataType` includes stable type tags used by the datatype wire format:

| Type | Tag |
|---|---|
| BOOLEAN | `0x01` |
| INT | `0x02` |
| BIGINT | `0x03` |
| DOUBLE | `0x04` |
| FLOAT | `0x05` |
| TEXT | `0x06` |
| TIMESTAMP | `0x07` |
| DATE | `0x08` |
| DATETIME | `0x09` |
| TIME | `0x0A` |
| JSON | `0x0B` |
| BYTES | `0x0C` |
| EMBEDDING(dim) | `0x0D` |
| UUID | `0x0E` |
| DECIMAL(p,s) | `0x0F` |
| SMALLINT | `0x10` |
| FILE | `0x11` |

---

## 2) SQL → KalamDataType → Arrow/Parquet mapping

This is the runtime mapping used by DDL parsing and schema conversion.

| SQL type (examples) | KalamDataType | Arrow physical type | Notes |
|---|---|---|---|
| `BOOLEAN`, `BOOL` | `Boolean` | `Boolean` | |
| `SMALLINT`, `INT2` | `SmallInt` | `Int16` | |
| `INT`, `INTEGER`, `INT4`, `MEDIUMINT` | `Int` | `Int32` | |
| `BIGINT`, `INT8` | `BigInt` | `Int64` | |
| `FLOAT`, `REAL`, `FLOAT4` | `Float` | `Float32` | |
| `DOUBLE`, `DOUBLE PRECISION`, `FLOAT8`, `FLOAT64` | `Double` | `Float64` | |
| `DECIMAL(p,s)` | `Decimal { p, s }` | `Decimal128(p, s)` | `1 <= p <= 38`, `s <= p` |
| `TEXT`, `VARCHAR`, `CHAR`, `STRING`, `NVARCHAR` | `Text` | `Utf8` | |
| `BYTES`, `BINARY`, `VARBINARY`, `BLOB`, `BYTEA` | `Bytes` | `Binary` | |
| `JSON`, `JSONB` | `Json` | `Utf8` | JSON stored as UTF-8 string in Arrow path |
| `TIMESTAMP` | `Timestamp` | `Timestamp(Microsecond, None)` | no timezone |
| `DATE` | `Date` | `Date32` | days since epoch |
| `DATETIME` | `DateTime` | `Timestamp(Microsecond, Some("UTC"))` | timezone-aware logical type, normalized to UTC |
| `TIME` | `Time` | `Time64(Microsecond)` | |
| `UUID` | `Uuid` | `FixedSizeBinary(16)` | RFC 4122 bytes |
| `EMBEDDING(n)` | `Embedding(n)` | `FixedSizeList(Float32, n)` | `1 <= n <= 8192` |
| `FILE` | `File` | `Utf8` | stores serialized `FileRef` JSON |

### Important alias behavior

Some aliases are accepted in SQL compatibility mapping as Arrow integer variants.
In practice, schema contracts should use canonical KalamDB integer types (`SMALLINT`, `INT`, `BIGINT`) because Arrow alias handling and logical-type recovery can differ by code path.

Unsigned SQL forms (`UnsignedInteger`, etc.) map to Arrow unsigned integers in SQL compatibility code, but the canonical Kalam schema model remains signed-focused (`SmallInt`, `Int`, `BigInt`).

For user-facing schema contracts, prefer canonical names:
`SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `TEXT`, `DECIMAL`, `UUID`, `JSON`, `EMBEDDING(n)`, `FILE`.

---

## 3) What is actually stored

KalamDB has two storage layers with different representations:

1. **Hot path (RocksDB via row codec)**
2. **Flushed path (Arrow/Parquet segments)**

## 3.1 Hot path (RocksDB row payload)

Rows are encoded by `kalamdb-serialization` as ordinal `KOBJ` values. Identity fields
(`user_id`, `_seq`) live on the RocksDB key, not in the payload. Nested `STRUCT` / `List`
values use the same crate.

Key points:
- Row fields are stored as typed ordinal scalars, not a name-keyed JSON blob and not FlatBuffers `KENV`.
- `Int64`/`UInt64` are represented as strings in `StoredScalarValue` JSON-facing form to avoid JS precision loss.
- `Decimal128` stores integer mantissa + precision/scale metadata.
- `Embedding` stores `size` + vector float values.
- `UUID` can be stored as fixed-size binary scalar in typed path.

### 3.1.1 Upgrading to 0.7 (wipe-data)

0.7 does not dual-decode legacy RocksDB bytes. Unenveloped FlexBuffers, MessagePack
snapshots, JSON PK arrays, and old `KENV` / name-keyed rows are rejected on read.

For every **dev and test** cluster when moving to 0.7:

1. Stop the server.
2. Delete the data directory (`storage.data_path`, default `./data`; CLI local projects use `kalam/server/data`; tests often set `KALAMDB_DATA_DIR`).
3. Start 0.7 so it can recreate empty RocksDB / Parquet / snapshot dirs.
4. Re-apply schema and reload data (SQL / `kalam deploy`). Do not copy an old `rocksdb` directory forward.

Production clusters that still have pre-0.7 bytes need a **separate offline rewrite tool** (not the 0.7 server). Until that tool exists, wipe-and-reload is the supported path.

## 3.2 Flushed path (Parquet)

When rows are flushed, schema uses Arrow types from `KalamDataType::to_arrow_type()`.

Examples:
- `Uuid` → `FixedSizeBinary(16)` in Arrow/Parquet.
- `Decimal { precision: 10, scale: 2 }` → `Decimal128(10, 2)`.
- `DateTime` → `Timestamp(..., Some("UTC"))`.
- `Embedding(384)` → `FixedSizeList(Float32, 384)`.
- `File` and `Json` → `Utf8` physical columns.

---

## 4) Kalam-specific metadata in Arrow schema

KalamDB adds field metadata key:
- `kalam_data_type`

This stores serialized `KalamDataType` on each Arrow field. It preserves logical type identity for cases where Arrow physical type alone is ambiguous (for example `Text` vs `Json` vs `File`, all of which may physically use `Utf8`).

This metadata is written during table definition/schema generation and read back during reconstruction paths.

---

## 5) Developer usage patterns (by type group)

## 5.1 Numeric

```sql
CREATE TABLE app.metrics (
  id BIGINT PRIMARY KEY,
  samples SMALLINT,
  count INT,
  total BIGINT,
  ratio FLOAT,
  score DOUBLE,
  amount DECIMAL(12, 2)
);
```

Guidance:
- Use `DECIMAL` for money and exact arithmetic.
- Use `DOUBLE`/`FLOAT` for approximate floating-point.

## 5.2 Text / JSON / bytes

```sql
CREATE TABLE app.events (
  id BIGINT PRIMARY KEY,
  name TEXT,
  payload JSON,
  raw BYTES
);
```

Guidance:
- `JSON` is logically JSON, physically UTF-8 in Arrow schema.
- `BYTES` is binary payload (`Binary`).

## 5.3 Temporal

```sql
CREATE TABLE app.timeline (
  id BIGINT PRIMARY KEY,
  created_at TIMESTAMP,
  due_date DATE,
  local_time TIME,
  happened_at DATETIME
);
```

Guidance:
- `TIMESTAMP` = no timezone.
- `DATETIME` = timezone-aware logical type, stored with UTC timezone marker.

## 5.4 UUID

```sql
CREATE TABLE app.sessions (
  session_id UUID PRIMARY KEY,
  user_id BIGINT
);
```

Guidance:
- UUID literals are validated and coerced to 16-byte binary in typed execution paths.
- Query/API output may be rendered as canonical UUID strings.

## 5.5 EMBEDDING

```sql
CREATE TABLE app.docs (
  id BIGINT PRIMARY KEY,
  embedding EMBEDDING(768)
);
```

Guidance:
- Dimension is mandatory.
- Valid range is `1..=8192`.
- Physical storage is fixed-size float32 list.

## 5.6 FILE

```sql
CREATE TABLE app.assets (
  id BIGINT PRIMARY KEY,
  attachment FILE
);
```

Guidance:
- `FILE` columns store serialized `FileRef` JSON.
- `FILE("placeholder")` placeholders in SQL are replaced with escaped `FileRef` JSON strings by API file utilities.
- `FileRef` includes fields such as `id`, `sub`, `name`, `size`, `mime`, `sha256`, optional `shard`.

Example stored JSON (logical value):

```json
{
  "id": "1234567890123456789",
  "sub": "f0001",
  "name": "document.pdf",
  "size": 1048576,
  "mime": "application/pdf",
  "sha256": "abc123..."
}
```

---

## 6) Runtime introspection

For runtime type visibility:
- Query `system.datatypes` for Arrow↔SQL mapping hints.
- Use `DESCRIBE TABLE <table>` for table schema.
- For tooling, prefer `kalam_data_type` field metadata when available to disambiguate logical types sharing the same Arrow physical type.

---

## 7) Constraints and validation summary

- `EMBEDDING(n)`: `1 <= n <= 8192`
- `DECIMAL(p,s)`: `1 <= p <= 38`, `0 <= s <= p`
- `UUID`: must be valid UUID if provided as string, and stored as 16 bytes in typed paths
- `FILE`: value is serialized `FileRef` JSON

---

## 8) Notes for SDK and API developers

- Treat `KalamDataType` as the logical contract.
- Treat Arrow type as physical execution/storage representation.
- Do not infer `Json` vs `Text` vs `File` from `Utf8` alone; use `kalam_data_type` metadata where present.
- Large integer values may be rendered as strings in JSON outputs to avoid precision loss in JavaScript clients.
