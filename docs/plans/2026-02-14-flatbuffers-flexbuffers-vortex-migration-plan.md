# Centralized Database Object Serialization Protocol Migration Plan

> Revised 2026-09-01. This replaces the earlier design that spread FlatBuffers/FlexBuffers logic across model, storage, Raft, topic, and stream modules.

> **0.7 program:** This is one of three essential tracks for KalamDB 0.7. Implement it alongside [functions V1](functions-v1-implementation.md) and [scalar secondary indexes](2026-08-30-scalar-secondary-indexes.md). Combined sequence and release gate: [2026-09-01-kalamdb-0.7.md](2026-09-01-kalamdb-0.7.md).

Functions nested `STRUCT`/`List` persistence **is** this plan's row codec (Phase 2), not a second serializer. Secondary-index JSON values **are** Phase 4 cleanup, not a private index codec.

## 1. Goal

KalamDB must have exactly one internal serialization subsystem for persisted database objects.

The target invariant is:

```text
KalamDB model / row / command
            |
            v
   kalamdb-serialization
            |
            v
   Kalam Object Protocol bytes
            |
            v
 RocksDB / stream segment / Raft durable payload
```

No table model, system provider, topic store, Raft module, stream store, vector module, or secondary-index implementation may choose or implement its own persistence codec.

If KalamDB later replaces FlatBuffers, FlexBuffers, or changes the binary layout, the implementation change must be contained inside `kalamdb-serialization` plus protocol-version compatibility code in that same crate. Callers must not change.

The design must also keep the hot row format compact enough that RocksDB behaves like an efficient object store rather than a store of repeated JSON field names and duplicated type metadata.

---

## 2. Core Principles

1. **One serialization crate**
   - Add `backend/crates/kalamdb-serialization`.
   - This is the only crate allowed to implement KalamDB internal persisted-object encoding and decoding.

2. **One storage-object protocol**
   - Every structured object persisted by KalamDB uses the same versioned Kalam Object Protocol envelope.
   - Rows, metadata objects, topic envelopes, Raft durable objects, index values, and stream records all enter through this protocol.

3. **Codec choice is private**
   - Callers never pass `FlatBuffers`, `FlexBuffers`, MessagePack, JSON, or another codec name.
   - Codec selection is an implementation detail of `kalamdb-serialization`.
   - Changing the backing codec must not require edits to table stores, models, Raft, topics, or system providers.

4. **Models contain data, not persistence logic**
   - Remove model-owned `encode()` / `decode()` implementations.
   - Remove custom `KSerializable` implementations from domain models.
   - A model must not know whether it is stored using FlatBuffers, FlexBuffers, or a future format.

5. **Storage boundaries serialize once**
   - Serialization happens immediately before persistence and deserialization immediately after reading persisted bytes.
   - No publisher/service should pre-serialize an object only for a store to serialize or wrap it again.

6. **Hot rows remain optimized**
   - Centralization does not mean forcing every object through one slow generic representation.
   - `kalamdb-serialization` may internally have optimized payload profiles, but all profiles share one public API, envelope, versioning, errors, metrics, compatibility policy, and scalar/type implementation.

7. **External protocols stay external**
   - HTTP JSON, WebSocket JSON/MessagePack, PGWire, export metadata, SDK wire formats, and user topic payload bytes are not RocksDB object persistence codecs.
   - They must not be mixed into the storage protocol simply to remove every use of `serde_json` from the repository.

---

## 3. Current Duplication That Must Be Removed

The current code contains several independent persistence serialization paths. The migration is not complete until these are removed or explicitly classified as non-object boundaries.

### 3.1 `kalamdb-commons` owns generic and row codecs

Current files:

- `backend/crates/kalamdb-commons/src/serialization.rs`
- `backend/crates/kalamdb-commons/src/serialization/envelope.rs`
- `backend/crates/kalamdb-commons/src/serialization/row_codec.rs`
- `backend/crates/kalamdb-commons/src/serialization/schema/`
- `backend/crates/kalamdb-commons/src/serialization/generated/`

Current problems:

- `KSerializable` defaults to FlexBuffers.
- Individual models are allowed to override `encode()` / `decode()`.
- Row serialization separately implements FlatBuffers scalar conversion.
- The envelope directly knows FlatBuffers.
- `kalamdb-commons` therefore contains persistence implementation instead of only shared models/types.

Target:

- Move all persistence codec code, generated FlatBuffer code, schema files, version constants, errors, and compatibility logic to `kalamdb-serialization`.
- `kalamdb-commons` keeps domain types only.
- Remove direct `flatbuffers` and `flexbuffers` dependencies from `kalamdb-commons` once migration is complete.

### 3.2 `Row` currently contains a second scalar serialization representation

Current file:

- `backend/crates/kalamdb-commons/src/models/rows/row.rs`

`StoredScalarValue` converts DataFusion `ScalarValue` into a serde representation while `row_codec.rs` separately converts `ScalarValue` into FlatBuffer scalar tags.

That creates two places that must be changed whenever a DataFusion/KalamDB value type is added.

Target:

- `Row` remains an in-memory/query model around `ScalarValue`.
- Client JSON conversion, if still needed, is explicitly named as client/wire JSON conversion rather than storage serialization.
- The **only storage scalar encoder/decoder** lives in `kalamdb-serialization`.
- Adding `STRUCT`, list/array, map, a new numeric type, or another KalamDB data type requires one storage-codec change only.

### 3.3 Row models currently select their own codec

Current files:

- `backend/crates/kalamdb-commons/src/models/rows/row.rs`
- `backend/crates/kalamdb-commons/src/models/rows/user_table_row.rs`
- `backend/crates/kalamdb-tables/src/shared_tables/shared_table_store.rs`

Problems:

- `Row` overrides `KSerializable`.
- `UserTableRow` overrides `KSerializable`.
- `SharedTableRow` defines another override in the tables crate.
- `SharedTableRow` mixes the row data model and store implementation in one file.

Target:

- Move the pure `SharedTableRow` data model to `kalamdb-commons/src/models/rows/shared_table_row.rs` beside `UserTableRow`.
- Remove all codec methods from `Row`, `UserTableRow`, and `SharedTableRow`.
- Stores call the centralized row/object serializer.

### 3.4 `EntityStore` and `IndexedEntityStore` delegate codec ownership to models

Current files:

- `backend/crates/kalamdb-store/src/entity_store.rs`
- `backend/crates/kalamdb-store/src/indexed_store.rs`

Current behavior calls `entity.encode()` / `V::decode()` through `KSerializable`.

Target:

- The storage layer calls `kalamdb_serialization` directly.
- A domain type no longer needs a storage-codec trait implementation merely to be persisted.
- Generic `EntityStore` values use the generic object serializer by default.
- Specialized row stores use the centralized row serializer without moving codec logic back into the row models.

### 3.5 Secondary indexes still persist JSON values

Current file:

- `backend/crates/kalamdb-store/src/index/secondary_index.rs`

Non-unique index values are currently persisted as JSON arrays of primary keys.

Target:

- Replace persistent JSON array encoding with `kalamdb-serialization` object encoding.
- Unique-index primary-key references may remain raw key bytes because they are storage-key references, not serialized database objects.
- This raw-key exception must be explicit and must not become a way to bypass object serialization for arbitrary values.

### 3.6 Raft has multiple independent codecs

Current files include:

- `backend/crates/kalamdb-raft/src/codec/command_codec.rs`
- `backend/crates/kalamdb-raft/src/state_machine/serde_helpers.rs`
- `backend/crates/kalamdb-store/src/raft_storage.rs`

Current behavior includes:

- a custom FlexBuffers command envelope with its own version and `kind` fields;
- a separate MessagePack state-machine codec;
- `KSerializable` for durable Raft storage models.

Target:

- Remove codec implementation from `kalamdb-raft`.
- Keep only command/domain definitions and stable semantic protocol-kind identifiers where useful.
- Encode/decode commands, responses, durable Raft metadata, state-machine payloads, and Kalam-owned snapshots through `kalamdb-serialization`.
- OpenRaft/tonic/protobuf network transport may remain separate because it is a network/library protocol boundary, not KalamDB RocksDB object persistence.

### 3.7 Stream log files call FlexBuffers directly

Current file:

- `backend/crates/kalamdb-streams/src/file_store.rs`

`write_record_bytes()` and `visit_records()` directly call FlexBuffers.

Target:

- `FileStreamLogStore` owns file placement, buffering, windows, and I/O only.
- `StreamLogRecord` payload encoding/decoding comes from `kalamdb-serialization`.
- The length-prefixed record frame also uses one centralized helper so the stream file format has one implementation and version policy.

### 3.8 Topic persistence is serialized outside the store

Current files include:

- `backend/crates/kalamdb-tables/src/topics/topic_message_models.rs`
- `backend/crates/kalamdb-tables/src/topics/topic_message_store.rs`
- `backend/crates/kalamdb-publisher/src/service/publish.rs`

Current publisher code already contains a TODO about redundant topic-message serialization and manually pre-encodes `TopicMessage` before passing raw bytes to the store.

Target:

- Publisher creates `TopicMessage` objects and public topic payload bytes only.
- `TopicMessageStore` is the persistence boundary and asks `kalamdb-serialization` to encode each message exactly once.
- Retention accounting obtains the encoded length from that same encode result.
- Remove APIs that exist only to pass pre-encoded database objects between KalamDB modules.

Important distinction:

- `TopicMessage` **envelope** is a KalamDB persisted object and uses the centralized protocol.
- `TopicMessage.payload` is opaque user/consumer payload bytes and remains unchanged.

### 3.9 Vector hot staging and system entities depend on per-model markers

Examples include:

- `backend/crates/kalamdb-vector/src/hot_staging/models.rs`
- `backend/crates/kalamdb-system/src/providers/**`

Target:

- Remove `impl KSerializable for ...` declarations from vector staging and system models.
- Do not create a dedicated `.fbs` file per system model.
- System metadata is lower-volume and should use the centralized generic object profile so adding a field does not require changing Rust plus a second hand-maintained FlatBuffer schema.

---

## 4. New Crate: `kalamdb-serialization`

Add:

```text
backend/crates/kalamdb-serialization/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs
│   ├── envelope.rs
│   ├── object.rs
│   ├── row/
│   │   ├── mod.rs
│   │   ├── encode.rs
│   │   ├── decode.rs
│   │   ├── scalar.rs
│   │   └── metadata.rs
│   ├── protocol.rs
│   ├── stream_frame.rs
│   ├── version.rs
│   └── generated/
└── schema/
    ├── object_envelope.fbs
    └── row.fbs
```

Keep the crate deliberately small. Do not create one codec module or `.fbs` schema per application model.

### Dependency direction

```text
kalamdb-commons          domain IDs, rows, data types, shared models
       ^
       |
kalamdb-serialization    only persisted-object codec implementation
       ^
       |
kalamdb-store            RocksDB persistence boundary
       ^
       +---- kalamdb-system
       +---- kalamdb-tables
       +---- kalamdb-vector
       +---- kalamdb-raft
       +---- other server crates
```

Rules:

- `kalamdb-serialization` may depend on `kalamdb-commons`.
- `kalamdb-commons` must not depend on `kalamdb-serialization`.
- `kalamdb-serialization` must not depend on `kalamdb-system`, `kalamdb-tables`, `kalamdb-raft`, or other high-level domain crates.
- This prevents cyclic dependencies and prevents the serialization crate from becoming a second domain-model repository.

---

## 5. Public API Boundary

Callers should see a small semantic API. Exact Rust names may change during implementation, but the architectural boundary should look like this:

```rust
// Generic structured database objects.
pub fn encode_object<T: Serialize>(value: &T) -> Result<EncodedObject>;
pub fn decode_object<T: DeserializeOwned>(bytes: &[u8]) -> Result<T>;

// Hot table rows.
pub fn encode_user_row(row: &UserTableRow, schema: &StorageSchema) -> Result<EncodedObject>;
pub fn decode_user_row(bytes: &[u8], schema: &StorageSchema) -> Result<UserTableRow>;

pub fn encode_shared_row(row: &SharedTableRow, schema: &StorageSchema) -> Result<EncodedObject>;
pub fn decode_shared_row(bytes: &[u8], schema: &StorageSchema) -> Result<SharedTableRow>;

// Centralized typed protocol objects such as Raft commands/responses.
pub fn encode_protocol<T: Serialize>(kind: ProtocolKind, value: &T) -> Result<EncodedObject>;
pub fn decode_protocol<T: DeserializeOwned>(
    bytes: &[u8],
    expected_kind: ProtocolKind,
) -> Result<T>;
```

`EncodedObject` exposes bytes and encoded length without causing a second serialization:

```rust
pub struct EncodedObject {
    bytes: Vec<u8>,
}

impl EncodedObject {
    pub fn as_slice(&self) -> &[u8];
    pub fn len(&self) -> usize;
    pub fn into_bytes(self) -> Vec<u8>;
}
```

For batch hot paths, add buffer reuse inside this same crate rather than duplicating serialization in callers:

```rust
pub struct ObjectEncoder { /* reusable builders/buffers */ }

impl ObjectEncoder {
    pub fn encode_object<T: Serialize>(&mut self, value: &T) -> Result<EncodedObject>;
    pub fn encode_user_row(
        &mut self,
        row: &UserTableRow,
        schema: &StorageSchema,
    ) -> Result<EncodedObject>;
}
```

The caller chooses **what semantic object it is encoding**, never the binary library used to encode it.

---

## 6. Kalam Object Protocol Envelope

Every structured persisted object uses one envelope.

Conceptually:

```text
+----------------------+---------------------------------------------+
| magic                | KOBJ                                        |
| protocol_version     | Kalam Object Protocol version               |
| object_kind          | generic / row / protocol / stream           |
| schema_version       | logical payload/storage-schema version      |
| flags                | reserved, initially zero                    |
| payload              | codec-private bytes                         |
+----------------------+---------------------------------------------+
```

The exact FlatBuffer schema may use equivalent fields, but these semantics remain stable.

### Why keep `object_kind` but hide `codec_kind`

Application code needs to validate that a row is a row or a Raft command is the expected protocol kind. Application code does not need to know whether the payload was produced by FlatBuffers, FlexBuffers, or a future codec.

If codec migration requires old and new codecs to coexist, codec/version dispatch belongs entirely inside `kalamdb-serialization`. A private codec ID may be stored in the envelope if needed, but it is not exposed as a caller choice.

### Version rules

1. `protocol_version` changes only when the common envelope/global decoding contract changes.
2. `schema_version` represents logical payload/storage schema version where required.
3. Decoders validate magic, object kind, protocol version, payload bounds, and required discriminants before constructing domain values.
4. Unknown future versions return typed compatibility errors, never panics.
5. No callsite performs its own protocol/version check.

---

## 7. Internal Payload Profiles

Centralization should not sacrifice efficiency. The crate may use two internal profiles initially.

### 7.1 Generic Object Profile

Use for relatively low-volume structured objects whose Rust shape changes over time:

- system table entities;
- jobs/policies/metadata records;
- topic message envelopes and retention entries;
- vector hot-staging metadata;
- secondary-index collection values;
- Raft metadata and Kalam-owned durable protocol objects;
- other `EntityStore` values that do not need the optimized row layout.

Initial codec: FlexBuffers/serde.

Why:

- one generic implementation;
- no `.fbs` per Rust model;
- additive optional fields are simple;
- model field changes do not require hand-editing a second schema file.

The selected binary library is private. If benchmarks later show another generic object codec is better, only `kalamdb-serialization::object` changes.

### 7.2 Table Row Profile

Use for USER and SHARED table rows because they dominate object count and storage volume.

Initial codec: FlatBuffers with one generic schema-aware row format.

The row codec is optimized around table/type metadata instead of repeating self-describing names and type metadata for every value.

This still satisfies the one-place rule because FlatBuffers-specific row logic exists only inside `kalamdb-serialization`.

---

## 8. Efficient Row/Object Storage

### 8.1 Stop persisting column names per row

The current row FlatBuffer uses `ColumnValue { name, value }`. The target row layout must become a true ordinal representation.

Conceptually:

```text
UserRowPayload {
    user_id
    seq
    commit_seq
    deleted
    schema_version
    null_bitmap
    values[]
}
```

and:

```text
SharedRowPayload {
    seq
    commit_seq
    deleted
    schema_version
    null_bitmap
    values[]
}
```

Column names live once in catalog/table schema metadata, not inside every RocksDB value.

### 8.2 Do not repeat type information the schema already knows

For a normal table column, the table schema already says whether physical slot 0 is `BIGINT`, slot 1 is `TEXT`, slot 2 is `STRUCT`, and so on.

Therefore:

- encode values according to schema ordinals;
- use compact null/presence metadata;
- avoid storing a full scalar tag for every value when the schema already determines the type;
- keep explicit dynamic tags only where the value is genuinely dynamic, such as arbitrary JSON/dynamic content.

This reduces both RocksDB value size and CPU spent repeatedly decoding metadata already known from the schema.

### 8.3 Physical storage ordinals are stable

Schema evolution must not make old bytes ambiguous.

Rules:

1. Assign a stable physical slot/field ID when a table column or composite-type field is created.
2. New fields append new physical slots.
3. Renaming a field changes the catalog name, not its physical slot.
4. Dropping a field does not immediately reuse its slot.
5. Do not reorder stored slots merely because SQL display order changes.
6. Persist a storage `schema_version` with a row so compatibility/default behavior is applied centrally.

This is similar in spirit to append-only binary schema evolution and avoids full RocksDB rewrites for ordinary additive schema changes.

### 8.4 Nested `STRUCT` / `CREATE TYPE` values

The storage value codec must support recursive values in exactly one place.

Logical example:

```sql
CREATE TYPE app.customer AS (
    id BIGINT,
    name TEXT
);

CREATE TABLE orders (
    id BIGINT,
    customer app.customer
);
```

Runtime representation remains DataFusion/Arrow `Struct`.

Storage representation uses the resolved type schema's stable field slots recursively:

```text
orders row
  values[0] = BIGINT
  values[1] = STRUCT
                  values[0] = BIGINT
                  values[1] = TEXT
```

Named `CREATE TYPE` identity belongs to catalog/schema metadata. The row payload stores the compact structural value.

The same central value implementation covers:

- scalar primitives;
- `STRUCT`;
- nested `STRUCT`;
- list/array values;
- map values when supported;
- embeddings;
- binary values;
- decimal values;
- timestamps/timezones;
- null values.

No second Struct/List/Map storage serializer may be added to table stores, DataFusion sources, function runtime, or SDK-facing code.

### 8.5 Additive schema reads do not eagerly rewrite old data

If a new nullable field is appended:

```sql
ALTER TYPE app.customer ADD ATTRIBUTE username TEXT;
```

old objects remain readable.

The decoder sees the older `schema_version` / shorter ordinal representation and materializes the missing field using centralized evolution rules:

- nullable additive field -> `NULL`;
- field with a safe database default -> apply explicitly defined read/migration semantics;
- incompatible required/type changes -> explicit migration requirement.

Do not rewrite millions of RocksDB values merely because a safe additive field was introduced.

### 8.6 Compression

Do not add per-object compression in this migration unless benchmarks prove it is required.

Prefer RocksDB block compression for general storage compression. Per-object compression adds headers, CPU cost, and complexity for small values.

The centralized protocol may reserve a compression flag for future use without implementing it now.

---

## 9. Storage Keys Are Intentionally Separate

`StorageKey` encoding has a different requirement from value serialization: keys must preserve deterministic byte ordering and efficient prefix/range scans.

Therefore:

```text
RocksDB key   -> StorageKey / storekey ordered encoding
RocksDB value -> kalamdb-serialization Kalam Object Protocol
```

Do not replace ordered key encoding with FlatBuffers/FlexBuffers.

Raw primary-key references in an index may remain raw ordered/reference bytes where that is the most efficient representation. They are not database-object payloads.

---

## 10. Subsystem Migration Map

### 10.1 Workspace and dependencies

Files:

- `Cargo.toml`
- `backend/crates/kalamdb-commons/Cargo.toml`
- affected crate `Cargo.toml` files

Changes:

1. Add `backend/crates/kalamdb-serialization` to workspace members.
2. Add `kalamdb-serialization` as a workspace dependency.
3. Move `flatbuffers` / `flexbuffers` dependencies to the new crate for internal persistence use.
4. Remove persistence-only FlatBuffers/FlexBuffers dependencies from commons/store/tables/system/raft/streams/vector as direct callsites disappear.
5. Keep MessagePack/JSON dependencies where genuinely required by external WebSocket/API/export contracts.

### 10.2 `kalamdb-commons`

Changes:

1. Move `src/serialization.rs` and `src/serialization/**` implementation into the new crate.
2. Remove persistence codec exports from `lib.rs`.
3. Remove model-owned `KSerializable` implementations.
4. Keep domain models, IDs, `Row`, `UserTableRow`, and data type definitions.
5. Move `SharedTableRow` model into commons.
6. Rename/separate client JSON scalar conversion from storage scalar conversion.
7. Reassess `KalamDataType::tag()/from_tag()`: if those tags are only the persisted wire representation, move the mapping into `kalamdb-serialization`; keep logical SQL type identity in commons.

### 10.3 `kalamdb-store`

Files include:

- `src/entity_store.rs`
- `src/indexed_store.rs`
- `src/index/secondary_index.rs`
- `src/raft_storage.rs`

Changes:

1. Replace `KSerializable` model delegation with central serializer calls.
2. Serialize exactly at `put`/batch persistence boundaries.
3. Decode exactly after backend reads/scans.
4. Replace persisted JSON non-unique index lists with centralized object encoding.
5. Persist Kalam-owned Raft objects through the same object protocol.
6. Do not add codec-specific helpers to `kalamdb-store`.

### 10.4 `kalamdb-tables`

Changes:

1. User/shared row stores call the central row encoder/decoder.
2. Remove `SharedTableRow` codec/model logic from `shared_table_store.rs`.
3. Topic message and retention objects use central generic object encoding.
4. Remove APIs accepting pre-serialized database objects unless they are explicitly raw/opaque payload APIs.
5. Retain ordered topic/storage keys unchanged.

### 10.5 `kalamdb-publisher`

Changes:

1. Keep public topic payload construction (`JSON`/raw bytes) as a separate consumer contract.
2. Stop serializing `TopicMessage` persistence envelopes in publisher code.
3. Pass typed messages to the topic store.
4. Let the store/central encoder serialize once and return encoded byte size for retention metrics.
5. Remove the existing redundant-serialization TODO by deleting the redundant path, not by creating a second serializer abstraction.

### 10.6 `kalamdb-system`

Changes:

1. Remove all per-model `KSerializable` markers/overrides.
2. Do **not** introduce one `.fbs` schema per system table.
3. Persist models through the generic object profile at the storage boundary.
4. Model tests can test logical/JSON behavior separately, but persisted-object compatibility tests belong to `kalamdb-serialization` and store integration tests.

### 10.7 `kalamdb-vector`

Changes:

1. Remove storage codec ownership from `VectorHotOp`.
2. Persist staging objects through the generic object profile.
3. Vector index engine/file formats stay independent unless they persist KalamDB structured objects through the common storage boundary.

### 10.8 `kalamdb-raft`

Changes:

1. Replace direct FlexBuffers use in `src/codec/command_codec.rs` with centralized protocol encode/decode calls, then delete/reduce the file when no codec logic remains.
2. Replace MessagePack helpers in `src/state_machine/serde_helpers.rs` with centralized serialization and remove the helper module when possible.
3. Command kind/version validation belongs in centralized protocol implementation.
4. Durable Raft state stored in RocksDB uses the common object envelope.
5. Tonic/protobuf/OpenRaft network RPC serialization remains a network transport concern unless KalamDB itself persists those encoded frames.

### 10.9 `kalamdb-streams`

Changes:

1. Remove direct FlexBuffers from `src/file_store.rs`.
2. Serialize `StreamLogRecord` through the central crate.
3. Centralize the record frame/version helper.
4. `FileStreamLogStore` remains responsible only for path layout, file handles, buffering, windows, cleanup, and reads/writes.

### 10.10 Other `EntityStore` users

Search the complete workspace for:

- `impl KSerializable`;
- `.encode()` / `::decode()` used for persistence;
- direct `flexbuffers::`;
- direct `flatbuffers::`;
- direct `bincode::`;
- direct `rmp_serde::` in durable/internal paths;
- `serde_json::to_vec/from_slice` immediately around `StorageBackend::put/get`.

Every internal persistence match must either:

1. move to `kalamdb-serialization`, or
2. be documented as an explicit non-object exception such as an ordered key/reference or opaque user payload.

---

## 11. What Is Explicitly Out of Scope

These are legitimate serialization/wire formats but are not KalamDB persisted-object serialization:

- HTTP request/response JSON;
- WebSocket JSON and MessagePack negotiation;
- PGWire/PostgreSQL protocol encoding;
- CLI output/input JSON;
- SDK public wire formats;
- transfer/export manifest JSON intended as a portable/public format;
- user-provided topic payload bytes;
- Parquet/Arrow file encoding;
- future Vortex segment format;
- object/file/blob contents;
- ordered RocksDB storage keys;
- tonic/protobuf network transport owned by the Raft RPC layer.

This boundary prevents accidental coupling of user-facing protocols to RocksDB's internal object format.

---

## 12. Protocol Evolution Policy

### Generic objects

1. Additive optional/defaulted fields are allowed.
2. Removing/renaming fields requires compatibility analysis or explicit migration handling.
3. Enum/discriminant changes require stable semantic IDs; do not rely on incidental Rust enum ordering for long-lived persisted contracts.
4. Compatibility decisions live in `kalamdb-serialization`, not scattered serde attributes plus local decode fallbacks.

### Table and named-type rows

1. Physical slots/field IDs are append-only and never silently reused.
2. Renames preserve physical identity.
3. Additive nullable fields decode safely from older rows.
4. Required/incompatible field changes are migration operations.
5. Nested structs recursively follow the same slot/version rules.

### Protocol objects such as Raft commands

1. Use stable protocol-kind IDs.
2. Decode validates expected kind centrally.
3. New optional fields are additive.
4. Retired incompatible command kinds fail with a typed compatibility error.
5. Persisted Raft data remains decodable for the supported upgrade window.

---

## 13. Error Model

Use one error type from `kalamdb-serialization` for internal object codecs, for example:

```rust
pub enum SerializationError {
    InvalidMagic,
    UnsupportedProtocolVersion { found: u16, supported: u16 },
    UnexpectedObjectKind { expected: ObjectKind, found: ObjectKind },
    UnsupportedSchemaVersion { found: u32 },
    MalformedPayload { reason: String },
    TypeMismatch { expected: String, found: String },
    LegacyFormat { detected: String },
}
```

Storage/Raft/stream layers convert this error into their own domain error only at their boundary.

Do not construct codec-specific error strings independently across callers.

---

## 14. Legacy Data Policy

Keep the existing decision not to perform silent automatic historical rewrites.

Rules:

1. Detect legacy bincode/old unwrapped formats where practical.
2. Return one clear compatibility error with remediation guidance.
3. Do not guess between multiple codecs on every read forever.
4. If production migration tooling is later required, implement it as an explicit one-time/offline migration using the centralized decoder/encoder.
5. Once a supported legacy window expires, remove its decoder from the serialization crate without touching callers.

---

## 15. CI Architecture Guard

Centralization must be mechanically enforced.

Add a CI/check script that fails when internal persistence crates introduce codec usage outside the approved serialization boundary.

At minimum inspect server persistence paths for forbidden direct use of:

```text
flatbuffers::
flexbuffers::
bincode::
rmp_serde::
```

and flag persistence-adjacent uses of:

```text
serde_json::to_vec
serde_json::from_slice
```

outside approved external-wire/test files.

Also validate Cargo dependencies so persistence crates do not regain direct FlatBuffers/FlexBuffers dependencies.

Allowed internal implementation location:

```text
backend/crates/kalamdb-serialization/**
```

External WebSocket/API/SDK/export exceptions must be explicit in the guard configuration rather than ignored globally.

This turns "serialization lives in one place" into an architectural invariant instead of a convention that can regress later.

---

## 16. Test Plan

### 16.1 Central protocol tests

- envelope magic/version validation;
- wrong object kind;
- truncated/corrupt payload;
- unsupported future protocol version;
- legacy format detection;
- generic object roundtrip;
- protocol-kind roundtrip;
- stable golden-byte fixtures for supported versions.

### 16.2 Scalar/value tests

One test matrix in `kalamdb-serialization` covers every supported storage value:

- null;
- boolean;
- signed/unsigned integers;
- float/double;
- decimal;
- UTF-8/binary;
- date/time/timestamp/timezone;
- UUID/file representation where applicable;
- embedding;
- `STRUCT`;
- nested `STRUCT`;
- list/array;
- map when supported;
- JSON/dynamic value.

Do not duplicate this matrix in every table/store crate.

### 16.3 Row tests

- user/shared row roundtrip;
- null-heavy rows;
- sparse logical rows normalized to schema slots;
- no column names stored in normal row payload;
- additive column evolution;
- renamed logical field with same physical slot;
- nested type field addition;
- metadata-only decode for count/version-resolution paths;
- schema-version mismatch behavior.

### 16.4 Subsystem integration tests

- system `EntityStore` roundtrip;
- indexed `EntityStore` roundtrip;
- secondary non-unique index list roundtrip;
- topic publish -> one serialized message -> fetch;
- retention byte accounting uses actual encoded length;
- vector hot staging roundtrip;
- Raft command/response roundtrip;
- Raft durable metadata restart test;
- stream append/restart/read roundtrip.

### 16.5 Single-serialization tests

Instrument the central encoder in tests and assert that hot paths do not encode the same database object twice.

Especially verify:

- batch topic publishing;
- `EntityStore` batch writes;
- indexed writes;
- user/shared batch inserts;
- Raft durable/proposal paths where applicable.

---

## 17. Performance Gates

Centralization is accepted only if it remains efficient.

Measure before and after on representative objects.

### Rows

- encoded user/shared row size must be <= the current name-keyed FlatBuffer format for representative schemas;
- p95 encode/decode latency regression <= 5%;
- batch insert throughput regression <= 5%;
- report storage-size improvement from removing repeated column names;
- allocations per row must not increase.

### Generic objects

- compare representative system metadata, topic envelopes, Raft objects, and vector staging values;
- report bytes/object and encode/decode latency;
- prioritize maintainability for low-volume metadata while avoiding pathological size growth.

### Batch paths

- reuse encoder buffers/builders where meaningful;
- avoid payload `Vec` -> envelope `Vec` -> store `Vec` chains when one owned output can be produced;
- confirm topic messages are serialized once;
- benchmark 1, 100, 1,000, and 10,000 object batches.

### Nested values

Benchmark:

- shallow struct;
- struct nested 2-4 levels;
- list of structs;
- null-heavy nested struct;
- representative AI/chat object-like payload stored as a typed struct.

---

## 18. Implementation Phases

### Phase 0 - Inventory and Baseline

1. Search the complete workspace for all internal persistence serialization callsites.
2. Classify each match as:
   - persisted object;
   - storage key/reference;
   - external/public wire format;
   - test only.
3. Record baseline payload size, encode/decode latency, allocation count, and batch throughput.
4. Add the CI architecture guard in report-only mode first.

### Phase 1 - Create `kalamdb-serialization`

1. Add the crate and dependency direction described above.
2. Move common envelope/version/error implementation into it.
3. Move FlatBuffer schema generation and generated code into it.
4. Expose the small semantic APIs.
5. Add protocol/golden tests.

After this phase, no caller should directly choose an internal persistence codec.

### Phase 2 - Move Row Serialization Completely Out of Commons/Models

This phase is the 0.7 Wave 1 serialization gate and **unblocks Functions Task 3**. Nested `STRUCT`/`List` for `CREATE TYPE` / procedure payloads is implemented here, not in `kalamdb-functions`.

1. Move `row_codec.rs`, row `.fbs`, generated code, and scalar storage conversion into the new crate.
2. Convert row format from name-keyed columns to true schema-ordinal values.
3. Add nested `STRUCT`/list/map recursion in the same scalar/value implementation.
4. Remove storage `encode/decode` implementations from `Row` and `UserTableRow`.
5. Move `SharedTableRow` model into commons and remove its local codec implementation.
6. Separate client JSON scalar representation from storage representation.
7. Preserve the metadata-only decoder optimization used by count/version-resolution scans.

### Phase 3 - Refactor Storage Boundary

1. Update `EntityStore` and `IndexedEntityStore` to call the central serializer.
2. Remove model-owned `KSerializable` as the persistence mechanism.
3. Add central batch/buffer reuse where benchmarks justify it.
4. Ensure row stores use the optimized row profile while ordinary `EntityStore` objects use the generic profile.

### Phase 4 - System, Topic, Vector, and Index Cleanup

Coordinate with the [scalar secondary index plan](2026-08-30-scalar-secondary-indexes.md): item 6 is the 0.7 index-value format. Do not leave JSON PK arrays as the shipped 0.7 path.

1. Remove all `KSerializable` implementations from system models.
2. Do not add per-system-model `.fbs` schemas.
3. Move topic message/retention persistence to central encoding.
4. Delete publisher pre-serialization of `TopicMessage`.
5. Move vector hot staging to central encoding.
6. Replace JSON non-unique index values with central object encoding.

### Phase 5 - Raft Cleanup

1. Replace custom FlexBuffers command encoding with centralized protocol encoding.
2. Replace MessagePack state-machine helpers.
3. Move Kalam-owned durable Raft values to the common object protocol.
4. Delete redundant Raft codec/version helpers once callers use the central API.
5. Keep network RPC transport serialization separate.

### Phase 6 - Stream Log Cleanup

1. Replace direct FlexBuffers calls in `FileStreamLogStore`.
2. Use centralized stream-record encoding and frame helpers.
3. Add restart/compatibility tests for stream segments.

### Phase 7 - Delete Old Serialization Code and Dependencies

1. Delete `kalamdb-commons/src/serialization*` after all callsites move.
2. Remove old generated schemas/code from commons.
3. Remove direct FlatBuffers/FlexBuffers dependencies from migrated crates.
4. Remove obsolete `KSerializable` implementations/trait exports.
5. Remove Raft MessagePack/FlexBuffers persistence helpers.
6. Remove stale comments claiming rows are persisted as JSON.
7. Turn the CI architecture guard from report-only to blocking.

### Phase 8 - Benchmark and Rollout

1. Run compatibility and restart suites.
2. Run row/object/batch/nested-value benchmarks.
3. Compare RocksDB size and write throughput.
4. Validate no duplicate serialization in hot paths.
5. Add startup diagnostics for unsupported persisted protocol versions.
6. Document the dev reset workflow for unsupported legacy data.

---

## 19. Definition of Done

The migration is complete only when all of the following are true:

1. `backend/crates/kalamdb-serialization` is the single internal persisted-object codec crate.
2. Every structured RocksDB value written by KalamDB goes through its API, except documented raw key/reference/opaque-byte cases.
3. USER/SHARED rows use the same centralized optimized row codec.
4. Nested objects (`STRUCT` / named `CREATE TYPE`) are encoded recursively by that same value codec.
5. System models do not have separate `.fbs` schemas.
6. Domain models do not implement codec-specific persistence methods.
7. `EntityStore`/`IndexedEntityStore` do not delegate binary format decisions to models.
8. Secondary non-unique indexes no longer store ad-hoc JSON values.
9. Topic persistence serializes each message object exactly once.
10. Raft has no separate Kalam-owned FlexBuffers/MessagePack persistence implementation.
11. Stream logs do not call FlexBuffers directly.
12. `kalamdb-commons` no longer owns FlatBuffers/FlexBuffers persistence code.
13. CI prevents reintroducing direct internal codec usage outside the centralized crate.
14. Changing the generic object codec or row codec requires edits only under `kalamdb-serialization` plus its tests/version compatibility logic; stores/models remain unchanged.
15. Performance gates pass.

---

## Phase 0 inventory (2026-09-05)

Wave 1 recorded callsites. Baseline encode/decode benches stay on Phase 8; this inventory classifies persistence vs wire vs test.

| Location | Kind | Notes |
| --- | --- | --- |
| `kalamdb-commons/src/serialization*` | persisted object (legacy) | Envelope + row FlatBuffers; move/delete in Phases 2–7 |
| `kalamdb-commons` schema models (`table_definition`, column types) | persisted object | FlexBuffers via `KSerializable` |
| `kalamdb-store` `entity_store` / `index/secondary_index` | persisted object | JSON index values; EntityStore delegates to models |
| `kalamdb-system` provider models | persisted object | FlexBuffers/JSON per model |
| `kalamdb-publisher/src/payload.rs` | persisted object | Topic payload JSON |
| `kalamdb-streams/src/file_store.rs` | persisted object | Direct FlexBuffers |
| `kalamdb-raft/src/codec/command_codec.rs` | persisted object | FlexBuffers commands |
| `kalamdb-raft/src/state_machine/serde_helpers.rs` | persisted object | MessagePack |
| `kalamdb-commons/src/websocket.rs`, `kalamdb-api` | external/public wire | Allowed exception |
| `kalamdb-auth` JWT/JSON | external/public wire | Allowed exception |
| `**/tests/**`, `*_tests.rs` | test only | Allowed exception |
| `kalamdb-serialization` | approved codec crate | Wave 1 home |

CI: `python3 scripts/check-serialization-boundary.py` (report-only). `--fail` becomes blocking in Phase 7.

---

## 20. Vortex Learnings Retained

Vortex remains a design reference rather than a file-format dependency in this phase.

Keep these lessons:

1. **Type/value separation** - use schema/type metadata instead of repeating names/types in every value.
2. **Central scalar logic** - one canonical mapping for numeric/binary/decimal/nested values.
3. **Strict validation** - reject malformed or impossible value/type combinations.
4. **Compact hot path** - avoid unnecessary allocations/copies and repeated metadata.
5. **Schema-aware evolution** - stable physical identity and additive evolution.

Do not adopt Vortex as the RocksDB object format or replace Parquet as part of this migration.

---

## 21. Explicit Non-Goals

1. Replacing Parquet flushed segments with Vortex.
2. Changing RocksDB ordered key format.
3. Replacing HTTP/WebSocket/PGWire protocols.
4. Changing the public payload format a topic consumer requested.
5. Automatic historical migration of every legacy dev database.
6. A plugin architecture for arbitrary codecs.
7. Runtime codec selection configured by users.
8. One FlatBuffer schema per KalamDB table or Rust system model.

The intended architecture is deliberately simple: **one crate, one envelope, one compatibility policy, one scalar/row implementation, and no persistence codec logic scattered through the database.**
