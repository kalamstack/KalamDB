# KalamDB Development Guidelines

## 🎯 Core Coding Principles

**ALWAYS follow these essential guidelines:**

1. **Model Separation**: Each model MUST be in its own separate file
2. **AppContext-First Pattern**: Use `Arc<AppContext>` parameter instead of individual fields
3. **Performance & Memory Optimization**: Focus on lightweight memory usage and high concurrency
   - Use `Arc<T>` for zero-copy sharing (no cloning data)
   - DashMap for lock-free concurrent access
   - Memoize expensive computations (e.g., Arrow schema construction)
   - Separate large structs from hot-path metadata (LRU timestamps in separate map)
   - Cache singleton instances (e.g., UserTableShared per table, not per user)

4. instead than doing user_id.map(kalamdb_commons::models::UserId::from); always add it to the head of the file:
```rust
use kalamdb_commons::models::UserId;
```

5. Well-organized code with minimal duplication
6. Leveraging DataFusion for query processing, version resolution, and deletion filtering
7. WHEN FINIDNG MULTIPLE ERRORS dont kleep running cargo check or cargo build run one time and output to a file and fix all of them at once!
8. Instead of passing both Namespaceid and TableName pass TableId which contains both
9. Don't import use inside methods always add them to the top of the rust file instead
10. EntityStore is used instead of using the EntityStorev2 alias
11. Always use type-safe enums and types instead of raw strings (e.g., `JobStatus`, `Role`, `TableType`, `NamespaceId`, `TableId`)

12. Filesystem vs RocksDB separation of concerns
   - Filesystem/file storage logic (cold storage, Parquet files, directory management, file size accounting) must live in `backend/crates/kalamdb-filestore`
   - Key-value storage engines and partition/column family logic must live in `backend/crates/kalamdb-store`
   - Orchestration layers in `kalamdb-core` (DDL/DML handlers, job executors) should delegate to these crates and avoid embedding filesystem or RocksDB specifics directly
   - When adding cleanup/compaction/file lifecycle functionality, implement it in `kalamdb-filestore` and call it from `kalamdb-core`

13. **Smoke Tests Priority**: Always ensure smoke tests are passing before committing changes. If smoke tests fail, fix them or the underlying backend issue immediately. Run `cargo test --test smoke` in the `cli` directory to verify.
14. **Tracing Table Field Convention**: In spans/events, log `table_id` (format `namespace.table`) instead of separate `table_namespace` and `table_name` fields.
15. **No SQL Rewrite in Hot Paths**: Do not add SQL/DML/SELECT rewrite passes in execution hot paths. Prefer type-safe coercion at typed boundaries (parameter binding, scalar coercion, provider write path, DataFusion-native casts/UDFs explicitly invoked by query authors) to avoid extra parse/transform overhead.
16. **SDK Changes Must Update Docs**: Any change under `link/sdks/**` or SDK bridge crates (for example `link/kalam-link-dart/**`) must also update the corresponding SDK docs in the `KalamSite` repo (typically `../KalamSite/content/sdk/**`) and include appropriate test coverage.
17. **Performance-First Execution**: Prefer approaches that reduce runtime, allocations, binary size, and compile time; avoid adding abstractions or dependencies that materially slow hot paths or build/test feedback loops without a clear benefit.
18. **Performance Test Timing**: Whenever you run performance tests, benchmarks, or perf-focused e2e cases, record and report how long each relevant test took in seconds.

> **⚠️ IMPORTANT**: Smoke tests require a running KalamDB server! Start the server first with `cargo run` in the `backend` directory before running smoke tests. The tests will fail if no server is running.

**When adding a new dependency:**
1. Add it to `Cargo.toml` (root) under `[workspace.dependencies]` with version
2. Reference it in individual crates using `{ workspace = true }`
3. Add crate-specific features if needed: `{ workspace = true, features = ["..."] }`
4. Enable only the features that are actually required; prefer `default-features = false` when defaults pull in unused code or slow compilation.

**To update a dependency version:**
- Only edit the version in root `Cargo.toml`
- All crates will automatically use the new version

## Active Technologies
- Rust 1.92+ (stable toolchain, edition 2021)
- RocksDB 0.24, Apache Arrow 52.0, Apache Parquet 52.0, DataFusion 40.0, Actix-Web 4.4
- RocksDB for write path (<1ms), Parquet for flushed storage (compressed columnar format)
- TypeScript/JavaScript ES2020+ (frontend SDKs)
- WASM for browser-based client library

**Job Management System** (Phase 9 + Phase 8.5 Complete):
- **UnifiedJobManager**: Typed JobIds (FL/CL/RT/SE/UC/CO/BK/RS), idempotency, retry logic (3× default with exponential backoff)
- **8 Job Executors**: FlushExecutor (complete), CleanupExecutor (✅), RetentionExecutor (✅), StreamEvictionExecutor (✅), UserCleanupExecutor (✅), CompactExecutor (placeholder), BackupExecutor (placeholder), RestoreExecutor (placeholder)
- **Status Transitions**: New → Queued → Running → Completed/Failed/Retrying/Cancelled
- **Crash Recovery**: Marks Running jobs as Failed on server restart
- **Idempotency Keys**: Format "{job_type}:{namespace}:{table}:{date}" prevents duplicate jobs

**Authentication & Authorization**:
- **bcrypt 0.15**: Password hashing (cost factor 12, min 8 chars, max 72 chars)
- **jsonwebtoken 9.2**: JWT token generation and validation (HS256 algorithm)
- **HTTP Basic Auth**: Base64-encoded username:password (Authorization: Basic header)
- **JWT Bearer Tokens**: Stateless authentication (Authorization: Bearer header)
- **OAuth 2.0 Integration**: Google Workspace, GitHub, Microsoft Azure AD
- **RBAC (Role-Based Access Control)**: Four roles (user, service, dba, system)
- **Actix-Web Middleware**: Custom authentication extractors and guards
- **StorageBackend Abstraction**: `Arc<dyn StorageBackend>` isolates RocksDB dependencies

## Project Navigation

- `backend/`: Main Rust server workspace; most database engine work starts here.
- `backend/crates/`: Core server crates grouped by responsibility; prefer editing the owning crate instead of cross-cutting changes.
- `cli/`: Kalam CLI, smoke tests, and CLI-facing integration flows.
- `link/`: SDK bridge workspace and shared link infrastructure.
- `link/sdks/typescript/`: TypeScript SDK.
- `link/sdks/dart/`: Dart/Flutter SDK. `link/sdks/dart/lib/src/generated` is generated; regenerate and prepare the SDK with `link/sdks/dart/build.sh`.
- `link/kalam-link-dart/`: Rust bridge/native layer used by the Dart SDK.
- `pg/`: PostgreSQL extension workspace for `pg_kalam`; see `pg/pg_kalam.control`, `pg/src/`, `pg/crates/`, and `pg/tests/`.
- `benchv2/`: Benchmark harness, scenarios, templates, and results for performance work.
- `ui/`: Frontend/admin UI.
- `docs/`: Architecture, API, security, and operational documentation.
- `specs/`: Historical and active design specs by feature/phase.
- `docker/`: Container builds and local deployment layouts.

## Project Structure
backend/crates/
- kalamdb-api: HTTP/REST + WebSocket server surface, routes, UI asset serving.
- kalamdb-auth: Authentication/authorization (bcrypt, JWT, RBAC, guards).
- kalamdb-commons: Shared types, IDs, constants, utilities.
- kalamdb-configs: Server configuration structs and loaders.
- kalamdb-core: Core orchestration (DDL/DML handlers, jobs, live queries, schema registry).
- kalamdb-filestore: Filesystem + object-store (S3/GCS/Azure/local) Parquet segment lifecycle.
- kalamdb-observability: Metrics/telemetry helpers and system stats.
- kalamdb-publisher: Durable topic publishing (route matching, offset allocation, payload extraction), synchronous write-path integration.
- kalamdb-raft: Raft consensus, replication, and cluster coordination.
- kalamdb-session: Session context + permission-aware table provider abstraction.
- kalamdb-sharding: Shard models and routing logic.
- kalamdb-sql: SQL parsing and query planning helpers.
- kalamdb-store: RocksDB backend and storage abstractions; provides `EntityStore` and `IndexedEntityStore` (indexed store) with automatic secondary indexes.
- kalamdb-streams: Stream storage and commit log utilities.
- kalamdb-system: System tables + metadata providers (EntityStore/IndexedEntityStore-based), `TopicPublisher` trait.
- kalamdb-tables: User/shared/stream table providers built on `EntityStore`/`IndexedEntityStore`.

## Code Style

- **Rust 2021 edition**: Follow standard Rust conventions
- **Type-safe wrappers**: Use `NamespaceId`, `TableName`, `UserId`, `StorageId`, `TableType` enum, `UserRole` enum, `TableAccessLevel` enum instead of raw strings
- **Error handling**: Use `Result<T, KalamDbError>` for all fallible operations
- **Async**: Use `tokio` runtime, prefer `async/await` over raw futures
- **Logging**: Use `log` macros (`info!`, `debug!`, `warn!`, `error!`)
- **Serialization**: Use `serde` with `#[derive(Serialize, Deserialize)]`

## Build & Check Cadence (MUST)

- Prefer batching compilation feedback to avoid slow edit-run loops.
- When iterating on multi-file changes, run a single workspace-wide check and capture output to a file, fix all issues, then re-check:
  - Example: `cargo check > batch_compile_output.txt 2>&1`
  - Parse and address all errors/warnings in one pass; avoid running `cargo check` repeatedly after each tiny edit.
- If a task requires multiple related code changes, finish the full edit batch first and only then run `cargo check` or `cargo build` when validation is actually needed.
- Re-run `cargo check` only after a meaningful batch of fixes. This keeps feedback fast and focused, and prevents thrashing CI and local builds.

## Testing (MUST)

- Use `cargo nextest run` for all test executions unless explicitly told otherwise.
- For CLI e2e tests: run `cargo nextest run --features e2e-tests` **without** `--no-fail-fast`, capture output to a file, then fix failures one-by-one by running only the failing test(s). Re-run the full suite after fixes.
- For e2e test runs, do NOT pass `--no-fail-fast`. Run normally, fix the first failure, re-run until it passes, then move to the next failing issue.
- To verify the full core repo test matrix, start the KalamDB server first and then run `./scripts/test-all.sh` from the repo root. This script runs the Rust workspace tests, the feature-gated FDW import test, PostgreSQL extension e2e tests, the TypeScript SDK tests, the admin UI tests, and the Dart SDK tests.
- For performance-focused tests, benchmarks, and perf e2e cases, capture and report the runtime for each relevant test in seconds in the final update.
- Always add `#[ntest::timeout(time)]` to every async test where `time` is the **actual observed runtime** × 1.5 (to cover slower machines).
   - Example: if a test took 40s, set `#[ntest::timeout(60000)]`.
   - Recalculate and update timeouts after significant changes to test behavior or data size.
- Timeouts are guardrails, not the fix: do not increase a test timeout just because a test started failing. Fix the hang, race, or slow path first, then set the timeout from the measured healthy runtime × 1.5.

## Workflows & Commands (Documented)

- Backend build: `cd backend && cargo build`
- Backend run (default config): `cd backend && cargo run` (server on `http://127.0.0.1:8080`)
- Backend run (explicit binary): `cd backend && cargo run --bin kalamdb-server`
- Full test sweep: `cd /path/to/KalamDB && ./scripts/test-all.sh` (start the backend server first)
- Backend config bootstrap: `cd backend && cp server.example.toml server.toml`
- Create API key user: `cd backend && cargo run --bin kalamdb-server -- create-user --name "demo-user" --role "user"`
- Backend config via env vars: `KALAMDB_SERVER_PORT=9000 KALAMDB_LOG_LEVEL=debug cargo run`
- CLI build: `cd cli && cargo build --release` (binary at `cli/target/release/kalam`)
- CLI smoke tests with env vars: `KALAMDB_SERVER_URL="http://localhost:3000" KALAMDB_ROOT_PASSWORD="mypass" cargo test --test smoke -- --nocapture`
- Docker build and run: `cd docker/build && docker build -f Dockerfile -t jamals86/kalamdb:latest ../..` then `cd ../run/single && docker-compose up -d`

**Authentication Patterns**:
- **Password Security**: ALWAYS use `bcrypt::hash()` for password storage, NEVER store plaintext
- **Timing-Safe Comparisons**: Use `bcrypt::verify()` for constant-time password verification
- **JWT Claims**: Include `user_id`, `role`, `exp` (expiration) in token payload
- **Role Hierarchy**: system > dba > service > user (enforced in authorization middleware)
- **Generic Error Messages**: Use "Invalid username or password" (NEVER "user not found" vs "wrong password")
- **Soft Deletes**: Set `deleted_at` timestamp, return same error as invalid credentials
- **Authorization Checks**: Verify role permissions BEFORE executing database operations
- **Storage Abstraction**: Use `Arc<dyn StorageBackend>` instead of `Arc<rocksdb::DB>` (except in kalamdb-store)

## Security Review Checklist (MUST)

Always check these before shipping changes that touch APIs, auth, SQL, or storage:
1. SQL injection: ensure any internal SQL built from user input is parsed/parameterized and not string-concatenated into privileged queries.
2. Auth bypass: confirm every protected endpoint uses `AuthSessionExtractor` or verified JWT, not just header presence.
3. Role escalation: verify role claims are validated against DB and `AS USER`/impersonation paths are gated.
4. Flooding/bruteforce: ensure pre-auth rate limits are enabled and login/refresh endpoints are throttled.
5. System tables: confirm non-admin roles cannot read or mutate `system.*` tables (including via views).
6. Nested queries: confirm subqueries/UNION/VIEW cannot bypass system table guards.
7. Anonymous access: enumerate public endpoints and validate the data they return is safe.
8. File upload/download: validate paths, size limits, storage access checks, and cleanup on failure.

Suggested extra checks:
1. Token handling: rotation, expiry, and refresh scope for access vs refresh tokens.
2. Secrets in logs: ensure SQL redaction and auth events never log plaintext secrets.
3. CORS/Origin: verify WS/HTTP origin checks align with deployment model.

## Security Policies (MUST)

1. Health endpoints must be localhost-only unless explicitly authenticated and authorized.
2. Never treat `Authorization` header presence as authentication. Always validate tokens.
3. Auth endpoints must be IP rate-limited in addition to account lockout.
4. JWT secrets must be non-default and at least 32 chars; refuse startup on non-localhost if not.
5. Cookies carrying auth tokens must be `HttpOnly` and `SameSite=Strict`; `Secure` in production.
6. WebSocket origins must be validated against config or rejected when strict mode is enabled.
7. Admin or root password by default should be set or is set to kalamdb123 for testing or writting in tests, and the user should be admin

the folder: link/sdks/dart/lib/src/generated is generated dont modify anything in it, to regenerate this run the link/sdks/dart/build.sh script which also prepares the SDK artefacts
