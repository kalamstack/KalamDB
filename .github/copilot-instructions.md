# KalamDB Development Guidelines

**⚠️ PRIMARY DOCUMENTATION**: See [AGENTS.md](../AGENTS.md) in the root directory for complete development guidelines.

This file references the main `AGENTS.md` which contains:
- System Table Models Architecture
- Dependency Management (workspace dependencies)
- Active Technologies
- Project Structure
- SDK Architecture Principles
- Commands and Code Style
- Recent Changes

Always refer to `AGENTS.md` for the most up-to-date guidelines and best practices.

Priority reminders from `AGENTS.md`:
- Keep performance as a primary constraint, including compile-time and hot-path costs.
- Batch related edits before running `cargo check` or `cargo build`.
- For performance tests, report per-test runtime in seconds.
- When adding dependencies, enable only the minimal required features.

## Active Technologies
- Rust 1.90 (edition 2021) + DataFusion 40, Apache Arrow 52, Apache Parquet 52, Actix-Web 4, `kalamdb-store` EntityStore traits, `kalamdb-commons` system models (007-user-auth)
- RocksDB 0.24 for buffered writes, Parquet files for flushed segments via StorageBackend abstraction (007-user-auth)
- Rust 1.90+ (stable toolchain, edition 2021) + Apache Arrow 52.0, Apache Parquet 52.0, DataFusion 40.0, RocksDB 0.24, Actix-Web 4.4, serde 1.0, bincode, DashMap (lock-free concurrent HashMap) (008-schema-consolidation)
- RocksDB 0.24 for EntityStore backend, Parquet files for flushed segments via StorageBackend abstraction (008-schema-consolidation)
- Rust 1.90+ (stable toolchain, edition 2021) + DataFusion 40.0, Apache Arrow 52.0, Apache Parquet 52.0, RocksDB 0.24, DashMap (lock-free concurrent HashMap) (010-core-architecture-v2)
- RocksDB for write path (<1ms), Parquet for flushed storage (compressed columnar format) (010-core-architecture-v2)
- Rust 1.90+ (edition 2021) + DataFusion 40.0, Apache Arrow 52.0, Apache Parquet 52.0, Actix-Web 4.4, sqlparser-rs (via DataFusion), tokio 1.48, serde 1.0 (011-sql-handlers-prep)
- RocksDB 0.24 (write path), Parquet files (flushed segments) (011-sql-handlers-prep)
- Rust 1.90 (edition 2021) + DataFusion 40, Apache Arrow 52, Apache Parquet 52, RocksDB 0.24, Actix-Web 4.4, serde 1.0, DashMap 5 (012-full-dml-support)
- RocksDB fast path with Parquet batch files on filesystem/S3 for flushed segments (012-full-dml-support)
- Rust 1.90 (edition 2021) for backend; TypeScript (ES2020+) for Kalam-link SDK + Actix-Web 4 for HTTP/WebSocket handling; existing KalamDB live query infrastructure in `kalamdb-core`; Kalam-link TypeScript client stack; RocksDB and Parquet via existing storage abstractions (014-live-queries-websocket)
- Existing RocksDB-backed write path and Parquet segment storage; `system.live_queries` system table backed by current metadata mechanisms (014-live-queries-websocket)
- Rust 1.90+ (backend) + TypeScript 5.x / React 18 (frontend) (015-admin-ui)
- Existing RocksDB + Parquet (no new storage needed) (015-admin-ui)

## Recent Changes
- 007-user-auth: Added Rust 1.90 (edition 2021) + DataFusion 40, Apache Arrow 52, Apache Parquet 52, Actix-Web 4, `kalamdb-store` EntityStore traits, `kalamdb-commons` system models
