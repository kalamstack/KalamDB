# KalamDB Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-03-23

## Active Technologies

- Rust 1.92+ (edition 2021) for backend and PostgreSQL extension crates + DataFusion 40.0, Apache Arrow 52.0, Apache Parquet 52.0, RocksDB 0.24, Actix-Web 4.4, tonic/prost for pg RPC transport, DashMap for concurrent registries (027-pg-transactions)

## Project Structure

```text
src/
tests/
```

## Commands

cargo test [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] cargo clippy

## Code Style

Rust 1.92+ (edition 2021) for backend and PostgreSQL extension crates: Follow standard conventions

## Recent Changes

- 027-pg-transactions: Added Rust 1.92+ (edition 2021) for backend and PostgreSQL extension crates + DataFusion 40.0, Apache Arrow 52.0, Apache Parquet 52.0, RocksDB 0.24, Actix-Web 4.4, tonic/prost for pg RPC transport, DashMap for concurrent registries

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
