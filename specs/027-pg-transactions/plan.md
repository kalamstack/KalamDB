# Implementation Plan: PostgreSQL-Style Transactions for KalamDB

**Branch**: `027-pg-transactions` | **Date**: 2025-01-20 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/027-pg-transactions/spec.md`

## Summary

Add explicit `BEGIN`/`COMMIT`/`ROLLBACK` transaction support to KalamDB, enabling pg_kalam (PostgreSQL FDW) and the SQL REST endpoint to stage multi-statement writes with read-your-writes visibility and atomic commit. Transactions use snapshot isolation based on a global `commit_seq` counter. The `TransactionCoordinator` in `kalamdb-core` stages mutations in memory and commits atomically through a single `TransactionCommit` Raft command, preserving the existing write path (`DmlExecutor` → `_seq` generation → index updates → notifications → publisher events). Non-transactional autocommit path remains unchanged with near-zero overhead.

## Technical Context

**Language/Version**: Rust 1.90+ (edition 2021)
**Primary Dependencies**: DataFusion 40.0, Apache Arrow 52.0, RocksDB 0.24, Actix-Web 4.4, DashMap 5, serde 1.0, tokio 1.48
**Storage**: RocksDB for write path (<1ms), Parquet for flushed segments. Transaction staged writes are in-memory only until commit.
**Testing**: `cargo nextest run` for unit/integration tests; `cargo test --test smoke` for CLI smoke tests
**Target Platform**: Linux/macOS server (aarch64, x86_64)
**Project Type**: Database engine (multi-crate Rust workspace)
**Performance Goals**: Autocommit hot path must not regress >5%. Transaction commit latency target: <10ms for typical write sets (<1000 mutations). TransactionCoordinator presence check on non-transactional path: <100ns (single DashMap lookup).
**Constraints**: Transaction memory budget configurable (default 100MB). Transaction timeout configurable (default 300s). Single-node only in Phase 1 — no cross-Raft-group transactions.
**Scale/Scope**: Concurrent transactions bounded by memory budget. Affects 6 crates: `kalamdb-commons`, `kalamdb-configs`, `kalamdb-core`, `kalamdb-tables`, `kalamdb-pg`, `kalamdb-sql`, `kalamdb-api`. ~60 tasks across 8 phases.

## Architecture Decisions Summary

Key decisions from [research.md](research.md):

1. **Snapshot isolation via `commit_seq`** (Decision 1): Global monotonic counter, NOT per-row `_seq` Snowflake ID. Each commit increments `commit_seq`; transactions read `commit_seq <= snapshot_commit_seq`.
2. **Coordinator in `kalamdb-core`** (Decision 2): `TransactionCoordinator` with `Arc<AppContext>` dependency (Decision 7A), not `StorageBackend`.
3. **Commit through Raft** (Decision 7/7B): Single `TransactionCommit` Raft command for atomic apply through `DmlExecutor`. Never bypass to raw `StorageBackend::batch()`.
4. **Future-proof Raft commands** (Decision 7C): `Option<TransactionId>` in Raft data commands now to avoid protocol migration later.
5. **TransactionOverlayExec** (Decision 15): Custom DataFusion `ExecutionPlan` for overlay scan merging.
6. **DDL rejected in transactions** (Decision 16): Schema operations don't support rollback semantics.
7. **Type-safe TransactionId** (Decision 13): Newtype following established ID pattern in `kalamdb-commons/src/models/ids/`.
8. **Unified TransactionState** (Decision 14): Single enum in `kalamdb-commons` replacing pg crate's version.
9. **fdw_xact at XACT_EVENT_COMMIT** (Decision 8): Aligned with existing `fdw_xact.rs` code, not PRE_COMMIT.

## Critical Write Path

```
pg_kalam / SQL REST
    │
    ▼
TransactionCoordinator::stage()   ← buffers StagedMutation in memory
    │
    ▼ (on COMMIT)
TransactionCoordinator::commit()
    │
    ▼
UnifiedApplier::apply(TransactionCommit { tx_id, mutations })
    │
    ▼
RaftApplier → Raft consensus → state machine
    │
    ▼
CommandExecutorImpl::dml() → DmlExecutor
    │
    ▼
Table providers: _seq generation, index updates,
                 notifications, publisher events,
                 manifest updates, file refs
    │
    ▼
commit_seq++ (atomic), tag rows with new commit_seq
```

**CRITICAL**: Direct `StorageBackend::batch()` is NEVER used for transaction commit. It would bypass `_seq`, indexes, notifications, Raft replication, and publisher events.

## Constitution Check

*GATE: Passed*

- Model separation: Each transaction entity in its own file ✓
- AppContext-First: TransactionCoordinator depends on `Arc<AppContext>` ✓
- Type-safe IDs: `TransactionId` newtype in commons ✓
- Performance: Near-zero-cost for non-transactional path ✓
- Filesystem vs RocksDB separation: Transaction logic in `kalamdb-core`, not `kalamdb-store` ✓
- No SQL rewrite in hot paths: Overlay via DataFusion ExecutionPlan, not SQL rewrite ✓

## Project Structure

### Documentation (this feature)

```text
specs/027-pg-transactions/
├── plan.md              # This file
├── spec.md              # Feature specification (6 user stories, 21 FRs)
├── research.md          # 17 research decisions
├── data-model.md        # 7 entities + 2 enums + state transitions
├── quickstart.md        # 8 implementation scenarios
├── contracts/
│   ├── pg-transaction-rpc.md     # pg gRPC contract
│   └── sql-transaction-batch.md  # SQL REST contract
├── tasks.md             # ~60 tasks across 8 phases
└── checklists/
    └── requirements.md  # Requirements checklist
```

### Source Code (affected crates)

```text
backend/crates/
├── kalamdb-commons/src/models/
│   ├── ids/transaction_id.rs        # TransactionId newtype (NEW)
│   └── transaction.rs               # TransactionState, TransactionOrigin, OperationKind (NEW)
├── kalamdb-configs/src/lib.rs       # transaction_timeout_secs, max_buffer_bytes fields
├── kalamdb-core/src/
│   ├── transactions/                # NEW MODULE
│   │   ├── mod.rs
│   │   ├── coordinator.rs           # TransactionCoordinator (DashMap, commit_seq)
│   │   ├── handle.rs                # TransactionHandle
│   │   ├── staged_mutation.rs       # StagedMutation
│   │   ├── overlay.rs               # TransactionOverlay
│   │   ├── overlay_exec.rs          # TransactionOverlayExec (DataFusion ExecutionPlan)
│   │   ├── commit_result.rs         # TransactionCommitResult, TransactionSideEffects
│   │   └── metrics.rs               # ActiveTransactionMetric
│   ├── app_context.rs               # Wire TransactionCoordinator
│   ├── applier/                     # Add TransactionCommit Raft command variant
│   ├── operations/service.rs        # Route DML through coordinator when tx active
│   └── sql/executor/                # BEGIN/COMMIT/ROLLBACK handlers
├── kalamdb-tables/src/
│   ├── shared_tables/shared_table_provider.rs  # Overlay + snapshot filter
│   └── user_tables/user_table_provider.rs      # Overlay + snapshot filter
├── kalamdb-pg/src/
│   ├── service.rs                   # Wire RPC to coordinator
│   └── session_registry.rs          # Replace local TransactionState with commons
├── kalamdb-sql/src/
│   └── classifier/types.rs          # Already has transaction variants ✓
└── kalamdb-api/src/handlers/sql/execute.rs  # Request-end cleanup

pg/src/
└── fdw_xact.rs                      # Align Abort callback, keep XACT_EVENT_COMMIT

Tests:
├── backend/crates/kalamdb-pg/tests/             # pg transaction integration tests
├── backend/crates/kalamdb-core/tests/            # Core transaction tests
└── cli/tests/                                    # Smoke tests
```

## Complexity Tracking

No constitution violations requiring justification. All decisions follow established KalamDB patterns (AppContext dependency injection, type-safe IDs, Raft consensus for writes, DataFusion execution plans for query processing).

## Implementation Strategy

1. **MVP (Phase 1-3)**: pg_kalam atomic multi-statement transactions with read-your-writes. Deployable independently.
2. **Isolation (Phase 4)**: Snapshot isolation via `commit_seq` for production multi-user safety.
3. **SQL REST (Phase 5)**: Extends transactions to KalamDB's SQL surface. Parallel with Phase 3-4.
4. **Safety nets (Phase 6-7)**: Disconnect cleanup, timeout protection.
5. **Polish (Phase 8)**: Observability, performance regression tests, edge cases.

See [tasks.md](tasks.md) for the full task breakdown with dependency graph.
