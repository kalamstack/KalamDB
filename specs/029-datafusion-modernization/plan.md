# Implementation Plan: DataFusion Modernization Cutover

**Branch**: `029-datafusion-modernization` | **Date**: 2026-04-20 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/029-datafusion-modernization/spec.md`

## Summary

Modernize KalamDB against the current DataFusion 53.x family by replacing scan-time batch materialization with lightweight provider planning and custom execution plans and streams, so DataFusion can optimize the real source instead of a synthetic in-memory wrapper. The implementation is a large but phased refactor: freeze baselines, extract a shared scan execution substrate into a new `kalamdb-datafusion-sources` crate that centralizes provider base traits for user, shared, stream, view, vector, and system sources (eliminating the current duplication across `kalamdb-tables`, `kalamdb-system`, `kalamdb-views`, `kalamdb-vector`, and `kalamdb-transactions`), cut over non-MVCC providers first, tighten pushdown and metadata, redesign cold Parquet execution, move MVCC version-resolution work into streaming execution plans (replacing the HashMap-materializing `merge_versioned_rows`/`resolve_latest_kvs_from_cold_batch` in `kalamdb-tables/src/utils/version_resolution.rs` with a lazy, slice-oriented merge driven by `(commit_seq, seq_id)` that preserves zero-copy Arrow buffers where possible and aligns with the new source substrate), modernize DML and transaction overlays, tune session and planner behavior for low per-user memory and high concurrency, then remove legacy MemTable-backed scan paths only after correctness and performance validation. The plan explicitly includes removal of deprecated or obsolete query-engine interfaces from the targeted provider, planner, and execution surfaces, with no backward-compatibility shims retained.

## Technical Context

**Language/Version**: Rust 1.92+ (edition 2021) across backend crates and CLI  
**Primary Dependencies**: DataFusion 53.1.0 (`datafusion`, `datafusion-datasource`, `datafusion-common`, `datafusion-expr`), Arrow 58.1.0, Parquet 58.1.0, object_store 0.13.2, tokio 1.51, RocksDB 0.24, Actix-Web 4.13, moka plan cache  
**Storage**: RocksDB hot path plus manifest-directed Parquet cold storage via `kalamdb-filestore`, `StorageCached`, and `ManifestAccessPlanner`  
**Testing**: Batched `cargo check`, `cargo nextest run`, CLI end-to-end validation via `cli/run-tests.sh`, targeted `EXPLAIN` and `EXPLAIN ANALYZE` validation, and `benchv2` or focused performance harnesses with runtime reporting in seconds  
**Target Platform**: macOS/Linux backend server, CLI validation environment, single-node and cluster-aware runtime paths  
**Project Type**: Multi-crate Rust database engine plus CLI and benchmark harness  
**Performance Goals**: Deliver the spec targets: at least 30% better median latency for representative selective reads, at least 50% better latency for representative small-result queries, at least 2x higher concurrent read capacity at the same latency or memory threshold, materially lower planning overhead for repeatedly planned queries, and lower peak per-query memory for targeted lightweight-readable classes  
**Constraints**: Stay on the current supported DataFusion 53.x family during this effort; preserve query semantics, permissions, MVCC visibility, and transaction behavior; avoid planning-time I/O and full source materialization; avoid unrelated auth, SQL parser, or user-facing feature work; keep filesystem logic in `kalamdb-filestore` and storage-engine logic in `kalamdb-store`; batch validation and compile feedback per repo guidance  
**Scale/Scope**: Cross-crate refactor covering `kalamdb-core`, `kalamdb-session-datafusion`, `kalamdb-tables`, `kalamdb-system`, `kalamdb-views`, `kalamdb-vector`, `kalamdb-transactions`, `kalamdb-filestore`, benchmark coverage, and final CLI/server validation

## Cross-Cutting Conventions

- `TableProvider::scan()` remains a planning-only surface for targeted providers and must not perform source I/O or full batch materialization.
- Heavy source work moves to `ExecutionPlan::execute()` setup and stream polling; `execute()` itself stays lightweight.
- Provider capability reporting must be precise: `Exact`, `Inexact`, and `Unsupported` are used only when justified by real source behavior.
- Blocking source work must not stall the shared async query runtime; blocking work is offloaded appropriately.
- Query semantics, permission checks, transaction visibility, and MVCC correctness take priority over aggressive optimization.
- The modernized provider stack uses only the current supported query-engine interfaces for the selected dependency line; no targeted surface may retain stale compatibility-only usage by rollout completion.
- Cold-path ownership remains split along existing repo boundaries: file/object-store access and Parquet file lifecycle stay in `kalamdb-filestore`, while provider integration and execution orchestration remain in owning crates.
- Provider base traits, execution nodes, streams, pruning descriptors, and statistics live in the new `kalamdb-datafusion-sources` crate so user, shared, stream, view, vector, and system providers consume one implementation instead of duplicating scan scaffolding.
- MVCC hot+cold merge uses the `(commit_seq, seq_id)` ordering defined in `kalamdb-tables/src/utils/version_resolution.rs::prefers_version` and MUST be implemented as a lazy, slice-oriented execution operator that preserves zero-copy Arrow buffers where possible instead of rebuilt `HashMap<String, _>` tables per scan; the new execution substrate is the single place this merge lives.
- Per-user session footprint is a first-class constraint: avoid cloning `SessionState` or its `SessionConfig` options on every request, keep `SessionUserContext` injection allocation-free on the hot path, and share immutable catalog/runtime state through `Arc` so concurrent users do not pay per-session memory.
- Zero-copy is preferred throughout the source substrate: projections, filter evaluation, and version merging must operate on shared `ArrayRef`/`RecordBatch` slices and `Arc`-wrapped schemas rather than materializing owned copies, and must not widen ownership of hot-path byte buffers.
- Keep the new crate narrow and organized: it owns reusable descriptors, capability reporting, exec nodes, stream adapters, pruning, and stats, but it must not become a second planning framework layered over DataFusion.
- Prefer upstream DataFusion, Arrow, Parquet, and object_store primitives over bespoke abstractions whenever those libraries already expose the needed behavior.
- Tests are divided by ownership: contract tests live in `kalamdb-datafusion-sources`, provider and session regressions live in the owning backend crates, CLI smoke tests validate cross-crate behavior, and `benchv2` owns performance and allocation evidence.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`/Users/jamal/git/KalamDB/.specify/memory/constitution.md` still contains the stock placeholder template, so the effective gates for this plan come from `AGENTS.md`, the attached Copilot instructions, and the explicit repo architecture and testing rules.

### Pre-Phase 0 Gate Review

- `PASS`: The plan is performance-first and directly targets the root cause called out in the repo and upstream guidance: planning-time materialization and hidden source work.
- `PASS`: The design stays within existing owning crates and boundaries. Filesystem and Parquet file access remain in `kalamdb-filestore`; key-value engine concerns remain in `kalamdb-store`; orchestration remains in core, tables, system, views, vector, and transactions crates.
- `PASS`: The plan does not introduce a new SQL rewrite lane or alternate query side-lane; it modernizes provider and execution boundaries instead.
- `PASS`: No new dependency is required by the plan. The work standardizes on the current DataFusion 53.1.0 family already present in the workspace.
- `PASS`: Validation is explicitly batched and ends with targeted `nextest`, CLI smoke validation, and performance measurement, matching repo guidance.
- `PASS`: The plan preserves type-safe boundaries and existing auth or role semantics rather than widening raw-string or ad hoc interfaces.

### Post-Phase 1 Re-Check

- `PASS`: The research, data model, and contracts keep the core modernization on execution-model changes rather than leaking into unrelated user-facing feature scope.
- `PASS`: The design artifacts preserve the storage boundary by keeping cold-path file mechanics in `kalamdb-filestore` and provider integration in the owning crates.
- `PASS`: The design artifacts explicitly require supported-interface cleanup and forbid rollout completion while deprecated or obsolete query-engine usage remains on targeted surfaces.
- `PASS`: The design artifacts keep correctness gates ahead of legacy-path deletion by requiring explain-shape, regression, smoke, and benchmark evidence before old scan paths are removed.

## Project Structure

### Documentation (this feature)

```text
specs/029-datafusion-modernization/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── cold-path-pruning.md
│   ├── provider-execution.md
│   └── validation-rollout.md
└── tasks.md
```

### Source Code (repository root)

```text
Cargo.toml
backend/crates/
├── kalamdb-core/src/sql/
├── kalamdb-core/src/sql/context/
├── kalamdb-session-datafusion/src/
├── kalamdb-tables/src/
│   ├── manifest/
│   ├── shared_tables/
│   ├── stream_tables/
│   ├── user_tables/
│   └── utils/
├── kalamdb-system/src/providers/
├── kalamdb-transactions/src/
├── kalamdb-vector/src/sql/
├── kalamdb-views/src/
└── kalamdb-filestore/src/parquet/
benchv2/src/
cli/
```

**Structure Decision**: Keep the existing multi-crate backend layout and add one new workspace crate, `backend/crates/kalamdb-datafusion-sources`, that owns the shared provider base traits, execution nodes, streams, pruning descriptors, and statistics. This is the deduplication seam that lets user, shared, stream, view, vector, and system providers consume a single implementation instead of reimplementing scan scaffolding per crate. The crate stays intentionally narrow through internal module boundaries (`provider`, `exec`, `stream`, `pruning`, `stats`) and builds on DataFusion/Arrow/Parquet primitives instead of inventing a second framework. All other work lands in the owning crates: `kalamdb-tables`, `kalamdb-system`, `kalamdb-views`, `kalamdb-vector`, and `kalamdb-transactions` consume the shared crate, while `kalamdb-core`, `kalamdb-session-datafusion`, and `kalamdb-filestore` adopt the new contracts where they own planning, execution, overlay, session shaping, or cold-path behavior. No backward-compatibility wrappers are retained in either the existing crates or the new crate.

## Complexity Tracking

No constitution exceptions or complexity waivers are required at planning time.
