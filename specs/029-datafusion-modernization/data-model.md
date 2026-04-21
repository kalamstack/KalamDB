# Data Model: DataFusion Modernization Cutover

## Overview

This feature does not introduce a new user-facing data model. Instead, it formalizes the execution-side entities that govern how KalamDB providers describe work to DataFusion, how historical reads are reduced safely, and how rollout completion is measured.

## Entity 1: Provider Family

**Purpose**: A group of KalamDB scan sources that share execution semantics and migration strategy.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | enum | Yes | `Stream`, `System`, `View`, `Vector`, `User`, `Shared` |
| `source_layout` | enum | Yes | `HotOnly`, `HotPlusCold`, `Computed`, `LogicalView`, `VectorIndexBacked`, or equivalent |
| `requires_mvcc_resolution` | bool | Yes | True for families whose visibility requires version resolution |
| `supports_incremental_execution` | bool | Yes | Whether the family can safely emit rows incrementally once execution starts |
| `supports_historical_reads` | bool | Yes | Whether the family participates in the cold Parquet path |
| `supports_special_fast_paths` | bool | Yes | Whether the family retains narrow fast paths such as PK lookup or count-only behavior |
| `migration_state` | enum | Yes | `LegacyMaterializing`, `ExecBacked`, `Validated`, `LegacyRemoved` |
| `owner_paths` | list of paths | Yes | Owning crate and module locations |

### Validation Rules

- Every targeted source belongs to exactly one provider family during rollout.
- A provider family that requires MVCC resolution cannot claim fully incremental execution if that would violate visibility correctness.
- A provider family reaches `LegacyRemoved` only after its validation evidence satisfies the rollout gates.

### State Transitions

| From | To | Trigger |
|------|----|---------|
| `LegacyMaterializing` | `ExecBacked` | Custom execution substrate integrated for the family |
| `ExecBacked` | `Validated` | Explain, regression, smoke, and benchmark gates pass |
| `Validated` | `LegacyRemoved` | Legacy MemTable-backed scan path deleted |

## Entity 2: Provider Capability Contract

**Purpose**: The declared set of safe source behaviors exposed to the planner and executor.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `filter_capabilities` | map | Yes | Per-filter support classified as `Exact`, `Inexact`, or `Unsupported` |
| `projection_reduction` | bool | Yes | Whether the source can avoid reading unneeded columns |
| `nested_field_reduction` | bool | Yes | Whether the source can avoid reading unused subfields |
| `limit_reduction` | bool | Yes | Whether the source can stop early without violating semantics |
| `historical_pruning_levels` | set | No | `Partition`, `File`, `RowGroup`, `Page`, or equivalent |
| `ordering_guarantee` | optional descriptor | No | Known output ordering |
| `partitioning_guarantee` | optional descriptor | No | Known output partitioning |
| `statistics_level` | enum | Yes | `None`, `GlobalOnly`, `PartitionAware`, or equivalent |
| `blocking_work_mode` | enum | Yes | `None`, `Isolated`, `Unknown` |

### Validation Rules

- `Exact` may be declared only when the source guarantees no false rows survive for that predicate.
- `Inexact` may be declared only when the source measurably reduces source work and a later correctness stage still exists where needed.
- `limit_reduction` may be enabled only when early termination preserves query semantics.
- `blocking_work_mode` must never allow blocking work to run unbounded in the shared async runtime.

## Entity 3: Scan Execution Descriptor

**Purpose**: The lightweight execution-oriented description emitted during planning for a scan source.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `schema` | schema reference | Yes | Output schema seen by downstream operators |
| `projection` | optional column set | No | Requested projected columns or fields |
| `filters` | list | No | Planner-provided filters retained for execution |
| `limit` | optional integer | No | Requested row cap |
| `boundedness` | enum | Yes | Whether the result is bounded |
| `output_partitioning` | descriptor | Yes | Declared partition layout |
| `output_ordering` | optional descriptor | No | Declared ordering guarantees |
| `statistics` | descriptor | Yes | Source statistics available to planning and execution |
| `preserve_order` | bool | Yes | Whether execution must preserve source order for correctness |
| `overlay_mode` | enum | No | Whether transaction overlay composition is required |

### Validation Rules

- Planning produces the descriptor without doing the underlying source work.
- Declared partitioning, ordering, and statistics must match the execution behavior that actually follows.
- Any descriptor that enables early termination or pruning must carry the correctness conditions needed to enforce it.

### State Transitions

| From | To | Trigger |
|------|----|---------|
| `Planned` | `Executing` | Execution begins for one or more partitions |
| `Executing` | `Streaming` | Source work begins producing batches |
| `Streaming` | `Completed` | All partitions finish successfully |
| `Streaming` | `Cancelled` | Query cancellation or early stop |
| `Streaming` | `Failed` | Execution error |

## Entity 4: Historical Selection Plan

**Purpose**: The set of historical storage units that remain eligible after safe cold-path reduction.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `partition_candidates` | set | No | High-level partition selections, if applicable |
| `file_candidates` | set | Yes | Candidate files after coarse pruning |
| `row_group_candidates` | set | No | Candidate row groups after metadata pruning |
| `fully_matching_groups` | set | No | Groups proven to satisfy the predicate entirely |
| `required_columns` | set | Yes | Columns or fields required for correctness and projection |
| `schema_version_map` | map | Yes | Mapping of selected historical units to schema versions |
| `preserve_order` | bool | Yes | Whether pruning must preserve original read order |
| `limit_target` | optional integer | No | Requested limit carried into cold-path execution |
| `pruning_evidence` | metrics descriptor | Yes | Recorded pruning results for explain and benchmark evidence |

### Validation Rules

- Any pruned historical unit must be provably irrelevant for the query.
- `fully_matching_groups` may be used for early termination only when `preserve_order` and other semantic conditions allow it.
- `required_columns` must include all fields needed for correctness, including compatibility or visibility columns.

## Entity 5: Validation Baseline

**Purpose**: The before-and-after evidence set used to judge whether the modernization is successful.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `query_class` | enum | Yes | `PKLookup`, `SelectiveRead`, `LimitRead`, `CountStar`, `Join`, `SystemRead`, `ViewRead`, `VectorRead`, or equivalent |
| `dataset_profile` | descriptor | Yes | Data volume and layout used for validation |
| `explain_signature` | descriptor | Yes | Expected plan shape or key operators |
| `latency_metrics` | set | Yes | Planning and execution latency baselines |
| `memory_metrics` | set | Yes | Peak or sampled memory measures |
| `throughput_metrics` | set | No | Mixed workload throughput or concurrency results |
| `correctness_suites` | set | Yes | Required regression, permission, and transaction suites |
| `interface_audit_status` | enum | Yes | `Pending`, `Clean`, `Blocked` |

### Validation Rules

- Every migrated provider family must map to one or more validation baselines before legacy deletion.
- A provider family cannot pass rollout gates while `interface_audit_status` is not `Clean`.
- Success measurements are compared to phase-0 baselines rather than to undefined expectations.

## Relationship Rules

- Each `Provider Family` exposes one primary `Provider Capability Contract` during a rollout stage.
- Each planning event for a `Provider Family` produces one or more `Scan Execution Descriptor` instances.
- A `Scan Execution Descriptor` for historical data may reference one `Historical Selection Plan`.
- Each `Provider Family` must be covered by at least one `Validation Baseline` before `LegacyRemoved`.