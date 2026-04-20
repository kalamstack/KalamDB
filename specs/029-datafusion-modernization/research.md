# Phase 0 Research: DataFusion Modernization Cutover

## Decision 1: Freeze the effort on the current DataFusion 53.1.x family

**Decision**: Treat DataFusion 53.1.x and the matching Arrow/Parquet 58.1.x line as the fixed compatibility target for this refactor, and standardize the targeted provider, planner, and execution surfaces on the current supported interfaces in that line.

**Rationale**: The repo is already on DataFusion 53.1.0. Combining a large execution-model rewrite with another dependency-line upgrade would blur root-cause attribution for regressions and slow validation.

**Alternatives considered**:

- Upgrade beyond 53.x during the same effort: rejected because it mixes API churn with architectural change.
- Keep the current architecture and only do a dependency audit: rejected because the main performance problem is planning-time materialization, not just stale API usage.

## Decision 2: Use a custom execution-plan-first migration strategy

**Decision**: Replace MemTable-backed production scan paths with lightweight provider planning and custom `ExecutionPlan` plus stream implementations as the primary migration path.

**Rationale**: The upstream table-provider guidance is explicit that `scan()` should stay lightweight and that work belongs in execution and stream layers. This aligns with KalamDB’s biggest current gap and gives DataFusion visibility into real source behavior.

**Alternatives considered**:

- Keep MemTable wrappers and only optimize inside them: rejected because it still hides source properties from DataFusion and keeps planning-time work too heavy.
- Rewrite immediately to full DataFusion datasource or file-format integration: rejected as a first move because it is a much larger cutover than needed to recover most benefits.

## Decision 3: Split provider families by the source model that matches their semantics

**Decision**: Migrate each provider family to the source model that best matches its semantics instead of forcing them all through one generic wrapper.

**Rationale**: Upstream guidance highlights that not all sources should use the same abstraction. KalamDB has naturally incremental sources, computed sources, view-like sources, vector-result sources, and MVCC-backed sources with very different behavior.

**Alternatives considered**:

- One execution model for every provider family: rejected because it either overfits simple sources or under-serves complex ones.
- Delay source-model selection until implementation: rejected because it makes the migration sequence ambiguous and harder to validate.

## Decision 4: Treat filter pushdown as a correctness contract, not a marketing signal

**Decision**: Replace blanket pushdown declarations with precise `Exact`, `Inexact`, or `Unsupported` reporting and only claim source-side handling when it truly eliminates I/O or rows before later operators run.

**Rationale**: DataFusion already applies filters efficiently. Provider pushdown is only valuable when the source can do strictly better, such as partition, file, row-group, or index elimination.

**Alternatives considered**:

- Keep generic `Inexact` for most providers: rejected because it overstates capability and reduces plan clarity.
- Claim `Exact` aggressively for convenience: rejected because it risks correctness regressions.

## Decision 5: Make source metadata first-class in execution plans

**Decision**: Execution plans created by modernized providers must surface real plan properties, including partitioning, ordering, boundedness, and statistics, and must implement `partition_statistics` where meaningful.

**Rationale**: DataFusion 53 improves planning performance and optimizer effectiveness when immutable plan metadata is cheap to clone and trustworthy. KalamDB currently leaves these benefits on the table by returning synthetic or low-information scan plans.

**Alternatives considered**:

- Defer metadata until after provider cutovers: rejected because it would hide whether the architecture change is actually giving DataFusion better planning inputs.
- Only provide row-count statistics: rejected because partitioning and ordering are also key optimizer signals.

## Decision 6: Keep the cold Parquet path KalamDB-owned in the first stage

**Decision**: Preserve KalamDB-owned manifest selection, schema evolution, and cold-path orchestration, but move the cold Parquet path from batch materialization in `scan()` to execution-time Parquet scan plans and streams.

**Rationale**: KalamDB already owns valuable manifest-aware behavior. The first-stage goal is to stop collapsing cold-path reads into a materialized batch, not to discard all existing cold-path knowledge.

**Alternatives considered**:

- Immediate ListingTable/FileFormat migration: rejected as a first stage because it adds complexity before verifying that a KalamDB-owned Parquet exec cannot meet the targets.
- Leave the cold path as-is and modernize only hot-path providers: rejected because cold historical reads are central to the spec stories.

## Decision 7: Add multi-layer pruning, then add limit-aware pruning only where safe

**Decision**: Modernize historical reads so pruning can occur at every safe level, then add limit-aware pruning only for order-insensitive cases where the system can prove that enough guaranteed matches exist.

**Rationale**: The upstream pruning guidance shows that the big gains come from layered reduction, and the limit-pruning guidance makes clear that `LIMIT`-aware pruning is only safe when order is not being changed.

**Alternatives considered**:

- Implement limit-aware pruning before broader historical pruning: rejected because the proof signals needed for safe limit pruning depend on the earlier pruning machinery.
- Apply limit-aware pruning unconditionally: rejected because it can change query behavior when ordering matters.

## Decision 8: Preserve MVCC correctness by moving MVCC work into execution, not by weakening it

**Decision**: User and shared table modernization will move hot-plus-cold merge, version resolution, tombstone filtering, and snapshot visibility into execution plans while preserving PK lookup and count-only fast paths.

**Rationale**: MVCC-backed sources cannot simply be treated as naive stream sources. The architecture needs to move the work later, not remove the work.

**Alternatives considered**:

- Force MVCC sources into a purely incremental stream model: rejected because it risks returning rows before visibility resolution is complete.
- Leave MVCC sources on the legacy path permanently: rejected because it would leave the most important user-facing tables outside the modernization.

## Decision 9: Use the modernized stack to remove stale query-engine usage at the same time

**Decision**: Treat provider, planner, and execution modernization as the moment to eliminate deprecated or obsolete query-engine interface usage from targeted surfaces and standardize the modernized stack on the current supported DataFusion 53.x interfaces.

**Rationale**: A large provider and execution refactor that leaves stale compatibility usage behind would guarantee more cleanup later. The modernization should end on one supported surface, not two.

**Alternatives considered**:

- Defer interface cleanup to a later pass: rejected because the relevant surfaces are already being rewritten.
- Treat API cleanup as purely mechanical: rejected because it needs to be validated alongside plan-shape and execution changes.

## Decision 10: Use `TransactionOverlayExec` as the reference pattern, not the final abstraction

**Decision**: Reuse the existing `TransactionOverlayExec` design for property ownership, child-plan composition, and stream wrapping, but do not assume its current full-batch merge pattern is sufficient for every modernized source.

**Rationale**: It already demonstrates modern `Arc<PlanProperties>` handling and `RecordBatchStreamAdapter`, but the new scan substrate needs to support a broader range of execution behaviors and source metadata.

**Alternatives considered**:

- Copy `TransactionOverlayExec` wholesale into every provider family: rejected because it would preserve too much batch-materializing behavior.
- Ignore the existing exec implementation and start from scratch: rejected because it would discard a good in-repo reference.

## Decision 11: Blocking work must be explicit and isolated

**Decision**: When a modernized source still needs blocking I/O or long-running CPU work, that work is isolated from the shared async query runtime and surfaced through bounded execution streams.

**Rationale**: The upstream provider guidance is explicit that blocking work should not run directly in the async runtime. This is also consistent with the repo’s async guidance.

**Alternatives considered**:

- Let source implementations block in `execute()` or while holding async runtime threads: rejected because it harms concurrency and hides runtime contention.
- Force all work into fully async form regardless of cost: rejected because some existing storage or decoding paths may remain blocking in the first cutover.

## Decision 12: Validation evidence is part of the design, not an afterthought

**Decision**: The rollout is gated on recorded `EXPLAIN` and `EXPLAIN ANALYZE` evidence, targeted regression suites, CLI smoke validation, and explicit latency, memory, and concurrency baselines for representative query classes.

**Rationale**: The feature is large, performance-driven, and semantics-sensitive. It must prove both correctness and measurable improvement before legacy paths are deleted.

**Alternatives considered**:

- Rely only on unit and integration tests: rejected because they cannot prove plan-shape or performance outcomes.
- Delay baseline capture until implementation is underway: rejected because it weakens before-and-after comparisons.