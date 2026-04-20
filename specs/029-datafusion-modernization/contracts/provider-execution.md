# Contract: Provider and Execution Responsibilities

## Purpose

Define the internal contract for modernized KalamDB scan sources so planning, execution, and streaming each own the right work.

## Planning Contract

- Targeted production scan sources must keep planning lightweight.
- Planning may inspect metadata needed to describe work, but it must not perform the full underlying source read.
- Planning must preserve requested projection, filters, and limit information for execution.
- Planning must expose source metadata that the optimizer can trust, including boundedness and any real partitioning or ordering guarantees.

## Execution Contract

- A modernized scan source returns an execution description instead of a fully materialized result batch.
- Execution descriptions own immutable plan metadata through the current supported query-engine interface set for the selected dependency line.
- Execution descriptions surface statistics and partition-aware statistics where meaningful.
- If a source can satisfy different partition counts safely, it may participate in partition-aware execution rather than always forcing a single opaque partition.

## Streaming Contract

- Execution setup remains lightweight; the stream owns actual source I/O and data production.
- Incremental sources emit batches as they become available.
- Sources that still need blocking work isolate that work from the shared async runtime.
- Streams must stop cleanly on cancellation or safe early termination.

## Capability Contract

- Provider capability reporting must distinguish:
  - `Exact`: the source guarantees the predicate is fully enforced before later operators run.
  - `Inexact`: the source reduces work but later correctness filtering may still be required.
  - `Unsupported`: the source does not help with that predicate.
- Providers must not claim stronger capabilities than the source can prove.
- Source-level limit reduction is allowed only when it preserves query semantics.

## Prohibited Behaviors On Targeted Production Paths

- Full source I/O or materialization during planning.
- Hiding real source behavior behind a synthetic MemTable wrapper for production scan execution.
- Leaving deprecated or obsolete query-engine interface usage on targeted provider, planner, or execution surfaces after rollout completion.

## Rollout Notes

- The contract applies to stream tables, system tables, views, vector search, and eventually MVCC-backed user and shared tables.
- Provider families may use different internal source models as long as they satisfy this contract and preserve query semantics.