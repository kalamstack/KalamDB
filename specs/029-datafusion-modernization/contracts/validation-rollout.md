# Contract: Validation and Rollout Evidence

## Purpose

Define the evidence required before a provider family can delete its legacy scan path.

## Required Query Classes

At minimum, validation evidence must cover:

- primary-key lookup
- selective read
- limit-heavy read
- count-star
- join-sensitive read
- system-table read
- view-backed read
- vector-backed read when applicable
- transaction-visibility read when applicable
- historical read with schema evolution when applicable

## Required Evidence Types

- Before-and-after explain output that shows the intended execution shape.
- Explain-analyze or equivalent evidence that the intended reductions and execution behaviors actually occurred.
- Regression results for permissions, transaction visibility, and query correctness.
- Targeted `nextest` coverage for the owning crates.
- CLI smoke coverage against a running backend.
- Benchmark evidence for latency, planning time, memory, and concurrent throughput, with runtimes reported in seconds where applicable.
- An interface audit showing zero remaining deprecated or obsolete query-engine usage on the targeted provider, planner, and execution surfaces.

## Release Gate

A provider family may delete its legacy MemTable-backed scan path only when:

1. its correctness suites pass,
2. its explain-shape evidence matches the intended modernized path,
3. its benchmark deltas are understood and accepted,
4. its interface audit is clean, and
5. its owner signs off that rollback is no longer needed.

## Non-Goals

- This contract does not require all provider families to land in one change.
- This contract does not require immediate migration beyond the current supported query-engine dependency line.