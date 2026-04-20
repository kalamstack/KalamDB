# Contract: Cold-Path Historical Pruning

## Purpose

Define how KalamDB historical reads reduce cold-path work while preserving correctness.

## Reduction Layers

Historical reads may reduce work at any safe layer, including:

- partition-level or equivalent coarse selection
- file-level selection
- row-group-level selection
- page-level or equivalent finer-grained selection
- narrow projection and nested-field reduction

Each later layer may only remove data that the system can prove is irrelevant to the query.

## Ordering and Correctness Rules

- Reduction must preserve the query’s visible semantics.
- If ordering or visibility rules make aggressive reduction unsafe, the system must fall back to a correct less-aggressive path.
- Historical schema evolution must remain transparent to users; reduced reads still return the same visible result set as the baseline path.

## Limit-Aware Rules

- Limit-aware pruning is permitted only when the system can prove that early termination will not change the result semantics.
- If the source can prove that enough guaranteed matches exist, it may stop reading additional historical data.
- If those proof conditions are absent, the limit is enforced without unsafe early termination.

## Evidence Requirements

- Historical pruning must produce explainable evidence showing which reduction stages were applied.
- Validation must be able to distinguish baseline full-read behavior from reduced historical read behavior.
- Benchmarks for limit-heavy and selective historical reads must show the amount of cold-path work avoided relative to the baseline.

## Scope Notes

- Manifest-driven selection and schema evolution remain part of the supported cold-path contract.
- A deeper migration to a more native file-scan integration is optional and must be justified by benchmark evidence rather than assumed up front.