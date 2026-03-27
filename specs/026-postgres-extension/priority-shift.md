# Priority Shift and Scope Status

## Date

2026-03-21

## Decision

Work under `specs/026-postgres-extension` is now **remote-first**.

The main priority is delivering the PostgreSQL extension as a client of a **remote KalamDB server**.

Embedded mode remains documented only because some progress already exists there. It is now treated as:

* temporary compatibility scope
* useful for preserving current work while remote mode is designed
* a candidate for future removal

## What Changed

Previous direction:

* embedded mode was receiving active design attention
* the folder described remote and embedded mode more symmetrically

Current direction:

* remote mode is the main target
* documents in this folder should optimize for remote execution semantics
* embedded mode should only reuse the same contracts where possible and should not drive new design decisions
* earlier dual-mode and embedded-heavy design work should remain preserved as historical reference, not discarded
* backend implementation should prefer a dedicated `kalamdb-pg` crate and keep `kalamdb-server` wiring minimal
* the PostgreSQL extension should stay thin, with session/schema propagation handled through shared request/session contracts
* remote connectivity must be proven first through a minimal authenticated gRPC handshake between the extension client and the backend `kalamdb-pg` service

## Practical Rules For This Folder

When editing or extending this spec set:

1. write requirements against remote mode first
2. keep extension-to-Kalam interfaces transport-friendly and typed
3. only mention embedded mode when documenting compatibility or migration concerns
4. avoid new embedded-only features unless required to preserve already-built work during the transition
5. assume embedded mode may be deleted later without changing the public PostgreSQL FDW SQL surface
6. prefer transport/auth patterns already used by cluster gRPC where that reduces new infrastructure, but do not reuse cluster-only peer authorization semantics for PostgreSQL clients

## Impact On Companion Specs

### `README.md`

Must describe the architecture as remote-first and explicitly de-prioritize embedded-only exploration.

### `legacy-dual-mode-reference.md`

Preserves the earlier detailed design so prior work stays available for reference and comparison.

### `transactional-fdw.md`

Must define transaction semantics around a remote transaction handle carried between the FDW and the KalamDB server.

### `direct-entitystore-dml.md`

Must define direct typed mutation handling on the KalamDB side without routing FDW-originated writes back through generic SQL execution.

## Remote connectivity milestone

Before broader remote scan and DML support is considered complete, this spec set expects:

* a lightweight PG remote tonic service in `backend/crates/kalamdb-pg`
* a lightweight remote client in `kalam-pg-client`
* authorization metadata on each request
* explicit remote session open/reuse with current schema propagation
* at least one passing connectivity test proving successful authenticated connection end to end

## Deferred / Transitional Embedded Notes

Embedded mode can remain temporarily if it helps preserve work already done, but it should satisfy these constraints:

* it reuses the same typed request/response boundary when practical
* it does not become a blocker for remote mode
* it does not force long-term public configuration or API shape

## Exit Condition

This priority shift is complete when the documents in this folder can be read top-to-bottom without implying that embedded mode is the primary or equal-priority architecture.
