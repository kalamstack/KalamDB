# Rust Method Map

Generated from repo-wide Rust function discovery using ripgrep.

- Rust source files indexed: 1331
- Function definitions indexed: 11725
- Impl blocks indexed: 1743

## Crate Summary

| Crate | Function defs | Top modules |
| --- | ---: | --- |
| kalamdb-core | 1309 | tests (206), src (136), manager (90) |
| kalamdb-commons | 1297 | ids (341), schemas (160), src (153) |
| link | 1214 | src (226), src (178), tests (164) |
| cli | 1212 | src (255), common (187), cluster (92) |
| kalamdb-raft | 710 | applier (112), manager (110), state_machine (110) |
| kalamdb-system | 669 | models (85), src (67), tables (59) |
| backend | 642 | testserver (161), src (77), auth (73) |
| kalamdb-dialect | 556 | ddl (323), src (93), parser (88) |
| kalamdb-tables | 524 | utils (201), shared_tables (86), user_tables (79) |
| benchv2 | 407 | benchmarks (313), src (86), reporter (8) |
| pg | 338 | src (139), e2e_common (64), e2e_dml (44) |
| kalamdb-jobs | 298 | executors (206), src (57), jobs_manager (35) |
| kalamdb-store | 263 | src (245), index (18) |
| kalamdb-pg | 258 | src (140), tests (97), support (21) |
| kalamdb-handlers | 247 | table (46), namespace (40), storage (39) |
| kalamdb-configs | 223 | config (215), src (8) |
| kalamdb-filestore | 222 | registry (96), health (33), files (24) |
| kalamdb-api | 219 | sql (43), ws (33), models (29) |
| kalamdb-auth | 211 | helpers (48), tests (34), providers (26) |
| kalamdb-views | 156 | src (156) |
| kalamdb-vector | 114 | src (46), sql (35), hot_staging (33) |
| kalamdb-publisher | 82 | src (82) |
| kalamdb-session | 69 | src (69) |
| kalamdb-streams | 68 | src (68) |
| kalamdb-transactions | 59 | src (59) |
| kalam-pg-client | 49 | tests (25), src (24) |
| kalamdb-session-datafusion | 45 | src (45) |
| kalamdb-observability | 42 | src (42) |
| kalam-pg-fdw | 41 | tests (24), src (17) |
| kalamdb-sharding | 41 | src (41) |
| kalam-pg-api | 38 | src (29), tests (9) |
| kalamdb-dba | 38 | repository (17), src (12), models (8) |
| kalamdb-plan-cache | 23 | src (23) |
| kalamdb-server-auth | 15 | src (15) |
| kalamdb-macros | 15 | src (15) |
| kalam-pg-common | 6 | src (5), tests (1) |
| kalam-pg-types | 5 | tests (3), src (2) |

## Top Modules

- `backend/crates/kalamdb-commons/src/models/ids`: 341
- `backend/crates/kalamdb-dialect/src/ddl`: 323
- `benchv2/src/benchmarks`: 313
- `cli/src`: 255
- `backend/crates/kalamdb-store/src`: 245
- `link/kalam-link-dart/src`: 226
- `backend/crates/kalamdb-configs/src/config`: 215
- `backend/crates/kalamdb-core/tests`: 206
- `backend/crates/kalamdb-jobs/src/executors`: 206
- `backend/crates/kalamdb-tables/src/utils`: 201
- `cli/tests/common`: 187
- `link/link-common/src`: 178
- `link/kalam-client/tests`: 164
- `backend/tests/common/testserver`: 161
- `backend/crates/kalamdb-commons/src/models/schemas`: 160
- `backend/crates/kalamdb-views/src`: 156
- `backend/crates/kalamdb-commons/src`: 153
- `backend/crates/kalamdb-commons/src/models`: 144
- `backend/crates/kalamdb-commons/src/serialization/generated`: 144
- `backend/crates/kalamdb-pg/src`: 140
- `pg/src`: 139
- `backend/crates/kalamdb-core/src`: 136
- `backend/crates/kalamdb-raft/src/applier`: 112
- `backend/crates/kalamdb-commons/src/conversions`: 110
- `backend/crates/kalamdb-raft/src/manager`: 110
- `backend/crates/kalamdb-raft/src/state_machine`: 110
- `backend/crates/kalamdb-pg/tests`: 97
- `link/link-common/src/models`: 97
- `backend/crates/kalamdb-filestore/src/registry`: 96
- `backend/crates/kalamdb-dialect/src`: 93
- `link/link-common/src/wasm`: 93
- `cli/tests/cluster`: 92
- `backend/crates/kalamdb-core/src/live/manager`: 90
- `backend/crates/kalamdb-core/src/sql/functions`: 88
- `backend/crates/kalamdb-dialect/src/parser`: 88
- `backend/crates/kalamdb-core/src/manifest`: 87
- `backend/crates/kalamdb-core/src/sql/executor`: 86
- `backend/crates/kalamdb-raft/src/storage`: 86
- `backend/crates/kalamdb-tables/src/shared_tables`: 86
- `benchv2/src`: 86
- `backend/crates/kalamdb-core/src/transactions`: 85
- `backend/crates/kalamdb-system/src/providers/manifest/models`: 85
- `backend/crates/kalamdb-publisher/src`: 82
- `backend/crates/kalamdb-raft/tests`: 81
- `backend/crates/kalamdb-raft/src/network`: 79
- `backend/crates/kalamdb-tables/src/user_tables`: 79
- `backend/crates/kalamdb-commons/src/ids`: 77
- `backend/src`: 77
- `backend/crates/kalamdb-commons/src/helpers`: 76
- `backend/tests/misc/auth`: 73
- `backend/crates/kalamdb-session/src`: 69
- `backend/tests/misc/sql`: 69
- `backend/crates/kalamdb-streams/src`: 68
- `backend/crates/kalamdb-system/src`: 67
- `cli/tests/smoke/usecases`: 65
- `link/link-common/src/consumer/core`: 65
- `pg/tests/e2e_common`: 64
- `backend/crates/kalamdb-core/src/schema_registry/registry`: 62
- `backend/crates/kalamdb-system/src/providers/tables`: 59
- `backend/crates/kalamdb-tables/src/stream_tables`: 59
- `backend/crates/kalamdb-transactions/src`: 59
- `backend/crates/kalamdb-jobs/src`: 57
- `backend/tests/scenarios`: 55
- `cli/tests/subscription`: 53
- `backend/crates/kalamdb-system/src/providers/jobs`: 51
- `link/link-common/src/client`: 51
- `backend/crates/kalamdb-core/src/live/models`: 50
- `cli/tests`: 49
- `backend/crates/kalamdb-auth/src/helpers`: 48
- `backend/crates/kalamdb-core/src/applier/executor`: 48
- `backend/crates/kalamdb-core/src/applier`: 47
- `cli/tests/cli`: 47
- `link/link-common/src/subscription`: 47
- `backend/crates/kalamdb-handlers/crates/ddl/src/table`: 46
- `backend/crates/kalamdb-vector/src`: 46
- `link/link-common/src/consumer/models`: 46
- `backend/crates/kalamdb-session-datafusion/src`: 45
- `backend/crates/kalamdb-core/src/live`: 44
- `pg/tests/e2e_dml`: 44
- `backend/crates/kalamdb-api/src/http/sql`: 43

## Raw Index

- Full function index: `docs/development/rust-method-index.txt`
