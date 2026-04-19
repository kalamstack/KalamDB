# Implementation Plan: User ID-Only Auth Integration

**Branch**: `028-auth-integration` | **Date**: 2026-04-16 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/028-auth-integration/spec.md`

## Summary

Replace the username-based auth surface with a canonical `UserId`-only identity model across backend auth, system user storage, session propagation, SQL user-management flows, CLI flows, and Kalam-link. The implementation removes the `system.users.username` identity path, deletes provider-derived username mapping and OIDC auto-provisioning, normalizes tokens and session models around `sub = UserId`, and uses typed `Role` directly from trusted JWT claims when present, falling back to the stored account role only when the claim is absent. Public and operator-facing surfaces use `user` or `id` naming, such as `--user`, `KALAMDB_USER`, and JSON fields like `user` or `id`, while internal code keeps the typed `UserId` model. Password-bearing credentials are constrained to `POST /v1/api/auth/login`; every other protected backend or SDK path must use JWT/cookie/Bearer auth after the initial login exchange.

## Technical Context

**Language/Version**: Rust 1.92+ (edition 2021) for backend, CLI, link-common, and Dart bridge; TypeScript/JavaScript ES2020+ and Dart only for downstream contract consumers and docs  
**Primary Dependencies**: Actix-Web 4.4, jsonwebtoken 9.2, kalamdb-auth OIDC/JWKS validator, kalamdb-commons typed models, kalamdb-store IndexedEntityStore, tokio, serde, link-common, flutter_rust_bridge bridge models  
**Storage**: RocksDB-backed `system.users` via `IndexedEntityStore`; broader platform storage remains RocksDB + Parquet through existing abstractions  
**Testing**: `cargo nextest run` for Rust packages, batched `cargo check` passes, `link/sdks/dart/build.sh` for bridge regeneration, final end-to-end validation with `cli/run-tests.sh` against a running backend  
**Target Platform**: macOS/Linux backend server, CLI, Rust SDK consumers, and Dart/Flutter bridge clients  
**Project Type**: Multi-crate Rust backend plus CLI plus SDK bridge  
**Performance Goals**: Keep auth/session hot paths lightweight by using `Arc<str>`-backed `UserId` and typed `Role`, removing username secondary-index lookups from login/bearer flows, and skipping redundant role reloads when a trusted JWT already carries a valid role  
**Constraints**: No backward compatibility, no username identity fallback, no provider-link mapping layer, no new heavy dependencies, preserve signature/issuer/audience/expiry/rate-limit checks, keep link-common on minimal `kalamdb-commons` features, keep user-facing env/CLI/API/message naming on `user` or `id`, keep password-bearing requests scoped to `POST /v1/api/auth/login`, avoid naked `USER` env collisions by using namespaced vars such as `KALAMDB_USER`, batch validation and end with `cli/run-tests.sh`  
**Scale/Scope**: Breaking cross-crate refactor covering `kalamdb-commons`, `kalamdb-system`, `kalamdb-auth`, `kalamdb-api`, `kalamdb-session`, `kalamdb-session-datafusion`, SQL user handlers, CLI auth/setup, `link-common`, `kalam-link-dart`, and related tests/docs

## Cross-Cutting Conventions

- Internal implementation keeps typed `UserId` and typed `Role` as the canonical identity and authorization models.
- Operator-facing and public surfaces use `user` or `id` naming rather than `user_id`, `userid`, or `UserId`.
- User/password credentials are login-only inputs; protected non-login traffic uses JWT, refresh-token, cookie, or anonymous/public semantics as appropriate.
- Environment variables remain namespaced for safety, for example `KALAMDB_USER` instead of `KALAMDB_USER_ID`; do not rely on the shell's raw `USER` variable.
- Human-readable errors, prompts, setup messages, and CLI output use `user` or `credentials` wording and never expose `user_id` implementation details.
- Before final sign-off, run an explicit auth and server hardening review against the security checklist in `AGENTS.md` and fix or document any findings.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`/Users/jamal/git/KalamDB/.specify/memory/constitution.md` still contains the stock placeholder template, so the effective gates for this plan come from the repository rules in `AGENTS.md` and the attached Copilot instructions.

### Pre-Phase 0 Gate Review

- `PASS`: The design uses typed `UserId` and typed `Role` as the canonical identity/authorization models instead of raw strings.
- `PASS`: The refactor stays inside existing owning crates and does not introduce a new auth side-lane, SQL rewrite pass, or extra service boundary.
- `PASS`: No new dependency is required. The link path reuses `kalamdb-commons` with a minimal feature set rather than inventing duplicate SDK-only identity types.
- `PASS`: The plan removes provider-derived username mapping and avoids reintroducing identity-linking complexity, matching the requested no-compatibility cutover.
- `PASS`: Validation remains batched. The feature will be verified after the coordinated rename with targeted regeneration/checks and a final `cli/run-tests.sh` pass.
- `PASS`: Security gates remain intact: trusted issuer/audience validation, signed-token verification, lockout/rate-limit handling, and role validation are preserved, and the plan now includes an explicit hardening/vulnerability review before final validation.

### Post-Phase 1 Re-Check

- `PASS`: The research, data model, and contracts keep `UserId` as the sole identity key and explicitly delete username/provider mapping from supported flows.
- `PASS`: The design keeps `Role` typed in backend and link-common surfaces while still serializing clean lowercase wire values for HTTP/WebSocket/Dart contracts.
- `PASS`: The plan preserves compile-surface discipline by reusing `kalamdb-commons` rather than adding parallel auth model crates.
- `PASS`: The design includes the required downstream SDK/doc synchronization for `kalam-link-dart` and KalamSite.
- `PASS`: The design explicitly separates internal `UserId` naming from operator-facing `user` naming for CLI, environment, payload, and message surfaces.

## Project Structure

### Documentation (this feature)

```text
specs/028-auth-integration/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── http-auth.md
│   ├── link-auth-models.md
│   └── sql-identity.md
└── tasks.md
```

### Source Code (repository root)

```text
backend/src/lifecycle.rs                           # Server bootstrap / default system user creation
backend/crates/
├── kalamdb-commons/src/models/
├── kalamdb-system/src/providers/users/
├── kalamdb-auth/src/
├── kalamdb-api/src/http/auth/
├── kalamdb-api/src/ws/events/auth.rs               # WebSocket auth handler
├── kalamdb-session/src/
├── kalamdb-session-datafusion/src/
├── kalamdb-core/src/sql/
├── kalamdb-raft/src/commands/meta.rs                # Raft MetaCommand::CreateUser (carries User struct)
└── kalamdb-handlers/crates/user/src/user/

backend/tests/common/testserver/                    # Test harness (test_server.rs, auth_helper.rs)
backend/tests/scenarios/                            # Scenario tests (01-14+)
cli/src/
cli/tests/                                          # auth_retry_test.rs, auth/, cluster/
link/link-common/src/
link/kalam-link-dart/src/
link/kalam-consumer/src/                            # Re-exports Username
link/kalam-consumer-wasm/src/                       # Parses username from consume messages
link/sdks/typescript/client/src/                    # BasicAuthCredentials.username, login flow
link/sdks/dart/
pg/tests/e2e_ddl_common/mod.rs                      # PG e2e test harness (login_username, JSON)
pg/docker/test.sh                                   # PG Docker test script (login/setup JSON)
benchv2/src/                                        # Benchmark client login helper
docs/
examples/                                           # setup.sh scripts and .env files
```

**Structure Decision**: Keep the existing multi-crate layout. Implement the identity cutover in the owning crates instead of introducing a new adapter layer: `kalamdb-commons` for shared types, `kalamdb-system` for persisted user shape/indexes, `kalamdb-auth` and `kalamdb-api` (including WS auth handler) for auth flow/token contracts, `kalamdb-session*` for runtime context, SQL user handlers/core for DDL and impersonation, `kalamdb-raft/commands/meta.rs` for Raft-replicated user creation, `backend/src/lifecycle.rs` for server bootstrap, `cli` for operator-facing login/setup, `link` crates for SDK contracts and Dart bridge regeneration, `link/sdks/typescript` for the TypeScript SDK, `link/kalam-consumer*` for consumer WASM, `pg/tests` and `pg/docker` for PG extension test harnesses, `benchv2` for benchmark login, and `examples/` for setup scripts and env files.

## Complexity Tracking

No constitution exceptions or additional complexity waivers are required at planning time.

