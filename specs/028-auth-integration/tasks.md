# Tasks: User ID-Only Auth Integration

**Input**: Design documents from `/specs/028-auth-integration/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/http-auth.md, contracts/link-auth-models.md, contracts/sql-identity.md, quickstart.md
**Last Updated**: 2026-04-16

**Tests**: Not split into per-story TDD tasks. This feature uses batched compile/regeneration validation plus final end-to-end verification with `cli/run-tests.sh`, as requested in the spec and quickstart.

**Organization**: Tasks are grouped by user story so each story remains independently deliverable after the foundational identity cutover.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks in the same phase)
- **[Story]**: Which user story this task belongs to (`US1`, `US2`, `US3`, `US4`)
- Every task includes exact file paths

[P]: #
[Story]: #
[US1]: #
[US2]: #
[US3]: #
[US4]: #

## Path Conventions

- Backend crates: `backend/crates/kalamdb-{crate}/src/`
- Backend auth HTTP surface: `backend/crates/kalamdb-api/src/http/auth/`
- CLI surface: `cli/src/`
- Link shared SDK surface: `link/link-common/src/`
- Dart bridge: `link/kalam-link-dart/src/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Align the shared dependency surface before the breaking identity rename so backend and SDK crates can use the same typed models.

- [x] T001 Configure `link/link-common/Cargo.toml` and `link/kalam-link-dart/Cargo.toml` to consume `kalamdb-commons` `UserId` and `Role` with the minimal feature set required by the auth model refactor

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Remove username/provider identity from the core storage, auth model, Raft, and bootstrap layers before any story-specific behavior changes.

**⚠️ CRITICAL**: No user story work should begin until this phase is complete.

- [x] T002 Refactor `backend/crates/kalamdb-system/src/providers/users/models/user.rs`, `backend/crates/kalamdb-system/src/providers/users/models/auth_data.rs`, `backend/crates/kalamdb-system/src/providers/users/users_indexes.rs`, and `backend/crates/kalamdb-system/src/providers/users/users_provider.rs` to remove username/provider identity fields and username-based lookup/index maintenance
- [x] T003 [P] Replace username-keyed repository and cache access with canonical `UserId` access in `backend/crates/kalamdb-auth/src/repository/user_repo.rs` and its service callers under `backend/crates/kalamdb-auth/src/services/`
- [x] T004 [P] Normalize shared JWT/session/auth context types around `UserId` and typed `Role` in `backend/crates/kalamdb-auth/src/oidc/claims.rs`, `backend/crates/kalamdb-auth/src/providers/jwt_auth.rs`, `backend/crates/kalamdb-auth/src/models/context.rs`, `backend/crates/kalamdb-session/src/user_context.rs`, `backend/crates/kalamdb-session/src/auth_session.rs`, and `backend/crates/kalamdb-session-datafusion/src/context.rs`
- [x] T005 [P] Remove provider-derived username helpers and OIDC auto-create config from `backend/crates/kalamdb-commons/src/models/user_name.rs`, `backend/crates/kalamdb-auth/src/providers/jwt_config.rs`, and `backend/crates/kalamdb-configs/src/config/types.rs`
- [x] T005A [P] Update `MetaCommand::CreateUser` in `backend/crates/kalamdb-raft/src/commands/meta.rs` to carry the updated `User` struct without the username field; accept that pre-cutover Raft log entries are incompatible
- [x] T005B [P] Update server bootstrap in `backend/src/lifecycle.rs` to replace `get_user_by_username(DEFAULT_SYSTEM_USERNAME)` with `UserId`-keyed lookup and creation in `create_default_system_user()`
- [x] T005C [P] Update WebSocket auth handler in `backend/crates/kalamdb-api/src/ws/events/auth.rs` to replace `extract_username_for_audit()` and `username` tracing span field with `UserId`-based audit logging

**Checkpoint**: `system.users`, auth repositories, JWT claims, session context, Raft user creation, server bootstrap, and WS auth are all keyed by internal `UserId` only, with no provider-derived username path left to block story work.

---

## Phase 3: User Story 1 - Sign In With One Canonical Identity (Priority: P1) 🎯 MVP

**Goal**: Make local, internal-token, and trusted OIDC sign-in resolve the same account by canonical internal identity while public/operator-facing surfaces use `user` or `id` naming.

**Independent Test**: With one pre-created account keyed by canonical identity, local password login, internal token login, and trusted OIDC login all authenticate the same account; username-only inputs fail; CLI and HTTP payloads show `user` / `id` naming instead of `user_id`.

### Implementation for User Story 1

- [x] T006 [US1] Refactor Basic/password authentication to accept canonical users instead of usernames in `backend/crates/kalamdb-auth/src/helpers/basic_auth.rs`, `backend/crates/kalamdb-auth/src/services/unified/password.rs`, and `backend/crates/kalamdb-api/src/http/auth/models/login_request.rs`
- [x] T007 [US1] Rewrite bearer/OIDC authentication to resolve pre-existing accounts by `sub` / `UserId` and enforce allowlisted user admission in `backend/crates/kalamdb-auth/src/services/unified/bearer.rs`
- [x] T008 [US1] Update public auth model modules and handler outputs to `user` / `id` naming plus user-facing messages in `backend/crates/kalamdb-api/src/http/auth/models/login_response.rs`, `backend/crates/kalamdb-api/src/http/auth/models/user_info.rs`, `backend/crates/kalamdb-api/src/http/auth/models/setup_request.rs`, `backend/crates/kalamdb-api/src/http/auth/models/setup_response.rs`, `backend/crates/kalamdb-api/src/http/auth/setup.rs`, `backend/crates/kalamdb-api/src/http/auth/me.rs`, and `backend/crates/kalamdb-api/src/http/auth/mod.rs`
- [x] T009 [P] [US1] Convert SQL user management and impersonation to canonical user identity in `backend/crates/kalamdb-handlers/crates/user/src/user/create.rs`, `backend/crates/kalamdb-handlers/crates/user/src/user/alter.rs`, `backend/crates/kalamdb-handlers/crates/user/src/user/drop.rs`, and `backend/crates/kalamdb-core/src/sql/impersonation.rs`
- [x] T010 [P] [US1] Update CLI auth/setup flags, namespaced env handling, and displayed messages to `--user` / `KALAMDB_USER` semantics in `cli/src/args.rs`, `cli/src/connect.rs`, and `cli/src/commands/init.rs`

**Checkpoint**: Canonical sign-in works end-to-end, public/operator-facing naming is `user` / `id`, and no supported login or setup path resolves users by username.

---

## Phase 4: User Story 2 - Use JWT Roles Without Extra Resolution (Priority: P2)

**Goal**: Trusted JWTs use the typed role claim directly when valid, fall back to the stored account role only when the claim is absent, and reject invalid role claims.

**Independent Test**: A trusted JWT with a valid role claim establishes that role directly; a trusted JWT without a role claim falls back to the stored role; a token with an invalid role claim is rejected.

### Implementation for User Story 2

- [x] T011 [US2] Update internal access/refresh token issuance and refresh validation to use `sub` plus typed role claims without username baggage in `backend/crates/kalamdb-auth/src/providers/jwt_auth.rs`, `backend/crates/kalamdb-api/src/http/auth/login.rs`, and `backend/crates/kalamdb-api/src/http/auth/refresh.rs`
- [x] T012 [US2] Implement claim-first role resolution with missing-claim fallback and invalid-claim rejection in `backend/crates/kalamdb-auth/src/services/unified/bearer.rs` and `backend/crates/kalamdb-auth/src/oidc/claims.rs`
- [x] T013 [P] [US2] Propagate the effective typed role through auth/session/query permission contexts in `backend/crates/kalamdb-auth/src/models/context.rs`, `backend/crates/kalamdb-session/src/auth_session.rs`, `backend/crates/kalamdb-session/src/user_context.rs`, `backend/crates/kalamdb-session-datafusion/src/context.rs`, and `backend/crates/kalamdb-session-datafusion/src/permissions.rs`

**Checkpoint**: Role handling is lightweight and typed across bearer auth and runtime session contexts, with no extra role-only fetch when a trusted token already carries a valid role.

---

## Phase 5: User Story 3 - Consume One Typed Identity Contract (Priority: P3)

**Goal**: Backend, link-common, and the Dart bridge expose one typed internal identity contract while keeping public fields/operator-facing naming on `user` / `id`.

**Independent Test**: `link-common` and the Dart bridge compile against `kalamdb-commons` `UserId` / `Role`, their auth/login/setup models expose `user` / `id` naming, and no public auth-facing SDK model still carries `username` or raw role strings.

### Implementation for User Story 3

- [x] T014 [US3] Replace link-common auth/setup/credential models with `kalamdb-commons` `UserId` and `Role` plus public `user` / `id` naming in `link/link-common/src/auth/models/login.rs`, `link/link-common/src/auth/models/setup_models.rs`, `link/link-common/src/auth/models/mod.rs`, `link/link-common/src/credentials.rs`, and `link/link-common/src/auth/models/username.rs`
- [x] T015 [P] [US3] Update link-common client/auth/websocket consumers to the canonical user contract in `link/link-common/src/client/endpoints.rs`, `link/link-common/src/auth/provider.rs`, `link/link-common/src/connection/models/server_message.rs`, `link/link-common/src/client/builder.rs`, and `link/link-common/src/lib.rs`
- [x] T016 [P] [US3] Update the Dart bridge auth models and APIs in `link/kalam-link-dart/src/models.rs`, `link/kalam-link-dart/src/api.rs`, and `link/kalam-link-dart/src/tests.rs`, then regenerate generated artifacts with `link/sdks/dart/build.sh`
- [x] T016A [P] [US3] Update the TypeScript SDK auth contract in `link/sdks/typescript/client/src/auth.ts` (rename `BasicAuthCredentials.username` to `user`, update `Auth.basic()` and `encodeBasicAuth()`), `link/sdks/typescript/client/src/client.ts` (login sends `user`, rename `wrapExecuteAsUser` param), and `link/sdks/typescript/client/src/index.ts` (export `UserId` instead of `Username`)
- [x] T016B [P] [US3] Update the consumer and consumer-WASM crates to replace `Username` with `UserId` in `link/kalam-consumer-wasm/src/client.rs`, `link/kalam-consumer/src/lib.rs`, and any related re-exports in `link/kalam-consumer-wasm/src/lib.rs`

**Checkpoint**: Kalam-link surfaces share typed identity/role models with the backend while presenting `user` / `id` naming to consumers.

---

## Phase 5B: User Story 4 - Keep Password Auth On Login Only (Priority: P1)

**Goal**: Ensure the backend accepts direct user/password credentials only on `POST /v1/api/auth/login`, and ensure SDK/shared client layers exchange those credentials for JWTs before any authenticated SQL, topic, refresh, `/me`, or WebSocket traffic.

**Independent Test**: `POST /v1/api/auth/login` accepts valid `user` / `password` credentials, representative non-login protected endpoints reject the same credentials, and SDK/client flows that start from user/password perform a login exchange before SQL/topic/WebSocket activity.

### Implementation for User Story 4

- [x] T016C [US4] Enforce and validate the backend login-only password boundary in `backend/crates/kalamdb-auth/src/helpers/extractor.rs`, `backend/crates/kalamdb-auth/src/services/unified/mod.rs`, `backend/crates/kalamdb-api/src/http/auth/mod.rs`, and `cli/tests/smoke/security/smoke_test_rpc_auth.rs`
- [x] T016D [P] [US4] Restrict shared client and SDK flows to use user/password only for `POST /v1/api/auth/login` in `link/link-common/src/auth/provider.rs`, `link/link-common/src/query/executor.rs`, `link/link-common/src/client/runtime.rs`, `link/link-common/src/consumer/core/poller.rs`, `link/link-common/src/wasm/client.rs`, `link/sdks/typescript/client/src/auth.ts`, `link/sdks/typescript/consumer/src/client.ts`, `link/sdks/typescript/consumer/src/types.ts`, `link/sdks/typescript/consumer/src/index.ts`, and `link/sdks/typescript/consumer/src/agent.ts`

**Checkpoint**: Password-bearing requests are confined to `POST /v1/api/auth/login`, backend protected endpoints reject Basic/password auth elsewhere, and SDK-managed SQL/topic/WebSocket traffic uses JWT auth after the login exchange.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Finish documentation, security hardening, batched validation, and the final requested end-to-end sweep.

- [x] T017 [P] Update user-facing docs and examples for `user` / `id` naming in `cli/README.md`, `link/README.md`, `docs/`, and `examples/`, plus sync SDK docs in `../KalamSite/content/sdk/`
- [x] T017A [P] Update example setup scripts and env files to use `"user"` JSON key and `KALAMDB_USER` env var in `examples/chat-with-ai/setup.sh`, `examples/simple-typescript/setup.sh`, `examples/simple-typescript/.env.example`, `examples/summarizer-agent/setup.sh`, and `examples/summarizer-agent/.env.example`
- [x] T018 [P] Run batched backend and link compile validation using `auth-integration-backend-check.txt` and `auth-integration-link-check.txt`, then fix all auth/session/link compile drift in touched files
- [x] T018A [P] Update backend test harness in `backend/tests/common/testserver/test_server.rs` and `backend/tests/common/testserver/auth_helper.rs` to remove `UserName` usage, switch user creation to `UserId`-keyed patterns, and fix `create_jwt_token` calls across all `backend/tests/scenarios/scenario_*.rs` files
- [x] T018B [P] Update CLI test assertions in `cli/tests/auth_retry_test.rs`, `cli/tests/auth/test_auth.rs`, `cli/tests/auth/test_keycloak_auth.rs`, and `cli/tests/cluster/*.rs` to use `--user` flag, query `system.users` without `username` column, and expect post-cutover auth contract
- [x] T018C [P] Update PG extension e2e test harness in `pg/tests/e2e_ddl_common/mod.rs` to rename `login_username` / `setup_username` fields, send `"user"` instead of `"username"` in login/setup JSON payloads, and update `pg/docker/test.sh` for the same
- [x] T018D [P] Update benchmark login helper parameter naming from `username` to `user` in `benchv2/src/client.rs`
- [x] T019 Review and harden auth/server security in `backend/crates/kalamdb-auth/src/services/unified/bearer.rs`, `backend/crates/kalamdb-auth/src/providers/jwt_auth.rs`, `backend/crates/kalamdb-api/src/http/auth/`, and `backend/crates/kalamdb-configs/src/config/types.rs` against the `AGENTS.md` security checklist
- [x] T020 Validate the documented migration flow in `specs/028-auth-integration/quickstart.md`, including the namespaced `KALAMDB_USER` surface and user-facing message expectations, and fix any drift in touched auth/CLI/link files
- [ ] T021 Start a fresh backend from `backend/` and run `cli/run-tests.sh` from `cli/`, fixing any remaining end-to-end failures until the full suite passes

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: Can start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 and blocks all user stories
- **User Story 1 (Phase 3)**: Depends on Phase 2
- **User Story 2 (Phase 4)**: Depends on Phase 2; benefits from US1’s canonical auth path but can begin once the foundational JWT/session model changes are in place
- **User Story 3 (Phase 5)**: Depends on Phase 2 and should start after the public auth field naming from US1/US2 is stable
- **User Story 4 (Phase 5B)**: Depends on the canonical auth contracts from US1/US3 so the password boundary is enforced consistently across backend and SDK surfaces
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **US1**: First deliverable and MVP; establishes the canonical sign-in flow and public naming contract
- **US2**: Builds on the canonical bearer/session model from the foundational phase and aligns token role handling with the new identity path
- **US3**: Consumes the stabilized backend/link contract and propagates it through link-common and the Dart bridge
- **US4**: Hardens the password-auth boundary after the canonical auth contracts exist, and should land before the final validation/security sweep

### Within Each User Story

- Shared model/storage work must land before handler changes that consume it
- Public contract/model files should be updated before downstream client/CLI surfaces
- Batched compile/regeneration validation happens after meaningful groups of edits, not after every rename
- The final server hardening review and `cli/run-tests.sh` sweep happen only after the coordinated cutover is complete

### Parallel Opportunities

- **Phase 2**: `T003`, `T004`, `T005`, `T005A`, `T005B`, and `T005C` can run in parallel after `T002`
- **US1**: `T009` and `T010` can run in parallel after the core auth path in `T006`–`T008` is stable
- **US2**: `T012` and `T013` can run in parallel once `T011` defines the token issuance/refresh shape
- **US3**: `T015`, `T016`, `T016A`, and `T016B` can run in parallel after `T014` establishes the shared link-common auth models
- **US4**: `T016C` and `T016D` can run in parallel once the canonical login/auth contracts are stable
- **Polish**: `T017`, `T017A`, `T018`, `T018A`, `T018B`, `T018C`, and `T018D` can run in parallel before the security review and final end-to-end sweep

---

## Parallel Example: User Story 1

```bash
# After T006-T008 stabilize the canonical auth path, run these in parallel:
Task: "Convert SQL user management and impersonation to canonical user identity in backend/crates/kalamdb-handlers/crates/user/src/user/create.rs, backend/crates/kalamdb-handlers/crates/user/src/user/alter.rs, backend/crates/kalamdb-handlers/crates/user/src/user/drop.rs, and backend/crates/kalamdb-core/src/sql/impersonation.rs"
Task: "Update CLI auth/setup flags, namespaced env handling, and displayed messages to --user / KALAMDB_USER semantics in cli/src/args.rs, cli/src/connect.rs, and cli/src/commands/init.rs"
```

## Parallel Example: User Story 2

```bash
# After T011 defines token issuance/refresh semantics, run these in parallel:
Task: "Implement claim-first role resolution with missing-claim fallback and invalid-claim rejection in backend/crates/kalamdb-auth/src/services/unified/bearer.rs and backend/crates/kalamdb-auth/src/oidc/claims.rs"
Task: "Propagate the effective typed role through auth/session/query permission contexts in backend/crates/kalamdb-auth/src/models/context.rs, backend/crates/kalamdb-session/src/auth_session.rs, backend/crates/kalamdb-session/src/user_context.rs, backend/crates/kalamdb-session-datafusion/src/context.rs, and backend/crates/kalamdb-session-datafusion/src/permissions.rs"
```

## Parallel Example: User Story 3

```bash
# After T014 establishes the shared link-common auth models, run these in parallel:
Task: "Update link-common client/auth/websocket consumers to the canonical user contract in link/link-common/src/client/endpoints.rs, link/link-common/src/auth/provider.rs, link/link-common/src/connection/models/server_message.rs, link/link-common/src/client/builder.rs, and link/link-common/src/lib.rs"
Task: "Update the Dart bridge auth models and APIs in link/kalam-link-dart/src/models.rs, link/kalam-link-dart/src/api.rs, and link/kalam-link-dart/src/tests.rs, then regenerate generated artifacts with link/sdks/dart/build.sh"
Task: "Update TypeScript SDK auth contract in link/sdks/typescript/client/src/auth.ts and client.ts"
Task: "Update consumer/consumer-WASM crates to replace Username with UserId in link/kalam-consumer-wasm/src/client.rs and link/kalam-consumer/src/lib.rs"
```

## Parallel Example: Phase 6 Polish

```bash
# These all touch independent files and can run in parallel:
Task: "Update backend test harness in backend/tests/common/testserver/ and all scenario tests"
Task: "Update CLI test assertions in cli/tests/auth/ and cli/tests/cluster/"
Task: "Update PG extension e2e test harness in pg/tests/e2e_ddl_common/mod.rs and pg/docker/test.sh"
Task: "Update example setup scripts in examples/*/setup.sh and .env files"
Task: "Update benchmark login helper in benchv2/src/client.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1 and Phase 2
2. Complete Phase 3 (US1)
3. Validate that canonical sign-in works across local, internal-token, and trusted OIDC flows with public `user` / `id` naming
4. Stop and confirm the core breaking auth cutover before moving deeper into role propagation and SDK surfaces

### Incremental Delivery

1. Finish Setup + Foundational so the repo has one canonical internal identity path
2. Deliver US1 to stabilize the auth boundary and operator-facing naming
3. Deliver US2 to optimize role handling without extra resolution cost
4. Deliver US3 to align SDK and Dart bridge contracts with the backend
5. Finish with documentation, security review, batched compile validation, and the final `cli/run-tests.sh` sweep

### Notes

- `[P]` tasks are safe to parallelize because they touch different files or can begin after an earlier stabilizing task in the same phase
- No per-change test-first tasks are listed because the feature request explicitly prefers one coordinated validation pass at the end
- `link/sdks/dart/lib/src/generated` remains generated output; regenerate it with `link/sdks/dart/build.sh` instead of editing it directly
- Any SDK surface change under `link/**` must also update the corresponding docs in `../KalamSite/content/sdk/`
- Pre-cutover Raft log entries with `MetaCommand::CreateUser` carrying the old `User` struct (with `username`) are incompatible with the post-cutover struct; this is accepted as part of the no-backward-compatibility design
- Total tasks: 32 (T001–T021 plus T005A–T005C, T016A–T016D, T017A, T018A–T018D)