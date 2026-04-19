# Phase 0 Research: User ID-Only Auth Integration

## Decision 1: Replace username-keyed lookup with `UserId` as the only account key

**Decision**: Refactor `system.users`, `UsersTableProvider`, `UserRepository`, auth caches, and all auth/session callers to look up and cache users by typed `UserId` only. Remove the username secondary index and all `get_user_by_username` repository/provider APIs from supported flows.

**Rationale**: `user_id` is already the primary key in `system.users`, and the project already has an `Arc<str>`-backed `UserId` optimized for high-concurrency hot paths. Keeping username as a second identity key preserves duplicate resolution logic, extra index maintenance, and accidental fallback paths.

**Alternatives considered**:

- Keep username as display-only persisted data: rejected because the requested design removes username from supported identity contracts entirely, and a retained column would invite fallback lookups.
- Keep a transition period with both lookups enabled: rejected because the feature explicitly forbids backward compatibility.
- Derive a synthetic username from `user_id`: rejected because it preserves redundant identity state without adding value.

## Decision 2: Standardize trusted token identity on `sub = UserId`

**Decision**: Require trusted internal and external bearer tokens to carry the canonical identity in `sub`, serialized as the string form of `UserId`. Remove `username` from `JwtClaims`, internal token generation, refresh-token validation, and all token-derived auth/session models.

**Rationale**: The current code already treats `sub` as the identity root while layering `username` back in for lookup and response shaping. Making `sub` the only canonical identity matches JWT norms, removes duplicated claims, and keeps all transports aligned on the same field.

**Alternatives considered**:

- Accept both `sub` and a separate `user_id` claim long-term: rejected to keep one canonical token shape.
- Preserve `username` for internal tokens only: rejected because it keeps the refresh/login/session stack dual-keyed.
- Trust bearer tokens solely by header presence: rejected because the repo security rules require signature and claim validation.

## Decision 3: Use existing user records as the OIDC subject allowlist

**Decision**: Remove provider-derived usernames, hashed provider user IDs, `AuthData`, and `auto_create_users_from_provider`. A trusted OIDC token is accepted only when the issuer/audience is trusted and the `sub` resolves to an already existing `system.users` record whose auth policy allows OIDC entry.

**Rationale**: The requested architecture does not want provider mapping, account linking, or first-login auto-provisioning. Using the existence of the canonical user record as the allowlist keeps state minimal and makes admission explicit.

**Alternatives considered**:

- Keep auto-provisioning from trusted providers: rejected because it bypasses the requested whitelist requirement.
- Add a separate OIDC subject allowlist table: rejected because it adds another identity registry outside `system.users`.
- Keep provider metadata only for diagnostics: rejected because the feature explicitly wants KalamDB to stop caring about provider identity resolution.

## Decision 4: Keep a typed auth policy on the user account, but not provider identity metadata

**Decision**: Preserve a typed field on the user record that determines which auth paths are allowed for that account (`Password`, `Internal`, `Oidc`, or an equivalent renamed enum), while removing provider identity linkage fields.

**Rationale**: The product still needs to distinguish which users may sign in locally versus via trusted OIDC, but that can be expressed as account policy rather than provider identity mapping.

**Alternatives considered**:

- Remove auth-type policy completely: rejected because local-password, internal-token, and OIDC admission rules still differ.
- Store provider issuer/subject on the user row: rejected because it reintroduces upstream identity coupling the feature is trying to remove.

## Decision 5: Trust valid JWT role claims directly and fall back only when absent

**Decision**: When a trusted JWT contains a valid typed `Role`, the session uses that role directly without an extra role-only reload. When the role claim is absent, the role falls back to the role on the single user record already loaded to validate account existence and auth policy. Invalid role claims are rejected.

**Rationale**: This matches the requested lightweight path while still preventing deleted, locked, or disallowed accounts from authenticating. It also preserves role validation rather than silently coercing unknown values.

**Alternatives considered**:

- Always reload the user from storage to verify role even when the token carries one: rejected because it adds unnecessary hot-path cost.
- Trust the token completely and skip user lookup for bearer auth: rejected because account deletion/lockout/auth-policy checks would be bypassed.
- Fall back silently on malformed role claims: rejected because that hides privilege bugs.

## Decision 6: Convert bootstrap, SQL user DDL, impersonation, and operator-facing naming to canonical user identity

**Decision**: Server setup, SQL `CREATE USER` / `ALTER USER` / `DROP USER`, `EXECUTE AS USER`, CLI login/setup flags, environment variables, and auth-facing messages move to canonical user identity semantics. Internal code and persisted storage keep the typed `UserId` model, while user-facing contracts use `user` or `id` naming, for example `--user`, `KALAMDB_USER`, and JSON fields like `user` or `id`. Reserved root/system identities remain `UserId` constants instead of usernames.

**Rationale**: If creation, setup, impersonation, env vars, CLI flags, or human-readable messages still speak username or leak `user_id` implementation wording while auth has been standardized internally, the identity split simply moves to the operator experience.

**Alternatives considered**:

- Keep setup and DDL naming the account by username while auth uses canonical user identity: rejected because it preserves dual identity entry points.
- Expose `user_id` literally on public env vars, CLI flags, or messages: rejected because the requested operator-facing naming convention is `user`, not internal implementation terms.
- Use the shell's raw `USER` environment variable as the product contract: rejected because it collides with standard process/user environment behavior; a namespaced variable such as `KALAMDB_USER` is safer.

## Decision 7: Make `link-common` reuse `kalamdb-commons` types with minimal features

**Decision**: Replace `link-common`'s SDK-local username wrapper and raw role strings with `kalamdb-commons::UserId` and `kalamdb-commons::Role`, using the smallest viable feature set and validating the compile surface the same way the PG extension work did.

**Rationale**: Shared types prevent auth contract drift between backend, CLI, link-common, and the Dart bridge. The PG extension memory also shows that minimal `kalamdb-commons` feature selection matters to keep compile size and dependency drag under control.

**Alternatives considered**:

- Keep duplicate SDK-local identity wrappers: rejected because they drift from backend semantics.
- Depend on the full `kalamdb-commons` feature surface by default: rejected because it increases compile and binary costs without need.
- Create a new tiny auth-types crate: rejected because the project already has the right shared models in `kalamdb-commons`.

## Decision 8: Batch validation and finish with the requested CLI end-to-end sweep

**Decision**: Implement the rename and model changes in large coherent batches, run focused compile/regeneration checks after each major batch, then perform final runtime validation with a running backend and `cli/run-tests.sh` once the full cutover is in place.

**Rationale**: This matches the user request not to test each rename separately and aligns with the repo guidance to batch compile feedback instead of thrashing `cargo check`.

**Alternatives considered**:

- Re-run the full test flow after each file rename: rejected because it is slow and provides poor signal for a coordinated breaking refactor.
- Skip targeted compile/regeneration checks entirely and rely only on the final CLI sweep: rejected because the Dart bridge and multi-crate Rust surface still need earlier structural validation.

## Decision 9: Treat security verification as a first-class deliverable of the cutover

**Decision**: The implementation includes an explicit security vulnerability and server-hardening review before sign-off. The review covers token signature/issuer/audience/expiry handling, invalid-role rejection, auth bypass risks, rate limits, system table protections, cookie/JWT secret/origin rules, localhost-only surfaces, and public endpoint exposure using the repo security checklist in `AGENTS.md`.

**Rationale**: This feature changes the entire authentication boundary. Functional correctness alone is not enough; the cutover must prove the server remains secure after usernames, provider mapping, and role resolution logic are removed.

**Alternatives considered**:

- Rely only on the final CLI sweep: rejected because it does not systematically cover auth/server hardening regressions.
- Defer security review until after implementation lands: rejected because this is a breaking auth change and the security review is part of acceptance, not an optional follow-up.

## Decision 10: Handle Raft MetaCommand::CreateUser serialization after User struct changes

**Decision**: `MetaCommand::CreateUser { user: User }` carries the full serialized `User` struct through the Raft log. When the `username` field is removed from `User`, in-flight and persisted Raft log entries become undeserializable. Because this feature explicitly forbids backward compatibility, the fix is to add `#[serde(default)]` on the removed field during the transition or accept that any pre-cutover Raft state must be cleared. Since the feature is a clean-break cutover, the preferred path is to accept that Raft state from before the cutover is incompatible.

**Rationale**: Raft log entries are persisted bincode/serde payloads. Removing a field without a serde default breaks deserialization of old entries.

**Alternatives considered**:

- Keep `username` as `Option<UserName>` with `#[serde(default)]` in the `User` struct permanently: rejected because it preserves dead code, but a short-lived `#[serde(default)]` during rollout is acceptable if needed.
- Require a clean Raft state wipe on upgrade: acceptable since no backward compatibility is provided.

## Decision 11: Update all peripheral surfaces that carry username or send username payloads

**Decision**: The cutover must also update these surfaces that were not covered in the initial plan scope:

1. **Server bootstrap** (`backend/src/lifecycle.rs`): `create_default_system_user()` calls `get_user_by_username()` — must switch to `get_by_id()`.
2. **WebSocket auth** (`backend/crates/kalamdb-api/src/ws/events/auth.rs`): uses `extract_username_for_audit()` and logs `username` field in tracing spans — must switch to user_id-based audit.
3. **Backend test harness** (`backend/tests/common/testserver/`): `test_server.rs` and `auth_helper.rs` create `User` structs with `username` and call `get_user_by_username()` — must switch to `UserId`-keyed creation.
4. **Backend scenario tests** (`backend/tests/scenarios/`): all 14+ scenarios import `UserName` and call `create_jwt_token(&UserName::new(...))` — must switch to `UserId`-based patterns.
5. **CLI tests** (`cli/tests/auth/`, `cli/tests/auth_retry_test.rs`, `cli/tests/cluster/`): test `--username` flag, query `system.users.username` column, assert username-based OIDC provisioning — must switch to `--user` and `user`-based queries.
6. **PG extension tests** (`pg/tests/e2e_ddl_common/mod.rs`): sends `"username"` JSON key to login/setup endpoints — must send `"user"`.
7. **PG Docker test script** (`pg/docker/test.sh`): hardcoded `"username"` JSON payloads — must send `"user"`.
8. **TypeScript SDK** (`link/sdks/typescript/client/src/auth.ts`, `client.ts`): `BasicAuthCredentials.username`, `Auth.basic(username, password)`, login sends `this.auth.username` — must rename to `user`.
9. **Consumer/Consumer-WASM** (`link/kalam-consumer-wasm/src/client.rs`, `link/kalam-consumer/src/lib.rs`): imports `Username` from `link_common::models` — must switch to `UserId`.
10. **Example scripts** (`examples/*/setup.sh`, `.env*`): send `"username"` JSON and use `KALAMDB_USERNAME` env var — must send `"user"` and use `KALAMDB_USER`.
11. **Benchmark harness** (`benchv2/src/client.rs`): `login(urls, username, password)` parameter naming — must rename to `user`.

**Rationale**: A breaking identity cutover that leaves test harnesses, Docker scripts, SDKs, and examples still sending `"username"` will fail the final `cli/run-tests.sh` sweep and leave downstream consumers broken.
