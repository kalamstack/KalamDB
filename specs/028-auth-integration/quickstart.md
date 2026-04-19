# Quickstart: User ID-Only Auth Integration

## Goal

Implement the breaking auth cutover so every supported path uses one canonical internal user identity backed by typed `UserId`, typed `Role`, operator-facing `user` naming, and no username/provider-compatibility behavior remains.

## Recommended Implementation Order

1. Update shared identity types and persisted user shape.
   - Remove username/provider mapping from `kalamdb-system` and shared auth/session models.
   - Replace username-keyed repository/provider/cache interfaces with `UserId`-keyed ones.

2. Update backend auth and runtime context.
   - Refactor password login, bearer login, refresh flow, setup flow, and `/me` responses.
   - Remove username from JWT claims and session/user context.
   - Delete OIDC auto-provisioning and provider-derived username logic.

3. Update SQL, CLI, link surfaces, and server bootstrap.
   - Convert user DDL, impersonation, bootstrap/setup, CLI login flags, environment variable names, public auth fields, and test helpers to canonical user semantics.
   - Use operator-facing names like `--user`, `KALAMDB_USER`, request field `user`, and response field `id`, while preserving internal `UserId` semantics.
   - Keep direct user/password credentials scoped to `POST /v1/api/auth/login`; all subsequent SDK or client traffic must use JWT/cookie/Bearer auth.
   - Move `link-common` to `kalamdb-commons::UserId` and `Role`, then update `kalam-link-dart` bridge models, the TypeScript SDK, and the consumer/consumer-WASM crates.
   - Update server bootstrap (`backend/src/lifecycle.rs`) to use `UserId`-keyed lookup in `create_default_system_user()`.
   - Update `MetaCommand::CreateUser` in `kalamdb-raft` to carry the updated `User` struct.
   - Update the WebSocket auth handler to use `UserId`-based audit logging.

4. Regenerate generated artifacts and docs.
   - Regenerate Dart bridge/output after changing Rust-facing link models.
   - Update the corresponding KalamSite SDK docs for any Dart/SDK surface changes.

5. Update peripheral test harnesses, Docker scripts, example scripts, and benchmarks.
   - Backend test harness (`backend/tests/common/testserver/`) and all scenario tests.
   - CLI tests (`cli/tests/auth/`, `cli/tests/cluster/`).
   - PG extension e2e test harness (`pg/tests/e2e_ddl_common/mod.rs`) and Docker script (`pg/docker/test.sh`).
   - Example setup scripts (`examples/*/setup.sh`, `.env*`).
   - Benchmark login helper (`benchv2/src/client.rs`).

## Batched Validation Flow

1. Run one backend compile batch after the major auth/system/session edit set.

```bash
cd /Users/jamal/git/KalamDB && CARGO_TERM_COLOR=never cargo check \
   -p kalamdb-server \
   -p kalamdb-api \
   -p kalamdb-auth \
   -p kalamdb-session \
   -p kalamdb-session-datafusion \
   -p kalamdb-core \
   -p kalamdb-system \
   -p kalamdb-configs \
   -p kalamdb-commons \
   -p kalamdb-handlers-user \
   -p kalamdb-raft \
   -p kalamdb-plan-cache \
   -p kalam-cli \
   > auth-integration-backend-check.txt 2>&1
```

2. Run one link workspace compile batch after the `link-common`, Dart bridge, and consumer edits.

```bash
cd /Users/jamal/git/KalamDB && CARGO_TERM_COLOR=never cargo check \
   -p link-common \
   -p kalam-consumer \
   -p kalam-consumer-wasm \
   -p kalam-link-dart \
   > auth-integration-link-check.txt 2>&1
```

3. Regenerate the Dart SDK/bridge artifacts after the link model changes.

```bash
cd /Users/jamal/git/KalamDB/link/sdks/dart && ./build.sh
```

4. Start a fresh backend instance for end-to-end validation.

```bash
cd /Users/jamal/git/KalamDB/backend && export KALAMDB_DATA_DIR="$(mktemp -d -t kalamdb-cli-run-tests)" && export KALAMDB_RATE_LIMIT_ENABLE_CONNECTION_PROTECTION=false && cargo run --bin kalamdb-server -- server.toml
```

5. Run the auth and server hardening review before final sign-off.

- Validate the completed change set against the `AGENTS.md` security review checklist.
- Verify trusted issuer/audience/expiry checks, invalid-role rejection, rate limiting, localhost-only setup/health surfaces, system table protections, origin/cookie/JWT secret protections, and absence of username fallback.

6. Run the requested final integration sweep only after the full cutover is in place.

```bash
cd /Users/jamal/git/KalamDB/cli && ./run-tests.sh
```

## Expected Verification Outcomes

- Login/setup/auth payloads use public `user` or `id` naming while mapping to canonical `UserId`, and reject username-only identity.
- Protected non-login backend endpoints reject user/password or Basic auth, and SDK-managed queries/topics/WebSockets use JWT after the initial login exchange.
- Trusted JWTs with a valid role claim do not perform an extra role-only fetch.
- Trusted JWTs without a role still resolve the stored account role correctly.
- OIDC sign-in succeeds only for trusted issuers whose `sub` already exists as an allowed account.
- `system.users` no longer requires a username column or username index.
- `link-common` and `kalam-link-dart` expose typed identity/role semantics consistent with backend responses.
- TypeScript SDK sends `"user"` in login payloads and exports typed identity rather than `Username`.
- Consumer/consumer-WASM crates use `UserId` instead of `Username`.
- CLI flags, environment variables, and human-readable messages use `user` terminology rather than `user_id`.
- Server bootstrap creates the default system user by `UserId`-keyed lookup, not `get_user_by_username`.
- WebSocket auth handler logs `user_id` in tracing spans instead of `username`.
- Raft `MetaCommand::CreateUser` carries the updated `User` struct without a `username` field.
- PG extension e2e tests send `"user"` JSON key to login/setup endpoints and pass.
- Example setup scripts and `.env` files use `KALAMDB_USER` and `"user"` JSON key.
- Backend test harness and all scenario tests create users via `UserId`-keyed patterns.
- The explicit security review leaves no unresolved high-severity auth or server-hardening vulnerability.

## Required Follow-Through

- Update KalamSite SDK docs for any `link/sdks/**` or `link/kalam-link-dart/**` API changes.
- Regenerate generated Dart artifacts instead of editing generated files directly.
