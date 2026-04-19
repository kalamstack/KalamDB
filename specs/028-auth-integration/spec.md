# Feature Specification: User ID-Only Auth Integration

**Feature Branch**: `028-auth-integration`  
**Created**: 2026-04-16  
**Status**: Draft  
**Input**: User description: "Replace username-based identity with one canonical internal user identity everywhere, remove backward compatibility, trust a verified local or OIDC token subject as the direct user identity, use typed UserId and Role across backend and Kalam-link, use JWT role claims directly when present, and keep user-facing surfaces labeled as user rather than user_id."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Sign In With One Canonical Identity (Priority: P1)

As an authenticated KalamDB user, I sign in with a single canonical user_id regardless of whether I use a local password, an internal token, or a trusted OIDC token, so the same account is used everywhere without username translation or provider mapping.

**Why this priority**: This is the core behavior change. If canonical identity remains ambiguous, every downstream auth, session, and storage flow stays inconsistent.

**Independent Test**: Can be fully tested by creating one account keyed only by user_id, then authenticating that account through each supported sign-in path and verifying every session resolves to the same identity without any username lookup.

**Acceptance Scenarios**:

1. **Given** an existing user account identified by `usr_123`, **When** the user signs in locally with valid `user_id` and password credentials, **Then** the session is established for `usr_123` without consulting any username field.
2. **Given** a trusted internal token whose canonical identity claim is `usr_123`, **When** the token is presented, **Then** the system signs in `usr_123` directly without secondary identity translation.
3. **Given** a trusted OIDC token whose verified canonical subject is `usr_123` and whose subject is allowlisted, **When** the token is presented, **Then** the system signs in `usr_123` directly without storing or resolving provider-specific identity metadata.
4. **Given** a caller attempts to authenticate using a username-only payload, **When** the request reaches the supported auth interface after cutover, **Then** the request is rejected as unsupported.

---

### User Story 2 - Use JWT Roles Without Extra Resolution (Priority: P2)

As a client or service presenting a trusted JWT, I want authorization to use the role already present in the token when available, so requests stay lightweight and avoid unnecessary account lookups.

**Why this priority**: The performance and simplicity goal depends on not re-resolving identity data that has already been trusted and verified.

**Independent Test**: Can be fully tested by presenting one trusted JWT with a valid role claim and one trusted JWT without a role claim, then verifying the resulting session uses the claimed role in the first case and the stored account role in the second case.

**Acceptance Scenarios**:

1. **Given** a trusted JWT for `usr_123` that includes a valid role claim, **When** the request is authorized, **Then** the established session uses that role claim directly.
2. **Given** a trusted JWT for `usr_123` that omits a role claim, **When** the request is authorized, **Then** the established session uses the stored role for `usr_123`.
3. **Given** a trusted JWT for `usr_123` that contains an invalid or unsupported role value, **When** the request is authorized, **Then** the request is rejected instead of silently remapping permissions.

---

### User Story 3 - Consume One Typed Identity Contract (Priority: P3)

As an SDK or internal integration consumer, I want every supported auth and session contract to expose one typed identity and one typed role model, so integrations do not need to juggle username fields, raw identity strings, or compatibility aliases.

**Why this priority**: The refactor is only complete when backend and Kalam-link surfaces agree on the same identity model; otherwise complexity just moves to the integration boundary.

**Independent Test**: Can be fully tested by inspecting supported auth/session payloads and end-to-end flows after cutover to confirm they use one canonical internal identity, expose user-facing `user` or `id` labels rather than `user_id`, and leave no supported username compatibility path remaining.

**Acceptance Scenarios**:

1. **Given** a supported auth or session contract, **When** identity is exchanged across the boundary, **Then** the contract carries one canonical user value using user-facing `user` or `id` naming instead of duplicate username and user_id identity fields.
2. **Given** a supported auth or session contract, **When** authorization data is exchanged across the boundary, **Then** the contract carries one canonical role value rather than an untyped free-form role string.
3. **Given** the coordinated release cutover is complete, **When** the full release validation flow runs, **Then** no supported path requires username-based compatibility behavior.

---

### User Story 4 - Keep Password Auth On Login Only (Priority: P1)

As a platform operator or SDK integrator, I want user/password credentials to be accepted only by `POST /v1/api/auth/login`, so password-bearing requests are never replayed against SQL, topic, refresh, `/me`, or WebSocket endpoints.

**Why this priority**: This is a hard security and contract boundary. If password auth leaks beyond login, clients can keep sending raw credentials to protected endpoints and the cutover still leaves an unsafe parallel auth path.

**Independent Test**: Can be fully tested by verifying `POST /v1/api/auth/login` accepts valid `user` / `password` credentials, representative non-login protected endpoints reject the same credentials, and every SDK path that starts with user/password performs a login exchange before any authenticated SQL, topic, or WebSocket request.

**Acceptance Scenarios**:

1. **Given** a caller sends valid `user` / `password` credentials to `POST /v1/api/auth/login`, **When** the request is processed, **Then** the server authenticates the account and returns JWT-based session material.
2. **Given** a caller sends the same user/password credentials to a protected non-login endpoint such as `/v1/api/sql`, `/v1/api/auth/me`, `/v1/api/auth/refresh`, or a WebSocket upgrade, **When** the request is processed, **Then** the server rejects it and requires the endpoint's supported token/cookie auth mechanism instead.
3. **Given** an SDK or shared client is configured with user/password credentials, **When** it needs to access SQL, topic, or WebSocket functionality, **Then** it first exchanges those credentials through `POST /v1/api/auth/login` and uses the resulting JWT for all subsequent authenticated requests.

### Edge Cases

- A token is cryptographically valid but does not contain a valid canonical user_id claim or verified subject.
- A trusted OIDC provider presents a valid token whose subject is not allowlisted for entry.
- A trusted JWT includes a role claim that is missing, malformed, or outside the supported role set.
- A local password or internal token targets an account that is deleted, disabled, or locked.
- A legacy client sends username-only login or session payloads after the breaking cutover.
- Two identity records attempt to represent the same person through different identifiers; only one canonical user_id is allowed.
- A user-facing CLI flag, environment variable, payload, or message accidentally exposes `user_id`, `userid`, or `UserId` wording after the cutover.
- An SDK attempts to forward raw user/password credentials to SQL, topic, refresh, `/me`, or WebSocket endpoints instead of exchanging them through `POST /v1/api/auth/login` first.
- A Raft cluster replays a pre-cutover log entry containing a `MetaCommand::CreateUser` with the old `User` struct that includes a `username` field.
- The server bootstrap creates the default system user for the first time after the cutover; the old `get_user_by_username` path no longer exists.
- A WebSocket auth handler logs or traces a `username` field that no longer exists in the auth context.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST treat user_id as the only supported identity key for account storage, authentication, session context, authorization context, row ownership, subscriptions, and user-scoped resource access.
- **FR-002**: The system MUST remove username as a supported identity attribute from the system user record and all supported authentication, session, and integration contracts.
- **FR-003**: The system MUST not provide backward compatibility aliases, translation layers, or fallback lookups that allow username-based identity resolution after the cutover.
- **FR-004**: The system MUST use typed UserId as the canonical identity model anywhere a user identity is exchanged, persisted, or cached across backend and Kalam-link boundaries.
- **FR-005**: The system MUST use typed Role as the canonical authorization model anywhere a role is exchanged, persisted, or cached across backend and Kalam-link boundaries.
- **FR-006**: Local authentication MUST allow direct sign-in to a user account by canonical user_id using password credentials.
- **FR-007**: Local authentication MUST allow direct sign-in to a user account by canonical user_id using a trusted internally issued or self-signed token.
- **FR-008**: OIDC authentication MUST accept only tokens from explicitly trusted providers and MUST authenticate directly to the account identified by the verified canonical subject or user_id claim.
- **FR-009**: OIDC authentication MUST require the verified canonical subject or user_id claim to be allowlisted before granting access.
- **FR-010**: The system MUST not persist or consult provider-specific identity mapping data, synthetic usernames, issuer-subject link tables, or provider-derived usernames for account resolution.
- **FR-011**: When a trusted JWT includes a valid role claim, the system MUST establish the session using that role claim directly.
- **FR-012**: When a trusted JWT omits a role claim, the system MUST establish the session using the stored role associated with the canonical user_id.
- **FR-013**: When a trusted JWT includes an invalid or unsupported role claim, the system MUST reject the token instead of falling back silently.
- **FR-014**: When a trusted JWT already contains both a valid canonical user_id and a valid role, the system MUST not perform an additional identity-resolution step solely to recover role information.
- **FR-015**: The system MUST reject authentication attempts that provide only username-based identity or omit a valid canonical user_id claim.
- **FR-016**: The system MUST expose supported login, token, and user-session responses in canonical identity and role semantics only, without username-based identity fields.
- **FR-017**: The coordinated release MUST apply the identity rename consistently across backend, internal auth flows, and Kalam-link surfaces as one breaking change.
- **FR-018**: The release validation flow MUST confirm that all supported CLI end-to-end auth scenarios still succeed after the cutover without any username compatibility mode enabled.
- **FR-019**: The system MUST keep `UserId` as the internal typed identity model while using `user` or `id` naming on operator-facing environment variables, CLI arguments, public auth payloads, and displayed messages instead of `user_id`, `userid`, or `UserId`.
- **FR-020**: User-facing environment and CLI surfaces MUST use names like `KALAMDB_USER` and `--user` rather than `KALAMDB_USER_ID` or `--user-id`.
- **FR-021**: User-facing auth, setup, and status messages MUST refer to `user` or `credentials` and MUST NOT expose internal implementation wording like `user_id`.
- **FR-022**: The implementation MUST include an explicit security vulnerability and server-hardening review covering auth bypass, issuer/audience/expiry validation, role escalation, rate limiting, system table protection, origin/cookie/JWT secret protections, and public endpoint exposure before sign-off.
- **FR-023**: The server bootstrap path (`create_default_system_user`) MUST use `UserId`-keyed lookup and creation instead of username-based lookup.
- **FR-024**: The WebSocket auth handler MUST replace username-based audit extraction and tracing with `UserId`-based equivalents.
- **FR-025**: The Raft `MetaCommand::CreateUser` MUST carry the updated `User` struct without the username field. Pre-cutover Raft log entries are not required to remain compatible.
- **FR-026**: All peripheral test harnesses, Docker scripts, SDK clients, example scripts, and benchmark helpers MUST be updated to use the post-cutover identity contract.
- **FR-027**: `POST /v1/api/auth/login` MUST be the only supported backend endpoint that accepts direct user/password credentials after the cutover.
- **FR-028**: Protected non-login backend endpoints, including SQL, topic, refresh, `/me`, and WebSocket auth paths, MUST reject user/password or HTTP Basic auth attempts and require their supported Bearer/cookie/JWT mechanism instead.
- **FR-029**: Any SDK or shared client surface that accepts user/password credentials MUST use them only to call `POST /v1/api/auth/login`, then switch to JWT-based auth for all subsequent authenticated requests.

### Out of Scope

- Supporting both username and user_id during transition.
- Preserving provider identity linking, issuer-subject mapping, or synthetic provider usernames.
- Accepting arbitrary third-party tokens outside the configured trust boundary.
- Keeping a separate username-based identity path for SDKs, internal APIs, or local login flows.

### Key Entities *(include if feature involves data)*

- **User Account**: The single persisted identity record keyed by canonical user_id and carrying role, credential eligibility, account status, and audit timestamps.
- **Trusted Auth Token**: A signed local or OIDC token that carries a canonical user_id and may carry a role used to establish the authenticated session.
- **Authenticated Session**: The runtime access context that contains the effective canonical user_id, the effective role, and the authenticated entry path.
- **Provider Allowlist**: The approved set of token issuers and canonical subjects that are permitted to enter the system through OIDC.

## Assumptions

- Upstream authentication provides a stable, globally unique, never-reassigned canonical user_id in the token subject or equivalent canonical identity claim.
- Exactly one user account exists for each canonical user_id.
- Username, if ever needed in the future, would be treated only as non-identity profile metadata in a separate feature and not as an authentication key.
- Signature, issuer, audience, expiry, and subject validation remain mandatory for trusted tokens even though provider mapping is removed.
- The implementation may batch renames and perform release validation at the end of the coordinated cutover rather than validating each intermediate rename in isolation.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of supported authentication entry points accept canonical user_id as the only login identity after release.
- **SC-002**: 100% of username-only authentication attempts are rejected consistently after the breaking cutover.
- **SC-003**: 100% of supported trusted JWT requests that include a valid role claim establish the session with that claimed role and no additional role-recovery step.
- **SC-004**: 100% of supported trusted JWT requests that omit a role claim establish the session with the stored account role and no permission regressions in release validation.
- **SC-005**: 100% of supported OIDC sign-ins succeed or fail solely on trusted token validation and canonical subject allowlist status, without provider-specific identity mapping.
- **SC-006**: The full release validation suite completes successfully with no supported username compatibility path enabled.
- **SC-007**: 100% of supported operator-facing CLI flags, environment variables, auth payloads, and displayed messages use `user` or `id` naming and ship no `user_id`/`UserId` wording.
- **SC-008**: The final security review finds no unresolved high-severity auth or server-hardening vulnerability introduced by the cutover.
- **SC-009**: 100% of tested protected non-login backend endpoints reject direct user/password or HTTP Basic auth after release.
- **SC-010**: 100% of supported SDK-managed SQL, topic, and WebSocket requests that start from user/password credentials first perform a successful login exchange and then use JWT auth rather than forwarding the raw credentials.
