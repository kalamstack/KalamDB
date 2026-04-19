# Data Model: User ID-Only Auth Integration

## Overview

The end state has one canonical identity key everywhere: `UserId`. Username and provider-link metadata are removed from the persisted user account and from all supported auth/session contracts. Authorization remains role-based through the existing `Role` enum.

## Entity 1: User Account

**Purpose**: Canonical persisted identity record in `system.users`.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `user_id` | `UserId` | Yes | Primary key and only supported identity key |
| `role` | `Role` | Yes | Canonical account role |
| `auth_policy` | typed enum | Yes | Governs which auth entry paths are allowed for this account |
| `password_hash` | string | Conditional | Required for password-capable accounts; empty/absent for token-only accounts |
| `email` | optional string | No | Profile/contact data only |
| `failed_login_attempts` | integer | Yes | Lockout counter |
| `locked_until` | optional timestamp | No | Account lockout state |
| `last_login_at` | optional timestamp | No | Successful login audit field |
| `last_seen` | optional timestamp | No | Last authenticated activity |
| `created_at` | timestamp | Yes | Creation audit field |
| `updated_at` | timestamp | Yes | Mutation audit field |
| `deleted_at` | optional timestamp | No | Soft-delete marker |
| `storage_mode` | typed enum | Yes | Existing table/storage preference |
| `storage_id` | optional typed ID | No | Existing storage placement preference |

### Removed Fields

- `username`
- provider-derived username index keys
- provider issuer/subject mapping payloads formerly carried in `auth_data`

### Validation Rules

- `user_id` must pass `UserId` validation and be globally unique.
- `role` must be one of the supported `Role` enum values.
- `password_hash` must exist only when the account's auth policy allows password login.
- OIDC-capable accounts must already exist before OIDC login; no auto-provisioning path remains.
- Soft-deleted accounts cannot authenticate through any path.

### State Transitions

| From | To | Trigger |
|------|----|---------|
| `Created` | `Active` | Account persisted successfully |
| `Active` | `Locked` | Failed login threshold exceeded |
| `Locked` | `Active` | Lockout expiry or admin reset |
| `Active` | `SoftDeleted` | User drop/delete operation |
| `SoftDeleted` | `Active` | Explicit restore/undelete flow |

## Entity 2: Trusted Token Claims

**Purpose**: Canonical identity and authorization payload accepted from internal JWTs and trusted OIDC tokens.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `sub` | `UserId` string | Yes | Canonical user identity in all trusted tokens |
| `iss` | string | Yes | Token issuer |
| `exp` | timestamp/epoch | Yes | Expiry claim |
| `iat` | timestamp/epoch | Yes | Issued-at claim |
| `role` | optional `Role` | No | Used directly when present and valid |
| `token_type` | optional typed enum | No | Required for internally-issued access vs refresh distinction |
| `email` | optional string | No | Profile data only |

### Validation Rules

- `sub` must deserialize into a valid `UserId`.
- `role`, when present, must deserialize into a valid `Role`.
- Internal tokens must be signed by KalamDB and include the required access/refresh token type.
- External OIDC tokens must come from a trusted issuer, satisfy configured audience rules, and must not be refresh tokens.
- Tokens without a valid `sub` are rejected.

## Entity 3: Authenticated Session

**Purpose**: Runtime auth context propagated from transport/auth middleware into execution and subscription layers.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `user_id` | `UserId` | Yes | Effective authenticated identity |
| `role` | `Role` | Yes | Effective role after claim-or-account resolution |
| `auth_method` | typed enum | Yes | `Basic`, `Bearer`, `Direct`, or equivalent |
| `read_context` | typed enum | Yes | Existing read-routing mode |
| `connection_info` | typed struct | Yes | IP/localhost context |
| `request_id` | optional ID | No | Audit correlation |
| `timestamp` | time | Yes | Session creation time |

### Validation Rules

- Session identity must always originate from a validated `UserId`.
- Username is not present in the session model.
- If a JWT carries a valid role claim, the session uses it directly.
- If the JWT omits a role, the session uses the role from the already validated user account.

## Entity 4: Issuer Trust Policy

**Purpose**: Configuration-time allowlist of token issuers and audiences accepted for bearer authentication.

### Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `issuer` | string | Yes | Trusted token issuer |
| `audience` | optional string | No | Per-issuer audience/client ID rule |
| `enabled` | bool | Yes | Whether issuer is trusted |

### Validation Rules

- Only configured trusted issuers may reach OIDC validation.
- Internal issuer remains explicitly trusted for KalamDB-issued tokens.
- This policy authorizes providers, not user identities.

## Relationship Rules

- One `Trusted Token Claims` payload resolves to exactly one `User Account` by `sub = user_id`.
- One `Authenticated Session` is created from exactly one validated user account and one validated auth request.
- OIDC user admission requires both a trusted `Issuer Trust Policy` match and a pre-existing `User Account` allowed for OIDC entry.
- No separate provider-link or subject-mapping entity exists in the end state.
