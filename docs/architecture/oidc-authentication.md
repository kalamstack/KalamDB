# OIDC Authentication Architecture

## Overview

KalamDB supports two authentication paths for bearer tokens:

- **Internal tokens** — HS256-signed JWTs issued by KalamDB itself (via login/refresh endpoints).
- **External provider tokens** — RS256/ES256-signed JWTs issued by an OIDC provider such as Keycloak, Google, GitHub, Azure AD, Auth0, or Okta.

Both paths share the same `Authorization: Bearer <token>` header and are routed automatically based on the token's algorithm and issuer. No separate header or endpoint is needed.

This document reflects the current implementation across:
- [backend/crates/kalamdb-auth/src/services/unified/bearer.rs](../../backend/crates/kalamdb-auth/src/services/unified/bearer.rs)
- [backend/crates/kalamdb-auth/src/providers/jwt_auth.rs](../../backend/crates/kalamdb-auth/src/providers/jwt_auth.rs)
- [backend/crates/kalamdb-auth/src/providers/jwt_config.rs](../../backend/crates/kalamdb-auth/src/providers/jwt_config.rs)
- [backend/crates/kalamdb-oidc/src/validator.rs](../../backend/crates/kalamdb-oidc/src/validator.rs)
- [backend/crates/kalamdb-oidc/src/config.rs](../../backend/crates/kalamdb-oidc/src/config.rs)

---

## Configuration

All auth settings live under `[auth]` in `server.toml`:

```toml
[auth]
# HS256 secret for internally-issued tokens (min 32 chars in production)
jwt_secret = "your-secret-key-at-least-32-chars-change-me-in-production"

# Comma-separated list of trusted issuer URLs.
# Must include every OIDC provider you want to accept.
# Defaults to "kalamdb" (internal only) if empty.
jwt_trusted_issuers = "https://keycloak.example.com/realms/myrealm"

# When true, the first login from an unknown provider user auto-creates
# a Role::User account. When false, the user must be pre-created manually.
auto_create_users_from_provider = true
```

To trust multiple providers at once, use a comma-separated list:

```toml
jwt_trusted_issuers = "https://keycloak.example.com/realms/myrealm,https://accounts.google.com"
```

The internal issuer `"kalamdb"` is always implicitly trusted regardless of this setting.

---

## Request Flow

Every `Authorization: Bearer <token>` request enters [`authenticate_bearer`](../../backend/crates/kalamdb-auth/src/services/unified/bearer.rs) and follows this path:

```
┌─────────────────────────────────────────────────────────────┐
│  Bearer token arrives                                        │
└──────────────────────────────┬──────────────────────────────┘
                               │
                  Peek header (no sig check)
                  read: alg, iss
                               │
                  verify_issuer(iss, trusted_issuers)
                  ─ fast-reject unknown issuers ─
                               │
              ┌────────────────┴────────────────┐
              │                                 │
         HS256 alg                       RS256/ES256/PS256 alg
              │                                 │
    iss == "kalamdb"?                  iss == "kalamdb"?
         │        │                        │         │
        YES       NO                      YES        NO
         │        │                        │         │
    validate   REJECT                   REJECT  get_oidc_validator(iss)
    (shared    (symmetric                (internal   │
     secret)    key cannot               must be  validate(token)
                prove external           HS256)   via JWKS
                origin)                           │
         │                                        │
         └───────────────────┬────────────────────┘
                             │
                   parse JwtClaims
                             │
             token_type == Refresh? → REJECT
                             │
              ┌──────────────┴──────────────┐
              │ Internal token               │ External token
              │ lookup user by              │ compose oidc:kcl:{sub}
              │ claims.username             │ lookup or auto-provision
              └──────────────┬──────────────┘
                             │
                   role mismatch check
                             │
                   AuthenticatedUser
```

### Security constraints

| Algorithm | Issuer | Result |
|---|---|---|
| HS256 | `kalamdb` | ✅ Validate with shared secret |
| HS256 | external | ❌ Rejected — symmetric algorithm cannot prove external origin |
| RS256/ES256 | external | ✅ Validate via OIDC JWKS |
| RS256/ES256 | `kalamdb` | ❌ Rejected — internal tokens must use HS256 |

---

## OIDC Discovery and Caching

KalamDB uses standard OIDC Discovery — no manual JWKS URI configuration is needed. When a token from an unknown issuer (but in `trusted_issuers`) is seen for the first time, KalamDB automatically fetches:

```
GET {issuer_url}/.well-known/openid-configuration
→ parse jwks_uri from response
```

There are **two separate cache layers** to make this efficient:

### Layer 1 — OidcValidator registry (`JwtConfig`)

`JwtConfig` holds a `RwLock<HashMap<String, OidcValidator>>` keyed by issuer URL. This is a process-scoped singleton initialized at startup.

```
First request for issuer X:
  1. Read lock → miss
  2. GET {issuer}/.well-known/openid-configuration  (network, once per issuer)
  3. Parse response → extract jwks_uri
  4. Construct OidcValidator, insert into map
  5. Return clone (cloning shares the inner Arc — same JWKS cache)

All subsequent requests for issuer X:
  1. Read lock → hit → return clone immediately (no network, no write lock)
```

This means OIDC Discovery is performed at most **once per issuer per process lifetime**. The write lock is only held during that initial insertion.

### Layer 2 — JWKS key cache (per `OidcValidator`)

Each `OidcValidator` holds its own `Arc<RwLock<HashMap<String, Jwk>>>` keyed by `kid` (key ID from the token header).

```
Token arrives with kid = "abc-123":

  ── Hot path (99% of requests) ──────────────────────────────
  Read lock → kid found → return JWK immediately

  ── Cache miss (key not seen before, or after rotation) ─────
  1. Read lock → miss
  2. GET {jwks_uri}  (Keycloak's public key endpoint)
  3. Compare new key set vs cached set (size + kid presence)
  4. If changed: write lock, replace entire cache
  5. Retry read → found → return JWK
  6. Still not found → OidcError::KeyNotFound
```

Key rotation is handled automatically. When Keycloak rotates its signing key, the next request with a `kid` not in the cache triggers a refresh that picks up the new key — no restart or manual intervention required.

### Shared cache across clones

`OidcValidator` is `Clone` — but cloning it shares the same inner `Arc`, not a copy. All clones of a validator for the same issuer read from and write to the same JWKS HashMap.

```
                         ┌──────────────────────────┐
  clone A ─────────────► │  Arc<RwLock<HashMap>>    │
  clone B ─────────────► │  (single shared JWKS map)│
  clone C ─────────────► │                          │
                         └──────────────────────────┘
```

This means a JWKS refresh triggered by any request immediately benefits all concurrent request handlers.

---

## User Identity for Provider Tokens

Every OIDC user gets a **deterministic username** stored in KalamDB:

```
Format:  oidc:{provider-code}:{subject}
Example: oidc:kcl:f47ac10b-58cc-4372-a567-0e02b2c3d479
```

The 3-character provider code is derived from the issuer URL:

| Issuer pattern | Code |
|---|---|
| Contains `keycloak` or `/realms/` | `kcl` |
| `accounts.google.com` | `ggl` |
| `github.com` | `ghb` |
| `login.microsoftonline.com` / `sts.windows.net` | `msf` |
| `auth0.com` | `a0x` |
| `okta.com` | `okt` |
| Unknown | First 3 hex chars of SHA-256(`issuer_url`) |

This username is indexed in RocksDB (via `IndexedEntityStore`), so user lookups on every authenticated request are O(1) — no table scan.

The `user_id` for auto-provisioned users is also deterministic:

```
user_id = "u_oidc_" + first 16 hex chars of SHA-256("{issuer}:{sub}")
```

This means the same Keycloak user always maps to the same `user_id` even if the KalamDB user record is deleted and recreated.

### Auto-provisioning

When `auto_create_users_from_provider = true` and no user with the composed username exists:

1. A new `User` record is created with `role = Role::User`, `auth_type = AuthType::OAuth`.
2. `email` is taken from the token's `email` claim if present.
3. `auth_data` stores `{ "provider": issuer, "subject": sub }` as JSON.
4. `password_hash` is empty — the user cannot log in via password.

When `auto_create_users_from_provider = false`, the user must be pre-created in KalamDB with a username matching `oidc:{code}:{sub}` before their first login attempt.

---

## Keycloak Setup Checklist

1. Create a realm (e.g. `myrealm`).
2. Create a client (e.g. `kalamdb-api`). Set access type to `bearer-only` for pure API use.
3. Ensure your Keycloak realm is reachable from the KalamDB server at `https://keycloak.example.com/realms/myrealm`.
4. Add the realm URL to `jwt_trusted_issuers` in `server.toml`.
5. Set `auto_create_users_from_provider = true` (or pre-create users manually).
6. Configure your client application to obtain tokens from Keycloak and send them as `Authorization: Bearer <token>`.

No client secret or JWKS URI needs to be configured in KalamDB — Discovery handles it automatically.

---

## Crate Responsibilities

| Crate | Owns |
|---|---|
| `kalamdb-auth/oidc` | `JwtClaims`, `TokenType`, `OidcConfig` (discovery), `OidcValidator` (JWKS cache + validation), `OidcError`, algorithm and issuer extraction helpers |
| `kalamdb-auth/jwt_auth.rs` | `KALAMDB_ISSUER`, `is_internal_issuer`, `verify_issuer`, HS256 signing/validation (`create_and_sign_token`, `validate_jwt_token`) |
| `kalamdb-auth/jwt_config.rs` | `JwtConfig` singleton, `OidcValidator` registry (Layer 1 cache), `parse_trusted_issuers` |
| `kalamdb-auth/bearer.rs` | Algorithm-based routing, user resolution, auto-provisioning orchestration |
