# HTTP Auth Contract

## Contract Scope

This contract covers the supported HTTP auth endpoints after the `UserId`-only cutover.

## Canonical Wire Rules

- Public request payloads use `user` and public response payloads use `id` inside the returned user object; both map to the canonical internal `UserId`.
- `role` is backed by the shared `Role` enum and serialized as one of `anonymous`, `user`, `service`, `dba`, or `system`.
- `username` is not part of any supported auth response.
- Trusted bearer tokens use `sub` as the canonical internal `UserId` claim.
- Human-readable auth/setup messages use `user` or `credentials` wording rather than `user_id`.
- Direct user/password credentials are supported only on `POST /v1/api/auth/login`; protected non-login endpoints reject Basic/password auth and require their supported Bearer/cookie mechanism.

## POST `/v1/api/auth/login`

### Request

```json
{
  "user": "usr_admin",
  "password": "SecretPassword123!"
}
```

- `POST /v1/api/auth/login` is the only supported endpoint that accepts direct `user` / `password` credentials.

### Response

```json
{
  "user": {
    "id": "usr_admin",
    "role": "dba",
    "email": "admin@example.com",
    "created_at": "2026-04-16T12:00:00Z",
    "updated_at": "2026-04-16T12:00:00Z"
  },
  "admin_ui_access": true,
  "expires_at": "2026-04-17T12:00:00Z",
  "access_token": "<jwt>",
  "refresh_token": "<refresh-jwt>",
  "refresh_expires_at": "2026-04-23T12:00:00Z"
}
```

## POST `/v1/api/auth/refresh`

### Request

- Authentication uses a refresh token in the refresh cookie or `Authorization: Bearer <refresh-token>`.
- `Authorization: Basic ...` is unsupported on this endpoint.

### Response

- Same response shape as `POST /v1/api/auth/login`.
- The refreshed response still exposes `user.id` and typed `role` semantics only.

## GET `/v1/api/auth/me`

### Response

```json
{
  "user": {
    "id": "usr_admin",
    "role": "dba",
    "email": "admin@example.com",
    "created_at": "2026-04-16T12:00:00Z",
    "updated_at": "2026-04-16T12:00:00Z"
  },
  "admin_ui_access": true
}
```

- `Authorization: Basic ...` is unsupported on this endpoint.

## POST `/v1/api/auth/setup`

### Request

```json
{
  "user": "usr_admin",
  "password": "SecretPassword123!",
  "root_password": "RootSecret123!",
  "email": "admin@example.com"
}
```

### Response

```json
{
  "user": {
    "id": "usr_admin",
    "role": "dba",
    "email": "admin@example.com",
    "created_at": "2026-04-16T12:00:00Z",
    "updated_at": "2026-04-16T12:00:00Z"
  },
  "message": "Server setup complete. Root password configured and DBA user 'usr_admin' created. Please login to continue."
}
```

## Bearer Token Claim Contract

### Canonical Access Token Claims

```json
{
  "sub": "usr_admin",
  "iss": "kalamdb",
  "role": "dba",
  "exp": 1770000000,
  "iat": 1769996400,
  "token_type": "access"
}
```

### Rules

- `sub` is mandatory and must deserialize into `UserId`.
- `role` is optional for trusted external tokens and mandatory for internally issued access tokens.
- If `role` is present, it must be valid or the token is rejected.
- `username` is not a supported claim.

## Rejection Rules

- Requests that provide only `username` or unsupported internal field names such as `user_id` are unsupported.
- Username-only login payloads must return an auth failure rather than a compatibility redirect.
- Protected non-login endpoints reject direct user/password or HTTP Basic auth requests rather than forwarding them through a compatibility path.
- Tokens without a valid `sub` or with an invalid `role` are rejected.
- User-facing error messages refer to `user` or `credentials`, not `user_id`.
