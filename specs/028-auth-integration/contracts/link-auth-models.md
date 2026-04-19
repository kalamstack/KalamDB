# Link Auth Model Contract

## Contract Scope

This contract covers `link-common` and `kalam-link-dart` auth/session models after the `UserId`-only cutover.

## Shared Rust Model Rules

- `link-common` reuses `kalamdb-commons::UserId` and `kalamdb-commons::Role` instead of SDK-local username wrappers or raw role strings.
- Identity values remain JSON strings on the wire, but Rust APIs are typed.
- `Username`/`username` is removed from supported auth models.
- Public SDK field names use `user` or `id`, not `user_id`.
- SDK password credentials are login-only inputs; they are exchanged through `POST /v1/api/auth/login` before any authenticated SQL, topic, or WebSocket call.

## `link-common` Rust Shapes

### Login Request

```rust
pub struct LoginRequest {
    pub user: UserId,
    pub password: String,
}
```

### Login User Info

```rust
pub struct LoginUserInfo {
    pub id: UserId,
    pub role: Role,
    pub email: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}
```

### Stored Credentials

```rust
pub struct Credentials {
    pub instance: String,
    pub jwt_token: String,
    pub user: Option<UserId>,
    pub expires_at: Option<String>,
    pub server_url: Option<String>,
    pub refresh_token: Option<String>,
    pub refresh_expires_at: Option<String>,
}
```

### Auth Provider

- Basic/password constructors and login helpers accept a canonical user value backed by `UserId` as the identity input, but the resulting credentials are used only for `POST /v1/api/auth/login`.
- Bearer auth remains token-based and identity is recovered from `sub`.

### WebSocket Auth Success

```rust
ServerMessage::AuthSuccess {
    user: UserId,
    role: Role,
    protocol: ProtocolOptions,
}
```

## Dart Bridge Expectations

- Rust-to-Dart bridge models stop exposing `username` in login/setup/auth-success payloads.
- Role is exposed as a typed Dart enum or equivalent generated typed surface rather than a free-form string.
- Dart login helpers accept a canonical `user` parameter backed by `UserId`.
- Generated bridge files are regenerated via `link/sdks/dart/build.sh`; they are not edited manually.

## Dependency Rule

- `link-common` must depend on `kalamdb-commons` with the smallest feature set needed for `UserId`, `Role`, and serialization support.
- If workspace inheritance prevents minimal feature selection, use the same compile-surface discipline already applied to the PG extension work.
