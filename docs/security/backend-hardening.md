# KalamDB Backend Hardening Guide

This guide is for operators and app teams deploying KalamDB for end users.

## 1) Security goals

At a minimum, production deployment should ensure:

- Confidential transport (TLS for client traffic + mTLS for inter-node traffic)
- Strong authentication (Bearer JWT with secure secret and issuer policy)
- Strict authorization (role-based checks, no privilege escalation)
- Abuse resistance (rate limits + request/body size limits)
- Reduced attack surface (localhost-only admin/health where appropriate)

## 2) Baseline production posture

Use this as your baseline before exposing the service to user traffic:

- Bind HTTP API behind a TLS termination proxy (Nginx/Caddy/Ingress)
- Set `server.host` to a non-local address **only** after setting a strong JWT secret
- Keep `authentication.allow_remote_setup = false` unless in controlled bootstrap windows
- Keep `rate_limit.enable_connection_protection = true`
- Restrict CORS origins to exact domains (no wildcard in production)
- Keep setup and health endpoints reachable only through trusted network paths

## 3) Authentication hardening

KalamDB supports authentication and enforces auth in API handlers (for example SQL execution paths use auth extractors and reject invalid/malformed credentials).

### Required settings

In `backend/server.toml`:

```toml
[authentication]
bcrypt_cost = 12
min_password_length = 8
max_password_length = 72
disable_common_password_check = false
jwt_secret = "replace-with-strong-random-secret"
jwt_trusted_issuers = "https://accounts.google.com,https://your-idp.example.com"
allow_remote_setup = false
```

### Important behaviors to rely on

- Insecure/short JWT secrets are rejected for non-localhost startup.
- JWT validation includes signature/expiry checks.
- If a JWT includes `role`, KalamDB validates it against the user’s role in storage (prevents claim tampering privilege escalation).
- Refresh tokens are not accepted as access tokens for regular API auth.

## 4) Authorization and data protection

KalamDB applies authorization at multiple layers:

- HTTP extraction/auth layer (401/403 mapping)
- SQL classification and execution guards (admin-only operations remain admin-only)
- Role-aware execution context for statement handling

### What this protects

- Unauthorized users cannot run admin statements requiring DBA/System privileges.
- Invalid/forged tokens are rejected before query execution.
- End users cannot escalate privileges by modifying JWT role claims.

## 5) Request abuse and DoS controls

Use the rate limiter and body-size constraints in production.

In `backend/server.toml`:

```toml
[rate_limit]
max_queries_per_sec = 100
max_messages_per_sec = 50
max_subscriptions_per_user = 10
max_auth_requests_per_ip_per_sec = 20
max_connections_per_ip = 100
max_requests_per_ip_per_sec = 200
request_body_limit_bytes = 10485760
ban_duration_seconds = 300
enable_connection_protection = true
cache_max_entries = 100000
cache_ttl_seconds = 600

[security]
max_request_body_size = 10485760
max_ws_message_size = 1048576
strict_ws_origin_check = true
allowed_ws_origins = ["https://app.example.com"]
trusted_proxy_ranges = ["10.0.1.9", "10.0.0.0/8"]
```

KalamDB also rejects localhost-spoofing via proxy headers for connection protection logic (helps prevent rate-limit bypass tricks).
Only proxies listed in `security.trusted_proxy_ranges` are allowed to supply `X-Forwarded-For` or `X-Real-IP` when they are not loopback.

## 6) CORS hardening

Avoid `*` origins in production.

```toml
[security.cors]
allowed_origins = ["https://app.example.com", "https://admin.example.com"]
allowed_methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
allowed_headers = ["Authorization", "Content-Type", "Accept", "Origin", "X-Requested-With"]
allow_credentials = true
max_age = 3600
allow_private_network = false
```

## 7) Localhost-only operational endpoints

KalamDB includes localhost-only protections for specific operational endpoints (for example health and setup status/setup flows). Keep this model in place by:

- Not exposing these paths directly to the public Internet
- Restricting ingress/firewall so only trusted hosts can reach them
- Running health probes from local sidecars, node agents, or internal LB addresses

## 8) Logging and observability hygiene

- Use structured logs (`json`) in production.
- Do not log raw secrets, tokens, or plaintext credentials.
- Keep logs in restricted paths with controlled retention.
- Alert on repeated auth failures, 401 spikes, 403 spikes, and IP ban growth.

## 9) End-user data protection best practices

- Use least-privilege roles (`user`, `service`, `dba`, `system`) and avoid broad `system` role use.
- Issue short-lived access tokens and rotate signing secrets periodically.
- Separate application users from operational/admin identities.
- Enforce tenant isolation in application query design and review admin APIs carefully.

## 10) Quick go-live checklist

- [ ] TLS enabled at edge (HTTPS only)
- [ ] Strong `jwt_secret` (32+ chars random)
- [ ] Trusted JWT issuers configured
- [ ] `allow_remote_setup = false`
- [ ] CORS origins are explicit
- [ ] Rate limiting enabled and tuned
- [ ] Request body limits configured
- [ ] Logs/metrics/alerts configured
- [ ] Multi-node: mTLS enabled on cluster RPC
