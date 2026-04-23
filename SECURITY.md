# Security Policy

This repository includes both application code and deployment-facing tooling. Treat security fixes and hardening work as first-class contribution areas.

For the detailed hardening checklist, see [docs/security/security-checklist.md](./docs/security/security-checklist.md).
For the current audit history, see [docs/SECURITY-VULNERABILITIES.md](./docs/SECURITY-VULNERABILITIES.md).

## Supported Security Targets

Security fixes land on the current codebase first.

| Target | Support Status | Notes |
|--------|----------------|-------|
| `main` | supported | Primary branch for fixes and validation |
| latest published release | supported on a best-effort basis | Prefer upgrading to the newest release when possible |
| older releases, feature branches, forks, local snapshots | not supported | Rebase or upgrade before requesting security help |

## Reporting a Vulnerability

Do not open a public GitHub issue for an unpatched security problem.

Preferred process:

1. Use GitHub Security Advisories or the repository's private vulnerability reporting flow if it is enabled.
2. If private reporting is not available in your environment, contact the maintainers through a private channel before public disclosure.
3. Include enough detail to reproduce and triage the issue safely.

Please include:

- affected branch, tag, or commit SHA
- deployment mode: single-node, cluster, Docker, PG extension, UI, SDK, or local dev
- exact configuration details that matter to reproduction, without sharing secrets
- steps to reproduce
- expected impact and likely severity
- proof-of-concept requests, SQL, or HTTP examples with credentials redacted

Please do not include live secrets, production tokens, private keys, or customer data in the report.

## Disclosure Expectations

- Give maintainers time to reproduce, fix, and validate the issue before public disclosure.
- Coordinate any regression tests or documentation changes with the fix.
- When a report affects configuration defaults or deployment posture, update the hardening docs in `docs/security/` as part of the patch.

## Secure Deployment Baseline

Before exposing KalamDB outside localhost, at minimum:

- use HTTPS or trusted TLS termination
- set a strong `KALAMDB_JWT_SECRET` and avoid defaults; use at least 32 characters
- disable remote setup with `authentication.allow_remote_setup = false`
- restrict CORS origins to known domains
- validate WebSocket origins with strict origin checking where appropriate
- enable pre-auth connection protection and rate limiting
- keep health, setup, and admin-sensitive endpoints restricted to trusted paths
- use least-privilege roles for users, service accounts, and automation
- rotate secrets, tokens, and node certificates on a defined schedule

For cluster deployments, also:

- enable cluster RPC TLS
- keep Raft and internal RPC ports private
- configure per-node certificates and server names correctly
- avoid reusing private keys across nodes

## Secure Development Expectations

Contributors touching auth, SQL, APIs, storage, or SDK transport layers should verify the following before merging:

- protected endpoints validate authentication, not just header presence
- role checks are enforced by role data, not username conventions
- system tables stay inaccessible to non-admin users
- SQL built from user input is parameterized or escaped safely
- secrets and passwords are never logged in plaintext
- cookies and browser-facing auth defaults remain secure for production use
- public, localhost-only, and authenticated-only surfaces remain clearly separated

## Security Review Checklist

Use this checklist for code review and release readiness:

1. Confirm auth cannot be bypassed by forged headers, stale role claims, or anonymous fallback paths.
2. Confirm SQL and live-query paths do not interpolate unescaped user-controlled values.
3. Confirm system-table access, impersonation, and admin-only flows are still properly gated.
4. Confirm rate limiting, lockout, and origin protections still apply to login and WebSocket flows.
5. Confirm sensitive logs, traces, and errors do not expose passwords, tokens, or internal secrets.
6. Confirm new storage, backup, upload, or path-handling code validates paths and permissions.

## Related Documents

- [docs/security/security-checklist.md](./docs/security/security-checklist.md)
- [docs/security/README.md](./docs/security/README.md)
- [docs/SECURITY-VULNERABILITIES.md](./docs/SECURITY-VULNERABILITIES.md)
- [AGENTS.md](./AGENTS.md)