# SQL Identity Contract

## Contract Scope

This contract covers SQL-visible identity behavior after the auth cutover.

## Canonical Rules

- Any SQL surface that names a user now names that user by the canonical internal identity backed by `UserId`.
- No SQL user-management or impersonation path performs a username fallback.
- `CURRENT_USER()` returns the canonical user string.
- `CURRENT_ROLE()` continues to expose the current role value from the shared `Role` enum.
- User-facing SQL errors and audit messages refer to `user`, not `user_id`.

## User DDL Semantics

### CREATE USER

- The user identifier supplied to `CREATE USER` is the canonical user value backed by `UserId`.
- Implementations may preserve the current syntactic slot in the parser, but the semantic meaning is the canonical user identity only.
- No secondary username is generated or stored.

### ALTER USER

- The target of `ALTER USER` is the canonical user value backed by `UserId`.
- Password, role, and email modifications apply to the account keyed by that canonical identity.

### DROP USER

- The target of `DROP USER` is the canonical user value backed by `UserId`.
- Drop/soft-delete resolution must not fall back to username.

## Impersonation Semantics

- `EXECUTE AS USER` and related impersonation flows accept only canonical user values backed by `UserId`.
- Authorization checks compare the actor role against the resolved target account role.
- No fallback lookup by username is permitted.

## Examples

```sql
CREATE USER 'svc_ingest' WITH PASSWORD 'SecretPassword123!' ROLE service;
ALTER USER 'svc_ingest' SET ROLE dba;
DROP USER 'svc_ingest';
SELECT CURRENT_USER(), CURRENT_ROLE();
```

## Validation Rules

- User identifiers supplied through SQL must pass `UserId` validation.
- Invalid or missing canonical user identifiers fail the statement.
- Statements that previously depended on username identity are unsupported after the cutover.
