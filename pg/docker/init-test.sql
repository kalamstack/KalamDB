-- KalamDB PostgreSQL Extension — E2E Test Init
-- Runs on first container start.

CREATE EXTENSION IF NOT EXISTS pg_kalam;

-- Create the foreign server with the pre-shared auth token.
-- The auth_header value must match KALAMDB_PG_AUTH_TOKEN in docker-compose.test.yml.
CREATE SERVER IF NOT EXISTS kalam_server
    FOREIGN DATA WRAPPER pg_kalam
    OPTIONS (
        host 'kalamdb',
        port '9188',
        auth_header 'Bearer e2e-pg-shared-secret-token-for-testing'
    );
