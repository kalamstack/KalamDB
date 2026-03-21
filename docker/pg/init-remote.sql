-- KalamDB PostgreSQL Extension — Remote Mode Initialization Script
-- Runs automatically on first container start via docker-entrypoint-initdb.d

-- 1. Install the extension
CREATE EXTENSION IF NOT EXISTS pg_kalam;

-- 2. Create the remote foreign server pointing to KalamDB gRPC port
--    host.docker.internal resolves to the Docker host machine
CREATE SERVER IF NOT EXISTS kalam_server
    FOREIGN DATA WRAPPER pg_kalam
    OPTIONS (
        host 'host.docker.internal',
        port '9188',
        auth_header 'Bearer kalamdb-local-dev-2026-02-23-strong-jwt-secret-32plus'
    );
