-- KalamDB PostgreSQL Extension — Initialization Script
-- Runs automatically on first container start via docker-entrypoint-initdb.d

-- 1. Install the extension
CREATE EXTENSION IF NOT EXISTS pg_kalam;

-- 2. Create the default foreign server
CREATE SERVER IF NOT EXISTS kalam_server FOREIGN DATA WRAPPER pg_kalam;
