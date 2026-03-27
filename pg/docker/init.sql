-- KalamDB PostgreSQL Extension — Remote Initialization Script
-- Runs automatically on first container start via docker-entrypoint-initdb.d

-- 1. Install the extension
CREATE EXTENSION IF NOT EXISTS pg_kalam;

-- 2. Create the remote foreign server pointing to the KalamDB compose service.
--    When using docker-compose, 'kalamdb' resolves to the KalamDB container.
--    The auth_header JWT must match KALAMDB_JWT_SECRET in docker-compose.yml.
CREATE SERVER IF NOT EXISTS kalam_server
	FOREIGN DATA WRAPPER pg_kalam
	OPTIONS (
		host 'kalamdb',
		port '9188',
		auth_header 'Bearer kalamdb-docker-dev-secret-please-change-32chars'
	);
