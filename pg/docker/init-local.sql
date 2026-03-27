-- KalamDB PostgreSQL Extension — Local Host Initialization Script
-- Connects to KalamDB running on the host machine (macOS: host.docker.internal)

-- 1. Install the extension
CREATE EXTENSION IF NOT EXISTS pg_kalam;

-- 2. Create the remote foreign server pointing to the host's KalamDB gRPC port.
--    On macOS Docker, 'host.docker.internal' resolves to the host machine.
CREATE SERVER IF NOT EXISTS kalam_server
	FOREIGN DATA WRAPPER pg_kalam
	OPTIONS (
		host 'host.docker.internal',
		port '9188'
	);
