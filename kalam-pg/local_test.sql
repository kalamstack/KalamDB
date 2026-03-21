-- Reinstall extension
DROP EXTENSION IF EXISTS pg_kalam CASCADE;
CREATE EXTENSION pg_kalam;

-- Boot embedded runtime
SELECT pg_kalam_embedded_start(NULL);

-- Create the foreign server
CREATE SERVER IF NOT EXISTS kalam_server FOREIGN DATA WRAPPER pg_kalam;

-- ============================================================
-- SCHEMA OPT-IN: only 'app' is Kalam-managed
-- ============================================================

-- Enable the 'app' schema for Kalam (creates PG schema + Kalam namespace)
SELECT kalam.enable_schema('app', 'user');

-- ============================================================
-- REGULAR PG TABLE (not intercepted – public is NOT enabled)
-- ============================================================

CREATE TABLE public.vanilla_pg (id SERIAL PRIMARY KEY, info TEXT);
INSERT INTO public.vanilla_pg (info) VALUES ('this is a normal PG table');
SELECT * FROM public.vanilla_pg;

-- ============================================================
-- KALAM TABLE: native CREATE TABLE in enabled schema
-- ============================================================

CREATE TABLE app.profiles (
    id TEXT PRIMARY KEY,
    name TEXT,
    age INTEGER
);
-- ^ Event trigger intercepts: drops regular table, creates Kalam table + foreign table

-- Check the foreign table exists
SELECT column_name, data_type FROM information_schema.columns
 WHERE table_schema = 'app' AND table_name = 'profiles'
 ORDER BY ordinal_position;

-- ============================================================
-- NATIVE DML: INSERT / SELECT / UPDATE / DELETE
-- ============================================================

SET kalam.user_id = 'user-alice';

INSERT INTO app.profiles (id, name, age) VALUES ('p1', 'Alice', 30);
INSERT INTO app.profiles (id, name, age) VALUES ('p2', 'Bob', 25);

SELECT * FROM app.profiles;

UPDATE app.profiles SET age = 31 WHERE id = 'p1';
SELECT id, name, age FROM app.profiles WHERE id = 'p1';

DELETE FROM app.profiles WHERE id = 'p2';
SELECT id, name, age FROM app.profiles;

-- ============================================================
-- NATIVE ALTER TABLE: adds column on both PG + Kalam
-- ============================================================

ALTER TABLE app.profiles ADD COLUMN email TEXT;

SELECT column_name, data_type FROM information_schema.columns
 WHERE table_schema = 'app' AND table_name = 'profiles'
 ORDER BY ordinal_position;

-- ============================================================
-- STREAM TABLE via GUC override
-- ============================================================

SET kalam.table_type = 'stream';
SET kalam.stream_ttl_seconds = '7200';

CREATE TABLE app.events (
    id TEXT PRIMARY KEY,
    event_type TEXT,
    payload TEXT
);

RESET kalam.table_type;
RESET kalam.stream_ttl_seconds;

SET kalam.user_id = 'service-a';
INSERT INTO app.events (id, event_type, payload) VALUES ('e1', 'click', '{"x":10}');
SELECT * FROM app.events;

-- ============================================================
-- DROP via DROP FOREIGN TABLE (native)
-- ============================================================

DROP FOREIGN TABLE app.events;
-- ^ sql_drop trigger auto-drops Kalam table

DROP FOREIGN TABLE app.profiles;

SELECT table_name FROM information_schema.tables WHERE table_schema = 'app';

-- Clean up the vanilla PG table
DROP TABLE public.vanilla_pg;

-- Disable the schema (no more interception)
SELECT kalam.disable_schema('app');
