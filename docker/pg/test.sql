-- ==========================================================================
-- KalamDB PostgreSQL Extension — End-to-End Test (Schema Opt-in)
-- ==========================================================================
-- Tests schema-level opt-in, native DDL/DML, stream tables via GUC, and
-- verifies that non-enabled schemas remain plain PostgreSQL.
--
-- Run with:
--   psql -h localhost -p 5433 -U kalamdb -d kalamdb -f test.sql
-- ==========================================================================

\echo '--- Extension info ---'
SELECT pg_kalam_version()       AS version,
       pg_kalam_compiled_mode() AS mode,
       pg_kalam_embedded_status() AS status;

-- ==========================================================================
-- 1. SCHEMA OPT-IN
-- ==========================================================================
\echo ''
\echo '=== 1. Enable schema for Kalam ==='

SELECT kalam.enable_schema('myapp', 'user');

-- ==========================================================================
-- 1b. REGULAR PG TABLE (not intercepted)
-- ==========================================================================
\echo ''
\echo '=== 1b. Regular PG table in public (not intercepted) ==='

CREATE TABLE public.vanilla (id SERIAL PRIMARY KEY, info TEXT);
INSERT INTO public.vanilla (info) VALUES ('normal PG table');
SELECT * FROM public.vanilla;
DROP TABLE public.vanilla;

-- ==========================================================================
-- 2. NATIVE CREATE TABLE (in enabled schema)
-- ==========================================================================
\echo ''
\echo '=== 2. Native CREATE TABLE in enabled schema ==='

CREATE TABLE myapp.profiles (
    id TEXT PRIMARY KEY,
    name TEXT,
    age INTEGER
);

-- Verify the foreign table columns
\echo '--- Foreign table columns ---'
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'myapp' AND table_name = 'profiles'
 ORDER BY ordinal_position;

-- ==========================================================================
-- 3. NATIVE INSERT / SELECT / UPDATE / DELETE
-- ==========================================================================
\echo ''
\echo '=== 3. Native DML ==='

SET kalam.user_id = 'user-alice';

INSERT INTO myapp.profiles (id, name, age) VALUES ('p1', 'Alice', 30);
INSERT INTO myapp.profiles (id, name, age) VALUES ('p2', 'Bob', 25);

\echo '--- SELECT * FROM myapp.profiles ---'
SELECT * FROM myapp.profiles;

\echo '--- UPDATE + SELECT ---'
UPDATE myapp.profiles SET age = 31 WHERE id = 'p1';
SELECT id, name, age FROM myapp.profiles WHERE id = 'p1';

\echo '--- DELETE + SELECT ---'
DELETE FROM myapp.profiles WHERE id = 'p2';
SELECT id, name, age FROM myapp.profiles;

-- ==========================================================================
-- 4. NATIVE ALTER TABLE
-- ==========================================================================
\echo ''
\echo '=== 4. Native ALTER TABLE ==='

ALTER TABLE myapp.profiles ADD COLUMN email TEXT;

\echo '--- Updated columns ---'
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'myapp' AND table_name = 'profiles'
 ORDER BY ordinal_position;

-- ==========================================================================
-- 5. STREAM TABLE (via GUC override)
-- ==========================================================================
\echo ''
\echo '=== 5. Stream table via SET kalam.table_type ==='

SET kalam.table_type = 'stream';
SET kalam.stream_ttl_seconds = '3600';

CREATE TABLE myapp.events (
    id TEXT PRIMARY KEY,
    event_type TEXT,
    payload TEXT
);

RESET kalam.table_type;
RESET kalam.stream_ttl_seconds;

\echo '--- Stream table columns ---'
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'myapp' AND table_name = 'events'
 ORDER BY ordinal_position;

-- Insert into stream table
SET kalam.user_id = 'service-a';
INSERT INTO myapp.events (id, event_type, payload) VALUES ('e1', 'click', '{"x":10}');

\echo '--- SELECT * FROM myapp.events ---'
SELECT * FROM myapp.events;

-- ==========================================================================
-- 6. NATIVE DROP FOREIGN TABLE
-- ==========================================================================
\echo ''
\echo '=== 6. Drop tables (native DROP FOREIGN TABLE) ==='

DROP FOREIGN TABLE myapp.events;
DROP FOREIGN TABLE myapp.profiles;

-- Verify the foreign tables are gone
\echo '--- Remaining tables in myapp schema ---'
SELECT table_name
  FROM information_schema.tables
 WHERE table_schema = 'myapp';

-- ==========================================================================
-- 7. Explicit helper functions still work
-- ==========================================================================
\echo ''
\echo '=== 7. Explicit helper functions ==='

SELECT kalam_create_user_table('myapp', 'widget', 'id TEXT PRIMARY KEY, label TEXT');
SELECT kalam_drop_table('myapp', 'widget');

-- ==========================================================================
-- 8. Disable schema
-- ==========================================================================
\echo ''
\echo '=== 8. Disable schema ==='

SELECT kalam.disable_schema('myapp');

\echo ''
\echo '=== All tests completed ==='
