-- ==========================================================================
-- KalamDB PostgreSQL Extension — Remote Mode End-to-End Test
-- ==========================================================================
-- Tests remote FDW connectivity to a running KalamDB gRPC server.
--
-- Prerequisites:
--   1. KalamDB server running on host:9188 (cd backend && cargo run)
--   2. Namespace "rmtest" and user table "profiles" created on KalamDB server
--   3. Extension loaded in remote mode with kalam_server configured
--
-- Run with:
--   psql -h localhost -p 5434 -U kalamdb -d kalamdb -f test-remote.sql
-- ==========================================================================

\echo '--- Extension info ---'
SELECT pg_kalam_version()       AS version,
       pg_kalam_compiled_mode() AS mode;

-- ==========================================================================
-- 0. VERIFY REMOTE MODE
-- ==========================================================================
\echo ''
\echo '=== 0. Verify compiled mode is remote ==='

DO $$
BEGIN
    IF pg_kalam_compiled_mode() <> 'remote' THEN
        RAISE EXCEPTION 'Extension is NOT compiled in remote mode (got: %)', pg_kalam_compiled_mode();
    END IF;
END;
$$;

-- ==========================================================================
-- 1. CREATE SCHEMA + FOREIGN TABLE pointing to KalamDB remote table
-- ==========================================================================
\echo ''
\echo '=== 1. Create schema + foreign table for remote KalamDB table ==='

CREATE SCHEMA IF NOT EXISTS rmtest;

-- Drop existing foreign table if any (for idempotent re-runs)
DROP FOREIGN TABLE IF EXISTS rmtest.profiles;

-- Create the foreign table matching the KalamDB "rmtest.profiles" table.
-- The namespace/table/table_type OPTIONS tell the FDW which KalamDB table to target.
CREATE FOREIGN TABLE rmtest.profiles (
    id TEXT,
    name TEXT,
    age INTEGER,
    _userid TEXT,
    _seq BIGINT,
    _deleted BOOLEAN
) SERVER kalam_server
  OPTIONS (namespace 'rmtest', "table" 'profiles', table_type 'user');

\echo '--- Foreign table columns ---'
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'rmtest' AND table_name = 'profiles'
 ORDER BY ordinal_position;

-- ==========================================================================
-- 2. INSERT rows via FDW (remote gRPC)
-- ==========================================================================
\echo ''
\echo '=== 2. INSERT rows ==='

SET kalam.user_id = 'user-remote-test';

INSERT INTO rmtest.profiles (id, name, age) VALUES ('p1', 'Alice', 30);
INSERT INTO rmtest.profiles (id, name, age) VALUES ('p2', 'Bob', 25);
INSERT INTO rmtest.profiles (id, name, age) VALUES ('p3', 'Charlie', 35);

\echo 'Inserted 3 rows.'

-- ==========================================================================
-- 3. SELECT rows via FDW (remote gRPC scan)
-- ==========================================================================
\echo ''
\echo '=== 3. SELECT * FROM rmtest.profiles ==='

SELECT id, name, age FROM rmtest.profiles ORDER BY id;

-- Verify row count
DO $$
DECLARE
    cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM rmtest.profiles;
    IF cnt < 3 THEN
        RAISE EXCEPTION 'Expected at least 3 rows, got %', cnt;
    END IF;
    RAISE NOTICE 'Row count OK: %', cnt;
END;
$$;

-- ==========================================================================
-- 4. UPDATE via FDW (remote gRPC)
-- ==========================================================================
\echo ''
\echo '=== 4. UPDATE row ==='

UPDATE rmtest.profiles SET age = 31 WHERE id = 'p1';

\echo '--- Verify UPDATE ---'
SELECT id, name, age FROM rmtest.profiles WHERE id = 'p1';

DO $$
DECLARE
    v_age INTEGER;
BEGIN
    SELECT age INTO v_age FROM rmtest.profiles WHERE id = 'p1';
    IF v_age <> 31 THEN
        RAISE EXCEPTION 'Expected age=31 after UPDATE, got %', v_age;
    END IF;
    RAISE NOTICE 'UPDATE OK: age=%', v_age;
END;
$$;

-- ==========================================================================
-- 5. DELETE via FDW (remote gRPC)
-- ==========================================================================
\echo ''
\echo '=== 5. DELETE row ==='

DELETE FROM rmtest.profiles WHERE id = 'p3';

\echo '--- Verify DELETE ---'
SELECT id, name, age FROM rmtest.profiles ORDER BY id;

DO $$
DECLARE
    cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM rmtest.profiles WHERE id = 'p3';
    IF cnt <> 0 THEN
        RAISE EXCEPTION 'Expected p3 to be deleted, but found % row(s)', cnt;
    END IF;
    RAISE NOTICE 'DELETE OK: p3 no longer visible';
END;
$$;

-- ==========================================================================
-- 6. CLEANUP (delete remaining rows for idempotent re-runs)
-- ==========================================================================
\echo ''
\echo '=== 6. Cleanup ==='

DELETE FROM rmtest.profiles WHERE id IN ('p1', 'p2');

\echo ''
\echo '========================================'
\echo ' All remote-mode tests passed!'
\echo '========================================'
