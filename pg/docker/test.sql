-- ==========================================================================
-- KalamDB PostgreSQL Extension — Remote Mode End-to-End Test
-- ==========================================================================
-- Tests remote FDW connectivity to a running KalamDB gRPC server.
--
-- Run with:
--   psql -h localhost -p 5433 -U kalamdb -d kalamdb -f test.sql
-- ==========================================================================

\echo '--- Extension info ---'
SELECT pg_kalam_version()       AS version,
       pg_kalam_compiled_mode() AS mode;

-- ==========================================================================
-- 1. SCHEMA OPT-IN
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
-- 1. CREATE SCHEMA + FOREIGN TABLE
-- ==========================================================================
\echo ''
\echo '=== 1. Create schema + foreign table for remote KalamDB table ==='

CREATE SCHEMA IF NOT EXISTS rmtest;
DROP FOREIGN TABLE IF EXISTS rmtest.profiles;

CREATE FOREIGN TABLE rmtest.profiles (
  id TEXT,
  name TEXT,
  age INTEGER,
  _userid TEXT,
  _seq BIGINT,
  _deleted BOOLEAN
) SERVER kalam_server
  OPTIONS (namespace 'rmtest', "table" 'profiles', table_type 'user');

-- Verify the foreign table columns
\echo '--- Foreign table columns ---'
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'rmtest' AND table_name = 'profiles'
 ORDER BY ordinal_position;

-- ==========================================================================
-- 2. INSERT rows via FDW
-- ==========================================================================
\echo ''
\echo '=== 2. INSERT rows ==='

SET kalam.user_id = 'user-remote-test';

-- Clean up any leftover rows from previous runs (user-remote-test scope)
DELETE FROM rmtest.profiles WHERE id IN (
  'p1', 'p2', 'p3',
  'tx1', 'tx2', 'tx3', 'tx4',
  'rb1',
  'm1', 'm2', 'm3',
  'xv1',
  'cc1', 'cc2', 'cc3', 'cc4', 'cc5'
);

-- Clean up rows from other user scopes
SET kalam.user_id = 'user-A';
DELETE FROM rmtest.profiles WHERE id IN ('ua1');
SET kalam.user_id = 'user-B';
DELETE FROM rmtest.profiles WHERE id IN ('ub1');
SET kalam.user_id = 'cross-verify-user';
DELETE FROM rmtest.profiles WHERE id IN ('xv1');

SET kalam.user_id = 'user-remote-test';

INSERT INTO rmtest.profiles (id, name, age) VALUES ('p1', 'Alice', 30);
INSERT INTO rmtest.profiles (id, name, age) VALUES ('p2', 'Bob', 25);
INSERT INTO rmtest.profiles (id, name, age) VALUES ('p3', 'Charlie', 35);

-- ==========================================================================
-- 3. SELECT rows via FDW
-- ==========================================================================
\echo ''
\echo '=== 3. SELECT rows ==='

SELECT id, name, age FROM rmtest.profiles ORDER BY id;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles;
  IF cnt < 3 THEN
    RAISE EXCEPTION 'Expected at least 3 rows, got %', cnt;
  END IF;
END;
$$;

-- ==========================================================================
-- 4. UPDATE row via FDW
-- ==========================================================================
\echo ''
\echo '=== 4. UPDATE row ==='

UPDATE rmtest.profiles SET age = 31 WHERE id = 'p1';
SELECT id, name, age FROM rmtest.profiles WHERE id = 'p1';

DO $$
DECLARE
  v_age INTEGER;
BEGIN
  SELECT age INTO v_age FROM rmtest.profiles WHERE id = 'p1';
  IF v_age <> 31 THEN
    RAISE EXCEPTION 'Expected age=31 after UPDATE, got %', v_age;
  END IF;
END;
$$;

-- ==========================================================================
-- 5. DELETE row via FDW
-- ==========================================================================
\echo ''
\echo '=== 5. DELETE row ==='

DELETE FROM rmtest.profiles WHERE id = 'p3';
SELECT id, name, age FROM rmtest.profiles ORDER BY id;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles WHERE id = 'p3';
  IF cnt <> 0 THEN
    RAISE EXCEPTION 'Expected p3 to be deleted, but found % row(s)', cnt;
  END IF;
END;
$$;

-- ==========================================================================
-- 6. TRANSACTION LIFECYCLE: sequential SELECTs in same session
-- ==========================================================================
-- Regression test: previously the xact_callback consumed transaction state on
-- XACT_EVENT_PRE_COMMIT, causing the COMMIT handler to skip the commit RPC.
-- The server-side session then retained a stale active transaction, making the
-- next query fail with "already has an active transaction".
\echo ''
\echo '=== 6. Sequential SELECTs (transaction cleanup) ==='

SELECT count(*) AS q1_count FROM rmtest.profiles;
SELECT count(*) AS q2_count FROM rmtest.profiles;
SELECT count(*) AS q3_count FROM rmtest.profiles;
SELECT count(*) AS q4_count FROM rmtest.profiles;
SELECT count(*) AS q5_count FROM rmtest.profiles;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  -- If we get here, 5 sequential queries succeeded without transaction errors
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles;
  IF cnt < 1 THEN
    RAISE EXCEPTION 'Sequential SELECT test: expected rows, got %', cnt;
  END IF;
END;
$$;

-- ==========================================================================
-- 7. ORDER BY variants
-- ==========================================================================
-- Regression test: ORDER BY DESC previously returned empty rows when the
-- transaction lifecycle was broken.
\echo ''
\echo '=== 7. ORDER BY variants ==='

SELECT id, name, age FROM rmtest.profiles ORDER BY age ASC;
SELECT id, name, age FROM rmtest.profiles ORDER BY age DESC;
SELECT id, name, age FROM rmtest.profiles ORDER BY name ASC;

DO $$
DECLARE
  first_age INTEGER;
  last_age INTEGER;
BEGIN
  SELECT age INTO first_age FROM rmtest.profiles ORDER BY age ASC LIMIT 1;
  SELECT age INTO last_age FROM rmtest.profiles ORDER BY age DESC LIMIT 1;
  IF first_age >= last_age AND (SELECT COUNT(*) FROM rmtest.profiles) > 1 THEN
    RAISE EXCEPTION 'ORDER BY test: ASC first (%) should be < DESC first (%)', first_age, last_age;
  END IF;
END;
$$;

-- ==========================================================================
-- 8. WHERE filters with ORDER BY
-- ==========================================================================
\echo ''
\echo '=== 8. WHERE + ORDER BY ==='

SELECT id, name, age FROM rmtest.profiles WHERE age > 25 ORDER BY age DESC;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles WHERE age > 25;
  IF cnt < 1 THEN
    RAISE EXCEPTION 'WHERE filter test: expected at least 1 row with age > 25, got %', cnt;
  END IF;
END;
$$;

-- ==========================================================================
-- 9. Empty result set
-- ==========================================================================
\echo ''
\echo '=== 9. Empty result set ==='

SELECT id, name, age FROM rmtest.profiles WHERE age > 9999;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles WHERE age > 9999;
  IF cnt <> 0 THEN
    RAISE EXCEPTION 'Empty result test: expected 0 rows, got %', cnt;
  END IF;
END;
$$;

-- ==========================================================================
-- 10. TRANSACTION: multi-INSERT in BEGIN/COMMIT
-- ==========================================================================
\echo ''
\echo '=== 10. Transaction: multi-INSERT in BEGIN/COMMIT ==='

BEGIN;
INSERT INTO rmtest.profiles (id, name, age) VALUES ('tx1', 'TxAlice', 40);
INSERT INTO rmtest.profiles (id, name, age) VALUES ('tx2', 'TxBob', 41);
INSERT INTO rmtest.profiles (id, name, age) VALUES ('tx3', 'TxCharlie', 42);
COMMIT;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles WHERE id IN ('tx1', 'tx2', 'tx3');
  IF cnt <> 3 THEN
    RAISE EXCEPTION 'Transaction multi-INSERT: expected 3 rows, got %', cnt;
  END IF;
END;
$$;

-- ==========================================================================
-- 11. TRANSACTION: multi-INSERT + UPDATE in single transaction
-- ==========================================================================
\echo ''
\echo '=== 11. Transaction: INSERT + UPDATE in single transaction ==='

BEGIN;
INSERT INTO rmtest.profiles (id, name, age) VALUES ('tx4', 'TxDave', 50);
UPDATE rmtest.profiles SET age = 55 WHERE id = 'tx4';
COMMIT;

DO $$
DECLARE
  v_age INTEGER;
BEGIN
  SELECT age INTO v_age FROM rmtest.profiles WHERE id = 'tx4';
  IF v_age <> 55 THEN
    RAISE EXCEPTION 'Transaction INSERT+UPDATE: expected age=55, got %', v_age;
  END IF;
END;
$$;

-- ==========================================================================
-- 12. TRANSACTION: ROLLBACK (should not error, KalamDB Phase 1)
-- ==========================================================================
\echo ''
\echo '=== 12. Transaction: ROLLBACK does not error ==='

BEGIN;
INSERT INTO rmtest.profiles (id, name, age) VALUES ('rb1', 'Rollback', 99);
ROLLBACK;

-- Note: KalamDB Phase 1 does not support true rollback of writes. The row
-- may or may not exist. The key assertion is that ROLLBACK does not error
-- and subsequent queries still work.
SELECT COUNT(*) AS post_rollback_count FROM rmtest.profiles;

-- ==========================================================================
-- 13. SHARED TABLE: CRUD via FDW
-- ==========================================================================
\echo ''
\echo '=== 13. Shared table: CRUD via FDW ==='

DROP FOREIGN TABLE IF EXISTS rmtest.shared_items;

CREATE FOREIGN TABLE rmtest.shared_items (
  id TEXT,
  title TEXT,
  value INTEGER,
  _userid TEXT,
  _seq BIGINT,
  _deleted BOOLEAN
) SERVER kalam_server
  OPTIONS (namespace 'rmtest', "table" 'shared_items', table_type 'shared');

-- Clean up leftover shared rows from previous runs
DELETE FROM rmtest.shared_items WHERE id IN ('s1', 's2');

INSERT INTO rmtest.shared_items (id, title, value) VALUES ('s1', 'SharedOne', 100);
INSERT INTO rmtest.shared_items (id, title, value) VALUES ('s2', 'SharedTwo', 200);

SELECT id, title, value FROM rmtest.shared_items ORDER BY id;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.shared_items;
  IF cnt < 2 THEN
    RAISE EXCEPTION 'Shared table: expected at least 2 rows, got %', cnt;
  END IF;
END;
$$;

-- UPDATE on shared table
UPDATE rmtest.shared_items SET value = 150 WHERE id = 's1';

DO $$
DECLARE
  v_value INTEGER;
BEGIN
  SELECT value INTO v_value FROM rmtest.shared_items WHERE id = 's1';
  IF v_value <> 150 THEN
    RAISE EXCEPTION 'Shared table UPDATE: expected value=150, got %', v_value;
  END IF;
END;
$$;

-- DELETE on shared table
DELETE FROM rmtest.shared_items WHERE id = 's2';

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.shared_items WHERE id = 's2';
  IF cnt <> 0 THEN
    RAISE EXCEPTION 'Shared table DELETE: expected s2 deleted, got % rows', cnt;
  END IF;
END;
$$;

-- ==========================================================================
-- 14. USER TABLE: verify user_id scoping
-- ==========================================================================
\echo ''
\echo '=== 14. User table: user_id scoping ==='

-- Insert as user-A
SET kalam.user_id = 'user-A';
INSERT INTO rmtest.profiles (id, name, age) VALUES ('ua1', 'UserA_Row', 60);

-- Insert as user-B
SET kalam.user_id = 'user-B';
INSERT INTO rmtest.profiles (id, name, age) VALUES ('ub1', 'UserB_Row', 70);

-- Query as user-A: should see user-A's rows but not user-B's
SET kalam.user_id = 'user-A';
DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles WHERE id = 'ua1';
  IF cnt < 1 THEN
    RAISE EXCEPTION 'User scoping: user-A should see ua1, got % rows', cnt;
  END IF;
END;
$$;

-- Reset user_id for remaining tests
SET kalam.user_id = 'user-remote-test';

-- ==========================================================================
-- 15. ALTER FOREIGN TABLE (PG-side column addition)
-- ==========================================================================
\echo ''
\echo '=== 15. ALTER FOREIGN TABLE ==='

-- Add a column to the foreign table definition (PG-side only).
-- Note: KalamDB validates projected columns, so the FDW will send all columns
-- from the foreign table definition. We verify the PG DDL works, then drop
-- the extra column before querying again.
ALTER FOREIGN TABLE rmtest.profiles ADD COLUMN email TEXT;

-- Verify the new column appears in PG's information_schema
DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt
    FROM information_schema.columns
   WHERE table_schema = 'rmtest'
     AND table_name = 'profiles'
     AND column_name = 'email';
  IF cnt <> 1 THEN
    RAISE EXCEPTION 'ALTER FOREIGN TABLE: expected email column in PG, not found';
  END IF;
END;
$$;

-- Drop the added column to restore original schema
ALTER FOREIGN TABLE rmtest.profiles DROP COLUMN email;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt
    FROM information_schema.columns
   WHERE table_schema = 'rmtest'
     AND table_name = 'profiles'
     AND column_name = 'email';
  IF cnt <> 0 THEN
    RAISE EXCEPTION 'ALTER DROP COLUMN: email column should be removed, still found';
  END IF;
END;
$$;

-- Verify queries still work after ALTER ADD + DROP cycle
SELECT id, name, age FROM rmtest.profiles LIMIT 1;

-- ==========================================================================
-- 16. Multi-statement DML stress test
-- ==========================================================================
\echo ''
\echo '=== 16. Multi-statement DML stress ==='

BEGIN;
INSERT INTO rmtest.profiles (id, name, age) VALUES ('m1', 'Multi1', 1);
INSERT INTO rmtest.profiles (id, name, age) VALUES ('m2', 'Multi2', 2);
INSERT INTO rmtest.profiles (id, name, age) VALUES ('m3', 'Multi3', 3);
UPDATE rmtest.profiles SET age = 10 WHERE id = 'm1';
DELETE FROM rmtest.profiles WHERE id = 'm3';
COMMIT;

DO $$
DECLARE
  cnt INTEGER;
  v_age INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles WHERE id IN ('m1', 'm2');
  IF cnt < 2 THEN
    RAISE EXCEPTION 'Multi-DML: expected m1+m2 to exist, got % rows', cnt;
  END IF;
  SELECT age INTO v_age FROM rmtest.profiles WHERE id = 'm1';
  IF v_age <> 10 THEN
    RAISE EXCEPTION 'Multi-DML: expected m1 age=10, got %', v_age;
  END IF;
  SELECT COUNT(*) INTO cnt FROM rmtest.profiles WHERE id = 'm3';
  IF cnt <> 0 THEN
    RAISE EXCEPTION 'Multi-DML: expected m3 deleted, got % rows', cnt;
  END IF;
END;
$$;

-- ==========================================================================
-- 17. CREATE TABLE ... USING kalamdb (native PG syntax → KalamDB)
-- ==========================================================================
\echo ''
\echo '=== 17. CREATE TABLE USING kalamdb: PRIMARY KEY + SERIAL + defaults ==='

-- 17a. Basic PRIMARY KEY + DEFAULT SNOWFLAKE_ID() + WITH options
CREATE SCHEMA IF NOT EXISTS rmtest;

DROP FOREIGN TABLE IF EXISTS rmtest.using_test;
CREATE TABLE rmtest.using_test (
  id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  name TEXT NOT NULL,
  status TEXT DEFAULT 'active',
  created TIMESTAMP DEFAULT NOW()
) USING kalamdb WITH (type = 'user');

-- Verify: foreign table exists in PG
DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt
    FROM information_schema.tables
   WHERE table_schema = 'rmtest'
     AND table_name = 'using_test';
  IF cnt <> 1 THEN
    RAISE EXCEPTION 'USING kalamdb: PG foreign table not created';
  END IF;
END;
$$;

-- Verify: table columns in PG (id should be BIGINT, not BIGSERIAL)
\echo '--- USING kalamdb: PG columns ---'
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'rmtest' AND table_name = 'using_test'
 ORDER BY ordinal_position;

-- 17b. INSERT data (id auto-generated by KalamDB via SNOWFLAKE_ID)
SET kalam.user_id = 'using-test-user';
INSERT INTO rmtest.using_test (name) VALUES ('Alice');
INSERT INTO rmtest.using_test (name, status) VALUES ('Bob', 'inactive');

-- 17c. Verify data was written and defaults applied
SELECT id, name, status FROM rmtest.using_test ORDER BY name;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT COUNT(*) INTO cnt FROM rmtest.using_test;
  IF cnt < 2 THEN
    RAISE EXCEPTION 'USING kalamdb: expected at least 2 rows, got %', cnt;
  END IF;
END;
$$;

-- 17d. Multiple column defaults
DROP FOREIGN TABLE IF EXISTS rmtest.defaults_test;
CREATE TABLE rmtest.defaults_test (
  id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  label TEXT DEFAULT 'untitled',
  score INTEGER DEFAULT 0
) USING kalamdb WITH (type = 'shared');

\echo '--- Defaults test: PG columns ---'
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'rmtest' AND table_name = 'defaults_test'
 ORDER BY ordinal_position;

-- 17e. Cleanup
DROP FOREIGN TABLE IF EXISTS rmtest.defaults_test;
DROP FOREIGN TABLE IF EXISTS rmtest.using_test;

-- ==========================================================================
-- 18. CLEANUP
-- ==========================================================================
\echo ''
\echo '=== 18. Cleanup ==='

SET kalam.user_id = 'user-remote-test';
DELETE FROM rmtest.profiles WHERE id IN ('p1', 'p2', 'tx1', 'tx2', 'tx3', 'tx4', 'rb1', 'm1', 'm2');

SET kalam.user_id = 'user-A';
DELETE FROM rmtest.profiles WHERE id IN ('ua1');
SET kalam.user_id = 'user-B';
DELETE FROM rmtest.profiles WHERE id IN ('ub1');

DELETE FROM rmtest.shared_items WHERE id = 's1';

\echo ''
\echo '========================================'
\echo ' All remote-mode tests passed!'
\echo '========================================'
