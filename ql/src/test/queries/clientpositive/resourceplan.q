-- Continue on errors, we do check some error conditions below.
set hive.cli.errors.ignore=true;

-- Prevent NPE in calcite.
set hive.cbo.enable=false;

-- Force DN to create db_privs tables.
show grant user hive_test_user;

-- Initialize the hive schema.
source ../../metastore/scripts/upgrade/hive/hive-schema-3.0.0.hive.sql;

--
-- Actual tests.
--

-- Empty resource plans.
SHOW RESOURCE PLANS;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Create and show plan_1.
CREATE RESOURCE PLAN plan_1;
SHOW RESOURCE PLANS;
SHOW RESOURCE PLAN plan_1;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Create and show plan_2.
CREATE RESOURCE PLAN plan_2 WITH QUERY_PARALLELISM 10;
SHOW RESOURCE PLANS;
SHOW RESOURCE PLAN plan_2;
SELECT * FROM SYS.WM_RESOURCEPLANS;

--
-- Rename resource plans.
--

-- Fail, duplicate name.
ALTER RESOURCE PLAN plan_1 RENAME TO plan_2;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Success.
ALTER RESOURCE PLAN plan_1 RENAME TO plan_3;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Change query parallelism, success.
ALTER RESOURCE PLAN plan_3 SET QUERY_PARALLELISM = 20;
SELECT * FROM SYS.WM_RESOURCEPLANS;

--
-- Activate, enable, disable.
--

-- DISABLED -> ACTIVE fail.
ALTER RESOURCE PLAN plan_3 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- DISABLED -> DISABLED success.
ALTER RESOURCE PLAN plan_3 DISABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- DISABLED -> ENABLED success.
ALTER RESOURCE PLAN plan_3 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ENABLED -> ACTIVE success.
ALTER RESOURCE PLAN plan_3 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ACTIVE -> ACTIVE success.
ALTER RESOURCE PLAN plan_3 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ACTIVE -> ENABLED fail.
ALTER RESOURCE PLAN plan_3 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ACTIVE -> DISABLED fail.
ALTER RESOURCE PLAN plan_3 DISABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- DISABLED -> ENABLED success.
ALTER RESOURCE PLAN plan_2 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- plan_2: ENABLED -> ACTIVE and plan_3: ACTIVE -> ENABLED, success.
ALTER RESOURCE PLAN plan_2 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ENABLED -> ENABLED success.
ALTER RESOURCE PLAN plan_3 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ENABLED -> DISABLED success.
ALTER RESOURCE PLAN plan_3 DISABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

--
-- Drop resource plan.
--

-- Fail, active plan.
DROP RESOURCE PLAN plan_2;

-- Success.
DROP RESOURCE PLAN plan_3;
SELECT * FROM SYS.WM_RESOURCEPLANS;