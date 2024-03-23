ALTER SESSION SET SEARCH_PATH = '$current, $public, snowflake.ml';
SHOW PARAMETERS LIKE 'search_path';

SHOW CLASSES IN DATABASE SNOWFLAKE;
SHOW FUNCTIONS IN CLASS ANOMALY_DETECTION;
SHOW PROCEDURES IN CLASS ANOMALY_DETECTION;

CREATE ANOMALY_DETECTION db.schema.obj(...);
CALL db.schema.obj!DETECT_ANOMALIES(...);

SHOW ANOMALY_DETECTION; 		-- show all instances
DROP ANOMALY_DETECTION obj;		-- drop class instance

ALTER CLASSIFICATION name RENAME TO ... / SET/UNSET TAG/COMMENT ...;

-- ===============================================================
-- account budget (built-in instance!)
CALL SNOWFLAKE.LOCAL.account_root_budget!ACTIVATE();
CALL SNOWFLAKE.LOCAL.account_root_budget!SET_SPENDING_LIMIT(1000);

CREATE NOTIFICATION INTEGRATION budgets_ni
   TYPE=EMAIL ENABLED=TRUE
   ALLOWED_RECIPIENTS=('costadmin@example.com', 'budgetadmin@example.com');
GRANT USAGE ON INTEGRATION budgets_ni TO APPLICATION snowflake;

CALL SNOWFLAKE.LOCAL.account_root_budget!SET_EMAIL_NOTIFICATIONS(
   'budgets_ni', 'costadmin@example.com, budgetadmin@example.com');

-- custom budget
SELECT SYSTEM$SHOW_BUDGETS_IN_ACCOUNT();

USE SCHEMA budgets_db.budgets_schema;
CREATE SNOWFLAKE.CORE.BUDGET my_budget();

CALL my_budget!SET_SPENDING_LIMIT(500);
CALL my_budget!SET_EMAIL_NOTIFICATIONS('budgets_ni', 'costadmin@example.com');
CALL my_budget!ADD_RESOURCE(
   SYSTEM$REFERENCE('TABLE', 't1', 'SESSION', 'applybudget'));
