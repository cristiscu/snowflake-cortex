-- account budget (built-in instance!) - not in trial account!
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
CALL my_budget!SET_EMAIL_NOTIFICATIONS(
   'budgets_ni', 'costadmin@example.com');
   
CALL my_budget!ADD_RESOURCE(
   SYSTEM$REFERENCE('TABLE', 't1', 'SESSION', 'applybudget'));
