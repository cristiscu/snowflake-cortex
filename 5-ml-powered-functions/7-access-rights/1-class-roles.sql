-- see https://docs.snowflake.com/en/sql-reference/snowflake-db-classes#label-class-roles
-- class roles --> for privs on the class methods

SHOW ROLES IN CLASS SNOWFLAKE.CORE.BUDGET;
GRANT SNOWFLAKE.CORE.BUDGET ROLE my_budget!ADMIN TO ROLE budget_admin;

-- instance roles = defined in class + instantiated in instance

SHOW GRANTS TO SNOWFLAKE.ML.ANOMALY_DETECTION ROLE my_ad!USER;

GRANT SNOWFLAKE.ML.ANOMALY_DETECTION ROLE my_ad!USER TO ROLE my_role;

GRANT CREATE ANOMALY_DETECTION ON SCHEMA mydb.myschema TO ROLE ml_admin;

