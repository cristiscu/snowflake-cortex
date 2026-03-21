-- see https://docs.snowflake.com/en/user-guide/ml-powered-forecasting#granting-privileges-to-create-forecast-objects

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE compute_wh;
USE SCHEMA test.ts;

-- cleanup
DROP ROLE IF EXISTS analyst;
DROP ROLE IF EXISTS consumer;
DROP SNOWFLAKE.ML.FORECAST IF EXISTS regressor;

-- show instances/roles/privileges in specific class
SHOW SNOWFLAKE.ML.FORECAST;
SHOW ROLES IN CLASS SNOWFLAKE.ML.FORECAST;

-- =======================================================================
-- recommended custom analyst role to create/own/train model in specific schema
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE ROLE analyst;
grant role analyst to role sysadmin;

GRANT USAGE ON database test TO ROLE analyst;
GRANT USAGE ON schema test.ts TO ROLE analyst;
GRANT SELECT ON VIEW view1_train TO ROLE analyst;
GRANT USAGE ON warehouse compute_wh TO ROLE analyst;
GRANT CREATE SNOWFLAKE.ML.FORECAST ON SCHEMA test.ts TO ROLE analyst;
SHOW GRANTS TO ROLE analyst;

USE ROLE analyst;
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST regressor(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view1_train'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

CALL regressor!EXPLAIN_FEATURE_IMPORTANCE();

-- =======================================================================
-- recommended custom consumer role to only forecast with existing models in specific schema
USE ROLE ACCOUNTADMIN;
CREATE ROLE consumer;
grant role consumer to role sysadmin;

GRANT USAGE ON database test TO ROLE consumer;
GRANT USAGE ON schema test.ts TO ROLE consumer;
GRANT USAGE ON warehouse compute_wh TO ROLE consumer;
GRANT SNOWFLAKE.ML.FORECAST ROLE regressor!USER TO ROLE consumer;
SHOW GRANTS TO ROLE consumer;

USE ROLE consumer;
CALL regressor!FORECAST(FORECASTING_PERIODS => 3);

CALL regressor!EXPLAIN_FEATURE_IMPORTANCE();   -- no privilege error!

-- =======================================================================
-- show instances/roles/privileges in specific class
USE ROLE ACCOUNTADMIN;
SHOW SNOWFLAKE.ML.FORECAST;
SHOW ROLES IN CLASS SNOWFLAKE.ML.FORECAST;
SHOW GRANTS TO SNOWFLAKE.ML.FORECAST ROLE regressor!USER;
