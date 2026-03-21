-- see https://docs.snowflake.com/en/user-guide/ml-powered-anomaly-detection#granting-privileges-to-create-anomaly-detection-objects

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE compute_wh;
USE SCHEMA test.ts;

-- cleanup
DROP ROLE IF EXISTS analyst;
DROP ROLE IF EXISTS consumer;
DROP SNOWFLAKE.ML.ANOMALY_DETECTION IF EXISTS detector;

-- show instances/roles/privileges in specific class
SHOW SNOWFLAKE.ML.ANOMALY_DETECTION;
SHOW ROLES IN CLASS SNOWFLAKE.ML.ANOMALY_DETECTION;

-- =======================================================================
-- recommended custom analyst role to create/own/train model in specific schema
USE ROLE ACCOUNTADMIN;
CREATE ROLE analyst;
grant role analyst to role sysadmin;

GRANT USAGE ON database test TO ROLE analyst;
GRANT USAGE ON schema test.ts TO ROLE analyst;
GRANT SELECT ON VIEW view1_train TO ROLE analyst;
GRANT USAGE ON warehouse compute_wh TO ROLE analyst;
GRANT CREATE SNOWFLAKE.ML.ANOMALY_DETECTION ON SCHEMA test.ts TO ROLE analyst;
SHOW GRANTS TO ROLE analyst;

USE ROLE analyst;
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION detector(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view1_train'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => '');

CALL detector!EXPLAIN_FEATURE_IMPORTANCE();
  
-- =======================================================================
-- recommended custom consumer role to only detect anomalies with existing models in specific schema
USE ROLE ACCOUNTADMIN;
CREATE ROLE consumer;
grant role consumer to role sysadmin;

GRANT USAGE ON database test TO ROLE consumer;
GRANT USAGE ON schema test.ts TO ROLE consumer;
GRANT SELECT ON VIEW view1_test TO ROLE consumer;
GRANT USAGE ON warehouse compute_wh TO ROLE consumer;
GRANT SNOWFLAKE.ML.ANOMALY_DETECTION ROLE detector!USER TO ROLE consumer;
SHOW GRANTS TO ROLE consumer;

USE ROLE consumer;
CALL detector!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view1_test'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

CALL detector!EXPLAIN_FEATURE_IMPORTANCE();   -- no privilege error!

-- =======================================================================
-- show instances/roles/privileges in specific class
USE ROLE ACCOUNTADMIN;
SHOW SNOWFLAKE.ML.ANOMALY_DETECTION;
SHOW ROLES IN CLASS SNOWFLAKE.ML.ANOMALY_DETECTION;
SHOW GRANTS TO SNOWFLAKE.ML.ANOMALY_DETECTION ROLE detector!USER;
