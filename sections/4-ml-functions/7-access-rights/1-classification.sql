-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ml-powered/classification#granting-privileges-to-create-classification-models
-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ml-powered/classification#model-roles-and-usage-privileges

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE compute_wh;
USE SCHEMA test.public;

-- cleanup
DROP ROLE IF EXISTS analyst;
DROP ROLE IF EXISTS consumer;
DROP SNOWFLAKE.ML.CLASSIFICATION IF EXISTS classifier;

-- show instances/roles/privileges in specific class
SHOW SNOWFLAKE.ML.CLASSIFICATION;
SHOW ROLES IN CLASS SNOWFLAKE.ML.CLASSIFICATION;

-- =======================================================================
-- recommended custom analyst role to create/own/train model in specific schema
USE ROLE ACCOUNTADMIN;
CREATE ROLE analyst;
grant role analyst to role sysadmin;

-- custom analyst role can create+train classification models in specific schema
GRANT USAGE ON database test TO ROLE analyst;
GRANT USAGE ON schema test.public TO ROLE analyst;
GRANT SELECT ON TABLE purchases TO ROLE analyst;
GRANT USAGE ON warehouse compute_wh TO ROLE analyst;
GRANT CREATE SNOWFLAKE.ML.CLASSIFICATION ON SCHEMA test.public TO ROLE analyst;
SHOW GRANTS TO ROLE analyst;

USE ROLE analyst;
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION classifier(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE(
        'SELECT interest, rating, label FROM purchases WHERE label IS NOT NULL'),
    TARGET_COLNAME => 'label');

CALL classifier!SHOW_EVALUATION_METRICS();

-- =======================================================================
-- recommended custom consumer role to only forecast with existing models in specific schema
USE ROLE ACCOUNTADMIN;
CREATE ROLE consumer;
grant role consumer to role sysadmin;

GRANT USAGE ON database test TO ROLE consumer;
GRANT USAGE ON schema test.public TO ROLE consumer;
GRANT SELECT ON TABLE purchases TO ROLE consumer;
GRANT USAGE ON warehouse compute_wh TO ROLE consumer;
GRANT SNOWFLAKE.ML.CLASSIFICATION ROLE classifier!MLCONSUMER TO ROLE consumer;
SHOW GRANTS TO ROLE consumer;

USE ROLE consumer;
SELECT interest, rating,
    classifier!PREDICT(INPUT_DATA => object_construct(*)) as preds
FROM purchases
WHERE label IS NULL;

CALL classifier!SHOW_EVALUATION_METRICS();   -- privilege error!

-- =======================================================================
-- show instances/roles/privileges in specific class
USE ROLE ACCOUNTADMIN;
SHOW SNOWFLAKE.ML.CLASSIFICATION;
SHOW ROLES IN CLASS SNOWFLAKE.ML.CLASSIFICATION;
SHOW GRANTS TO SNOWFLAKE.ML.CLASSIFICATION ROLE classifier!MLADMIN;
SHOW GRANTS TO SNOWFLAKE.ML.CLASSIFICATION ROLE classifier!MLCONSUMER;
