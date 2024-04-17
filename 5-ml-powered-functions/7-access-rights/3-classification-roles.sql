-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ml-powered/classification#granting-privileges-to-create-classification-models
-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ml-powered/classification#model-roles-and-usage-privileges

USE ROLE admin;
GRANT USAGE ON db TO ROLE analyst;
GRANT USAGE ON schema TO ROLE analyst;
GRANT CREATE CLASSIFICATION ON SCHEMA db.schema TO ROLE analyst;

-- can create models (--> builtin mladmin model role on them)
USE ROLE analyst;
USE SCHEMA db.schema;
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model(...);
GRANT SNOWFLAKE.ML.CLASSIFICATION ROLE model!mlconsumer TO ROLE consumer;

-- can use predict APIs (--> builtin mlconsumer model role on them)
USE ROLE consumer;
CALL model!PREDICT(...);
CALL model!SHOW_EVALUATION_METRICS();   -- privilege error!

SHOW GRANTS TO SNOWFLAKE.ML.CLASSIFICATION ROLE model!mladmin;
SHOW GRANTS TO SNOWFLAKE.ML.CLASSIFICATION ROLE model!mlconsumer;
