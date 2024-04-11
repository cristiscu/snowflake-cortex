-- paste into a SQL Worksheet in Snowsight and run one by one
-- see https://github.com/Snowflake-Labs/sfguide-intro-to-machine-learning-with-snowpark-ml-for-python/blob/main/setup.sql
USE SCHEMA TEST.PUBLIC;

CREATE OR REPLACE FILE FORMAT CSV_HEADER
    TYPE='CSV' SKIP_HEADER=1;

-- to list all: https://sfquickstarts.s3.us-west-1.amazonaws.com/
-- to download: https://sfquickstarts.s3.us-west-1.amazonaws.com/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv
CREATE OR REPLACE STAGE EXT_STAGE 
    FILE_FORMAT=CSV_HEADER
    URL='s3://sfquickstarts/intro-to-machine-learning-with-snowpark-ml-for-python/';

LIST @EXT_STAGE;

SELECT METADATA$FILE_ROW_NUMBER as RowId, $1, $2, $3
FROM @EXT_STAGE/diamonds.csv
LIMIT 10;