-- see https://github.com/Snowflake-Labs/sfguide-intro-to-machine-learning-with-snowpark-ml-for-python/blob/main/setup.sql
USE SCHEMA TEST.PUBLIC;

CREATE FILE FORMAT IF NOT EXISTS DIAMONDS_FORMAT 
    SKIP_HEADER = 1 
    TYPE = 'CSV';

-- download https://sfquickstarts.s3.us-west-1.amazonaws.com/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv
CREATE STAGE IF NOT EXISTS DIAMONDS_STAGE 
    FILE_FORMAT = DIAMONDS_FORMAT
    URL = 's3://sfquickstarts/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv';

LS @DIAMONDS_STAGE;

-- direct query (on file!)
SELECT METADATA$FILE_ROW_NUMBER as RowId,
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
FROM @DIAMONDS_STAGE/diamonds.csv;

