-- paste into a SQL Worksheet in Snowsight and run one by one
-- see https://github.com/Snowflake-Labs/sfguide-intro-to-machine-learning-with-snowpark-ml-for-python/blob/main/setup.sql
USE SCHEMA TEST.DIAMONDS;

CREATE OR REPLACE FILE FORMAT CSV_HEADER_PARSE
    TYPE='CSV' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- to list all: https://sfquickstarts.s3.us-west-1.amazonaws.com/
-- to download: https://sfquickstarts.s3.us-west-1.amazonaws.com/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv
CREATE OR REPLACE STAGE EXT_STAGE_PARSE
    FILE_FORMAT=CSV_HEADER_PARSE
    URL='s3://sfquickstarts/intro-to-machine-learning-with-snowpark-ml-for-python/';

LIST @EXT_STAGE_PARSE;

CREATE OR REPLACE TABLE DIAMONDS1 USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@EXT_STAGE_PARSE',
        FILES => 'diamonds.csv',
        FILE_FORMAT => 'CSV_HEADER_PARSE')));

COPY INTO DIAMONDS1
FROM @EXT_STAGE_PARSE
    FILES=('diamonds.csv')
    FILE_FORMAT=(FORMAT_NAME=CSV_HEADER_PARSE)
    MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
    FORCE=TRUE;

SELECT *
FROM DIAMONDS1
LIMIT 10;
