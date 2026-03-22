-- paste into a SQL Worksheet in Snowsight and run one by one
-- see https://github.com/Snowflake-Labs/sfguide-intro-to-machine-learning-with-snowpark-ml-for-python/blob/main/setup.sql
create schema if not exists test.diamonds;
USE SCHEMA TEST.DIAMONDS;

CREATE OR REPLACE FILE FORMAT CSV_HEADER_LIST
    TYPE='CSV' SKIP_HEADER=1;

CREATE OR REPLACE STAGE INT_STAGE
    FILE_FORMAT=CSV_HEADER_LIST;

-- to list all: https://sfquickstarts.s3.us-west-1.amazonaws.com/
-- to download: https://sfquickstarts.s3.us-west-1.amazonaws.com/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv
CREATE OR REPLACE STAGE EXT_STAGE_LIST
    FILE_FORMAT=CSV_HEADER_LIST
    URL='s3://sfquickstarts/intro-to-machine-learning-with-snowpark-ml-for-python/';

LIST @EXT_STAGE_LIST;

SELECT METADATA$FILE_ROW_NUMBER as RowId, $1, $2, $3
FROM @EXT_STAGE_LIST/diamonds.csv
LIMIT 10;
