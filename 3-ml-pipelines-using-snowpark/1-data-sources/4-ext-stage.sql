-- see https://github.com/Snowflake-Labs/sfguide-intro-to-machine-learning-with-snowpark-ml-for-python/blob/main/setup.sql
USE SCHEMA TEST.PUBLIC;

CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT 
    SKIP_HEADER = 1 
    TYPE = 'CSV';

-- download https://sfquickstarts.s3.us-west-1.amazonaws.com/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv
CREATE STAGE IF NOT EXISTS DIAMONDS_ASSETS 
    FILE_FORMAT = CSV_FORMAT 
    URL = 's3://sfquickstarts/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv';

LS @DIAMONDS_ASSETS;