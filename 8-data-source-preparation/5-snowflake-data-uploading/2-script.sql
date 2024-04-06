-- snowsql -c test_conn -f 8-gamma.sql
-- see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#feature-preprocessing-and-training-on-non-synthetic-data

USE SCHEMA TEST.PUBLIC;

CREATE OR REPLACE TABLE TEST.PUBLIC.Gamma_Telescope(
    F_LENGTH FLOAT,
    F_WIDTH FLOAT,
    F_SIZE FLOAT,
    F_CONC FLOAT,
    F_CONC1 FLOAT,
    F_ASYM FLOAT,
    F_M3_LONG FLOAT,
    F_M3_TRANS FLOAT,
    F_ALPHA FLOAT,
    F_DIST FLOAT,
    CLASS VARCHAR(10));

CREATE OR REPLACE STAGE TEST.PUBLIC.Gamma_Stage;

PUT file://..\..\.spool\datasets\gamma-telescope.csv @TEST.PUBLIC.Gamma_Stage
    overwrite=true auto_compress=false;

LIST @TEST.PUBLIC.Gamma_Stage;

SELECT METADATA$FILE_ROW_NUMBER as RowId, $1, $2, $3
FROM @TEST.PUBLIC.Gamma_Stage/gamma-telescope.csv
LIMIT 5;

COPY INTO TEST.PUBLIC.Gamma_Telescope
FROM @TEST.PUBLIC.Gamma_Stage/gamma-telescope.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=0);

SELECT *
FROM TEST.PUBLIC.Gamma_Telescope
LIMIT 5;

