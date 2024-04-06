-- run from current folder: snowsql -c test_conn -f 5-copy-into.sql
use schema test.public;

create or replace file format titanic_format
    TYPE = csv,
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';
create or replace stage titanic_stage
    FILE_FORMAT = titanic_format;

-- not supported from the UI!
put file://..\..\.spool\datasets\titanic.csv @titanic_stage
    overwrite=true auto_compress=false;

-- (1) INFER_SCHEMA --> show inferred columns in tabular format
SELECT *
FROM TABLE(INFER_SCHEMA(
    LOCATION => '@titanic_stage',
    FILES => 'titanic.csv',
    FILE_FORMAT => 'titanic_format'));

-- (2) INFER_SCHEMA --> show inferred columns w/ data types
SELECT GENERATE_COLUMN_DESCRIPTION(
    ARRAY_AGG(OBJECT_CONSTRUCT(*)), 'table') AS COLUMNS
FROM TABLE(INFER_SCHEMA(
    LOCATION => '@titanic_stage',
    FILES => 'titanic.csv',
    FILE_FORMAT => 'titanic_format'));

-- (3) INFER_SCHEMA --> create table directly
CREATE OR REPLACE TABLE titanic USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@titanic_stage',
        FILES => 'titanic.csv',
        FILE_FORMAT => 'titanic_format')));

SELECT GET_DDL('TABLE', 'TITANIC');

COPY INTO titanic FROM @titanic_stage
   FILES = ('titanic.csv')
   FILE_FORMAT = (FORMAT_NAME = titanic_format)
   MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
   FORCE = TRUE;

select * from titanic
limit 100;
