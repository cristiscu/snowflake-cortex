-- run from a terminal in VSCode: SNOWSQL -c test_conn -f 3-titanic.sql

-- CREATE OR REPLACE STAGE TEST.PUBLIC.INT_STAGE;

-- not supported from the UI!
PUT file://..\..\..\..\.spool\titanic.csv @TEST.PUBLIC.INT_STAGE
    OVERWRITE=true AUTO_COMPRESS=false;

LIST @TEST.PUBLIC.INT_STAGE;

SELECT METADATA$FILE_ROW_NUMBER as RowId, $1, $2, $3
FROM @TEST.PUBLIC.INT_STAGE/titanic.csv
LIMIT 10;

CREATE OR REPLACE TABLE TEST.PUBLIC.TITANIC (
	"PassengerId" INT,
	"Survived" INT,
	"Pclass" INT,
	"Name" VARCHAR,
	"Sex" VARCHAR,
	"Age" INT,
	"SibSp" INT,
	"Parch" INT,
	"Ticket" VARCHAR,
	"Fare" FLOAT,
	"Cabin" VARCHAR,
	"Embarked" VARCHAR);

COPY INTO TEST.PUBLIC.TITANIC
FROM @TEST.PUBLIC.INT_STAGE/titanic.csv
	FILE_FORMAT = (
		TYPE='CSV'
		SKIP_HEADER=1
		FIELD_OPTIONALLY_ENCLOSED_BY='"');

SELECT *
FROM TEST.PUBLIC.TITANIC
LIMIT 10;

