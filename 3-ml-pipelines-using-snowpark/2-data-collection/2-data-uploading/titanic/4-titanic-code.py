# run from a terminal in VSCode: python 5-snowpark-code.py

# connect to Snowflake through Snowpark
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

query = "CREATE OR REPLACE STAGE TEST.PUBLIC.INT_STAGE"
session.sql(query).collect()

session.file.put(
    "..\..\..\..\.spool\\titanic.csv",
    "TEST.PUBLIC.INT_STAGE",
    auto_compress=False,
    overwrite=True)

query = """
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
	"Embarked" VARCHAR)
"""
session.sql(query).collect()

query = """
COPY INTO TEST.PUBLIC.TITANIC
FROM @TEST.PUBLIC.INT_STAGE/titanic.csv
FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
"""
session.sql(query).collect()

query = """
SELECT *
FROM TEST.PUBLIC.TITANIC
LIMIT 10
"""
df = session.sql(query).to_pandas()
print(df.head())
