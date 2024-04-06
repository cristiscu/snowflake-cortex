# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#feature-preprocessing-and-training-on-non-synthetic-data

from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

query = """
CREATE OR REPLACE TABLE Gamma_Telescope(
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
    CLASS VARCHAR(10))
"""
session.sql(query).collect()

query = "CREATE OR REPLACE STAGE Gamma_Stage"
session.sql(query).collect()

session.file.put(
    "..\..\.spool\datasets\gamma-telescope.csv",
    "Gamma_Stage/gamma-telescope.csv",
    auto_compress=False,
    overwrite=True)

query = "LIST @TEST.PUBLIC.Gamma_Stage"
session.sql(query).collect()

query = """
SELECT METADATA$FILE_ROW_NUMBER as RowId, $1, $2, $3
FROM @TEST.PUBLIC.Gamma_Stage/gamma-telescope.csv
LIMIT 100
"""
df = session.sql(query).to_pandas()
print(df.head())

query = """
COPY INTO Gamma_Telescope
FROM @Gamma_Stage/gamma-telescope.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=0);
"""
session.sql(query).collect()

query = "SELECT * FROM Gamma_Telescope LIMIT 5"
df = session.sql(query).to_pandas()
print(df.head())
