USE SCHEMA test.public;

CREATE OR REPLACE PROCEDURE load_excel(file_path string)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python', 'pandas', 'openpyxl')
    HANDLER = 'main'
AS $$
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd
 
def main(session, file_path):
    with SnowflakeFile.open(file_path, 'rb') as f:
        data = load_workbook(f).active.values
        df = pd.DataFrame(data, columns=next(data)[0:])
        df = session.create_dataframe(df)
        df.write.mode("overwrite").save_as_table("excel_tests")
    return True
$$;

CALL load_excel(build_scoped_file_url(@INT_STAGE, 'test.xlsx'));

SELECT * FROM excel_tests;