from openpyxl import load_workbook
import pandas as pd
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.snowpark import Session

file_path = "../../.spool/test.xlsx"
with open(file_path, 'rb') as f:
    data = load_workbook(f).active.values
    df = pd.DataFrame(data, columns=next(data)[0:])
    
    session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
    df = session.create_dataframe(df)
    df.write.mode("overwrite").save_as_table("excel_tests_local")
