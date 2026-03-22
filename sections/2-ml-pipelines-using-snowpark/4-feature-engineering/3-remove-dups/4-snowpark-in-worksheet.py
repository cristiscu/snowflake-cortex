# see https://github.com/Snowflake-Labs/sfguide-snowpark-python-top-three-tips-for-optimal-performance/blob/main/lab1_avoid_pandas_df.ipynb?_fsi=vE4fuIXV&_fsi=vE4fuIXV
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    df = session.table('SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM')
    session.query_tag = 'snowpark-in-worksheet'
    df = df.dropDuplicates()
    df.write.mode("overwrite").save_as_table("TEST.PUBLIC.LINEITEM_SNOWPARK")
    return df