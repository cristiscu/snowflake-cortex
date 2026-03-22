# see https://github.com/Snowflake-Labs/sfguide-snowpark-python-top-three-tips-for-optimal-performance/blob/main/lab1_avoid_pandas_df.ipynb?_fsi=vE4fuIXV&_fsi=vE4fuIXV
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    df = session.table('SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM')
    session.query_tag = 'pandas-in-worksheet'
    dfp = df.to_pandas()
    dfp = dfp.drop_duplicates()
    df = session.write_pandas(dfp, "TEST.PUBLIC.LINEITEM_PANDAS", auto_create_table=True, overwrite=True)
    return df
