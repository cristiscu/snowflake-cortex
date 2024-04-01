-- from Query History
with PYTHON_WORKSHEET as procedure ()
    returns Table() 
    language python 
    runtime_version=3.11 
    packages=('snowflake-snowpark-python') 
    handler='main' 
as 'import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    df = session.table(''SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM'')
    dfp = df.to_pandas()
    dfp = dfp.drop_duplicates()
    df = session.write_pandas(dfp, "LINEITEM_PANDAS", auto_create_table=True, overwrite=True)
    return df' 
    
call PYTHON_WORKSHEET();