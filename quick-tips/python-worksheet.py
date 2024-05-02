from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

df = session.table('SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM')
df = df.dropDuplicates()
df.write.mode("overwrite").save_as_table("TEST.PUBLIC.LINEITEM_2")
df.show()
