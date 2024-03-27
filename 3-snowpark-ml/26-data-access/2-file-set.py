# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-filesystem-fileset#creating-and-using-a-fileset

import snowflake.ml.fileset as fileset
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

# create fileset from dataframe
df = session.table('mydata').limit(5000000)
fileset1 = fileset.FileSet.make(
    target_stage_loc="@ML_DATASETS.public.my_models/",
    name="from_dataframe",
    snowpark_dataframe=df,
    shuffle=True)
print(*fileset1.files())

# create fileset from running a query
fileset2 = fileset.FileSet.make(
    target_stage_loc="@ML_DATASETS.public.my_models/",
    name="from_connector",
    snowpark_session=session,
    query="SELECT * FROM MYDATA LIMIT 5000000",
    shuffle=True)
print(*fileset2.files())
