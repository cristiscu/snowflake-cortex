# Snowpark ML Framework Connector for TensorFlow
# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-framework-connectors#feeding-a-fileset-to-tensorflow

import snowflake.ml.fileset as fileset
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
df = session.table('mydata').limit(5000000)

fileset = fileset.FileSet.make(
    target_stage_loc="@ML_DATASETS.public.my_models/",
    name="from_dataframe",
    snowpark_dataframe=df,
    shuffle=True)

ds = fileset.to_tf_dataset(batch_size=4, shuffle=True, drop_last_batch=True)

for batch in ds: print(batch); break