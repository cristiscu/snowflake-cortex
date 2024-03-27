# Snowpark ML Framework Connector for TensorFlow
# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-framework-connectors#feeding-a-fileset-to-tensorflow

import snowflake.ml.fileset as fileset
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
import tensorflow as tf

# create fileset from running a query
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
fileset1 = fileset.FileSet.make(
    target_stage_loc="@ML_DATASETS.public.my_models/",
    name="from_connector",
    snowpark_session=session,
    query="SELECT * FROM MYDATA LIMIT 5000000",
    shuffle=True)

# feed fileset to TensorFlow
ds = fileset1.to_tf_dataset(batch_size=4, shuffle=True, drop_last_batch=True)
for batch in ds: print(batch); break