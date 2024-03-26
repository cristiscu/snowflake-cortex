# Snowpark ML Framework Connector for PyTorch
# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-framework-connectors#feeding-a-fileset-to-pytorch

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

pipe = fileset.to_torch_datapipe(batch_size=4, shuffle=True, drop_last_batch=True)

from torch.utils.data import DataLoader
for batch in DataLoader(pipe, batch_size=None, num_workers=0): print(batch); break