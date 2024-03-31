# Snowpark ML Framework Connector for PyTorch
# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-framework-connectors#feeding-a-fileset-to-pytorch

import snowflake.ml.fileset as fileset
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from torch.utils.data import DataLoader

# create fileset from running a query
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
fileset1 = fileset.FileSet.make(
    target_stage_loc="@ML_DATASETS.public.my_models/",
    name="from_connector",
    snowpark_session=session,
    query="SELECT * FROM MYDATA LIMIT 5000000",
    shuffle=True)

# feed fileset to PyTorch
dataset = fileset1.to_torch_datapipe(batch_size=4, shuffle=True, drop_last_batch=True)
for batch in DataLoader(dataset, batch_size=None, num_workers=0): print(batch); break