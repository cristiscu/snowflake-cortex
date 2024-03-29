# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-filesystem-fileset#creating-and-using-a-file-system
# use snowpark_session=session or sf_connection=connection

import gzip, fsspec
from snowflake.ml.fileset import sfcfs
import pyarrow.parquet as pq
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.snowpark import Session

# list files in local cache stage
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
fs1 = fsspec.filesystem("cached",
    target_protocol="sfc",
    target_options={
        "snowpark_session": session,
        "cache_types": "bytes",
        "block_size": 32 * 2**20 },
    cache_storage="/tmp/sf_files/")
print(*fs1.ls("@ML_DATASETS.public.my_models/sales_predict/"), end='\n')

# show compressed stage file contents
path = "sfc://@ML_DATASETS.public.my_models/dataset.csv.gz"
with fs1.open(path, mode='rb', snowpark_session=session) as f:
    g = gzip.GzipFile(fileobj=f)
    for i in range(3):
        print(g.readline())

# ==========================================================================
# show stage file contents
fs2 = sfcfs.SFFileSystem(snowpark_session=session)
path = '@ML_DATASETS.public.my_models/test/data_7_7_3.snappy.parquet'
with fs2.open(path, mode='rb') as f:
    print(f.read(16))

# read Parquet files w/ PyArrow
table = pq.read_table(path, filesystem=fs2)
table.take([1, 3])
