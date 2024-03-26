# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-filesystem-fileset#creating-and-using-a-file-system

import gzip
import fsspec
from snowflake.ml.fileset import sfcfs
from snowflake.snowpark import Session
import pyarrow.parquet as pq
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

# or sf_connection=sf_connection
sf_fs1 = sfcfs.SFFileSystem(snowpark_session=session)

local_cache_path = "/tmp/sf_files/"
cached_fs = fsspec.filesystem("cached",
    target_protocol="sfc",
    target_options={
        "snowpark_session": session,
        "cache_types": "bytes",
        "block_size": 32 * 2**20 },
    cache_storage=local_cache_path)
print(*cached_fs.ls("@ML_DATASETS.public.my_models/sales_predict/"), end='\n')

path = '@ML_DATASETS.public.my_models/test/data_7_7_3.snappy.parquet'
with sf_fs1.open(path, mode='rb') as f:
    print(f.read(16))

table = pq.read_table(path, filesystem=sf_fs1)
table.take([1, 3])

path = "sfc://@ML_DATASETS.public.my_models/dataset.csv.gz"
with cached_fs.open(path, mode='rb', snowpark_session=session) as f:
    g = gzip.GzipFile(fileobj=f)
    for i in range(3):
        print(g.readline())
