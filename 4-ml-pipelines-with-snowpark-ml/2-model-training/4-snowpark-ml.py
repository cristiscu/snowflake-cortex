import time, math
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.ml.modeling.xgboost import XGBClassifier

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
session.query_tag = "classification-sp"
df = session.table("CLASSIFICATION_DATASET")
train_data, _ = df.random_split(weights=[0.9, 0.1], seed=0)

# ========================================================================
# 71 sec for 1M, ~3 min for 10M
start_time = time.time()
# PUT 'file:///tmp/placeholder/udf_py_1651180125.zip'
# '@"TEST"."PUBLIC".SNOWPARK_TEMP_STAGE_XLYWDRNLHS/SNOWPARK_TEMP_FUNCTION_DZY8GOQZWH'
# PARALLEL = 4 AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = DEFLATE OVERWRITE = TRUE

clf = XGBClassifier(
    input_cols=["X1", "X2", "X3", "X4", "X5", "X6"],
    label_cols=["Y"],
    output_cols=["PREDICTIONS"])
clf.fit(train_data)

total_time = math.trunc(time.time() - start_time)
print(f"{total_time} seconds")
