# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#preprocessing

import time, math
from IPython.display import display
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from sklearn.preprocessing import OrdinalEncoder, MinMaxScaler

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
session.query_tag = "transformers-new"
X = session.table("REGRESSION_DATASET").to_pandas()

# ========================================================================
# 5 secs for 10M (0 for 1M)
start_time = time.time()

X[["C1O", "C2O"]] = OrdinalEncoder(
    ).fit_transform(X[["C1", "C2"]])
X[["N1FO", "N2FO", "C1FO", "C2FO"]] = MinMaxScaler(
    ).fit_transform(X[["N1", "N2", "C1O", "C2O"]])

total_time = math.trunc(time.time() - start_time)
print(f"{total_time} seconds")
display(X)
