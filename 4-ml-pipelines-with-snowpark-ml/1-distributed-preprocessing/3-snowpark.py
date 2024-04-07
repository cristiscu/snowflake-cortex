# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#preprocessing

import time, math
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.ml.modeling.preprocessing import MinMaxScaler, OrdinalEncoder
from snowflake.ml.modeling.pipeline import Pipeline

pars = SnowflakeLoginOptions("test_conn")
#pars["warehouse"] = "so_medium"
session = Session.builder.configs(pars).create()
session.query_tag = "transformers-new"
df = session.table("REGRESSION_DATASET")

# ========================================================================
# 23-30 secs, w/ temp table! for 1M/10M/100M (w/ so_medium) - 54 sec w/ XSmall for 100M
start_time = time.time()

pipe = Pipeline(steps=[
    ("encoder", OrdinalEncoder(
        input_cols=["C1", "C2"],
        output_cols=["C1O", "C2O"])),
    ("scaler", MinMaxScaler(
        input_cols=["N1", "N2", "C1O", "C2O"],
        output_cols=["N1FO", "N2FO", "C1FO", "C2FO"]))])
pipe.fit(df)
df = pipe.transform(df)

total_time = math.trunc(time.time() - start_time)
print(f"{total_time} seconds")
df.show()
