# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#preprocessing

import random, string
import pandas as pd
from sklearn.datasets import make_regression
from snowflake.ml.modeling.preprocessing import MinMaxScaler, OrdinalEncoder
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

NUM_COLS = ["X1", "X2", "X3"]
X, _ = make_regression(n_samples=1000, n_features=3, noise=0.1, random_state=0)
X = pd.DataFrame(X, columns=NUM_COLS)

CAT_COLS = ["C1", "C2", "C3"]
cat_features = {}
for c in CAT_COLS:
    cat_features[c] = ["".join(random.choices(string.ascii_uppercase, k=2))
        for _ in range(X.shape[0])]

X = X.assign(**cat_features)
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
df = session.create_dataframe(X)

FEATURE_COLS = NUM_COLS + CAT_COLS
CAT_OUTPUT_COLS = ["C1_OUT", "C2_OUT", "C3_OUT"]
FEATURE_OUTPUT_COLS = ["X1_FEAT_OUT", "X2_FEAT_OUT", "X3_FEAT_OUT",
    "C1_FEAT_OUT", "C2_FEAT_OUT", "C3_FEAT_OUT"]
pipeline = Pipeline(steps=[
    ("OE", OrdinalEncoder(input_cols=CAT_COLS, output_cols=CAT_OUTPUT_COLS)),
    ("MMS", MinMaxScaler(input_cols=NUM_COLS + CAT_OUTPUT_COLS, output_cols=FEATURE_OUTPUT_COLS))])
pipeline.fit(df)
result = pipeline.transform(df)
result.show()
