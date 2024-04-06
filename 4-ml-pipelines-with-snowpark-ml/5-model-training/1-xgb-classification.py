# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#training

# connect to Snowflake
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

# make synthetic dataset for classification, w/ 40K samples
from sklearn.datasets import make_classification
X, y = make_classification(n_samples=40000, n_features=6,
    n_informative=4, n_redundant=1, random_state=0, shuffle=True)

import pandas as pd
COLS = ["X1", "X2", "X3", "X4", "X5", "X6"]
X = pd.DataFrame(X, columns=COLS)
y = pd.DataFrame(y, columns=["Y"])

df = pd.concat([X, y], axis=1)
df = session.create_dataframe(df)

from snowflake.ml.modeling.xgboost import XGBClassifier
model = XGBClassifier(input_cols=COLS, label_cols=["Y"], output_cols=["PREDICTIONS"])
model.fit(df)
preds = model.predict(df)
preds[["PREDICTIONS"]].show()
