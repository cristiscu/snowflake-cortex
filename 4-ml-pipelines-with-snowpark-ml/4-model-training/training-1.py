# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#training

import pandas as pd
from sklearn.datasets import make_classification
from snowflake.ml.modeling.xgboost import XGBClassifier

# connect to your Snowflake account
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

FEATURE_COLS = ["X1", "X2", "X3", "X4", "X5", "X6"]
LABEL_COLS = ["Y"]
OUTPUT_COLS = ["PREDICTIONS"]

# Set up data.
X, y = make_classification(
    n_samples=40000,
    n_features=6,
    n_informative=4,
    n_redundant=1,
    random_state=0,
    shuffle=True,
)

X = pd.DataFrame(X, columns=FEATURE_COLS)
y = pd.DataFrame(y, columns=LABEL_COLS)

features_pandas = pd.concat([X, y], axis=1)
features_df = session.create_dataframe(features_pandas)

# Train an XGBoost model on snowflake.
xgboost_model = XGBClassifier(
    input_cols=FEATURE_COLS,
    label_cols=LABEL_COLS,
    output_cols=OUTPUT_COLS
)

xgboost_model.fit(features_df)

# Use the model to make predictions.
predictions = xgboost_model.predict(features_df)
predictions[OUTPUT_COLS].show()
