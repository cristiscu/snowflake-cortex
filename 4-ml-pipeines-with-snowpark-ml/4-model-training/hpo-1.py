# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#distributed-hyperparameter-optimization

import pandas as pd
from sklearn.datasets import make_classification
from snowflake.snowpark import DataFrame
from snowflake.ml.modeling.xgboost import XGBClassifier
from snowflake.ml.modeling.model_selection.grid_search_cv import GridSearchCV

# connect to your Snowflake account
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

FEATURE_COLS = ["X1", "X2", "X3", "X4", "X5", "X6"]
LABEL_COLS = ["Y"]
OUTPUT_COLS = ["PREDICTIONS"]

def set_up_data(session: Session, n_samples: int) -> DataFrame:
    X, y = make_classification(n_samples=n_samples, n_features=6,
        n_informative=2, n_redundant=0, random_state=0, shuffle=True)

    X = pd.DataFrame(X, columns=FEATURE_COLS)
    y = pd.DataFrame(y, columns=LABEL_COLS)

    features_pandas = pd.concat([X, y], axis=1)
    features_pandas.head()
    return session.create_dataframe(features_pandas)

df = set_up_data(session, 10**4)

# Create a warehouse to use for the tuning job.
query = """
CREATE or replace warehouse HYPERPARAM_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'Standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = FALSE;
"""
session.sql(query).collect()
session.use_warehouse("HYPERPARAM_WH")

# Tune an XGB Classifier model using sklearn GridSearchCV.
DISTRIBUTIONS = dict(n_estimators=[10, 50], learning_rate=[0.01, 0.1, 0.2])
grid_search_cv = GridSearchCV(estimator=XGBClassifier(),
    param_grid=DISTRIBUTIONS, input_cols=FEATURE_COLS, 
    label_cols=LABEL_COLS, output_cols=OUTPUT_COLS)

grid_search_cv.fit(df)

# Use the best model to make predictions.
predictions = grid_search_cv.predict(df)
predictions[OUTPUT_COLS].show()

# Retrieve sklearn model, and print the best score
sklearn_grid_search_cv = grid_search_cv.to_sklearn()
print(sklearn_grid_search_cv.best_score_)

# ==================================================================
# To really see the power of distributed optimization, train on a million rows of data.

# Scale up the warehouse for a faster fit. This takes 2m15s to run on an L warehouse versus 4m5s on a XS warehouse.
query = f"ALTER WAREHOUSE {session.get_current_warehouse()} SET WAREHOUSE_SIZE='LARGE'"
session.sql(query).collect()

df = set_up_data(session, 10**6)
grid_search_cv.fit(df)
print(grid_search_cv.to_sklearn().best_score_)
