# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#label-snowflake-ml-modeling-distributed-hyperparameter
import time, math
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

df = session.table("CALIFORNIA_HOUSING").to_pandas()
X, y = df.iloc[:, 0:-1], df.iloc[:, -1]

start_time = time.time()

# 4 x 3 x 3 x 3 x 5 (cv) = 540 combinations
pipe = GridSearchCV(
    estimator=RandomForestRegressor(),
    param_grid=dict(
        max_depth=[80, 90, 100, 110],
        min_samples_leaf=[1, 3, 10],
        min_samples_split=[1.0, 3, 10],
        n_estimators=[100, 200, 400]),
    cv=5)
pipe.fit(X, y)

total_time = math.trunc(time.time() - start_time)
print(f"{total_time} seconds")

print("Best params: ", pipe.best_params_)
print("Best score: ", pipe.best_score_)
