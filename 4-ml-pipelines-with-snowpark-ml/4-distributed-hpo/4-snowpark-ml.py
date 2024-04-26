# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#label-snowflake-ml-modeling-distributed-hyperparameter
import time, math
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.ml.modeling.ensemble.random_forest_regressor import RandomForestRegressor
from snowflake.ml.modeling.model_selection.grid_search_cv import GridSearchCV

pars = SnowflakeLoginOptions("test_conn")
pars["warehouse"] = "so_2xlarge"
session = Session.builder.configs(pars).create()
session.query_tag = "hpo-snowpark-2"
df = session.table("CALIFORNIA_HOUSING")

# ========================================================================
# 7+ mins on so_medium / 3 mins on so_xlarge / 2.5 mins on so_2xlarge
start_time = time.time()

# PUT file://C:/Users/crist/AppData/Local/Temp/... @SNOWPARK_TEMP_STAGE_...
#   parallel=4 source_compression='AUTO_DETECT' auto_compress=False overwrite=True
# 4 x 3 x 3 x 3 x 5 (cv) = 540 combinations
pipe = GridSearchCV(
    estimator=RandomForestRegressor(),
    param_grid=dict(
        max_depth=[80, 90, 100, 110],
        min_samples_leaf=[1, 3, 10],
        min_samples_split=[1.0, 3, 10],
        n_estimators=[100, 200, 400]),
    cv=5, 
    input_cols=[c for c in df.columns if not c.startswith("MEDHOUSEVAL")], 
    label_cols=['MEDHOUSEVAL'])
pipe.fit(df)

total_time = math.trunc(time.time() - start_time)
print(f"{total_time} seconds")

sk = pipe.to_sklearn()
print("Best params: ", sk.best_params_)
print("Best score: ", sk.best_score_)
