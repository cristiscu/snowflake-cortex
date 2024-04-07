# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#label-snowflake-ml-modeling-distributed-hyperparameter
# try S-optimized WH so_medium/large/xlarge/2xlarge: 7+ mins on Medium / 3 mins on XLarge / 2.5 mins on 2XLarge

from sklearn import datasets
# see https://scikit-learn.org/stable/datasets/real_world.html#california-housing-dataset (20k samples)
df = datasets.fetch_california_housing(as_frame=True).frame
df.columns = [c.upper() for c in df.columns]

from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
pars = SnowflakeLoginOptions("test_conn")
pars["warehouse"] = "so_2xlarge"
session = Session.builder.configs(pars).create()
session.query_tag = "2-hpo-scale"

# pandas Dataframe --> Snowpark DataFrame (in temp table) --> write_pandas() to persist!
df = session.create_dataframe(df)

from snowflake.ml.modeling.ensemble.random_forest_regressor import RandomForestRegressor
from snowflake.ml.modeling.model_selection.grid_search_cv import GridSearchCV
model = GridSearchCV(
    estimator=RandomForestRegressor(), 
    param_grid=dict(
        max_depth=[80, 90, 100, 110],
        min_samples_leaf=[1, 3, 10],
        min_samples_split=[1.0, 3,10],
        n_estimators=[100, 200, 400]),
    cv=5, 
    input_cols=[c for c in df.columns if not c.startswith("MEDHOUSEVAL")], 
    label_cols=[c for c in df.columns if c.startswith("MEDHOUSEVAL")])

# converted to: PUT file://C:/Users/crist/AppData/Local/Temp/... @SNOWPARK_TEMP_STAGE_...
#   parallel=4 source_compression='AUTO_DETECT' auto_compress=False overwrite=True
import time, math
start_time = time.time()
model.fit(df)
total_time = math.trunc(time.time() - start_time)
print(f"{total_time} seconds")
