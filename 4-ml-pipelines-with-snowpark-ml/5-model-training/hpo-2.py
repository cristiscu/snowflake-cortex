# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#label-snowflake-ml-modeling-distributed-hyperparameter

from snowflake.ml.modeling.ensemble.random_forest_regressor import RandomForestRegressor
from snowflake.ml.modeling.model_selection.grid_search_cv import GridSearchCV
from sklearn import datasets
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

df = datasets.fetch_california_housing(as_frame=True).frame
df.columns = [c.upper() for c in df.columns]

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
df = session.create_dataframe(df)

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
model.fit(df)