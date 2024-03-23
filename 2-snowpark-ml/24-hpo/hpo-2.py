# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#label-snowflake-ml-modeling-distributed-hyperparameter

from snowflake.ml.modeling.ensemble.random_forest_regressor import RandomForestRegressor
from snowflake.ml.modeling.model_selection.grid_search_cv import GridSearchCV
from sklearn import datasets

def load_housing_data() -> DataFrame:
    input_df_pandas = datasets.fetch_california_housing(as_frame=True).frame
    # Set the columns to be upper case for consistency with Snowflake identifiers.
    input_df_pandas.columns = [c.upper() for c in input_df_pandas.columns]
    input_df = session.create_dataframe(input_df_pandas)

    return input_df

input_df = load_housing_data()

# Use all the columns besides the median value as the features
input_cols = [c for c in input_df.columns if not c.startswith("MEDHOUSEVAL")]
# Set the target median value as the only label columns
label_cols = [c for c in input_df.columns if c.startswith("MEDHOUSEVAL")]


DISTRIBUTIONS = dict(
            max_depth=[80, 90, 100, 110],
            min_samples_leaf=[1,3,10],
            min_samples_split=[1.0, 3,10],
            n_estimators=[100,200,400]
        )
estimator = RandomForestRegressor()
n_folds = 5

clf = GridSearchCV(estimator=estimator, param_grid=DISTRIBUTIONS, cv=n_folds, input_cols=input_cols, label_cols=label_col)
clf.fit(input_df)