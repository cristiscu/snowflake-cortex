import snowflake.snowpark as snowpark
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV

#  12+ mins on so_2xlarge
def main(session: snowpark.Session): 
    df = session.table("CALIFORNIA_HOUSING").to_pandas()
    X, y = df.iloc[:, 0:-1], df.iloc[:, -1]
    
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
    return df
