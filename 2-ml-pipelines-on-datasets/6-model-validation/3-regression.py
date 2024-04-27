# gets 20K samples for regression
# see https://scikit-learn.org/stable/datasets/real_world.html#california-housing-dataset

from sklearn import datasets
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestRegressor

df = datasets.fetch_california_housing(as_frame=True).frame
X, y = df.iloc[:, 0:-1], df.iloc[:, -1]

# 1 x 2 x 5 (cv) = 10 combinations
regr = GridSearchCV(
    estimator=RandomForestRegressor(),
    param_grid={"max_depth": [10], "n_estimators": [20, 30]},
    verbose=10)
regr.fit(X, y)

print("Best params:", regr.best_params_)
print("Best score:", regr.best_score_)
