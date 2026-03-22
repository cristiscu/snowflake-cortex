# gets 20K samples for regression
# see https://scikit-learn.org/stable/datasets/real_world.html#california-housing-dataset

from sklearn import datasets
from sklearn.ensemble import RandomForestRegressor

df = datasets.fetch_california_housing(as_frame=True).frame
X, y = df.iloc[:, 0:-1], df.iloc[:, -1]

# 2 x 3 = 6 combinations
for n_estimators in [20, 30]:
    for max_depth in [4, 8, 10]:
        regr = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth)
        regr.fit(X, y)
        print("================================")
        print("n_estimators:", n_estimators, "max_depth:", max_depth)
        print("Score:", regr.score(X, y))
