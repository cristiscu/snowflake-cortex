# gets 20K samples for regression
# see https://scikit-learn.org/stable/datasets/real_world.html#california-housing-dataset

from sklearn import datasets
from sklearn.ensemble import RandomForestRegressor

df = datasets.fetch_california_housing(as_frame=True).frame
X, y = df.iloc[:, 0:-1], df.iloc[:, -1]

regr = RandomForestRegressor(n_estimators=100, max_depth=None)
regr.fit(X, y)
print("Score:", regr.score(X, y))
