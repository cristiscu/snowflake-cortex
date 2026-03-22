# gets 20K samples for regression
# see https://scikit-learn.org/stable/datasets/real_world.html#california-housing-dataset

from sklearn import datasets
from sklearn.model_selection import KFold
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

df = datasets.fetch_california_housing(as_frame=True).frame
X, y = df.iloc[:, 0:-1], df.iloc[:, -1]

regr = RandomForestRegressor(n_estimators=20)
kf = KFold(n_splits=3)
for train_i, eval_i in kf.split(X):
    print("===========================")
    print("TRAIN:", train_i)
    print("EVAL:", eval_i)

    X_train, X_eval = X.iloc[train_i, :], X.iloc[eval_i, :]
    y_train, y_eval = y.iloc[train_i], y.iloc[eval_i]
    regr.fit(X_train, y_train)

    print("R2:", regr.score(X_eval, y_eval))
    print("MSE:", mean_squared_error(y_eval, regr.predict(X_eval)))
