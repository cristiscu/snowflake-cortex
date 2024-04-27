from sklearn import svm
from sklearn import datasets
from sklearn.model_selection import train_test_split

# 150 total samples, 30 left for test
X, y = datasets.load_iris(return_X_y=True)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

clf = svm.SVC()
clf.fit(X_train, y_train)
print(clf.predict(X_test))

# ==================================================
# save/reload model as Pickle
import pickle
with open("../../.spool/clf.pickle", "wb") as f:
    pickle.dump(clf, f)

with open("../../.spool/clf.pickle", "rb") as f:
    clf2 = pickle.load(f)
    print(clf2.predict(X_test))

# ==================================================
# save/reload model w/ JobLib
from joblib import dump, load
dump(clf, "../../.spool/clf.joblib") 
clf2 = load("../../.spool/clf.joblib") 
print(clf2.predict(X_test))


