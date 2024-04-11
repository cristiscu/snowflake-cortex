import pandas as pd
from IPython.display import display
from sklearn.datasets import make_classification

# generate 1K rows with 6 features + 1 target var
X, y = make_classification(n_samples=1000, n_features=6,
    n_informative=2, n_redundant=0, random_state=0, shuffle=True)
X = pd.DataFrame(X, columns=["X1", "X2", "X3", "X4", "X5", "X6"])
y = pd.DataFrame(y, columns=["Y"])

df = pd.concat([X, y], axis=1)
display(df)