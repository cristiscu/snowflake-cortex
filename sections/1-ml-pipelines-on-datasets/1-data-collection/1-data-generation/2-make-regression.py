# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#preprocessing

import string, random
import pandas as pd
from sklearn.datasets import make_regression
from IPython.display import display

# generate 1K rows with 3 numerical columns (random float)
X, _ = make_regression(n_samples=1000, n_features=3, noise=0.1, random_state=0)
X = pd.DataFrame(X, columns=["X1", "X2", "X3"])

# generate 3 categorical columns (random text)
cat_features = {}
for c in ["C1", "C2", "C3"]:
    cat_features[c] = [
        "".join(random.choices(string.ascii_uppercase, k=2))
        for _ in range(X.shape[0])]

X = X.assign(**cat_features)
display(X)
