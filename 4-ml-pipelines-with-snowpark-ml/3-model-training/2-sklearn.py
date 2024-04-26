import time, math
import pandas as pd
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
session.query_tag = "classification"

df = session.table("CLASSIFICATION_DATASET").to_pandas()
X = pd.DataFrame(df, columns=["X1", "X2", "X3", "X4", "X5", "X6"])
y = pd.DataFrame(df, columns=["Y"])
X_train, _, y_train, _ = train_test_split(X, y, test_size=0.1)

# ========================================================================
# 3 mins for 1M (X-Small) --> [1 1 0 ... 1 0 1]
start_time = time.time()

clf = XGBClassifier()
clf.fit(X_train, y_train)

total_time = math.trunc(time.time() - start_time)
print(f"{total_time} seconds")
