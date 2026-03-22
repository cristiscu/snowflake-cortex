import pandas as pd
import snowflake.snowpark as snowpark
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

# ========================================================================
# 1:18 min for 1M, X-Small
def main(session: snowpark.Session): 
    df = session.table("CLASSIFICATION_DATASET")
    pdf = df.to_pandas()
    X = pd.DataFrame(pdf, columns=["X1", "X2", "X3", "X4", "X5", "X6"])
    y = pd.DataFrame(pdf, columns=["Y"])
    X_train, _, y_train, _ = train_test_split(X, y, test_size=0.1)

    clf = XGBClassifier()
    clf.fit(X_train, y_train)
    return df
