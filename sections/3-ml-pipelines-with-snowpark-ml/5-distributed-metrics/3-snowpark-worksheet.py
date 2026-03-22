import pandas as pd
import snowflake.snowpark as snowpark
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import *

def main(session: snowpark.Session): 
    df = session.table("CLASSIFICATION_DATASET_FOR_METRICS").to_pandas()
    X = pd.DataFrame(df, columns=["X1", "X2", "X3", "X4", "X5", "X6"])
    y = pd.DataFrame(df, columns=["Y"])
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4)
    
    clf = XGBClassifier()
    clf.fit(X_train, y_train)

    train_data_pred = clf.predict(X_train)
    test_data_pred = clf.predict(X_test)

    mdf = session.create_dataframe([
        ("training_accuracy", float(accuracy_score(y_train, train_data_pred))),
        ("eval_accuracy", float(accuracy_score(y_test, test_data_pred))),
        ("precision", float(precision_score(y_train, train_data_pred))),
        ("recall", float(recall_score(y_train, train_data_pred))),
        ("f1", float(f1_score(y_train, train_data_pred)))],
        schema=["metric", "value"])
    return mdf