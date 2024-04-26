# 40K samples for classification
import pandas as pd
from sklearn.datasets import make_classification
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

X, y = make_classification(n_samples=40000, n_features=6,
    n_informative=4, n_redundant=1, random_state=0, shuffle=True)

X = pd.DataFrame(X, columns=["X1", "X2", "X3", "X4", "X5", "X6"])
y = pd.DataFrame(y, columns=["Y"])
pdf = pd.concat([X, y], axis=1)

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
df = session.create_dataframe(pdf)
df.write.mode("overwrite").save_as_table("CLASSIFICATION_DATASET_FOR_METRICS")
