# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#snowpark-ml

import pandas as pd
import numpy as np
from sklearn import datasets
from snowflake.ml.modeling.xgboost import XGBClassifier
from model_utils import get_registry

iris = datasets.load_iris()
df = pd.DataFrame(
    data=np.c_[iris["data"], iris["target"]],
    columns=iris["feature_names"] + ["target"])
df.columns = [s.replace(" (CM)", "").replace(" ", "")
    for s in df.columns.str.upper()]

clf = XGBClassifier(
    input_cols=["SEPALLENGTH", "SEPALWIDTH", "PETALLENGTH", "PETALWIDTH"],
    output_cols="PREDICTED_TARGET",
    label_cols="TARGET",
    drop_input_cols=True)
clf.fit(df)

print("Registering the model...")
registry = get_registry()
model_ref = registry.log_model(
    clf,
    model_name="XGBClassifier",
    version_name="v3",
    conda_dependencies=["xgboost"])

print("Making a prediction...")
df = model_ref.run(
    df.drop(columns="TARGET").head(10),
    function_name='predict_proba')
print(df)

