# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#snowpark-ml

import pandas as pd
import numpy as np
from sklearn import datasets
from model_utils import get_model

iris = datasets.load_iris()
df = pd.DataFrame(
    data=np.c_[iris["data"], iris["target"]],
    columns=iris["feature_names"] + ["target"])
df.columns = [s.replace(" (CM)", "").replace(" ", "")
    for s in df.columns.str.upper()]

print("Making a prediction...")
model_ref = get_model("XGBClassifier")
df = model_ref.run(
    df.drop(columns="TARGET").head(10),
    function_name='predict_proba')
print(df)
