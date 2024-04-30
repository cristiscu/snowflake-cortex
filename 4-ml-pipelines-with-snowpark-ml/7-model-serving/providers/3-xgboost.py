# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#xgboost

from sklearn import datasets, model_selection
from model_utils import get_model

cal_X, cal_y = datasets.load_breast_cancer(as_frame=True, return_X_y=True)
_, cal_X_test, _, _ = model_selection.train_test_split(cal_X, cal_y)

print("Making a prediction...")
model_ref = get_model("xgBooster")
df = model_ref.run(cal_X_test[-10:])
print(df)