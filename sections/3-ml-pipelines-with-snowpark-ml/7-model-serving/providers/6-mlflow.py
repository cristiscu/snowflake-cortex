# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#mlflow

from sklearn import datasets, model_selection
from model_utils import get_model

db = datasets.load_diabetes(as_frame=True)
_, X_test, _, _ = model_selection.train_test_split(db.data, db.target)

print("Making a prediction...")
model_ref = get_model("mlflowModel")
df = model_ref.run(X_test)
print(df)