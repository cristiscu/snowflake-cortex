# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#scikit-learn

from sklearn import datasets
from model_utils import get_model

iris_X, _ = datasets.load_iris(return_X_y=True, as_frame=True)

print("Making a prediction...")
model_ref = get_model("RandomForestClassifier")
df = model_ref.run(
    iris_X[-10:],
    function_name='"predict_proba"')
print(df)