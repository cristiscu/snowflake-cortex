# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#scikit-learn

from sklearn import datasets, ensemble
from model_utils import get_registry

iris_X, iris_y = datasets.load_iris(return_X_y=True, as_frame=True)
model = ensemble.RandomForestClassifier(random_state=42)
model.fit(iris_X, iris_y)

print("Registering the model...")
registry = get_registry()
model_ref = registry.log_model(
    model,
    model_name="RandomForestClassifier",
    version_name="v1",
    conda_dependencies=["scikit-learn"],
    sample_input_data=iris_X,
    options={ "method_options": {
        "predict": {"case_sensitive": True},
        "predict_proba": {"case_sensitive": True},
        "predict_log_proba": {"case_sensitive": True}}})

print("Making a prediction...")
df = model_ref.run(
    iris_X[-10:],
    function_name='"predict_proba"')
print(df)