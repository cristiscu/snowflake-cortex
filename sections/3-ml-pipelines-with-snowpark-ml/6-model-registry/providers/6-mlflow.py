# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#mlflow

import mlflow
from sklearn import datasets, model_selection, ensemble
from model_utils import get_registry

db = datasets.load_diabetes(as_frame=True)
X_train, X_test, y_train, y_test = model_selection.train_test_split(db.data, db.target)

with mlflow.start_run() as run:
    rf = ensemble.RandomForestRegressor(n_estimators=100, max_depth=6, max_features=3)
    rf.fit(X_train, y_train)

    predictions = rf.predict(X_test)
    signature = mlflow.models.signature.infer_signature(X_test, predictions)
    mlflow.sklearn.log_model(rf, "model", signature=signature)
    run_id = run.info.run_id

print("Registering the model...")
registry = get_registry()
model_ref = registry.log_model(
    mlflow.pyfunc.load_model(f"runs:/{run_id}/model"),
    model_name="mlflowModel",
    version_name="v1",
    conda_dependencies=["mlflow<=2.4.0", "scikit-learn", "scipy"],
    options={"ignore_mlflow_dependencies": True})

print("Making a prediction...")
df = model_ref.run(X_test)
print(df)