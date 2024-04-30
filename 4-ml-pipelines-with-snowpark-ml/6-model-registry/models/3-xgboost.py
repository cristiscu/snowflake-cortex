# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#xgboost

import xgboost
from sklearn import datasets, model_selection
from model_utils import get_registry

cal_X, cal_y = datasets.load_breast_cancer(as_frame=True, return_X_y=True)
cal_X_train, cal_X_test, cal_y_train, cal_y_test = model_selection.train_test_split(cal_X, cal_y)
regr = xgboost.train(
    dict(n_estimators=100, reg_lambda=1, gamma=0, max_depth=3, objective="binary:logistic"),
    xgboost.DMatrix(data=cal_X_train, label=cal_y_train))

registry = get_registry()
model_ref = registry.log_model(
    regr,
    model_name="xgBooster",
    version_name="v1",
    sample_input_data=cal_X_test,
    options={
        "target_methods": ["predict"],
        "method_options": { "predict": {"case_sensitive": True} }})

model_ref.run(cal_X_test[-10:])