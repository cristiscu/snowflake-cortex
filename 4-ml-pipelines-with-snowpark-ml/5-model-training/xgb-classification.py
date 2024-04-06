# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#feature-preprocessing-and-training-on-non-synthetic-data

from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.ml.modeling.preprocessing import StandardScaler
from snowflake.ml.modeling.impute import SimpleImputer
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.ml.modeling.xgboost import XGBClassifier
from snowflake.ml.modeling.metrics import accuracy_score

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
query = "SELECT *, IFF(CLASS='g', 1.0, 0.0) AS LABEL FROM Gamma_Telescope"
all_data = session.sql(query).drop("CLASS")
train_data, test_data = all_data.random_split(weights=[0.9, 0.1], seed=0)

COLS = [c for c in train_data.columns if c != "LABEL"]
pipeline = Pipeline(steps = [
    ("imputer", SimpleImputer(input_cols=COLS, output_cols=COLS)),
    ("scaler", StandardScaler(input_cols=COLS, output_cols=COLS)),
    ("model", XGBClassifier(input_cols=COLS, label_cols=["LABEL"]))])
pipeline.fit(train_data)

predict_on_train_data = pipeline.predict(train_data)
training_accuracy = accuracy_score(
    df=predict_on_train_data, 
    y_true_col_names=["LABEL"], 
    y_pred_col_names=["OUTPUT_LABEL"])
print(f"Training accuracy: {training_accuracy}")

predict_on_test_data = pipeline.predict(test_data)
eval_accuracy = accuracy_score(
    df=predict_on_test_data, 
    y_true_col_names=["LABEL"], 
    y_pred_col_names=["OUTPUT_LABEL"])
print(f"Eval accuracy: {eval_accuracy}")
