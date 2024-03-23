# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-modeling#feature-preprocessing-and-training-on-non-synthetic-data

# connect to your Snowflake account
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

DATA_FILE_PATH = "~/Downloads/magic+gamma+telescope/magic04.data"
session.sql("""
CREATE OR REPLACE TABLE Gamma_Telescope_Data(
    F_LENGTH FLOAT,
    F_WIDTH FLOAT,
    F_SIZE FLOAT,
    F_CONC FLOAT,
    F_CONC1 FLOAT,
    F_ASYM FLOAT,
    F_M3_LONG FLOAT,
    F_M3_TRANS FLOAT,
    F_ALPHA FLOAT,
    F_DIST FLOAT,
    CLASS VARCHAR(10))
""").collect()
session.sql("CREATE OR REPLACE STAGE SNOWPARK_ML_TEST_DATA_STAGE").collect()
session.file.put(
    DATA_FILE_PATH,
    "SNOWPARK_ML_TEST_DATA_STAGE/magic04.data",
    auto_compress=False,
    overwrite=True,
)

session.sql("""
COPY INTO Gamma_Telescope_Data FROM @SNOWPARK_ML_TEST_DATA_STAGE/magic04.data
FILE_FORMAT = (TYPE = 'CSV' field_optionally_enclosed_by='"',SKIP_HEADER = 0);
""").collect()

session.sql("select * from Gamma_Telescope_Data limit 5").collect()

# ===================================================================

from snowflake.ml.modeling.preprocessing import StandardScaler
from snowflake.ml.modeling.impute import SimpleImputer
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.ml.modeling.xgboost import XGBClassifier

from snowflake.ml.modeling.metrics import accuracy_score

# Step 1: Create train and test dataframes
all_data = session.sql("select *, IFF(CLASS = 'g', 1.0, 0.0) as LABEL from Gamma_Telescope_Data").drop("CLASS")
train_data, test_data = all_data.random_split(weights=[0.9, 0.1], seed=0)

# Step 2: Construct training pipeline with preprocessing and modeling steps
FEATURE_COLS = [c for c in train_data.columns if c != "LABEL"]
LABEL_COLS = ["LABEL"]

pipeline = Pipeline(steps = [
    ("impute", SimpleImputer(input_cols=FEATURE_COLS, output_cols=FEATURE_COLS)),
    ("scaler", StandardScaler(input_cols=FEATURE_COLS, output_cols=FEATURE_COLS)),
    ("model", XGBClassifier(input_cols=FEATURE_COLS, label_cols=LABEL_COLS))
])

# Step 3: Train
pipeline.fit(train_data)

# Step 4: Eval
predict_on_training_data = pipeline.predict(train_data)
training_accuracy = accuracy_score(df=predict_on_training_data, y_true_col_names=["LABEL"], y_pred_col_names=["OUTPUT_LABEL"])

predict_on_test_data = pipeline.predict(test_data)
eval_accuracy = accuracy_score(df=predict_on_test_data, y_true_col_names=["LABEL"], y_pred_col_names=["OUTPUT_LABEL"])

print(f"Training accuracy: {training_accuracy} \nEval accuracy: {eval_accuracy}")
