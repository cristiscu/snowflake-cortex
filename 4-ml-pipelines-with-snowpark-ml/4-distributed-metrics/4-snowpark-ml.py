from matplotlib import pyplot
import seaborn as sns
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.ml.modeling.xgboost import XGBClassifier
import snowflake.ml.modeling.metrics as metrics

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
session.query_tag = "snowpark-ml-metrics"
df = session.table("CLASSIFICATION_DATASET_FOR_METRICS")
train_data, test_data = df.random_split(weights=[0.6, 0.4], seed=0)

clf = XGBClassifier(
    input_cols=["X1", "X2", "X3", "X4", "X5", "X6"],
    label_cols=["Y"],
    output_cols=["PREDICTIONS"])
clf.fit(train_data)

# ========================================================================
# Training accuracy: 0.98388
train_data_pred = clf.predict(train_data)
train_data_pred[["PREDICTIONS"]].show()

training_accuracy = metrics.accuracy_score(
    df=train_data_pred, 
    y_true_col_names=["Y"], 
    y_pred_col_names=["PREDICTIONS"])
print(f"Training accuracy: {training_accuracy}")

# Eval accuracy: 0.947878
test_data_pred = clf.predict(test_data)
test_data_pred[["PREDICTIONS"]].show()

eval_accuracy = metrics.accuracy_score(
    df=test_data_pred, 
    y_true_col_names=["Y"], 
    y_pred_col_names=["PREDICTIONS"])
print(f"Eval accuracy: {eval_accuracy}")

"""
SELECT avg(iff(("Y" = "PREDICTIONS"), 1, 0))
FROM (SELECT "X1", "X2", "X3", "X4", "X5", "X6", "Y", 
    CAST("TMP_RESULT"['PREDICTIONS'] AS BYTEINT) AS "PREDICTIONS"
    FROM (SELECT "X1", "X2", "X3", "X4", "X5", "X6", "Y",
        SNOWPARK_TEMP_FUNCTION_FJUIC4EEBX("X1", "X2", "X3", "X4", "X5", "X6") AS "TMP_RESULT"
        FROM "TEST"."PUBLIC"."SNOWPARK_TEMP_TABLE_7YIY1L2LNL"
        WHERE (("SNOWPARK_TEMP_COLUMN_1D2VUFABIZ" >= 600000 :: INT)
            AND ("SNOWPARK_TEMP_COLUMN_1D2VUFABIZ" < 1000000 :: INT))))

SELECT CAST("TMP_RESULT"['PREDICTIONS'] AS BYTEINT) AS "PREDICTIONS"
FROM (SELECT "X1", "X2", "X3", "X4", "X5", "X6", "Y",
    SNOWPARK_TEMP_FUNCTION_FJUIC4EEBX("X1", "X2", "X3", "X4", "X5", "X6") AS "TMP_RESULT"
    FROM "TEST"."PUBLIC"."SNOWPARK_TEMP_TABLE_7YIY1L2LNL"
    WHERE (("SNOWPARK_TEMP_COLUMN_1D2VUFABIZ" >= 600000 :: INT)
        AND ("SNOWPARK_TEMP_COLUMN_1D2VUFABIZ" < 1000000 :: INT)))
LIMIT 10
"""

print('Precision:', metrics.precision_score(
    df=test_data_pred,
    y_true_col_names=["Y"],
    y_pred_col_names=["PREDICTIONS"]))
print('Recall:', metrics.recall_score(
    df=test_data_pred,
    y_true_col_names=["Y"],
    y_pred_col_names=["PREDICTIONS"]))
print('F1:', metrics.f1_score(
    df=test_data_pred,
    y_true_col_names=["Y"],
    y_pred_col_names=["PREDICTIONS"]))

matrix = metrics.confusion_matrix(
    df=test_data_pred,
    y_true_col_name='Y',
    y_pred_col_name='PREDICTIONS')

pyplot.figure(figsize=(20, 20))
sns.heatmap(matrix, annot=True, fmt='.0f', cmap='Blues')
pyplot.show()