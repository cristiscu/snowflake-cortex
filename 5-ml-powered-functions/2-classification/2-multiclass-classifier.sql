-- see https://docs.snowflake.com/user-guide/snowflake-cortex/ml-functions/classification#training-and-using-a-multi-class-classifier
USE SCHEMA test.public;

SELECT * FROM purchases;

SELECT class, count(*)
FROM purchases
GROUP BY 1;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION clf_multiclass(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE(
        'SELECT interest, rating, class FROM purchases WHERE class IS NOT NULL'),
    TARGET_COLNAME => 'class');
SHOW snowflake.ml.classification;

SELECT interest, rating, clf_multiclass!PREDICT(
    INPUT_DATA => object_construct(*)) as preds
FROM purchases
WHERE class IS NULL;

WITH cte AS (
    SELECT interest, rating, clf_multiclass!PREDICT(
        INPUT_DATA => object_construct(*)) as preds
    FROM purchases
    WHERE class IS NULL)
SELECT preds:class::string AS pred_class,
    ROUND(preds:probability:not_interested, 4) AS not_interested_proba,
    ROUND(preds['probability']['purchase'], 4) AS purchase_proba,
    ROUND(preds['probability']['add_to_wishlist'], 4) AS add_to_wishlist_proba
FROM cte
LIMIT 10;

CALL clf_multiclass!SHOW_EVALUATION_METRICS();
CALL clf_multiclass!SHOW_GLOBAL_EVALUATION_METRICS();
CALL clf_multiclass!SHOW_THRESHOLD_METRICS();

CALL clf_multiclass!SHOW_CONFUSION_MATRIX();
CALL clf_multiclass!SHOW_FEATURE_IMPORTANCE();
CALL clf_multiclass!SHOW_TRAINING_LOGS();