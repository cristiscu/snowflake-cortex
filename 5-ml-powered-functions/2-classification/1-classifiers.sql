-- see https://docs.snowflake.com/user-guide/snowflake-cortex/ml-functions/classification#training-and-using-a-multi-class-classifier
USE SCHEMA test.public;

-- =====================================================================
-- binary classifier
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model_binary(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE(
        'SELECT interest, rating, label FROM purchases WHERE label IS NOT NULL'),
    TARGET_COLNAME => 'label',
    CONFIG_OBJECT => {'evaluate': TRUE});

SELECT interest, rating, model_binary!PREDICT(
    INPUT_DATA => object_construct(*)) as predictions
FROM purchases
WHERE label IS NULL;

CALL model_binary!SHOW_EVALUATION_METRICS();
CALL model_binary!SHOW_GLOBAL_EVALUATION_METRICS();
CALL model_binary!SHOW_CONFUSION_MATRIX();
CALL model_binary!SHOW_FEATURE_IMPORTANCE();

-- =====================================================================
-- multiclass classifier
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model_multiclass(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE(
        'SELECT interest, rating, class FROM purchases WHERE label IS NOT NULL'),
    TARGET_COLNAME => 'class');

SELECT interest, rating, model_multiclass!PREDICT(
    INPUT_DATA => object_construct(*)) as predictions
FROM purchases
WHERE label IS NULL;

SELECT interest, rating, model_multiclass!PREDICT(
    INPUT_DATA => object_construct(interest, rating)) as predictions,
    predictions:class AS predicted_class,
    ROUND(predictions:probability:not_interested, 4) AS not_interested_class_probability,
    ROUND(predictions['probability']['purchase'], 4) AS purchase_class_probability,
    ROUND(predictions['probability']['add_to_wishlist'], 4) AS add_to_wishlist_class_probability
FROM purchases
WHERE label IS NULL
LIMIT 10;

CALL model_multiclass!SHOW_EVALUATION_METRICS();
CALL model_multiclass!SHOW_GLOBAL_EVALUATION_METRICS();
CALL model_multiclass!SHOW_CONFUSION_MATRIX();
CALL model_multiclass!SHOW_FEATURE_IMPORTANCE();
