-- see https://docs.snowflake.com/user-guide/snowflake-cortex/ml-functions/classification#training-and-using-a-multi-class-classifier
USE SCHEMA test.public;

CREATE OR REPLACE VIEW multiclass_classification_view AS
    SELECT user_interest_score, user_rating, class
    FROM training_purchase_data;

SELECT * FROM multiclass_classification_view
ORDER BY RANDOM(42) 
LIMIT 10;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model_multiclass(
    INPUT_DATA => SYSTEM$REFERENCE('view', 'multiclass_classification_view'),
    TARGET_COLNAME => 'class');

SELECT *, model_multiclass!PREDICT(
    INPUT_DATA => object_construct(*))
    as predictions from prediction_purchase_data;

CREATE OR REPLACE TABLE my_predictions AS
    SELECT *, model_multiclass!PREDICT(INPUT_DATA => object_construct(*))
    as predictions from prediction_purchase_data;

SELECT * FROM my_predictions;

SELECT predictions:class AS predicted_class,
    ROUND(predictions:probability:not_interested, 4) AS not_interested_class_probability,
    ROUND(predictions['probability']['purchase'], 4) AS purchase_class_probability,
    ROUND(predictions['probability']['add_to_wishlist'], 4) AS add_to_wishlist_class_probability
FROM my_predictions
LIMIT 5;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model(
    INPUT_DATA => SYSTEM$REFERENCE('view', 'binary_classification_view'),
    TARGET_COLNAME => 'label',
    CONFIG_OBJECT => {'evaluate': TRUE});

CALL model_multiclass!SHOW_EVALUATION_METRICS();
CALL model_multiclass!SHOW_GLOBAL_EVALUATION_METRICS();
CALL model_multiclass!SHOW_CONFUSION_MATRIX();
CALL model_multiclass!SHOW_FEATURE_IMPORTANCE();
