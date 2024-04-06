-- see https://docs.snowflake.com/user-guide/snowflake-cortex/ml-functions/classification#training-and-using-a-binary-classifier
USE SCHEMA test.public;

CREATE OR REPLACE view binary_classification_view AS
    SELECT user_interest_score, user_rating, label
    FROM training_purchase_data;

SELECT * FROM binary_classification_view
ORDER BY RANDOM(42)
LIMIT 5;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model_binary(
    INPUT_DATA => SYSTEM$REFERENCE('view', 'binary_classification_view'),
    TARGET_COLNAME => 'label');

SELECT model_binary!PREDICT(INPUT_DATA => object_construct(*))
    as prediction from prediction_purchase_data;

SELECT *, model_binary!PREDICT(INPUT_DATA => object_construct(*))
    as predictions from prediction_purchase_data;