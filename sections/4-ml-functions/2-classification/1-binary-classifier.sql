-- see https://docs.snowflake.com/user-guide/snowflake-cortex/ml-functions/classification#training-and-using-a-binary-classifier
USE SCHEMA test.public;

SELECT * FROM purchases;

SELECT (label is not null) as labeled, count(*)
FROM purchases
GROUP BY 1;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION clf_binary(
    -- INPUT_DATA => SYSTEM$REFERENCE('VIEW', '<view name>')
    -- INPUT_DATA => SYSTEM$REFERENCE('TABLE', '<table name>')
    INPUT_DATA => SYSTEM$QUERY_REFERENCE(
        'SELECT interest, rating, label FROM purchases WHERE label IS NOT NULL'),
    TARGET_COLNAME => 'label',
    CONFIG_OBJECT => {'evaluate': TRUE});   -- disable eval if FALSE
SHOW snowflake.ml.classification;

SELECT interest, rating, clf_binary!PREDICT(
    INPUT_DATA => object_construct(*)) as preds
FROM purchases
WHERE label IS NULL;

CALL clf_binary!SHOW_EVALUATION_METRICS();
CALL clf_binary!SHOW_GLOBAL_EVALUATION_METRICS();
CALL clf_binary!SHOW_THRESHOLD_METRICS();

CALL clf_binary!SHOW_CONFUSION_MATRIX();
CALL clf_binary!SHOW_FEATURE_IMPORTANCE();
CALL clf_binary!SHOW_TRAINING_LOGS();
