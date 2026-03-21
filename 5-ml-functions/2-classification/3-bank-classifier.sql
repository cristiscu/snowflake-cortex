-- see https://quickstarts.snowflake.com/guide/cortex_ml_classification/index.html?index=..%2F..index#0
use schema test.public;

SELECT * FROM marketing;

SELECT y, COUNT(1)
FROM marketing
GROUP BY 1;

CREATE OR REPLACE VIEW marketing_view as (
  SELECT *,
    CASE WHEN UNIFORM(0::float, 1::float, RANDOM()) < .95
    THEN 'train' ELSE 'infer' END AS grp
  FROM marketing);

CREATE OR REPLACE VIEW marketing_train AS (
  SELECT * EXCLUDE grp
  FROM marketing_view 
  WHERE grp = 'train');

CREATE OR REPLACE VIEW marketing_infer AS (
  SELECT * EXCLUDE grp
  FROM marketing_view 
  WHERE grp = 'infer');
SELECT count(*) FROM marketing_infer;

-- USE WAREHOUSE ...
CREATE OR REPLACE snowflake.ml.classification bank_clf(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'marketing_train'),
    TARGET_COLNAME => 'Y',
    CONFIG_OBJECT => {'evaluate': TRUE , 'on_error': 'skip'});
SHOW snowflake.ml.classification;

-- USE WAREHOUSE ...
SELECT bank_clf!PREDICT(INPUT_DATA => object_construct(*)) AS preds
FROM marketing_infer
LIMIT 10;

CREATE OR REPLACE TABLE marketing_preds AS (
    SELECT Y,
        preds:class::boolean as pred,
        preds:probability:False as false_proba,
        preds:probability:True as true_proba
    FROM (
        SELECT bank_clf!PREDICT(object_construct(*)) AS preds, Y
        FROM marketing_infer));

SELECT *
FROM marketing_preds
LIMIT 100;

CALL bank_clf!SHOW_EVALUATION_METRICS();
CALL bank_clf!SHOW_GLOBAL_EVALUATION_METRICS();
CALL bank_clf!SHOW_THRESHOLD_METRICS();

CALL bank_clf!SHOW_CONFUSION_MATRIX();
CALL bank_clf!SHOW_FEATURE_IMPORTANCE();
CALL bank_clf!SHOW_TRAINING_LOGS();
