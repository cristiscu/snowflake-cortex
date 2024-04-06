-- see https://quickstarts.snowflake.com/guide/cortex_ml_classification/index.html?index=..%2F..index#0
use schema test.public;

SELECT COUNT(1) as num_rows
FROM marketing;

SELECT y, COUNT(1) as num_rows
FROM marketing
GROUP BY 1;

CREATE OR REPLACE VIEW partitioned_data as (
  SELECT *,
    CASE WHEN UNIFORM(0::float, 1::float, RANDOM()) < .95
    THEN 'training' ELSE 'inference' END AS split_group
  FROM marketing);

CREATE OR REPLACE VIEW training_view AS (
  SELECT * EXCLUDE split_group
  FROM partitioned_data 
  WHERE split_group LIKE 'training');

CREATE OR REPLACE VIEW inference_view AS (
  SELECT * EXCLUDE split_group
  FROM partitioned_data 
  WHERE split_group LIKE 'inference');

-- ======================================================
CREATE OR REPLACE snowflake.ml.classification bank_classifier(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'training_view'),
    TARGET_COLNAME => 'Y',
    CONFIG_OBJECT => {'evaluate': TRUE , 'on_error': 'skip'});
SHOW snowflake.ml.classification;

SELECT bank_classifier!PREDICT(INPUT_DATA => object_construct(*))
    AS prediction FROM inference_view;
CREATE OR REPLACE TABLE predictions AS (
    SELECT Y,
        prediction:class::boolean as prediction, 
        prediction:probability:False as false_probability,
        prediction:probability:True as true_probability
    FROM (
        SELECT bank_classifier!PREDICT(object_construct(*)) AS prediction, Y
        FROM inference_view));

SELECT *
FROM predictions
LIMIT 100;

CALL bank_classifier!SHOW_CONFUSION_MATRIX();
CALL bank_classifier!SHOW_EVALUATION_METRICS();
CALL bank_classifier!SHOW_GLOBAL_EVALUATION_METRICS();
CALL bank_classifier!SHOW_THRESHOLD_METRICS();
CALL bank_classifier!SHOW_FEATURE_IMPORTANCE();
