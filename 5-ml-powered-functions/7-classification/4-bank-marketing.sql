-- see https://quickstarts.snowflake.com/guide/cortex_ml_classification/index.html?index=..%2F..index#0
use schema test.public;

CREATE OR REPLACE TABLE bank_marketing(
    AGE NUMBER,
    JOB TEXT, 
    MARITAL TEXT, 
    EDUCATION TEXT, 
    DEFAULT TEXT, 
    HOUSING TEXT, 
    LOAN TEXT, 
    CONTACT TEXT, 
    MONTH TEXT, 
    DAY_OF_WEEK TEXT, 
    DURATION NUMBER(4, 0), 
    CAMPAIGN NUMBER(2, 0), 
    PDAYS NUMBER(3, 0), 
    PREVIOUS NUMBER(1, 0), 
    POUTCOME TEXT, 
    EMPLOYEE_VARIATION_RATE NUMBER(2, 1), 
    CONSUMER_PRICE_INDEX NUMBER(5, 3), 
    CONSUMER_CONFIDENCE_INDEX NUMBER(3,1), 
    EURIBOR_3_MONTH_RATE NUMBER(4, 3),
    NUMBER_EMPLOYEES NUMBER(5, 1),
    CLIENT_SUBSCRIBED BOOLEAN);

-- Ingest data from S3 into our table:
-- COPY INTO quickstart.ml_functions.bank_marketing
-- FROM @s3load/cortex_ml_classification.csv;

-- view a sample of the ingested data: 
SELECT * FROM bank_marketing
LIMIT 100;

-- ======================================================
-- Total count of rows in the dataset
SELECT COUNT(1) as num_rows
FROM bank_marketing;

-- Count of subscribed vs not subscribed: 
SELECT client_subscribed, COUNT(1) as num_rows
FROM bank_marketing
GROUP BY 1;

-- Create a view with a column that will be filtered for either training/inference purposes
CREATE OR REPLACE VIEW partitioned_data as (
  SELECT *,
    CASE WHEN UNIFORM(0::float, 1::float, RANDOM()) < .95
    THEN 'training' ELSE 'inference' END AS split_group
  FROM bank_marketing);

-- Training data view: 
CREATE OR REPLACE VIEW training_view AS (
  SELECT * EXCLUDE split_group
  FROM partitioned_data 
  WHERE split_group LIKE 'training');

-- Inference data view
CREATE OR REPLACE VIEW inference_view AS (
  SELECT * EXCLUDE split_group
  FROM partitioned_data 
  WHERE split_group LIKE 'inference');

-- ======================================================
-- Train our classifier: 
CREATE OR REPLACE snowflake.ml.classification bank_classifier(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'training_view'),
    TARGET_COLNAME => 'CLIENT_SUBSCRIBED',
    CONFIG_OBJECT => {'evaluate': TRUE , 'on_error': 'skip'});

-- Confirm model has been created
SHOW snowflake.ml.classification;

-- Create the Predictions
SELECT bank_classifier!PREDICT(INPUT_DATA => object_construct(*))
    AS prediction FROM inference_view;

CREATE OR REPLACE TABLE predictions AS (
    SELECT CLIENT_SUBSCRIBED,
        prediction:class::boolean as prediction, 
        prediction:probability:False as false_probability,
        prediction:probability:True as true_probability
    FROM (
        SELECT bank_classifier!PREDICT(object_construct(*)) AS prediction, CLIENT_SUBSCRIBED
        FROM inference_view));

SELECT *
FROM predictions
LIMIT 100;

-- Calculate the Confusion Matrix
CALL bank_classifier!SHOW_CONFUSION_MATRIX();

-- Calculate the evaluation metrics
CALL bank_classifier!SHOW_EVALUATION_METRICS();
CALL bank_classifier!SHOW_GLOBAL_EVALUATION_METRICS();
CALL bank_classifier!SHOW_THRESHOLD_METRICS();
CALL bank_classifier!SHOW_FEATURE_IMPORTANCE();
