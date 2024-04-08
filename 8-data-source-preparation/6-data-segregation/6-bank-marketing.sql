-- see https://quickstarts.snowflake.com/guide/cortex_ml_classification/index.html?index=..%2F..index#0
use schema test.public;

CREATE OR REPLACE VIEW partitioned_data AS (
  SELECT *,
    CASE WHEN UNIFORM(0::float, 1::float, RANDOM()) < .95
    THEN 'training'
    ELSE 'inference'
    END AS split_group
  FROM marketing);

CREATE OR REPLACE VIEW training_view AS (
  SELECT * EXCLUDE split_group
  FROM partitioned_data 
  WHERE split_group LIKE 'training');

CREATE OR REPLACE VIEW inference_view AS (
  SELECT * EXCLUDE split_group
  FROM partitioned_data 
  WHERE split_group LIKE 'inference');
