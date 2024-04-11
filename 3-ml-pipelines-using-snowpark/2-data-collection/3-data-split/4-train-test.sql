-- see https://quickstarts.snowflake.com/guide/cortex_ml_classification/index.html?index=..%2F..index#0
use schema test.public;

CREATE OR REPLACE VIEW marketing_all AS (
  SELECT *,
    CASE WHEN UNIFORM(0::float, 1::float, RANDOM()) < .95
    THEN 'train' ELSE 'test' END AS label
  FROM marketing);
SELECT label, count(*)
FROM marketing_all
GROUP BY label;

CREATE OR REPLACE VIEW marketing_train AS (
  SELECT * EXCLUDE label
  FROM marketing_all
  WHERE label = 'train');
CREATE OR REPLACE VIEW marketing_test AS (
  SELECT * EXCLUDE label
  FROM marketing_all
  WHERE label = 'test');
