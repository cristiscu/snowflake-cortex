-- see https://docs.snowflake.com/en/user-guide/ml-powered-contribution-explorer
-- see https://www.snowflake.com/blog/ml-powered-functions-improve-speed-quality/
create or replace database contribution_exporer;
use schema contribution_exporer.public;

CREATE OR REPLACE TABLE sales(
  ds DATE, transactions NUMBER, country VARCHAR, department VARCHAR);

-- ================================================================
-- control group
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'usa' AS country,
    'tech' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'usa' AS country,
    'auto' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, seq4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'usa' AS country,
    'fashion' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'usa' AS country,
    'finance' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'canada' AS country,
    'fashion' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'canada' AS country,
    'finance' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'canada' AS country,
    'tech' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'canada' AS country,
    'auto' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'france' AS country,
    'fashion' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'france' AS country,
    'finance' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'france' AS country,
    'tech' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 100, RANDOM()) AS transactions,
    'france' AS country,
    'auto' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

select *
from sales
order by ds;
  
-- ================================================================
-- test group
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 8, 1)) AS ds,
    UNIFORM(300, 320, RANDOM()) AS transactions,
    'usa' AS country,
    'auto' AS dim_vertica
  FROM TABLE(GENERATOR(ROWCOUNT => 365));
INSERT INTO sales
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 8, 1))  AS ds,
    UNIFORM(400, 420, RANDOM()) AS transactions,
    'usa' AS country,
    'finance' AS department
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

select *
from sales
order by ds;

-- ================================================================
-- separate control+test groups
SELECT ds, country, department, transactions as ct, null as tt
FROM sales
WHERE ds BETWEEN '2020-05-01' AND '2020-05-20'
UNION ALL
SELECT ds, country, department, null as ct, transactions as tt
FROM sales
WHERE ds BETWEEN '2020-08-01' AND '2020-08-20';

(select 'control' as grp, country, department, sum(transactions)
from sales
where ds BETWEEN '2020-05-01' AND '2020-05-20'
group by 1, 2, 3
union all
select 'test' as grp, country, department, sum(transactions)
from sales
where ds BETWEEN '2020-08-01' AND '2020-08-20'
group by 1, 2, 3)
order by 1, 4 DESC;

-- ================================================================
-- TOP_INSIGHTS calls
WITH input AS (
  SELECT
    {
      'country': country,
      'department': department
    } AS categorical_dimensions,
    {
    } AS continuous_dimensions,
    transactions::float AS metric,
    IFF(ds BETWEEN '2020-08-01' AND '2020-08-20', TRUE, FALSE) AS label
  FROM sales
  WHERE (ds BETWEEN '2020-05-01' AND '2020-05-20')
    OR (ds BETWEEN '2020-08-01' AND '2020-08-20'))
SELECT res.*
FROM input, TABLE(
  SNOWFLAKE.ML.TOP_INSIGHTS(
    categorical_dimensions,
    continuous_dimensions,
    metric,
    label
  ) OVER (PARTITION BY 0)) res
ORDER BY surprise DESC;

WITH source AS (
    SELECT *
    FROM sales
    WHERE (ds BETWEEN '2020-05-01' AND '2020-05-20')
        OR (ds BETWEEN '2020-08-01' AND '2020-08-20')),
input AS (
    SELECT
        {
            'country': country,
            'department': department
        } AS cat_dims,
        {
        } AS cont_dims,
        transactions::float AS metric,
        IFF(ds BETWEEN '2020-08-01' AND '2020-08-20', TRUE, FALSE)::boolean AS label
    FROM source),
analysis AS (
    SELECT res.*
    FROM input, 
        TABLE(SNOWFLAKE.ML.TOP_INSIGHTS(cat_dims, cont_dims, metric, label)
        OVER (PARTITION BY 0)) res)
SELECT contributor,
    TRUNC(expected_metric_test) AS expected,
    metric_test AS actual,
    TRUNC(relative_change * 100) || '%' AS ratio
FROM analysis
WHERE ABS(relative_change - 1) > 0.4
    AND NOT ARRAY_TO_STRING(contributor, ',') LIKE '%not%'
ORDER BY relative_change DESC
LIMIT 10;

WITH source AS (
    SELECT *
    FROM sales
    WHERE (ds BETWEEN '2020-05-01' AND '2020-05-20')
        OR (ds BETWEEN '2020-08-01' AND '2020-08-20')),
input AS (
    SELECT
        {
            'country': country,
            'department': department
        } AS cat_dims,
        {
            'transactions': transactions
        } AS cont_dims,
        1.0::float AS metric,
        IFF(ds BETWEEN '2020-08-01' AND '2020-08-20', TRUE, FALSE)::boolean AS label
    FROM source),
analysis AS (
    SELECT res.*
    FROM input, 
        TABLE(SNOWFLAKE.ML.TOP_INSIGHTS(cat_dims, cont_dims, metric, label)
        OVER (PARTITION BY 0)) res)
SELECT contributor,
    TRUNC(expected_metric_test) AS expected,
    metric_test AS actual,
    TRUNC(relative_change * 100) || '%' AS ratio
FROM analysis
WHERE ABS(relative_change - 1) > 0.4
    AND NOT ARRAY_TO_STRING(contributor, ',') LIKE '%not%'
ORDER BY relative_change DESC
LIMIT 10;
