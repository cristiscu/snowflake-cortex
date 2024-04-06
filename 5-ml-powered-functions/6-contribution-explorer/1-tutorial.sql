-- see https://docs.snowflake.com/en/user-guide/ml-powered-contribution-explorer
-- see https://www.snowflake.com/blog/ml-powered-functions-improve-speed-quality/
CREATE OR REPLACE TABLE input_table(
  ds DATE, metric NUMBER, dim_country VARCHAR, dim_vertical VARCHAR);

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'usa' AS dim_country,
    'tech' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'usa' AS dim_country,
    'auto' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, seq4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'usa' AS dim_country,
    'fashion' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'usa' AS dim_country,
    'finance' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'canada' AS dim_country,
    'fashion' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'canada' AS dim_country,
    'finance' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'canada' AS dim_country,
    'tech' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'canada' AS dim_country,
    'auto' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'france' AS dim_country,
    'fashion' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'france' AS dim_country,
    'finance' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'france' AS dim_country,
    'tech' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'france' AS dim_country,
    'auto' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

-- Data for the test group

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 8, 1)) AS ds,
    UNIFORM(300, 320, RANDOM()) AS metric,
    'usa' AS dim_country,
    'auto' AS dim_vertica
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO input_table
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 8, 1))  AS ds,
    UNIFORM(400, 420, RANDOM()) AS metric,
    'usa' AS dim_country,
    'finance' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

select * from input_table;
  
WITH input AS (
  SELECT
    {
      'country': dim_country,
      'vertical': dim_vertical
    } AS categorical_dimensions,
    {
         'length_of_vertical': length(dim_country)
    } AS continuous_dimensions,
    metric,
    IFF(ds BETWEEN '2020-08-01' AND '2020-08-20', TRUE, FALSE) AS label
  FROM input_table
  WHERE (ds BETWEEN '2020-05-01' AND '2020-05-20')
    OR (ds BETWEEN '2020-08-01' AND '2020-08-20')
)
SELECT res.* from input, TABLE(
  SNOWFLAKE.ML.TOP_INSIGHTS(
    input.categorical_dimensions,
    input.continuous_dimensions,
    CAST(input.metric AS FLOAT),
    input.label
  )
  OVER (PARTITION BY 0)
) res ORDER BY res.surprise DESC;


WITH source AS (
    SELECT *
    FROM TEST.PUBLIC.input_table
    WHERE (ds BETWEEN '2020-05-01' AND '2020-05-20')
        OR (ds BETWEEN '2020-08-01' AND '2020-08-20')),
input AS (
    SELECT
        {
            'country': dim_country,
            'vertical': dim_vertical
        } AS cat_dims,
        {
            'len_vertical': length(dim_vertical)
        } AS cont_dims,
        metric::float AS metric,
        IFF(ds BETWEEN '2020-08-01' AND '2020-08-20', TRUE, FALSE)::boolean AS label
    FROM source),
analysis AS (
    SELECT res.*
    FROM input, 
        TABLE(SNOWFLAKE.ML.TOP_INSIGHTS(cat_dims, cont_dims, metric, label)
        OVER (PARTITION BY 0)) res)
SELECT contributor,
    metric_test AS actual,
    TRUNC(expected_metric_test) AS expected,
    TRUNC(relative_change * 100) || '%' AS ratio
FROM analysis
WHERE ABS(relative_change - 1) > 0.4
    AND NOT ARRAY_TO_STRING(contributor, ',') LIKE '%not%'
ORDER BY actual DESC
LIMIT 10;
