-- see https://medium.com/snowflake/using-snowflake-to-predict-call-center-escalation-54261ea9bc84
create or replace table call_center(
    ts timestamp,
    location string,
    dept string,
    product string,
    customer_days number,
    escalated boolean,
    duration number);

insert into call_center values
    -- control group
    ('2023-03-15 00:00:00.000', 'Canada', 'Manager', 'iPhone15', 4926, FALSE, 615),
    ('2023-02-12 00:00:00.000', 'India', 'Specialist1', 'iPhone14', 647, FALSE, 465),
    ('2023-01-11 00:00:00.000', 'Canada', 'Front1', 'MacBookPro', 1519, FALSE, 274),
    ('2023-01-05 00:00:00.000', 'India', 'Front1', 'iPhone13', 4584, FALSE, 1034),
    ('2023-02-01 00:00:00.000', 'Canada', 'Front3', 'iPhone12', 429, FALSE, 476),
    ('2023-03-15 00:00:00.000', 'Canada', 'Manager', 'iPhone15', 4926, FALSE, 615),
    -- test group
    ('2023-02-12 00:00:00.000', 'India', 'Specialist1', 'iPhone14', 647, TRUE, 465),
    ('2023-01-11 00:00:00.000', 'Canada', 'Front1', 'MacBookPro', 119, TRUE, 274),
    ('2023-01-05 00:00:00.000', 'India', 'Front1', 'iPhone13', 4584, TRUE, 1034),
    ('2023-02-01 00:00:00.000', 'Canada', 'Front3', 'iPhone12', 234, TRUE, 476);

select * from call_center
order by ts;
    
WITH cte AS (
  SELECT
    {
      'location': location,
      'dept': dept,
      'product': product
    } as categorical_dimensions,
    {
       'duration': duration,
       'customer_days': customer_days
    } as continuous_dimensions,
    1.0 as metric,
    escalated::boolean as label
  FROM call_center
  WHERE ts BETWEEN '2023-01-01' AND '2023-04-15'
)
SELECT res.*
FROM cte, TABLE(SNOWFLAKE.ML.TOP_INSIGHTS(
    cte.categorical_dimensions,
    cte.continuous_dimensions,
    cte.metric::float,
    cte.label) OVER (PARTITION BY 0)) res
ORDER BY res.surprise DESC;

WITH source AS (
    SELECT *
    FROM TEST.PUBLIC.CALL_CENTER),
input AS (
    SELECT
        {
            'location': location,
            'dept' : dept,
            'product' : product
        } AS cat_dims,
        {
            'duration': duration,
            'customer_days': customer_days
        } AS cont_dims,
        1.0::float AS metric,
        escalated::boolean AS label
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

