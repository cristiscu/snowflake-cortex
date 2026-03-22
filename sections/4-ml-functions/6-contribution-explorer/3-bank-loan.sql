-- see https://cristian-70480.medium.com/why-snowflakes-top-insights-is-not-related-to-time-series-2d954c9f67ae
-- see https://medium.com/snowflake/identify-key-contributors-to-trends-and-anomalies-in-minutes-with-cortex-ml-function-0745cd981e39
-- see https://archive.ics.uci.edu/dataset/222/bank+marketing
use schema test.public;

WITH source AS (
    SELECT * FROM MARKETING
), input AS (
    SELECT
        {
            'job': job,
            'marital' : marital,
            'housing' : housing,
            'loan' : loan
        } AS cat_dims,
        {
            'age': age
        } AS cont_dims,
        1.0::float AS metric,
        y::boolean AS label
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

-- separate just 5+2 rows
WITH source AS (
    (SELECT job, marital, age, y
    FROM TEST.PUBLIC.MARKETING
    WHERE not y
    ORDER BY duration ASC
    LIMIT 5)
    UNION
    (SELECT job, marital, age, y
    FROM TEST.PUBLIC.MARKETING
    WHERE y
    ORDER BY duration DESC
    LIMIT 2)
    -- ORDER BY age
),
input AS (
    SELECT
        {
            'job': job,
            'marital' : marital
        } AS cat_dims,
        {
            'age': age
        } AS cont_dims,
        1.0::float AS metric,
        y::boolean AS label
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
