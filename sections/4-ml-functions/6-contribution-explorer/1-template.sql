WITH source AS (
    <your top query here>
), input AS (
    SELECT
        {
            <your optional categorical variable(s) here, in JSON format>
        } AS cat_dims,
        {
            <your optional continuous variable(s) here, in JSON format>
        } AS cont_dims,
        <your optional metric column here (1.0 if none)>::float AS metric,
        <your dataset split variable here>::boolean AS label
    FROM source
), analysis AS (
    SELECT res.*
    FROM input, 
        TABLE(SNOWFLAKE.ML.TOP_INSIGHTS(cat_dims, cont_dims, metric, label)
        OVER (PARTITION BY 0)) res
) SELECT contributor,
    TRUNC(expected_metric_test) AS expected,
    metric_test AS actual,
    TRUNC(relative_change * 100) || '%' AS ratio
FROM analysis
WHERE ABS(relative_change - 1) > 0.4
    AND NOT ARRAY_TO_STRING(contributor, ',') LIKE '%not%'
ORDER BY relative_change DESC
LIMIT 10;
