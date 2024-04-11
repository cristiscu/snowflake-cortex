use schema TEST.TS;

-- show chart
SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT';

-- data prep
with cte as (
    SELECT date FROM WEATHER
    WHERE name = 'ALBANY INTERNATIONAL AIRPORT')
select date, count(*) as cnt
from cte
group by date
having cnt > 1;

SELECT * FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
AND (date = '2022-02-28' or date = '2018-02-28');

DELETE FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
    AND (date = '2022-02-28' AND temp = 29.7
    OR date = '2018-02-28' AND temp = 41.2);

-- create anomaly detection
create snowflake.ml.ANOMALY_DETECTION adw1(
    input_data => SYSTEM$QUERY_REFERENCE($$
SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
AND date < '2021-01-01'
$$),
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME => 'temp',
    LABEL_COLNAME => '');
SHOW SNOWFLAKE.ML.ANOMALY_DETECTION;
    
call adw1!detect_anomalies(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE($$
SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
    AND date >= '2021-01-01'
$$),
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME => 'temp');

-- show chart
SELECT date AS ts, temp as actual,
    NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM WEATHER
    WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
UNION ALL
SELECT ts, NULL AS actual,
    forecast, lower_bound, upper_bound
    FROM TABLE(RESULT_SCAN(-1));
