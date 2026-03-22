use schema TEST.TS;

-- duplicates already removed in forecasting!
SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT';

use warehouse so_medium;
create or replace snowflake.ml.ANOMALY_DETECTION adw1(
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
    
use warehouse compute_wh;
call adw1!detect_anomalies(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE($$
SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
    AND date >= '2021-01-01'
$$),
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME => 'temp');
