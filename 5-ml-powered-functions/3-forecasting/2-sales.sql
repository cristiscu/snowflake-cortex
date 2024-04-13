-- see https://docs.snowflake.com/en/user-guide/ml-powered-forecasting
USE SCHEMA test.ts;

// single-series forecast
CREATE SNOWFLAKE.ML.FORECAST fore1(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view1_train'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

CALL fore1!FORECAST(FORECASTING_PERIODS => 3);
CALL fore1!FORECAST(
  FORECASTING_PERIODS => 3,
  CONFIG_OBJECT => {'prediction_interval': 0.8});

BEGIN
  CALL fore1!FORECAST(FORECASTING_PERIODS => 3);
  LET x := SQLID;
  CREATE TABLE sales_forecasts AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;
SELECT * FROM sales_forecasts;

// single-series forecast w/ exogenous vars
CREATE SNOWFLAKE.ML.FORECAST fore2(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE($$
SELECT date, sales temperature, humidity, holiday
FROM view1_train
$$),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

CALL fore2!FORECAST(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view1_test'),
  TIMESTAMP_COLNAME =>'date');

// multi-series forecast
CREATE SNOWFLAKE.ML.FORECAST fore3(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT store_item, date, sales FROM view_train'),
  SERIES_COLNAME => 'store_item',     -- [store_item, item] multiple TS
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

CALL fore3!FORECAST(FORECASTING_PERIODS => 2);
CALL fore3!FORECAST(
  FORECASTING_PERIODS => 2,
  SERIES_VALUE => [2,'umbrella']);

CALL fore3!EXPLAIN_FEATURE_IMPORTANCE();

// multi-series forecast w/ exogenous vars
CREATE SNOWFLAKE.ML.FORECAST fore4(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_train'),
  SERIES_COLNAME => 'store_item',
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales');

// visualizing forecasts
CALL fore4!FORECAST(FORECASTING_PERIODS => 3);

SELECT date AS ts, sales AS actual,
  NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
  FROM sales_ts
UNION ALL
SELECT ts, NULL AS actual,
  forecast, lower_bound, upper_bound
  FROM TABLE(RESULT_SCAN(-1));

CALL fore4!EXPLAIN_FEATURE_IMPORTANCE();
CALL fore4!SHOW_EVALUATION_METRICS();

// training logs
CALL fore4!SHOW_TRAINING_LOGS();

INSERT INTO sales_ts VALUES
  (1, 'jacket', to_timestamp_ntz('2020-01-03'), 5.0, false, 54, 0.2, 'duplicate');

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST fore_err(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales FROM view1_train'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  CONFIG_OBJECT => {'ON_ERROR': 'SKIP'});
CALL fore_err!SHOW_TRAINING_LOGS();

DELETE FROM sales_ts WHERE holiday = 'duplicate';
