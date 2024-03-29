-- see https://docs.snowflake.com/en/user-guide/ml-powered-forecasting

CREATE OR REPLACE TABLE sales_data (store_id NUMBER, item VARCHAR, date TIMESTAMP_NTZ,
  sales FLOAT, temperature NUMBER, humidity FLOAT, holiday VARCHAR);

INSERT INTO sales_data VALUES
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-01'), 2.0, 50, 0.3, 'new year'),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-02'), 3.0, 52, 0.3, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-03'), 4.0, 54, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-04'), 5.0, 54, 0.3, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-05'), 6.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-06'), 7.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-07'), 8.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-08'), 9.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-09'), 10.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-10'), 11.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-11'), 12.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-12'), 13.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-01'), 2.0, 50, 0.3, 'new year'),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-02'), 3.0, 52, 0.3, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-03'), 4.0, 54, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-04'), 5.0, 54, 0.3, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-05'), 6.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-06'), 7.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-07'), 8.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-08'), 9.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-09'), 10.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-10'), 11.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-11'), 12.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-12'), 13.0, 55, 0.2, NULL);

-- future values for exogenous variables (additional features)
CREATE OR REPLACE TABLE future_features (store_id NUMBER, item VARCHAR,
  date TIMESTAMP_NTZ, temperature NUMBER, humidity FLOAT, holiday VARCHAR);

INSERT INTO future_features VALUES
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-13'), 52, 0.3, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-14'), 53, 0.3, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-13'), 52, 0.3, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-14'), 53, 0.3, NULL);


// Forecasting on a Single Series
CREATE OR REPLACE VIEW v1 AS SELECT date, sales
  FROM sales_data WHERE store_id=1 AND item='jacket';
SELECT * FROM v1;

CREATE SNOWFLAKE.ML.FORECAST model1(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v1'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
);

call model1!FORECAST(FORECASTING_PERIODS => 3);

CALL model1!FORECAST(FORECASTING_PERIODS => 3, CONFIG_OBJECT => {'prediction_interval': 0.8});

BEGIN
  CALL model1!FORECAST(FORECASTING_PERIODS => 3);
  LET x := SQLID;
  CREATE TABLE my_forecasts AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

SELECT * FROM my_forecasts;

// Forecasting on a Single Series with Exogenous Variables

CREATE OR REPLACE VIEW v2 AS SELECT date, sales, temperature, humidity, holiday
  FROM sales_data WHERE store_id=1 AND item='jacket';
SELECT * FROM v2;

CREATE SNOWFLAKE.ML.FORECAST model2(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v2'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
);

CREATE OR REPLACE VIEW v2_forecast AS select date, temperature, humidity, holiday
  FROM future_features WHERE store_id=1 AND item='jacket';
SELECT * FROM v2_forecast;

CALL model2!FORECAST(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v2_forecast'),
  TIMESTAMP_COLNAME =>'date'
);

// Forecast on Multiple Series

CREATE OR REPLACE VIEW v3 AS SELECT [store_id, item] AS store_item, date, sales FROM sales_data;
SELECT * FROM v3;

CREATE SNOWFLAKE.ML.FORECAST model3(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v3'),
  SERIES_COLNAME => 'store_item',
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
);

CALL model3!FORECAST(FORECASTING_PERIODS => 2);

CALL model3!FORECAST(SERIES_VALUE => [2,'umbrella'], FORECASTING_PERIODS => 2);

// Forecast on Multiple Series with Exogenous Variables

CREATE OR REPLACE VIEW v4 AS SELECT [store_id, item] AS store_item,
  date, sales, temperature FROM sales_data;
SELECT * FROM v4;

CREATE SNOWFLAKE.ML.FORECAST model4(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v4'),
  SERIES_COLNAME => 'store_item',
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
);

CREATE SNOWFLAKE.ML.FORECAST model4(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT [store_id, item] AS store_item, date, sales, temperature FROM sales_data'),
  SERIES_COLNAME => 'store_item',
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
);

CREATE OR REPLACE VIEW V4_FORECAST AS SELECT [store_id, item] AS store_item,
  date, temperature FROM future_features;
SELECT * FROM v4_forecast;

CALL model4!FORECAST(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v4_forecast'),
  SERIES_COLNAME => 'store_item',
  TIMESTAMP_COLNAME =>'date'
);

// Visualizing Forecasts

CALL model4!FORECAST(FORECASTING_PERIODS => 3);

SELECT date AS ts, sales AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
  FROM sales_data
UNION ALL
SELECT ts, NULL AS actual, forecast, lower_bound, upper_bound
  FROM TABLE(RESULT_SCAN(-1));

// Understanding Feature Importance

CREATE OR REPLACE VIEW v_random_data AS SELECT
  DATEADD('minute', ROW_NUMBER() over (ORDER BY 1), '2023-12-01')::TIMESTAMP_NTZ ts,
  MOD(SEQ1(),10) y,
  UNIFORM(1, 100, RANDOM(0)) exog_a
FROM TABLE(GENERATOR(ROWCOUNT => 500));

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST forecast_feature_importance_demo(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v_random_data'),
  TIMESTAMP_COLNAME => 'ts',
  TARGET_COLNAME => 'y'
);

CALL forecast_feature_importance_demo!EXPLAIN_FEATURE_IMPORTANCE();

// Understanding Evaluation Metrics

CREATE OR REPLACE VIEW v_random_data AS SELECT
  DATEADD('minute', ROW_NUMBER() over (ORDER BY 1), '2023-12-01')::TIMESTAMP_NTZ ts,
  MOD(SEQ1(),10) y,
  UNIFORM(1, 100, RANDOM(0)) exog_a
FROM TABLE(GENERATOR(ROWCOUNT => 500));

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST model(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v_random_data'),
  TIMESTAMP_COLNAME => 'ts',
  TARGET_COLNAME => 'y'
);

CALL model!SHOW_EVALUATION_METRICS();

// Inspecting Training Logs

CREATE TABLE t_error(date TIMESTAMP_NTZ, sales FLOAT, series VARCHAR);
INSERT INTO t_error VALUES
  (TO_TIMESTAMP_NTZ('2019-12-20'), 1.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-21'), 2.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-22'), 3.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-23'), 2.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-24'), 1.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-25'), 2.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-26'), 3.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-27'), 2.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-28'), 1.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-29'), 2.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-30'), 3.0, 'A'),
  (TO_TIMESTAMP_NTZ('2019-12-31'), 2.0, 'A'),
  (TO_TIMESTAMP_NTZ('2020-01-01'), 2.0, 'A'),
  (TO_TIMESTAMP_NTZ('2020-01-02'), 3.0, 'A'),
  (TO_TIMESTAMP_NTZ('2020-01-03'), 3.0, 'A'),
  (TO_TIMESTAMP_NTZ('2020-01-04'), 7.0, 'A'),
  (TO_TIMESTAMP_NTZ('2020-01-05'), 10.0, 'B'),
  (TO_TIMESTAMP_NTZ('2020-01-06'), 13.0, 'B'),
  (TO_TIMESTAMP_NTZ('2020-01-06'), 12.0, 'B'), -- duplicate timestamp
  (TO_TIMESTAMP_NTZ('2020-01-07'), 15.0, 'B'),
  (TO_TIMESTAMP_NTZ('2020-01-08'), 14.0, 'B'),
  (TO_TIMESTAMP_NTZ('2020-01-09'), 18.0, 'B'),
  (TO_TIMESTAMP_NTZ('2020-01-10'), 12.0, 'B');

CREATE SNOWFLAKE.ML.FORECAST model(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT date, sales, series FROM t_error'),
  SERIES_COLNAME => 'series',
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  CONFIG_OBJECT => {'ON_ERROR': 'SKIP'}
);

CALL model!SHOW_TRAINING_LOGS();



