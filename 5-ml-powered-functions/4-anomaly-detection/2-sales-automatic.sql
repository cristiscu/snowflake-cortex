-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ml-functions/anomaly-detection#automate-anomaly-detection-with-snowflake-tasks-and-alerts
-- see https://docs.snowflake.com/en/user-guide/ml-powered-anomaly-detection
USE SCHEMA test.ts;

// ===================================================
-- re-train model every hour
CREATE OR REPLACE TASK retrain_model
  WAREHOUSE = compute_wh
  SCHEDULE = '60 MINUTE'
AS EXECUTE IMMEDIATE
BEGIN
  CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ad_auto(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_train'),
    TIMESTAMP_COLNAME => 'date',
    TARGET_COLNAME => 'sales',
    LABEL_COLNAME => 'label');
END;
ALTER TASK retrain_model RESUME;

// ===================================================
CREATE OR REPLACE TABLE anomaly_result (
  ts TIMESTAMP_NTZ, y FLOAT, forecast FLOAT,
  lower_bound FLOAT, upper_bound FLOAT,
  is_anomaly BOOLEAN, percentile FLOAT, distance FLOAT);

-- detect and save anomalies every minute
CREATE OR REPLACE TASK detect_anomalies
  WAREHOUSE = compute_wh
  SCHEDULE = '1 minute'
AS EXECUTE IMMEDIATE
BEGIN
  CALL ad_auto!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_test'),
    TIMESTAMP_COLNAME => 'date',
    TARGET_COLNAME => 'sales',
    CONFIG_OBJECT => {'prediction_interval':0.99});

  INSERT INTO anomaly_result (ts, y, forecast,
    lower_bound, upper_bound, is_anomaly, percentile, distance)
  SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
END;
ALTER TASK detect_anomalies RESUME;

// ===================================================
-- send email when anomalies (check every minute)
CREATE OR REPLACE ALERT sample_sales_alert
  WAREHOUSE = compute_wh
  SCHEDULE = '1 MINUTE'
IF (EXISTS(select * from anomaly_result
  where is_anomaly=True
  and ts > dateadd('day', -1, current_timestamp()))
CALL SYSTEM$SEND_EMAIL(
  'sales_email_alert',
  'your_email@snowflake.com',
  'Anomalous Sales Data!',
  'Anomalous sales data detected in data stream...');
ALTER ALERT sample_sales_alert RESUME;
