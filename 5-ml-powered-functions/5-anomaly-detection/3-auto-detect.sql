-- see https://docs.snowflake.com/en/user-guide/ml-powered-anomaly-detection
use schema test.public;

-- Set up a task to train your model on a weekly basis.
create or replace task train_anomaly_detection_task 
warehouse = LARGE_WAREHOUSE
SCHEDULE = 'USING CRON 0 0 * * 0 America/Los_Angeles' -- Run at midnight every Sunday.
as EXECUTE IMMEDIATE
$$
begin
  create or replace snowflake.ml.ANOMALY_DETECTION my_model(input_data => SYSTEM$REFERENCE('VIEW', 'view_of_your_input_data'),
      timestamp_colname => 'ts',
      target_colname => 'y',
      label_colname => '');
end;
$$;
alter task train_anomaly_detection_task resume;


-- Create a table to store your anomaly detection results.
create or replace table anomaly_detection_results (
    ts timestamp_ntz,
    y float,
    forecast float,
    lb float,
    ub float,
    is_anomaly boolean,
    percentile float,
    distance float);

-- Call your model to detect anomalies on a daily basis. 
create or replace task detect_anomalies_task 
warehouse = LARGE_WAREHOUSE
SCHEDULE = 'USING CRON 0 0 * * * America/Los_Angeles' -- Run at midnight, daily.
as EXECUTE IMMEDIATE
$$
begin
  call my_model!detect_anomalies(
    input_data => SYSTEM$REFERENCE('VIEW', 'view_of_your_data_to_monitor'),
                timestamp_colname =>'ts',
                target_colname => 'y',
                config_object => {'prediction_interval': 0.99});

  insert into anomaly_detection_results (ts, y, forecast, lb, ub, is_anomaly, percentile, distance)
      select * from table(result_scan(last_query_id()));
end;
$$;
alter task detect_anomalies_task resume;

-- Setup alert based on the results from anomaly detection
CREATE OR REPLACE ALERT anomaly_detection_alert
  WAREHOUSE = LARGE_WAREHOUSE
  SCHEDULE = 'USING CRON 0 1 * * * America/Los_Angeles' -- Run at 1 am, daily.
  IF (EXISTS (select * from anomaly_detection_results where is_anomaly=True and ts > dateadd('day',-1,current_timestamp()))
  THEN 
  call SYSTEM$SEND_EMAIL(
        'SNOWML_ANOMALY_DETECTION_ALERTS',
        'last.first@youremail.com',
        'Anomaly Detected in data stream',
        concat(
            'Anomaly Detected in data stream',
            'Value outside of confidence interval detected'
        )
    );
alter alert anomaly_detection_alert resume;
