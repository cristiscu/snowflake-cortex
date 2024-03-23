-- see https://docs.snowflake.com/en/user-guide/ml-powered-anomaly-detection
CREATE OR REPLACE TABLE historical_sales_data (
  store_id NUMBER, item VARCHAR, date TIMESTAMP_NTZ, sales FLOAT, label BOOLEAN,
  temperature NUMBER, humidity NUMBER, holiday VARCHAR);

INSERT INTO historical_sales_data VALUES
  (1, 'jacket', to_timestamp_ntz('2020-01-01'), 2.0, false, 50, 0.3, 'new year'),
  (1, 'jacket', to_timestamp_ntz('2020-01-02'), 3.0, false, 52, 0.3, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-03'), 5.0, false, 54, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-04'), 30.0, true, 54, 0.3, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-05'), 8.0, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-06'), 6.0, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-07'), 4.6, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-08'), 2.7, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-09'), 8.6, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-10'), 9.2, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-11'), 4.6, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-12'), 7.0, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-13'), 3.6, false, 55, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-14'), 8.0, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-01'), 3.4, false, 50, 0.3, 'new year'),
  (2, 'umbrella', to_timestamp_ntz('2020-01-02'), 5.0, false, 52, 0.3, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-03'), 4.0, false, 54, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-04'), 5.4, false, 54, 0.3, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-05'), 3.7, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-06'), 3.2, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-07'), 3.2, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-08'), 5.6, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-09'), 7.3, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-10'), 8.2, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-11'), 3.7, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-12'), 5.7, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-13'), 6.3, false, 55, 0.2, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-14'), 2.9, false, 55, 0.2, null);
  
CREATE OR REPLACE VIEW view_with_training_data
    AS SELECT date, sales FROM historical_sales_data
    WHERE store_id=1 AND item='jacket';

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION basic_model(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_training_data'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => '');

CREATE OR REPLACE VIEW view_with_data_to_analyze
  AS SELECT date, sales FROM new_sales_data
    WHERE store_id=1 and item='jacket';

CALL basic_model!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_to_analyze'),
  TIMESTAMP_COLNAME =>'date',
  TARGET_COLNAME => 'sales'
);

BEGIN
  CALL basic_model!DETECT_ANOMALIES(
     INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_to_analyze'),
     TIMESTAMP_COLNAME =>'date',
     TARGET_COLNAME => 'sales'
  );
  LET x := SQLID;
  CREATE TABLE my_anomalies AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

SELECT * FROM my_anomalies;


CREATE OR REPLACE VIEW view_with_labeled_data_for_training
  AS SELECT date, sales, label FROM historical_sales_data
    WHERE store_id=1 and item='jacket';

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model_trained_with_labeled_data(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_labeled_data_for_training'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => 'label'
);

CALL model_trained_with_labeled_data!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_to_analyze'),
  TIMESTAMP_COLNAME =>'date',
  TARGET_COLNAME => 'sales'
);


CALL model_trained_with_labeled_data!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_to_analyze'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  CONFIG_OBJECT => {'prediction_interval':0.995}
);

CREATE OR REPLACE VIEW view_with_training_data_extra_columns
  AS SELECT date, sales, label, temperature, humidity, holiday
    FROM historical_sales_data
    WHERE store_id=1 AND item='jacket';

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model_with_additional_columns(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_training_data_extra_columns'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => 'label'
);

CREATE OR REPLACE VIEW view_with_data_for_analysis_extra_columns
  AS SELECT date, sales, temperature, humidity, holiday
    FROM new_sales_data
    WHERE store_id=1 AND item='jacket';

CALL model_with_additional_columns!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_for_analysis_extra_columns'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  CONFIG_OBJECT => {'prediction_interval':0.93}
);
  -- ================================================================================
  -- see 
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

-- Start your task's execution.
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
    distance float
);


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

-- Start your task's execution.
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

-- Start your alert's execution.
alter alert anomaly_detection_alert resume;

