-- see https://docs.snowflake.com/en/user-guide/ml-powered-anomaly-detection
use schema test.public;

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
  TARGET_COLNAME => 'sales');

BEGIN
  CALL basic_model!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_to_analyze'),
    TIMESTAMP_COLNAME =>'date',
    TARGET_COLNAME => 'sales');
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
  LABEL_COLNAME => 'label');

CALL model_trained_with_labeled_data!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_to_analyze'),
  TIMESTAMP_COLNAME =>'date',
  TARGET_COLNAME => 'sales');


CALL model_trained_with_labeled_data!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_to_analyze'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  CONFIG_OBJECT => {'prediction_interval':0.995});

CREATE OR REPLACE VIEW view_with_training_data_extra_columns
  AS SELECT date, sales, label, temperature, humidity, holiday
    FROM historical_sales_data
    WHERE store_id=1 AND item='jacket';

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION model_with_additional_columns(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_training_data_extra_columns'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  LABEL_COLNAME => 'label');

CREATE OR REPLACE VIEW view_with_data_for_analysis_extra_columns
  AS SELECT date, sales, temperature, humidity, holiday
    FROM new_sales_data
    WHERE store_id=1 AND item='jacket';

CALL model_with_additional_columns!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'view_with_data_for_analysis_extra_columns'),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales',
  CONFIG_OBJECT => {'prediction_interval':0.93});
