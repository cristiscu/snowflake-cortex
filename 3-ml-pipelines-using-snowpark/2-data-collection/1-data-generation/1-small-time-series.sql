-- see https://docs.snowflake.com/en/user-guide/ml-powered-anomaly-detection
USE SCHEMA test.ts;

CREATE OR REPLACE TABLE sales_ts (
  store_id NUMBER, item VARCHAR,
  date TIMESTAMP_NTZ, sales FLOAT, outlier BOOLEAN,
  temperature NUMBER, humidity NUMBER, holiday VARCHAR);

INSERT INTO sales_ts VALUES
  -- train data store 1 (date < 2020-01-15)
  (1, 'jacket', to_timestamp_ntz('2020-01-01'), 2.0, false, 50, 0.3, 'new year'),
  (1, 'jacket', to_timestamp_ntz('2020-01-02'), 3.0, false, 52, 0.3, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-03'), 5.0, false, 54, 0.2, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-04'), 30.0, true, 54, 0.3, null),   -- labeled outlier!
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
  -- test data store 1 (date >= 2020-01-15)
  (1, 'jacket', to_timestamp_ntz('2020-01-15'), 6.0, false, 52, 0.3, null),
  (1, 'jacket', to_timestamp_ntz('2020-01-16'), 20.0, false, 53, 0.3, null),
  -- train data store 2 (date < 2020-01-15)
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
  (2, 'umbrella', to_timestamp_ntz('2020-01-14'), 2.9, false, 55, 0.2, null),
  -- test data store 2 (date >= 2020-01-15)
  (2, 'umbrella', to_timestamp_ntz('2020-01-15'), 3.0, false, 52, 0.3, null),
  (2, 'umbrella', to_timestamp_ntz('2020-01-16'), 70.0, false, 53, 0.3, null);

SELECT * FROM sales_ts;

-- ===================================================================
-- for store 1 only
CREATE OR REPLACE VIEW view1_train AS
  SELECT date, sales, outlier, temperature, humidity, holiday
  FROM sales_ts
  WHERE date < '2020-01-15' AND store_id=1 AND item='jacket';
SELECT * FROM view1_train;

CREATE OR REPLACE VIEW view1_test AS
  SELECT date, sales, outlier, temperature, humidity, holiday
  FROM sales_ts
  WHERE date >= '2020-01-15' AND store_id=1 and item='jacket';
SELECT * FROM view1_test;

-- for store 2 only
CREATE OR REPLACE VIEW view2_train AS
  SELECT date, sales, outlier, temperature, humidity, holiday
  FROM sales_ts
  WHERE date < '2020-01-15' AND store_id=2 AND item='umbrella';
SELECT * FROM view2_train;

CREATE OR REPLACE VIEW view2_test AS
  SELECT date, sales, outlier, temperature, humidity, holiday
  FROM sales_ts
  WHERE date >= '2020-01-15' AND store_id=2 and item='umbrella';
SELECT * FROM view2_test;

-- for both stores
CREATE OR REPLACE VIEW view_train AS
  SELECT [store_id, item] AS store_item,
    date, sales, outlier, temperature, humidity, holiday
  FROM sales_ts
  WHERE date < '2020-01-15';
SELECT * FROM view_train;

CREATE OR REPLACE VIEW view_test AS
  SELECT [store_id, item] AS store_item,
    date, sales, outlier, temperature, humidity, holiday
  FROM sales_ts
  WHERE date >= '2020-01-15';
SELECT * FROM view_test;
