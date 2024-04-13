-- see https://docs.snowflake.com/en/user-guide/ml-powered-anomaly-detection
USE SCHEMA test.ts;

SELECT * FROM sales_ts;

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
