-- see https://quickstarts.snowflake.com/guide/ml_forecasting_ad/index.html
USE SCHEMA test.ts;

CREATE OR REPLACE FILE FORMAT csv_ts
    type = 'csv'
    SKIP_HEADER = 1,
    COMPRESSION = AUTO;

CREATE OR REPLACE STAGE s3load_ts
    url = 's3://sfquickstarts/frostbyte_tastybytes/mlpf_quickstart/'
    file_format = csv_ts;

CREATE OR REPLACE TABLE sales2(
  	DATE DATE,
	PRIMARY_CITY VARCHAR(16777216),
	MENU_ITEM_NAME VARCHAR(16777216),
	TOTAL_SOLD NUMBER(17,0));

-- https://sfquickstarts.s3.us-west-1.amazonaws.com/frostbyte_tastybytes/mlpf_quickstart/ml_functions_quickstart.csv
COPY INTO sales2
FROM @s3load_ts/ml_functions_quickstart.csv;

SELECT * FROM sales2
WHERE menu_item_name = 'Lobster Mac & Cheese'
LIMIT 10;
