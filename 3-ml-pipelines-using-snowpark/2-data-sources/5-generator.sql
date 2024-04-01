-- see https://docs.snowflake.com/en/user-guide/ml-powered-contribution-explorer#example
USE SCHEMA test.public;

-- training data
CREATE OR REPLACE TABLE time_series(
    ds DATE, metric NUMBER,
    dim_country VARCHAR, dim_vertical VARCHAR);

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'usa' AS dim_country,
    'tech' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'usa' AS dim_country,
    'auto' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, seq4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'usa' AS dim_country,
    'fashion' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'usa' AS dim_country,
    'finance' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'canada' AS dim_country,
    'fashion' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'canada' AS dim_country,
    'finance' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'canada' AS dim_country,
    'tech' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'canada' AS dim_country,
    'auto' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'france' AS dim_country,
    'fashion' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'france' AS dim_country,
    'finance' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'france' AS dim_country,
    'tech' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 4, 1)) AS ds,
    UNIFORM(1, 10, RANDOM()) AS metric,
    'france' AS dim_country,
    'auto' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

-- test data
INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 8, 1)) AS ds,
    UNIFORM(300, 320, RANDOM()) AS metric,
    'usa' AS dim_country,
    'auto' AS dim_vertica
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

INSERT INTO time_series
  SELECT
    DATEADD(day, SEQ4(), DATE_FROM_PARTS(2020, 8, 1))  AS ds,
    UNIFORM(400, 420, RANDOM()) AS metric,
    'usa' AS dim_country,
    'finance' AS dim_vertical
  FROM TABLE(GENERATOR(ROWCOUNT => 365));

SELECT * FROM time_series;