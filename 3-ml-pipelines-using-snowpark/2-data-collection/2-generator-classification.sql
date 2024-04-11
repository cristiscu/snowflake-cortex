-- see https://docs.snowflake.com/user-guide/snowflake-cortex/ml-functions/classification#setting-up-the-data-for-the-examples
USE SCHEMA test.public;

CREATE OR REPLACE TABLE purchases AS (
    SELECT
        CAST(UNIFORM(0, 4, RANDOM()) as VARCHAR) as interest,
        UNIFORM(0, 3, RANDOM()) as rating,
        FALSE AS label,
        'not_interested' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(4, 7, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(3, 7, RANDOM()) AS rating,
        FALSE AS label,
        'add_to_wishlist' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(7, 10, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(7, 10, RANDOM()) AS rating,
        TRUE as label,
        'purchase' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(0, 4, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(0, 3, RANDOM()) AS rating,
        NULL as label,
        NULL AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(4, 7, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(3, 7, RANDOM()) AS rating,
        NULL as label,
        NULL AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(7, 10, RANDOM()) AS VARCHAR) AS interest,
        UNIFORM(7, 10, RANDOM()) AS rating,
        NULL as label,
        NULL AS class
    FROM TABLE(GENERATOR(rowCount => 100))
);
select * from purchases;
